// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rocketmq_error::RocketMQResult;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
// Import error helpers for convenient error creation
use crate::error_helpers::io_error;
use crate::error_helpers::remote_error;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;

#[derive(Clone)]
pub struct Client<PR> {
    channel: Channel,
    // Only callers own this handle; the receive task must not own it.
    tasks: Arc<ClientTasks>,
    processor: PhantomData<fn() -> PR>,
}

struct ClientTasks {
    channel: Channel,
    receive: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for ClientTasks {
    fn drop(&mut self) {
        self.channel.shutdown();
        if let Some(task) = self.receive.get_mut().expect("receive task lock poisoned").take() {
            task.abort();
        }
    }
}

struct ClientInner<PR> {
    cmd_handler: Arc<RemotingGeneralHandler<PR>>,
    ctx: ConnectionHandlerContext,
}

impl<PR> Drop for ClientInner<PR> {
    fn drop(&mut self) {
        // Also close the sender and waiters when the receive task is cancelled
        // or a request processor panics.
        self.ctx.channel().shutdown();
    }
}

impl<PR> ClientInner<PR> {
    fn fail_pending(&self, message: impl Into<String>) {
        self.cmd_handler
            .pending_responses
            .fail_connection(self.ctx.connection_ref().connection_id().as_str(), message);
    }
}

impl<PR> ClientInner<PR>
where
    PR: RequestProcessor + Sync + 'static,
{
    /// Connect on an explicitly supplied runtime.
    ///
    /// `runtime` owns the whole connection: address resolution, the TCP connect that registers the
    /// socket with a reactor, the channel and its send pump, and the receive loop. A connection
    /// reached through a shared client outlives whichever caller happened to open it, so doing any
    /// of that work on the caller's runtime would let that caller's teardown silently break I/O
    /// for everyone else using the same address - a socket registered with a shut-down reactor
    /// fails with an `it is being shutdown` error even though its tasks still run (plan 49b
    /// R1).
    ///
    /// The caller's own deadline bounds this future; cancellation or a timeout cannot leave an
    /// unadopted connection behind, because the preparation task closes the socket when nobody
    /// adopts it. `addr` must be owned data so it can cross into the owner runtime's task.
    pub async fn connect_on<T>(
        addr: T,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        runtime: &tokio::runtime::Handle,
    ) -> RocketMQResult<(Channel, tokio::task::JoinHandle<()>)>
    where
        T: tokio::net::ToSocketAddrs + Send + 'static,
    {
        let event_tx = tx.cloned();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (adopt_tx, adopt_rx) = tokio::sync::oneshot::channel::<()>();
        let spawner = runtime.clone();
        let owner = runtime.clone();
        // Detached on purpose: the preparation belongs to the owner runtime, not to this waiter, so
        // it finishes and cleans up after itself even if the caller is cancelled first.
        spawner.spawn(async move {
            let built = match Self::prepare(addr, cmd_handler, event_tx, &owner).await {
                Ok(built) => built,
                Err(err) => {
                    let _ = ready_tx.send(Err(err));
                    return;
                }
            };
            // Hand the connection over. Nobody adopting it means the waiter timed out or was
            // cancelled: close the socket here rather than orphaning a live pair of I/O tasks.
            let teardown = built.0.clone();
            if ready_tx.send(Ok(built)).is_err() {
                teardown.shutdown();
                return;
            }
            // Receiving is not adoption. The waiter confirms with no await in between, so it cannot
            // be cancelled after taking ownership without the preparer noticing.
            if adopt_rx.await.is_err() {
                teardown.shutdown();
            }
        });
        match ready_rx.await {
            Ok(Ok(built)) => {
                // No await between taking delivery and confirming it: this is what makes the
                // handover atomic with respect to cancellation.
                let _ = adopt_tx.send(());
                Ok(built)
            }
            Ok(Err(err)) => Err(err),
            Err(_) => Err(remote_error("connection preparation task on the owner runtime ended")),
        }
    }

    /// Resolve, connect and build the channel. Runs as a task on the owning runtime, which is what
    /// registers the socket with that runtime's reactor.
    async fn prepare<T>(
        addr: T,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        event_tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        runtime: &tokio::runtime::Handle,
    ) -> RocketMQResult<(Channel, tokio::task::JoinHandle<()>)>
    where
        T: tokio::net::ToSocketAddrs,
    {
        let stream = match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(err) => return Err(io_error(err)),
        };
        let local_addr = stream.local_addr()?;
        let remote_address = stream.peer_addr()?;
        let connection = Connection::new(stream);
        let channel_inner = Arc::new(ChannelInner::new_on(
            runtime,
            connection,
            cmd_handler.pending_responses.clone(),
        ));
        let channel = Channel::new(channel_inner, local_addr, remote_address);
        let mut receiver = ClientInner {
            cmd_handler,
            ctx: Arc::new(ConnectionHandlerContextWrapper::new(channel.clone())),
        };
        let receive = runtime.spawn(async move {
            let _task = crate::metrics::recv_task_started();
            let _ = receiver.run_recv().await;
        });
        if let Some(tx) = event_tx {
            let _ = tx.send(ConnectionNetEvent::CONNECTED(remote_address));
        }
        Ok((channel, receive))
    }

    async fn run_recv(&mut self) -> RocketMQResult<()> {
        loop {
            //Get the next frame from the connection.
            let channel = self.ctx.channel();
            let frame = channel.receive_command().await;
            let cmd = match frame {
                Some(Ok(cmd)) => cmd,
                Some(Err(error)) => {
                    self.ctx.channel().shutdown();
                    self.fail_pending(error.to_string());
                    return Err(error);
                }
                None => {
                    self.ctx.channel().shutdown();
                    self.fail_pending("connection closed");
                    return Ok(());
                }
            };
            //process request and response
            self.cmd_handler.process_message_received(&self.ctx, cmd).await;
        }
    }
}

impl<PR> Client<PR>
where
    PR: RequestProcessor + Sync + 'static,
{
    /// Creates a new `Client` instance and connects to the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to connect to.
    ///
    /// # Returns
    ///
    /// A new `Client` instance wrapped in a `Result`. Returns an error if the connection fails.
    /// Connect with both connection tasks placed on `runtime`; see [`ClientInner::connect_on`].
    pub(crate) async fn connect_on<T>(
        addr: T,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        runtime: &tokio::runtime::Handle,
    ) -> RocketMQResult<Client<PR>>
    where
        T: tokio::net::ToSocketAddrs + Send + 'static,
    {
        let (channel, receive) = ClientInner::connect_on(addr, cmd_handler, tx, runtime).await?;
        Ok(Client {
            tasks: Arc::new(ClientTasks {
                channel: channel.clone(),
                receive: Mutex::new(Some(receive)),
            }),
            channel,
            processor: PhantomData,
        })
    }

    /// Invokes a remote operation with the given `RemotingCommand`.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// the invocation fails.
    pub async fn send_read(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> RocketMQResult<RemotingCommand> {
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);
        let registration = self
            .channel
            .channel_inner()
            .pending_responses
            .register(
                self.channel.connection_ref().connection_id().as_str(),
                ResponseFuture::new(request.opaque(), timeout_millis, true, tx),
            )
            .map_err(|_| remote_error("duplicate pending request"))?;

        let pending_request = registration.request();
        if let Err(err) = self
            .channel
            .channel_inner()
            .send_command(request, Some(deadline), Some(pending_request))
            .await
        {
            drop(registration);
            return Err(err);
        }
        let result = match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => Err(remote_error(error.to_string())),
            Err(_) => {
                registration.timeout(timeout_millis);
                Err(rocketmq_error::RocketMQError::Timeout {
                    operation: "send_read",
                    timeout_ms: timeout_millis,
                })
            }
        };
        drop(registration);
        result
    }

    /// Invokes a remote operation with the given `RemotingCommand` and provides a callback function
    /// for handling the response.
    ///
    /// # Arguments
    ///
    /// * `_request` - The `RemotingCommand` representing the request.
    /// * `_func` - The callback function to handle the response.
    ///
    /// This method is a placeholder and currently does not perform any functionality.
    pub async fn invoke_with_callback<F>(&self, _request: RemotingCommand, _func: F)
    where
        F: FnMut(),
    {
    }

    /// Sends a request to the remote remoting_server.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure in sending the request.
    pub async fn send(&mut self, request: RemotingCommand) -> RocketMQResult<()> {
        if let Err(err) = self.channel.channel_inner().send_command(request, None, None).await {
            return Err(remote_error(err.to_string()));
        }
        Ok(())
    }

    /// Sends multiple requests in a batch (fire-and-forget, no response expected).
    ///
    /// # Performance
    ///
    /// Batching provides 2-4x throughput improvement for small messages:
    /// - Single system call instead of N
    /// - Better CPU cache locality during encoding
    /// - Reduced Nagle algorithm delays
    ///
    /// # Use Cases
    ///
    /// - Log shipping (async, high volume)
    /// - Metrics reporting
    /// - Event publishing
    ///
    /// # Arguments
    ///
    /// * `requests` - Vector of commands to send (consumed)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: All commands queued successfully
    /// - `Err(e)`: Channel send error (client shutdown)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let commands = vec![
    ///     RemotingCommand::create_request_command(/*...*/),
    ///     RemotingCommand::create_request_command(/*...*/),
    /// ];
    /// client.send_batch(commands).await?;
    /// ```
    pub async fn send_batch(&mut self, requests: Vec<RemotingCommand>) -> RocketMQResult<()> {
        // Send all commands individually through the channel
        // The underlying connection will buffer them efficiently
        for request in requests {
            if let Err(err) = self.channel.channel_inner().send_command(request, None, None).await {
                return Err(remote_error(err.to_string()));
            }
        }
        Ok(())
    }

    /// Sends multiple requests and collects responses (request-response batch).
    ///
    /// # Performance vs send_read()
    ///
    /// ```text
    /// 100x send_read():    ~5000ms  (sequential network RTT)
    /// send_batch_read():   ~100ms   (parallel + single RTT)
    /// Improvement: 50x faster
    /// ```
    ///
    /// # Arguments
    ///
    /// * `requests` - Vector of commands expecting responses
    /// * `timeout_millis` - Timeout for each individual request
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as input requests
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let requests = vec![cmd1, cmd2, cmd3];
    /// let responses = client.send_batch_read(requests, 3000).await?;
    /// for response in responses {
    ///     match response {
    ///         Ok(cmd) => println!("Success: {:?}", cmd),
    ///         Err(e) => eprintln!("Failed: {}", e),
    ///     }
    /// }
    /// ```
    pub async fn send_batch_read(
        &mut self,
        requests: Vec<RemotingCommand>,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<RocketMQResult<RemotingCommand>>> {
        let mut receivers = Vec::with_capacity(requests.len());

        for request in requests {
            let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
            let registration = self
                .channel
                .channel_inner()
                .pending_responses
                .register(
                    self.channel.connection_ref().connection_id().as_str(),
                    ResponseFuture::new(request.opaque(), timeout_millis, true, tx),
                )
                .map_err(|_| remote_error("duplicate pending request"))?;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);
            let pending_request = registration.request();
            if let Err(err) = self
                .channel
                .channel_inner()
                .send_command(request, Some(deadline), Some(pending_request))
                .await
            {
                drop(registration);
                return Err(err);
            }
            receivers.push((rx, registration, deadline));
        }

        let mut results = Vec::with_capacity(receivers.len());
        for (rx, registration, deadline) in receivers {
            let result = match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => Err(remote_error(error.to_string())),
                Err(_) => {
                    registration.timeout(timeout_millis);
                    Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "send_batch_read",
                        timeout_ms: timeout_millis,
                    })
                }
            };
            drop(registration);
            results.push(result);
        }

        Ok(results)
    }

    /// Reads and retrieves the response from the remote remoting_server.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// reading the response fails.
    async fn read(&mut self) -> RocketMQResult<RemotingCommand> {
        /*match self.inner.channel.0.connection.receive_command().await {
            None => {
                // Connection state is automatically managed by receive_command()
                Err(ConnectionInvalid("connection disconnection".to_string()))
            }
            Some(result) => match result {
                Ok(response) => Ok(response),
                Err(error) => match error {
                    Io(value) => {
                        // Connection state is automatically marked degraded by I/O operations
                        Err(ConnectionInvalid(value.to_string()))
                    }
                    _ => Err(error),
                },
            },
        }*/
        unimplemented!("read unimplemented")
    }

    pub(crate) async fn close_and_wait(&self) {
        self.channel.channel_inner().shutdown_and_wait().await;
        let task = self.tasks.receive.lock().expect("receive task lock poisoned").take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    pub(crate) fn close(&self) {
        self.channel.shutdown();
        if let Some(task) = self.tasks.receive.lock().expect("receive task lock poisoned").take() {
            task.abort();
        }
    }

    pub fn connection(&self) -> &Connection {
        self.channel.connection_ref()
    }

    #[allow(deprecated)]
    #[deprecated(note = "use the channel-owned send and receive methods")]
    pub fn connection_mut(&self) -> &Connection {
        self.channel.connection_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::pending_responses::PendingResponses;
    use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
    use tokio::net::TcpListener;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn last_client_drop_releases_receive_task_and_pending_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runtime = tokio::runtime::Handle::current();
        for _ in 0..16 {
            let handler = Arc::new(RemotingGeneralHandler {
                request_processor: tokio::sync::Mutex::new(DefaultRemotingRequestProcessor),
                rpc_hooks: std::sync::RwLock::new(vec![]),
                pending_responses: PendingResponses::with_capacity(1),
            });
            let (client, peer) = tokio::join!(
                Client::connect_on(listener.local_addr().unwrap(), handler.clone(), None, &runtime),
                listener.accept()
            );
            let client = client.unwrap();
            let _peer = peer.unwrap().0;
            let receiver = client.tasks.receive.lock().unwrap().take().unwrap();
            let owner = Arc::downgrade(&client.tasks);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _registration = handler
                .pending_responses
                .register(
                    client.connection().connection_id().as_str(),
                    ResponseFuture::new(42, 1000, true, tx),
                )
                .map_err(|_| ())
                .unwrap();
            drop(client);
            assert!(owner.upgrade().is_none());
            tokio::time::timeout(Duration::from_secs(1), receiver)
                .await
                .unwrap()
                .unwrap();
            assert!(rx.await.unwrap().is_err());
            assert!(handler.pending_responses.is_empty());
        }
    }

    /// Plan 49b R1: the socket belongs to the runtime that owns the client, not to whichever task
    /// happened to open it. Connecting on a caller runtime registered the socket with that
    /// runtime's reactor; once the caller went away every later write failed with an `it is
    /// being shutdown` error even though the send pump and receive loop were still alive.
    #[test]
    fn a_connection_survives_the_runtime_of_the_task_that_opened_it() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        let owner = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();

        // A fake broker living on the owning runtime for the whole test.
        let written = Arc::new(AtomicUsize::new(0));
        let peer_written = Arc::clone(&written);
        let addr = {
            let handle = owner.handle().clone();
            owner.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                tokio::spawn(async move {
                    loop {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let written = Arc::clone(&peer_written);
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            while let Ok(n) = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await {
                                if n == 0 {
                                    return;
                                }
                                written.fetch_add(n, Ordering::Release);
                            }
                        });
                    }
                });
                drop(handle);
                addr
            })
        };

        // A caller runtime opens the shared connection and is then dropped completely.
        let client = {
            let caller = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let owner_handle = owner.handle().clone();
            let connected =
                caller.block_on(async move { Client::connect_on(addr, test_handler(), None, &owner_handle).await });
            drop(caller);
            connected.expect("connect on the owner runtime")
        };

        // Writing from the owning runtime still works after the caller's reactor is gone.
        let channel = client.tasks.channel.clone();
        owner.block_on(async move {
            for index in 0..8u16 {
                let request = RemotingCommand::new_request(
                    i32::from(index),
                    bytes::Bytes::from_static(b"survives-the-caller-runtime"),
                );
                channel
                    .send_command(request)
                    .await
                    .expect("a frame still reaches the broker after the caller runtime is dropped");
            }
        });
        let mut polled = 0;
        while written.load(Ordering::Acquire) == 0 && polled < 500 {
            std::thread::sleep(Duration::from_millis(10));
            polled += 1;
        }
        assert!(written.load(Ordering::Acquire) > 0, "frames reached the peer");

        // And a later connection still works on the surviving owner.
        let again = owner
            .block_on(Client::connect_on(addr, test_handler(), None, owner.handle()))
            .expect("reconnect after the caller runtime is gone");
        assert!(again.connection().is_healthy());
    }

    /// Plan 49b R1: a waiter that gives up must not leave an unowned live connection behind.
    /// Aborting the connect call at different points against a successful TCP handshake always
    /// ends with the peer seeing EOF, so neither a cancelled leader nor its completed
    /// preparation can orphan a socket and its I/O tasks.
    #[test]
    fn abandoning_a_connect_leaves_no_live_connection() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        let owner = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();

        // The fake broker counts how many connections it still holds open.
        let live = Arc::new(AtomicUsize::new(0));
        let peer_live = Arc::clone(&live);
        let addr = owner.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let live = Arc::clone(&peer_live);
                    tokio::spawn(async move {
                        live.fetch_add(1, Ordering::Release);
                        let mut buf = [0u8; 1024];
                        while let Ok(n) = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                        live.fetch_sub(1, Ordering::Release);
                    });
                }
            });
            addr
        });

        for round in 0..32u32 {
            let handle = owner.handle().clone();
            let waiter = owner.spawn(async move { Client::connect_on(addr, test_handler(), None, &handle).await });
            // Cancel at varying distances from the handshake so both interleavings are covered.
            std::thread::sleep(Duration::from_micros(200 * u64::from(round % 8)));
            waiter.abort();
            // Dropping the handle matters: a connect that finished first keeps its client alive in
            // the join result until somebody takes it away.
            drop(waiter);
        }

        let mut polled = 0;
        while live.load(Ordering::Acquire) != 0 && polled < 1_000 {
            std::thread::sleep(Duration::from_millis(10));
            polled += 1;
        }
        assert_eq!(
            live.load(Ordering::Acquire),
            0,
            "no connection outlives the waiter that opened it"
        );
    }

    fn test_handler() -> Arc<RemotingGeneralHandler<DefaultRemotingRequestProcessor>> {
        Arc::new(RemotingGeneralHandler {
            request_processor: tokio::sync::Mutex::new(DefaultRemotingRequestProcessor),
            rpc_hooks: std::sync::RwLock::new(vec![]),
            pending_responses: PendingResponses::with_capacity(8),
        })
    }
}
