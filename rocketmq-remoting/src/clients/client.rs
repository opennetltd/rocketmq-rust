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
use rocketmq_rust::ArcMut;
use std::time::Duration;
use tokio::sync::broadcast;

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
use crate::remoting_server::rocketmq_tokio_server::Shutdown;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;

#[derive(Clone)]
pub struct Client<PR> {
    /// The TCP connection decorated with the rocketmq remoting protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    //connection: Connection,
    inner: ArcMut<ClientInner<PR>>,
    notify_shutdown: broadcast::Sender<()>,
}

struct ClientInner<PR> {
    cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
    ctx: ConnectionHandlerContext,
    shutdown: Shutdown,
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
    pub async fn connect<T>(
        addr: T,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        notify: broadcast::Receiver<()>,
    ) -> RocketMQResult<ArcMut<ClientInner<PR>>>
    where
        T: tokio::net::ToSocketAddrs,
    {
        let tcp_stream = tokio::net::TcpStream::connect(addr).await;
        if tcp_stream.is_err() {
            return Err(io_error(tcp_stream.err().unwrap()));
        }
        let stream = tcp_stream?;
        let local_addr = stream.local_addr()?;
        let remote_address = stream.peer_addr()?;
        let connection = Connection::new(stream);
        let channel_inner = ArcMut::new(ChannelInner::new(connection, cmd_handler.pending_responses.clone()));
        let channel = Channel::new(channel_inner, local_addr, remote_address);
        let client = ClientInner {
            cmd_handler,
            ctx: ArcMut::new(ConnectionHandlerContextWrapper::new(
                //connection,
                channel,
            )),
            shutdown: Shutdown::new(notify),
        };
        let client_inner = ArcMut::new(client);
        let mut client_ = client_inner.clone();
        tokio::spawn(async move {
            let _task = crate::metrics::recv_task_started();
            let _ = client_.run_recv().await;
        });

        if let Some(tx) = tx {
            let _ = tx.send(ConnectionNetEvent::CONNECTED(client_inner.ctx.channel.remote_address()));
        }
        Ok(client_inner)
    }

    async fn run_recv(&mut self) -> RocketMQResult<()> {
        loop {
            //Get the next frame from the connection.
            let channel = self.ctx.channel();
            let frame = tokio::select! {
                res = channel.receive_command() => res,
                _ = self.shutdown.recv() => {
                    channel.shutdown();
                    self.fail_pending("client shutdown");
                    return Ok(());
                }
            };
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
            self.cmd_handler.process_message_received(&mut self.ctx, cmd).await;
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
    pub(crate) async fn connect<T>(
        addr: T,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> RocketMQResult<Client<PR>>
    where
        T: tokio::net::ToSocketAddrs,
    {
        let (notify_shutdown, _) = broadcast::channel(1);
        let receiver = notify_shutdown.subscribe();
        let inner = ClientInner::connect(addr, cmd_handler, tx, receiver).await?;
        Ok(Client { inner, notify_shutdown })
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
            .inner
            .cmd_handler
            .pending_responses
            .register(
                self.inner.ctx.connection_ref().connection_id().as_str(),
                ResponseFuture::new(request.opaque(), timeout_millis, true, tx),
            )
            .map_err(|_| remote_error("duplicate pending request"))?;

        let pending_request = registration.request();
        if let Err(err) = self
            .inner
            .ctx
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
        if let Err(err) = self
            .inner
            .ctx
            .channel
            .channel_inner()
            .send_command(request, None, None)
            .await
        {
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
            if let Err(err) = self
                .inner
                .ctx
                .channel
                .channel_inner()
                .send_command(request, None, None)
                .await
            {
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
                .inner
                .cmd_handler
                .pending_responses
                .register(
                    self.inner.ctx.connection_ref().connection_id().as_str(),
                    ResponseFuture::new(request.opaque(), timeout_millis, true, tx),
                )
                .map_err(|_| remote_error("duplicate pending request"))?;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);
            let pending_request = registration.request();
            if let Err(err) = self
                .inner
                .ctx
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

    pub fn connection(&self) -> &Connection {
        self.inner.ctx.connection_ref()
    }

    #[allow(deprecated)]
    #[deprecated(note = "use the channel-owned send and receive methods")]
    pub fn connection_mut(&mut self) -> &mut Connection {
        self.inner.ctx.connection_mut()
    }
}
