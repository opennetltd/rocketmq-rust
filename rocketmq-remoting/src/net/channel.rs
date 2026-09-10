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

use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
// Use flume for high-performance async channel (40-60% faster than tokio::mpsc)
// Lock-free design provides better throughput under high load
use flume::Receiver;
use flume::Sender;
use rocketmq_error::RocketMQError;
use rocketmq_rust::ArcMut;
use tokio::time::timeout_at;
use tracing::error;
use uuid::Uuid;

use crate::base::pending_responses::PendingResponses;
use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::protocol::remoting_command::RemotingCommand;

pub type ChannelId = CheetahString;

pub type ArcChannel = ArcMut<Channel>;

/// High-level abstraction over a bidirectional network connection.
///
/// `Channel` represents a logical communication endpoint with identity,
/// address information, and access to the underlying connection and
/// response tracking infrastructure.
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │           Channel                       │
/// │  ┌─────────────────────────────────┐   │
/// │  │  Identity & Addressing          │   │
/// │  │  - channel_id (UUID)            │   │
/// │  │  - local_address (SocketAddr)   │   │
/// │  │  - remote_address (SocketAddr)  │   │
/// │  └─────────────────────────────────┘   │
/// │  ┌─────────────────────────────────┐   │
/// │  │  ChannelInner (shared state)    │   │
/// │  │  - Connection (I/O)             │   │
/// │  │  - ResponseTable (futures)      │   │
/// │  │  - Message queue (tx/rx)        │   │
/// │  └─────────────────────────────────┘   │
/// └─────────────────────────────────────────┘
/// ```
///
/// ## Design Rationale
///
/// - **Separation of concerns**: `Channel` handles identity/routing, `ChannelInner` handles I/O
/// - **Clone-friendly**: Lightweight outer type can be cloned, shares inner state via `Arc`
/// - **Equality/Hash**: Based on identity (addresses + ID), not inner state
#[derive(Clone)]
pub struct Channel {
    // === Core State ===
    /// Shared access to synchronized channel internals (connection, response tracking, etc.)
    inner: Arc<ChannelInner>,

    // === Identity & Addressing ===
    /// Local socket address (our end of the connection)
    local_address: SocketAddr,

    /// Remote peer socket address (their end of the connection)
    remote_address: SocketAddr,

    /// Unique identifier for this channel instance (UUID-based)
    ///
    /// Used for logging, routing, and distinguishing channels in maps/sets.
    channel_id: ChannelId,
}

impl Channel {
    /// Creates a new `Channel` with generated UUID identifier.
    ///
    /// # Arguments
    ///
    /// * `inner` - Shared channel state (connection, response table, etc.)
    /// * `local_address` - Our local socket address
    /// * `remote_address` - Remote peer socket address
    ///
    /// # Returns
    ///
    /// A new channel with a randomly generated UUID as its ID.
    pub fn new(inner: Arc<ChannelInner>, local_address: SocketAddr, remote_address: SocketAddr) -> Self {
        let channel_id = Uuid::new_v4().to_string().into();
        Self {
            inner,
            local_address,
            remote_address,
            channel_id,
        }
    }

    // === Address Mutators ===

    /// Updates the local address of this channel.
    ///
    /// # Arguments
    ///
    /// * `local_address` - New local socket address
    #[inline]
    pub fn set_local_address(&mut self, local_address: SocketAddr) {
        self.local_address = local_address;
    }

    /// Updates the remote address of this channel.
    ///
    /// # Arguments
    ///
    /// * `remote_address` - New remote socket address
    #[inline]
    pub fn set_remote_address(&mut self, remote_address: SocketAddr) {
        self.remote_address = remote_address;
    }

    /// Updates the channel identifier.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - New channel ID (convertible to `CheetahString`)
    ///
    /// # Warning
    ///
    /// Changing the ID after insertion into a HashMap/HashSet will break lookup.
    #[inline]
    pub fn set_channel_id(&mut self, channel_id: impl Into<CheetahString>) {
        self.channel_id = channel_id.into();
    }

    // === Address Accessors ===

    /// Gets the local socket address.
    ///
    /// # Returns
    ///
    /// The local address of this channel
    #[inline]
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Gets the remote peer socket address.
    ///
    /// # Returns
    ///
    /// The remote address of this channel
    #[inline]
    pub fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    /// Gets the channel identifier as a string slice.
    ///
    /// # Returns
    ///
    /// String slice of the channel ID
    #[inline]
    pub fn channel_id(&self) -> &str {
        self.channel_id.as_str()
    }

    /// Gets a cloned owned copy of the channel identifier.
    ///
    /// # Returns
    ///
    /// Owned `CheetahString` containing the channel ID
    pub fn channel_id_owned(&self) -> CheetahString {
        self.channel_id.clone()
    }

    // === Connection Access ===

    /// Legacy accessor returning the synchronized connection.
    ///
    /// Deprecated: internal remoting paths use the channel-owned I/O methods.
    #[allow(deprecated)]
    #[deprecated(note = "use Channel::send_command, send_bytes, or receive_command")]
    pub fn connection_mut(&self) -> &Connection {
        self.inner.connection.as_ref()
    }

    /// Gets immutable access to the underlying connection.
    ///
    /// # Returns
    ///
    /// Immutable reference to the `Connection` for inspection
    #[inline]
    pub fn connection_ref(&self) -> &Connection {
        self.inner.connection_ref()
    }

    pub async fn send_command(&self, command: RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_command(command, None, None).await
    }

    pub async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_bytes(bytes).await
    }

    pub(crate) async fn receive_command(&self) -> Option<rocketmq_error::RocketMQResult<RemotingCommand>> {
        self.inner.receive_command().await
    }

    pub(crate) fn shutdown(&self) {
        self.inner.shutdown();
    }

    // === Inner State Access ===

    /// Gets immutable access to the shared channel state.
    ///
    /// # Returns
    ///
    /// Immutable reference to `ChannelInner` (connection + response table)
    pub fn channel_inner(&self) -> &ChannelInner {
        self.inner.as_ref()
    }

    /// Legacy accessor returning immutable shared channel state.
    #[deprecated(note = "use channel_inner; shared channel state cannot be mutably borrowed")]
    pub fn channel_inner_mut(&self) -> &ChannelInner {
        self.inner.as_ref()
    }
}

impl PartialEq for Channel {
    fn eq(&self, other: &Self) -> bool {
        self.local_address == other.local_address
            && self.remote_address == other.remote_address
            && self.channel_id == other.channel_id
    }
}

impl Eq for Channel {}

impl Hash for Channel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.local_address.hash(state);
        self.remote_address.hash(state);
        self.channel_id.hash(state);
    }
}

impl Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel {{ local_address: {:?}, remote_address: {:?}, channel_id: {} }}",
            self.local_address, self.remote_address, self.channel_id
        )
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel {{ local_address: {}, remote_address: {}, channel_id: {} }}",
            self.local_address, self.remote_address, self.channel_id
        )
    }
}

/// Internal message type for the send queue.
///
/// Encapsulates a command to send along with optional response tracking.
enum ChannelMessage {
    Command(
        RemotingCommand,
        Option<crate::base::pending_responses::PendingRequest>,
        Option<tokio::time::Instant>,
    ),
    Bytes(Bytes),
}

/// Shared state for a `Channel` - handles I/O, async message queueing, and response tracking.
///
/// `ChannelInner` is the "heavy" part of a channel that is shared via `Arc` across
/// multiple `Channel` clones. It manages:
///
/// - **Connection**: Low-level TCP I/O
/// - **Send Queue**: Async message queueing to decouple caller from I/O backpressure
/// - **Response Table**: Tracks pending request-response pairs (opaque ID → future)
///
/// ## Threading Model
///
/// - **Send Task**: Dedicated task (`handle_send`) pulls from queue and writes to connection
/// - **Response Tracking**: Shared owner accessed by send and receive tasks
///
/// ## Lifecycle
///
/// 1. **Created**: Spawns background `handle_send` task
/// 2. **Active**: Processes send queue, tracks responses
/// 3. **Shutdown**: Queue closed, pending responses canceled
pub struct ChannelInner {
    // === Message Queue ===
    /// Sender half of the high-performance message queue channel.
    ///
    /// Uses `flume` instead of `tokio::mpsc` for:
    /// - 40-60% better throughput (lock-free for most operations)
    /// - Lower latency under contention
    /// - Better backpressure handling
    ///
    /// Callers use this to enqueue commands for asynchronous sending.
    /// The receive half is owned by the background `handle_send` task.
    outbound_queue_tx: Sender<ChannelMessage>,

    // === I/O Transport ===
    /// Underlying network connection shared by channel-owned transport tasks.
    ///
    /// `Connection` serializes all outbound operations and allows the reader and
    /// writer to run independently. The deprecated mutable accessor is retained
    /// only for source compatibility.
    pub(crate) connection: Arc<Connection>,

    // === Response Tracking ===
    /// Synchronized pending request owner keyed by connection and opaque ID.
    ///
    /// Shared between:
    /// - Send task: Inserts entries when request is sent
    /// - Receive task: Removes and completes entries when response arrives
    pub(crate) pending_responses: PendingResponses,
    connection_id: String,
    shutdown: tokio::sync::broadcast::Sender<()>,
    closed: Arc<AtomicBool>,
    shutdown_started: AtomicBool,
    send_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Background task that processes the outbound message queue.
///
/// # Performance Features
///
/// - Uses `flume` receiver for lock-free message reception
/// - Processes messages sequentially to maintain order
/// - Handles errors gracefully (marks connection as failed on I/O errors)
///
/// # Potential Optimization (TODO)
///
/// Consider implementing batch sending:
/// ```ignore
/// // Collect multiple pending messages
/// let mut batch = vec![first_msg];
/// while batch.len() < 32 {
///     match rx.try_recv() {
///         Ok(msg) => batch.push(msg),
///         Err(_) => break,
///     }
/// }
/// // Send batch together for better throughput
/// ```
///
/// This would reduce per-message overhead and improve throughput by ~20-40%
/// under high load, at the cost of slightly increased latency for small batches.
async fn handle_send(
    connection: Arc<Connection>,
    rx: Receiver<ChannelMessage>,
    pending_responses: PendingResponses,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    closed: Arc<AtomicBool>,
) {
    let _task = crate::metrics::send_task_started();
    loop {
        let msg = tokio::select! {
            msg = rx.recv_async() => match msg {
                Ok(msg) => msg,
                Err(_) => break,
            },
            _ = shutdown.recv() => break,
        };

        let (send, pending_request, deadline) = match msg {
            ChannelMessage::Command(send, pending_request, deadline) => (send, pending_request, deadline),
            ChannelMessage::Bytes(bytes) => {
                let result = tokio::select! {
                    result = connection.send_bytes(bytes) => result,
                    _ = shutdown.recv() => return,
                };
                if let Err(error) = result {
                    closed.store(true, Ordering::Release);
                    connection.close();
                    pending_responses.fail_connection(connection.connection_id().as_ref(), error.to_string());
                    let _ = shutdown_tx.send(());
                    return;
                }
                continue;
            }
        };

        if let Some(pending_request) = &pending_request {
            if !pending_request.is_registered() {
                continue;
            }
        }

        let result = match deadline {
            Some(deadline) if deadline <= tokio::time::Instant::now() => {
                if let Some(pending_request) = pending_request {
                    pending_request.timeout(pending_request.timeout_millis());
                }
                continue;
            }
            Some(deadline) => tokio::select! {
                result = tokio::time::timeout_at(deadline, connection.send_command(send)) => result,
                _ = shutdown.recv() => return,
            },
            None => tokio::select! {
                result = connection.send_command(send) => Ok(result),
                _ = shutdown.recv() => return,
            },
        };

        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => match error {
                rocketmq_error::RocketMQError::IO(error) => {
                    error!("send request failed: {}", error);
                    closed.store(true, Ordering::Release);
                    connection.close();
                    pending_responses.fail_connection(connection.connection_id().as_ref(), error.to_string());
                    let _ = shutdown_tx.send(());
                    return;
                }
                _ => {
                    if let Some(pending_request) = pending_request {
                        pending_request.fail(error.to_string());
                    }
                }
            },
            Err(_) => {
                if let Some(pending_request) = pending_request {
                    pending_request.timeout(pending_request.timeout_millis());
                }
                // A cancelled flush can leave a partial frame in the sink. Never
                // reuse that stream for a subsequent request.
                closed.store(true, Ordering::Release);
                connection.close();
                pending_responses.fail_connection(connection.connection_id().as_ref(), "send deadline expired");
                let _ = shutdown_tx.send(());
                return;
            }
        }
    }
}

impl Drop for ChannelInner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl ChannelInner {
    /// Creates a new `ChannelInner` and spawns the background send task.
    ///
    /// # Arguments
    ///
    /// * `connection` - The underlying TCP connection
    /// * `pending_responses` - Shared synchronized response tracking
    ///
    /// # Returns
    ///
    /// A new `ChannelInner` with an active background send task.
    ///
    /// # Implementation Note
    ///
    /// - Queue capacity: 1024 messages (adjust based on load)
    /// - Spawns `handle_send` task immediately
    /// - Task runs until channel is dropped or connection fails
    ///
    /// # Performance
    ///
    /// Uses `flume::bounded` channel for better performance:
    /// - Lock-free operations for most cases
    /// - ~40-60% higher throughput than tokio::mpsc
    /// - Better performance under contention
    pub fn new(connection: Connection, pending_responses: PendingResponses) -> Self {
        const QUEUE_CAPACITY: usize = 1024;

        // Use flume bounded channel for better performance
        // flume provides lock-free operations and better throughput than tokio::mpsc
        let (outbound_queue_tx, outbound_queue_rx) = flume::bounded(QUEUE_CAPACITY);

        let connection = Arc::new(connection);
        let (shutdown, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let closed = Arc::new(AtomicBool::new(false));
        let send_task = tokio::spawn(handle_send(
            connection.clone(),
            outbound_queue_rx,
            pending_responses.clone(),
            shutdown_rx,
            shutdown.clone(),
            closed.clone(),
        ));
        let connection_id = connection.connection_id().to_string();
        Self {
            outbound_queue_tx,
            connection,
            connection_id,
            pending_responses,
            shutdown,
            closed,
            shutdown_started: AtomicBool::new(false),
            send_task: Mutex::new(Some(send_task)),
        }
    }
}

impl ChannelInner {
    // === Connection Accessors ===

    /// Gets a cloned `Arc` handle to the synchronized connection.
    #[inline]
    pub fn connection(&self) -> Arc<Connection> {
        self.connection.clone()
    }

    /// Gets an immutable reference to the connection.
    ///
    /// # Returns
    ///
    /// Immutable reference to the underlying `Connection`
    #[inline]
    pub fn connection_ref(&self) -> &Connection {
        self.connection.as_ref()
    }

    /// Legacy accessor returning the synchronized connection.
    ///
    /// Deprecated: internal remoting paths use the channel-owned I/O methods.
    #[allow(deprecated)]
    #[deprecated(note = "use the channel-owned send and receive methods")]
    pub fn connection_mut(&self) -> &Connection {
        self.connection.as_ref()
    }

    pub(crate) fn shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        self.closed.store(true, Ordering::Release);
        let _ = self.shutdown.send(());
        self.connection.close();
        self.pending_responses
            .fail_connection(&self.connection_id, "connection closed");
        if let Some(task) = self.send_task.lock().expect("send task lock poisoned").take() {
            task.abort();
        }
    }

    async fn receive_command(&self) -> Option<rocketmq_error::RocketMQResult<RemotingCommand>> {
        self.connection.clone().receive_command().await
    }

    async fn enqueue(
        &self,
        message: ChannelMessage,
        deadline: Option<tokio::time::Instant>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut shutdown = self.shutdown.subscribe();
        if self.closed.load(Ordering::Acquire) || !self.connection.is_healthy() {
            return Err(RocketMQError::network_connection_failed("channel", "connection closed"));
        }
        let timeout_millis = match &message {
            ChannelMessage::Command(_, Some(pending_request), _) => Some(pending_request.timeout_millis()),
            _ => None,
        };
        let result = match deadline {
            Some(deadline) => {
                tokio::select! {
                    result = timeout_at(deadline, self.outbound_queue_tx.send_async(message)) => match result {
                        Ok(result) => result,
                        Err(_) => {
                            return Err(RocketMQError::Timeout {
                                operation: "send_queue",
                                timeout_ms: timeout_millis.unwrap_or(0),
                            })
                        }
                    },
                    _ = shutdown.recv() => {
                        return Err(RocketMQError::network_connection_failed("channel", "connection closed"));
                    }
                }
            }
            None => {
                tokio::select! {
                    result = self.outbound_queue_tx.send_async(message) => result,
                    _ = shutdown.recv() => {
                        return Err(RocketMQError::network_connection_failed("channel", "connection closed"));
                    }
                }
            }
        };
        result.map_err(|err| RocketMQError::network_connection_failed("channel", format!("send failed: {err}")))
    }

    pub(crate) async fn send_command(
        &self,
        request: RemotingCommand,
        deadline: Option<tokio::time::Instant>,
        pending_request: Option<crate::base::pending_responses::PendingRequest>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.enqueue(ChannelMessage::Command(request, pending_request, deadline), deadline)
            .await
    }

    pub(crate) async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.enqueue(ChannelMessage::Bytes(bytes), None).await
    }

    // === High-Level Send Methods ===

    /// Sends a request and waits for the response (request-response pattern).
    ///
    /// Enqueues the request, tracks it via opaque ID, and blocks until the
    /// response arrives or timeout expires.
    ///
    /// # Arguments
    ///
    /// * `request` - The command to send
    /// * `timeout_millis` - Maximum wait time for response (milliseconds)
    ///
    /// # Returns
    ///
    /// - `Ok(response)`: Response received within timeout
    /// - `Err(ChannelSendRequestFailed)`: Failed to enqueue request
    /// - `Err(ChannelRecvRequestFailed)`: Response channel closed or timeout
    ///
    /// # Lifecycle
    ///
    /// 1. Create oneshot channel for response
    /// 2. Enqueue request with response channel
    /// 3. Wait (with timeout) for response on channel
    /// 4. Clean up response table on error
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = RemotingCommand::create_request_command(10, header).into();
    /// let response = channel_inner.send_wait_response(request, 3000).await?;
    /// println!("Got response: {:?}", response);
    /// ```
    pub async fn send_wait_response(
        &self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let (response_tx, response_rx) =
            tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);
        let opaque = request.opaque();
        let registration = self
            .pending_responses
            .register(
                &self.connection_id,
                ResponseFuture::new(opaque, timeout_millis, true, response_tx),
            )
            .map_err(|_| RocketMQError::network_connection_failed("channel", "duplicate pending request"))?;

        // Enqueue request with response tracking
        // flume sender: use send_async() for async context
        if let Err(err) = self
            .enqueue(
                ChannelMessage::Command(request, Some(registration.request()), Some(deadline)),
                Some(deadline),
            )
            .await
        {
            drop(registration);
            return Err(err);
        }

        // Wait for response with timeout
        match timeout_at(deadline, response_rx).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(e) => {
                    // Response channel closed without sending (connection dropped?)
                    drop(registration);
                    Err(RocketMQError::network_connection_failed(
                        "channel",
                        format!("connection dropped: {}", e),
                    ))
                }
            },
            Err(_) => {
                // Timeout expired
                registration.timeout(timeout_millis);
                drop(registration);
                Err(RocketMQError::Timeout {
                    operation: "channel_recv",
                    timeout_ms: timeout_millis,
                })
            }
        }
    }

    /// Sends a one-way request without waiting for response (fire-and-forget).
    ///
    /// Marks the request as oneway and enqueues it. Does not track response.
    ///
    /// # Arguments
    ///
    /// * `request` - The command to send
    /// * `timeout_millis` - Timeout for enqueuing (not for response)
    ///
    /// # Returns
    ///
    /// - `Ok(().into())`: Request successfully enqueued
    /// - `Err(ChannelSendRequestFailed)`: Failed to enqueue
    ///
    /// # Use Case
    ///
    /// Notifications, heartbeats, or any scenario where response is not needed.
    /// More efficient than `send_wait_response` as it avoids response tracking overhead.
    pub async fn send_oneway(
        &self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = request.mark_oneway_rpc();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);

        // flume sender: use send_async() for async context
        if let Err(err) = self
            .enqueue(ChannelMessage::Command(request, None, Some(deadline)), Some(deadline))
            .await
        {
            error!("send oneway request failed: {}", err);
            return Err(RocketMQError::network_connection_failed(
                "channel",
                format!("send oneway failed: {}", err),
            ));
        }
        Ok(())
    }

    /// Sends a request without waiting for response (async enqueue only).
    ///
    /// Similar to `send_oneway`, but does not mark the request as oneway.
    /// Use when caller doesn't care about response but request is not marked as oneway protocol.
    ///
    /// # Arguments
    ///
    /// * `request` - The command to send
    /// * `timeout_millis` - Optional timeout for enqueuing
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Request successfully enqueued
    /// - `Err(ChannelSendRequestFailed)`: Failed to enqueue
    pub async fn send(
        &self,
        request: RemotingCommand,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        // flume sender: use send_async() for async context
        let deadline = timeout_millis.map(|timeout| tokio::time::Instant::now() + Duration::from_millis(timeout));
        if let Err(err) = self
            .enqueue(ChannelMessage::Command(request, None, deadline), deadline)
            .await
        {
            error!("send request failed: {}", err);
            return Err(RocketMQError::network_connection_failed(
                "channel",
                format!("send failed: {}", err),
            ));
        }
        Ok(())
    }

    // === Health Check ===

    /// Checks if the underlying connection is healthy.
    ///
    /// # Returns
    ///
    /// - `true`: Connection is operational
    /// - `false`: Connection has failed, channel should be discarded
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.connection.is_healthy()
    }

    /// Legacy alias for `is_healthy()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `is_healthy()` instead for clearer semantics.
    #[inline]
    #[deprecated(since = "0.1.0", note = "Use `is_healthy()` instead")]
    pub fn is_ok(&self) -> bool {
        self.connection.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_error::RocketMQResult;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn timeout_and_close_leave_no_pending_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(address);
        let (stream, _) = tokio::join!(client, listener.accept());
        let channel = ChannelInner::new(Connection::new(stream.unwrap()), PendingResponses::with_capacity(1));

        let request = RemotingCommand::new_request(1, Bytes::new());
        let result = channel.send_wait_response(request, 10).await;
        assert!(matches!(result, Err(RocketMQError::Timeout { .. })));
        assert_eq!(channel.pending_responses.len(), 0);

        let (tx, rx) = oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let registration = channel
            .pending_responses
            .register(&channel.connection_id, ResponseFuture::new(2, 1000, true, tx))
            .map_err(|_| "duplicate pending response")
            .unwrap();
        let send_task = channel.send_task.lock().unwrap().take().unwrap();
        channel.shutdown();
        assert!(matches!(rx.await.unwrap(), Err(RocketMQError::Network(_))));
        drop(registration);
        assert_eq!(channel.pending_responses.len(), 0);
        assert_eq!(channel.connection.state(), crate::connection::ConnectionState::Closed);
        tokio::time::timeout(Duration::from_secs(1), send_task)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn send_deadline_retires_connection_and_stops_writer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, server) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let _peer = server.unwrap().0;
        let channel = ChannelInner::new(Connection::new(client.unwrap()), PendingResponses::with_capacity(1));
        let _blocked = channel.connection.block_outbound_for_test().await;
        let send_task = channel.send_task.lock().unwrap().take().unwrap();
        let result = channel
            .send_wait_response(RemotingCommand::new_request(1, Bytes::new()), 20)
            .await;
        assert!(matches!(result, Err(RocketMQError::Timeout { .. })));
        tokio::time::timeout(Duration::from_secs(1), send_task)
            .await
            .expect("timed-out writer must exit instead of reusing a possibly partial frame")
            .unwrap();
        assert_eq!(channel.connection.state(), crate::connection::ConnectionState::Closed);
        assert_eq!(channel.pending_responses.len(), 0);
        assert!(channel
            .send(RemotingCommand::new_request(2, Bytes::new()), None)
            .await
            .is_err());
    }
}
