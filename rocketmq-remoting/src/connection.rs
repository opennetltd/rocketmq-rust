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

use std::hash::Hash;
use std::hash::Hasher;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::codec::remoting_command_codec::CompositeCodec;
use crate::protocol::remoting_command::RemotingCommand;

pub type ConnectionId = CheetahString;

/// Connection health state
///
/// Represents the current health status of a connection.
/// This enum is used with `watch` channel to broadcast state changes
/// to all interested parties without explicit polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is healthy and ready for I/O operations
    Healthy,
    /// Connection has encountered an error and should not be used
    Degraded,
    /// Connection is explicitly closed
    Closed,
}

/// Bidirectional TCP connection for RocketMQ protocol communication.
///
/// `Connection` handles low-level frame encoding/decoding and provides high-level
/// APIs for sending/receiving `RemotingCommand` messages. It manages I/O buffers
/// and broadcasts connection state changes via a watch channel.
///
/// ## Lifecycle & State Management
///
/// **Tokio Best Practice**: Connection health is determined by I/O operation results,
/// not by polling a boolean flag. State changes are broadcast via `watch` channel:
///
/// ```text
/// ┌──────────┐  I/O Success   ┌──────────┐
/// │ Healthy  │ ──────────────► │ Healthy  │
/// └──────────┘                 └──────────┘
///      │                            │
///      │ I/O Error                  │ I/O Error
///      ↓                            ↓
/// ┌──────────┐                 ┌──────────┐
/// │ Degraded │                 │ Degraded │
/// └──────────┘                 └──────────┘
///      │                            │
///      │ close()                    │
///      ↓                            ↓
/// ┌──────────┐                 ┌──────────┐
/// │  Closed  │                 │  Closed  │
/// └──────────┘                 └──────────┘
/// ```
///
/// 1. **Created**: New connection from `TcpStream` (Healthy)
/// 2. **Active**: Processing requests/responses (Healthy)
/// 3. **Degraded**: I/O error occurred, broadcast state change
/// 4. **Closed**: Stream ended or explicit shutdown
///
/// ## Threading
///
/// - Safe for concurrent sends (internal buffering)
/// - Receives must be sequential (single reader)
/// - State monitoring: Multiple tasks can watch state via `subscribe()`
///
/// ## Key Design Principles
///
/// - **No explicit `ok` flag**: Connection validity determined by I/O results
/// - **Broadcast state changes**: Using `watch` channel for reactive updates
/// - **Fail-fast**: I/O errors immediately update state and return error
/// - **Zero polling**: Subscribers notified automatically on state change
struct Outbound {
    sink: SplitSink<Framed<TcpStream, CompositeCodec>, Bytes>,
    encode_buffer: BytesMut,
}

pub struct Connection {
    // === I/O Transport ===
    outbound: Mutex<Outbound>,
    inbound_stream: Mutex<SplitStream<Framed<TcpStream, CompositeCodec>>>,

    // === State Management (Tokio Watch Channel) ===
    /// Broadcast channel for connection state changes
    ///
    /// **Design**: Uses `watch` channel to notify all subscribers of state changes.
    /// This is the Tokio-idiomatic way to share state without locks or polling.
    ///
    /// - **Sender**: Held by Connection to broadcast state changes
    /// - **Receivers**: Created via `subscribe()` for monitoring
    ///
    /// **Why not a boolean?**
    /// - Reactive: Subscribers notified immediately on change
    /// - Lock-free: No mutex/atomic overhead
    /// - Composable: Can use in `tokio::select!` for timeout/cancellation
    state_tx: watch::Sender<ConnectionState>,

    /// Cached current state receiver for quick local queries
    ///
    /// Used for fast `state()` queries without creating new receivers
    state_rx: watch::Receiver<ConnectionState>,

    // === Identification ===
    /// Unique identifier for this connection instance
    ///
    /// Generated via UUID, stable across the connection lifetime
    connection_id: ConnectionId,
}

impl Hash for Connection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.connection_id.hash(state);
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.connection_id == other.connection_id
    }
}

impl Eq for Connection {}

impl Connection {
    #[cfg(test)]
    pub(crate) async fn block_outbound_for_test(&self) -> impl Drop + '_ {
        self.outbound.lock().await
    }

    async fn wait_closed(mut state: watch::Receiver<ConnectionState>) {
        loop {
            if *state.borrow() == ConnectionState::Closed {
                return;
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    }

    async fn send_outbound(&self, outbound: &mut Outbound, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        if self.state() == ConnectionState::Closed {
            return Err(rocketmq_error::RocketMQError::network_connection_failed(
                "connection",
                "connection closed",
            ));
        }
        let result = tokio::select! {
            result = outbound.sink.send(bytes) => result,
            _ = Self::wait_closed(self.subscribe()) => Err(rocketmq_error::RocketMQError::network_connection_failed(
                "connection",
                "connection closed",
            )),
        };
        if result.is_err() && self.state() != ConnectionState::Closed {
            self.mark_degraded();
        }
        result
    }

    /// Creates a new `Connection` instance with initial Healthy state.
    ///
    /// # Arguments
    ///
    /// * `tcp_stream` - The `TcpStream` associated with the connection
    ///
    /// # Returns
    ///
    /// A new `Connection` instance with a watch channel for state monitoring
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stream = TcpStream::connect("127.0.0.1:9876").await?;
    /// let connection = Connection::new(stream);
    ///
    /// // Subscribe to state changes
    /// let mut state_watcher = connection.subscribe();
    /// tokio::spawn(async move {
    ///     while state_watcher.changed().await.is_ok() {
    ///         let state = *state_watcher.borrow();
    ///         println!("Connection state: {:?}", state);
    ///     }
    /// });
    /// ```
    pub fn new(tcp_stream: TcpStream) -> Connection {
        const CAPACITY: usize = 1024 * 1024; // 1 MB
        const BUFFER_SIZE: usize = 8 * 1024; // 8 KB
        let framed = Framed::with_capacity(tcp_stream, CompositeCodec::new(), CAPACITY);
        let (outbound_sink, inbound_stream) = framed.split();

        // Initialize watch channel with Healthy state
        let (state_tx, state_rx) = watch::channel(ConnectionState::Healthy);

        Self {
            outbound: Mutex::new(Outbound {
                sink: outbound_sink,
                encode_buffer: BytesMut::with_capacity(BUFFER_SIZE),
            }),
            inbound_stream: Mutex::new(inbound_stream),
            state_tx,
            state_rx,
            connection_id: CheetahString::from_string(Uuid::new_v4().to_string()),
        }
    }

    /// Receives the next `RemotingCommand` from the peer.
    ///
    /// Blocks until a complete frame is available or the stream ends.
    ///
    /// # Returns
    ///
    /// - `Some(Ok(command))`: Successfully received and decoded a command
    /// - `Some(Err(e))`: Decoding error occurred
    /// - `None`: Stream ended (peer closed connection)
    ///
    /// # Example
    ///
    /// ```ignore
    /// while let Some(result) = connection.receive_command().await {
    ///     match result {
    ///         Ok(cmd) => handle_command(cmd),
    ///         Err(e) => eprintln!("Decode error: {}", e),
    ///     }
    /// }
    /// // Connection closed
    /// ```
    pub async fn receive_command(&self) -> Option<rocketmq_error::RocketMQResult<RemotingCommand>> {
        let mut inbound = self.inbound_stream.lock().await;
        tokio::select! {
            result = inbound.next() => result,
            _ = Self::wait_closed(self.subscribe()) => None,
        }
    }

    /// Sends a `RemotingCommand` to the peer (consumes command).
    ///
    /// Encodes the command into the internal buffer, then flushes to the network.
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `command` - The command to send (consumed)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Command successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # State Management
    ///
    /// On error, this method:
    /// 1. Marks connection as `Degraded` via watch channel
    /// 2. Broadcasts state change to all subscribers
    /// 3. Returns the error to caller
    ///
    /// **No need to explicitly check `is_healthy()` before calling** - just
    /// handle the `Result` and the connection state is automatically managed.
    ///
    /// # Lifecycle
    ///
    /// 1. Encode command header + body into reusable buffer
    /// 2. Use zero-copy `split_to()` to extract buffer contents as `Bytes`
    /// 3. Send extracted bytes via outbound sink
    /// 4. Buffer is now empty and ready for next command (no clear() needed)
    ///
    /// # Performance Optimization
    ///
    /// - Uses `split_to(len)` instead of `split()` for better performance
    /// - `split_to()` returns all data and leaves buffer empty, eliminating need for clear()
    /// - `freeze()` converts BytesMut to Bytes with zero-copy (just refcount increment)
    ///
    /// Sends through the connection-owned outbound sink.
    pub async fn send_command(&self, mut command: RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        let mut outbound = self.outbound.lock().await;
        command.fast_header_encode(&mut outbound.encode_buffer);
        if let Some(body_inner) = command.take_body() {
            outbound.encode_buffer.put(body_inner);
        }
        let len = outbound.encode_buffer.len();
        let bytes = outbound.encode_buffer.split_to(len).freeze();
        self.send_outbound(&mut outbound, bytes).await
    }

    /// Sends a `RemotingCommand` to the peer (borrows command).
    ///
    /// Similar to `send_command`, but borrows the command mutably instead of
    /// consuming it. Use when the caller needs to retain ownership.
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `command` - Mutable reference to the command to send
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Command successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # Note
    ///
    /// This method may consume the command's body (`take_body()`), modifying
    /// the original command.
    pub async fn send_command_ref(&self, command: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        let mut outbound = self.outbound.lock().await;
        command.fast_header_encode(&mut outbound.encode_buffer);
        if let Some(body_inner) = command.take_body() {
            outbound.encode_buffer.put(body_inner);
        }
        let len = outbound.encode_buffer.len();
        let bytes = outbound.encode_buffer.split_to(len).freeze();
        self.send_outbound(&mut outbound, bytes).await
    }

    /// Sends multiple `RemotingCommand`s in a single batch (optimized for throughput).
    ///
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Performance Benefits
    ///
    /// - **Reduced system calls**: Multiple commands sent in one syscall
    /// - **Better CPU cache**: Encoding loop stays hot
    /// - **Lower latency**: No network round-trips between commands
    ///
    /// # Benchmarks
    ///
    /// ```text
    /// send_command() x 100:  ~50ms  (100 syscalls)
    /// send_batch() x 100:    ~15ms  (1 syscall)
    /// Improvement: 3.3x faster
    /// ```
    ///
    /// # Arguments
    ///
    /// * `commands` - Vector of commands to send (consumed for zero-copy)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: All commands sent successfully
    /// - `Err(e)`: Network I/O error (connection marked as Degraded)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let batch = vec![cmd1, cmd2, cmd3];
    /// connection.send_batch(batch).await?;
    /// ```
    pub async fn send_batch(&self, mut commands: Vec<RemotingCommand>) -> rocketmq_error::RocketMQResult<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let mut outbound = self.outbound.lock().await;
        for command in &mut commands {
            command.fast_header_encode(&mut outbound.encode_buffer);
            if let Some(body_inner) = command.take_body() {
                outbound.encode_buffer.put(body_inner);
            }
        }
        let len = outbound.encode_buffer.len();
        let bytes = outbound.encode_buffer.split_to(len).freeze();
        self.send_outbound(&mut outbound, bytes).await
    }

    /// Sends raw `Bytes` directly to the peer (zero-copy).
    ///
    /// Bypasses command encoding and sends pre-serialized bytes directly.
    /// Use for forwarding or when bytes are already encoded.
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to send (reference-counted, zero-copy)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Bytes successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # Performance
    ///
    /// This is the most efficient send method as it avoids intermediate buffering
    /// and serialization overhead.
    pub async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        let mut outbound = self.outbound.lock().await;
        self.send_outbound(&mut outbound, bytes).await
    }

    /// Sends a static byte slice to the peer (zero-copy).
    ///
    /// Converts a `&'static [u8]` to `Bytes` and sends. Use for compile-time
    /// known data (e.g., protocol constants).
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `slice` - Static byte slice with `'static` lifetime
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Slice successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// const PING: &[u8] = b"PING\r\n";
    /// connection.send_slice(PING).await?;
    /// ```
    pub async fn send_slice(&self, slice: &'static [u8]) -> rocketmq_error::RocketMQResult<()> {
        self.send_bytes(slice.into()).await
    }

    /// Gets the unique identifier for this connection.
    ///
    /// # Returns
    ///
    /// Reference to the connection ID (UUID-based string)
    #[inline]
    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    /// Gets the current connection state.
    ///
    /// # Returns
    ///
    /// Current `ConnectionState` (Healthy, Degraded, or Closed)
    ///
    /// # Performance
    ///
    /// This is a fast, lock-free read from the watch channel receiver.
    /// No system calls or network operations involved.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if connection.state() == ConnectionState::Healthy {
    ///     // Safe to send
    ///     connection.send_command(cmd).await?;
    /// }
    /// ```
    #[inline]
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Checks if the connection is in a healthy state (convenience method).
    ///
    /// # Returns
    ///
    /// - `true`: Connection is `Healthy` and operational
    /// - `false`: Connection is `Degraded` or `Closed`
    ///
    /// # Note
    ///
    /// **Prefer using `send_*()` methods directly** rather than checking state first.
    /// This method is provided for backward compatibility and specific use cases
    /// like connection pool eviction.
    ///
    /// **Best practice (Tokio-idiomatic)**:
    /// ```rust,ignore
    /// // Don't do this:
    /// if connection.is_healthy() {
    ///     connection.send_command(cmd).await?;
    /// }
    ///
    /// // Do this instead:
    /// match connection.send_command(cmd).await {
    ///     Ok(()) => { /* success */ }
    ///     Err(e) => { /* connection automatically marked as degraded */ }
    /// }
    /// ```
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.state() == ConnectionState::Healthy
    }

    /// Subscribes to connection state changes.
    ///
    /// # Returns
    ///
    /// A `watch::Receiver` that notifies on state transitions
    ///
    /// # Example: Monitor state in background task
    ///
    /// ```rust,ignore
    /// let mut state_watcher = connection.subscribe();
    /// tokio::spawn(async move {
    ///     while state_watcher.changed().await.is_ok() {
    ///         match *state_watcher.borrow() {
    ///             ConnectionState::Healthy => println!("Connection restored"),
    ///             ConnectionState::Degraded => println!("Connection degraded"),
    ///             ConnectionState::Closed => {
    ///                 println!("Connection closed");
    ///                 break;
    ///             }
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// # Example: Wait for state change with timeout
    ///
    /// ```rust,ignore
    /// let mut state_watcher = connection.subscribe();
    /// tokio::select! {
    ///     _ = state_watcher.changed() => {
    ///         println!("State changed to: {:?}", *state_watcher.borrow());
    ///     }
    ///     _ = tokio::time::sleep(Duration::from_secs(5)) => {
    ///         println!("No state change within 5 seconds");
    ///     }
    /// }
    /// ```
    pub fn subscribe(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }

    /// Marks the connection as degraded (internal use).
    ///
    /// Called automatically when I/O errors occur. Broadcasts state change
    /// to all subscribers.
    ///
    /// # Note
    ///
    /// This is an internal method. Users should rely on automatic state
    /// management via I/O operation results.
    #[inline]
    fn mark_degraded(&self) {
        let _ = self.state_tx.send(ConnectionState::Degraded);
    }

    /// Marks the connection as closed (internal use).
    ///
    /// Called when connection is explicitly closed. Broadcasts final state.
    #[inline]
    fn mark_closed(&self) {
        let _ = self.state_tx.send(ConnectionState::Closed);
    }

    /// Explicitly closes the connection and broadcasts Closed state.
    ///
    /// In-flight I/O observes the Closed state and returns without requiring
    /// mutable access to the public connection object.
    pub fn close(&self) {
        self.mark_closed();
    }

    /// Legacy alias for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `is_healthy()` or `state()` instead for clearer semantics.
    #[inline]
    #[deprecated(since = "0.7.0", note = "Use `is_healthy()` or `state()` instead")]
    pub fn connection_is_ok(&self) -> bool {
        self.is_healthy()
    }
}
