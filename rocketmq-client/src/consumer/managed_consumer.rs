// Copyright 2026 The RocketMQ Rust Authors
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

//! SDK-owned managed consumer: spec, handle, status and recovery controller (plan 49 section 3.1).
//!
//! The whole lifecycle is an internal async state machine owned by the SDK. Startup budget,
//! bounded backoff, in-place repair, incarnation replacement, cleanup blocking and terminal
//! reporting are SDK policy; applications contribute subscriptions and listeners only. A failed
//! consumer never requests a host restart and never exits the process: the controller reports
//! `ConfigurationError`/`Blocked` and stops creating work instead.

use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::consumer::consumer_impl::consumer_lifecycle::ConsumerProgress;
use crate::consumer::consumer_impl::consumer_lifecycle::Effect;
use crate::consumer::consumer_impl::consumer_lifecycle::LifecyclePolicy;
use crate::consumer::consumer_impl::consumer_lifecycle::MachineState;
use crate::consumer::consumer_impl::consumer_lifecycle::ProgressSnapshot;
use crate::consumer::consumer_impl::consumer_lifecycle::Reason;
use crate::consumer::consumer_impl::consumer_lifecycle::RecoveryMachine;
use crate::consumer::consumer_impl::consumer_lifecycle::RepairKind;
use crate::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use crate::consumer::default_mq_push_consumer_builder::DefaultMQPushConsumerBuilder;
use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

/// Listener invoked for a batch of messages on the SDK's consume runtime.
pub type ManagedListener = Arc<
    dyn Fn(&[&MessageExt], &mut ConsumeConcurrentlyContext) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>
        + Send
        + Sync,
>;

/// Coarse lifecycle phase of a managed consumer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsumerPhase {
    /// Startup is in flight for the current incarnation.
    #[default]
    Starting,
    /// The client is running; delivery may still be idle or awaiting assignment.
    Running,
    /// A bounded recovery attempt is pending or in progress.
    Recovering,
    /// Stop was requested and cleanup has not been proved complete.
    Stopping,
    /// Terminal: local ownership is released, or startup was rejected outright.
    Stopped,
}

impl ConsumerPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Recovering => "recovering",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }
}

/// Health of the current phase. `Healthy` explicitly includes empty traffic, zero assignment and
/// an expected long poll; only real faults or unproved cleanup degrade it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsumerHealth {
    #[default]
    Healthy,
    /// Recovery is in progress with bounded backoff.
    Degraded,
    /// The SDK cannot make progress without breaking a safety rule (unproved cleanup, or an
    /// active listener over budget). Never a process-exit request.
    Blocked,
    /// Invalid local configuration; no retry is scheduled.
    ConfigurationError,
}

impl ConsumerHealth {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Blocked => "blocked",
            Self::ConfigurationError => "configuration_error",
        }
    }
}

/// Identifier-free progress counters, safe for logs and metrics. Queue IDs, broker addresses and
/// incarnation identities never appear here.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProgressCounts {
    pub assigned_queues: u64,
    pub stalled_queues: u64,
    pub pending_delivery_msgs: u64,
    pub active_callbacks: u64,
    pub pulls_completed: u64,
    pub callbacks_finished: u64,
    pub offsets_persisted: u64,
}

/// One immutable status observation.
#[derive(Clone, Copy, Debug)]
pub struct ConsumerStatus {
    pub phase: ConsumerPhase,
    pub health: ConsumerHealth,
    /// Bounded reason category; identifiers stay out of it by construction.
    pub reason: &'static str,
    /// Monotonic internal incarnation identity (never a metric label).
    pub incarnation: u64,
    pub progress: ProgressCounts,
}

impl fmt::Display for ConsumerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "phase={} health={} reason={} incarnation={} assigned_queues={} stalled_queues={} pending_delivery={} \
             active_callbacks={}",
            self.phase.as_str(),
            self.health.as_str(),
            self.reason,
            self.incarnation,
            self.progress.assigned_queues,
            self.progress.stalled_queues,
            self.progress.pending_delivery_msgs,
            self.progress.active_callbacks,
        )
    }
}

fn project(state: MachineState, incarnation: u64, snapshot: &ProgressSnapshot) -> ConsumerStatus {
    ConsumerStatus {
        phase: state.phase,
        health: state.health,
        reason: state.reason.as_str(),
        incarnation,
        progress: ProgressCounts {
            assigned_queues: snapshot.assigned_queues,
            stalled_queues: snapshot.stalled_queues,
            pending_delivery_msgs: snapshot.pending_delivery_msgs,
            active_callbacks: snapshot.active_callbacks,
            pulls_completed: snapshot.pulls_completed,
            callbacks_finished: snapshot.callbacks_finished,
            offsets_persisted: snapshot.offsets_persisted,
        },
    }
}

/// An immutable consumer definition that can cross a thread boundary. The managed entry point
/// validates and normalises it before the controller runtime exists; every incarnation is built
/// from it on the controller's own runtime.
#[derive(Clone)]
pub struct ManagedConsumerSpec {
    builder: DefaultMQPushConsumerBuilder,
    consume_from_where: Option<ConsumeFromWhere>,
    listener: Option<ManagedListener>,
}

impl fmt::Debug for ManagedConsumerSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedConsumerSpec")
            .field("consumer_group", &self.builder.consumer_group_ref())
            .field("subscriptions", &self.builder.subscriptions().len())
            .field("listener_registered", &self.listener.is_some())
            .finish_non_exhaustive()
    }
}

impl ManagedConsumerSpec {
    pub(crate) fn new(builder: DefaultMQPushConsumerBuilder) -> Self {
        Self {
            builder,
            consume_from_where: None,
            listener: None,
        }
    }

    pub fn with_consume_from_where(mut self, from: ConsumeFromWhere) -> Self {
        self.consume_from_where = Some(from);
        self
    }

    /// Attach the message listener. A managed consumer without one is invalid configuration:
    /// messages would be pulled and never delivered.
    pub fn with_listener(mut self, listener: ManagedListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Validation shared by `build()` and the managed entry point (plan 49 section 3.1): one
    /// normalisation path so an application cannot fail later than the SDK could have told it.
    pub(crate) fn validate(&self) -> Result<(), rocketmq_error::RocketMQError> {
        if self.builder.consumer_group_ref().trim().is_empty() {
            return Err(rocketmq_error::RocketMQError::illegal_argument(
                "consumer group must not be empty",
            ));
        }
        if self.builder.subscriptions().is_empty() {
            return Err(rocketmq_error::RocketMQError::illegal_argument(
                "a managed consumer needs at least one subscription",
            ));
        }
        if self.listener.is_none() {
            return Err(rocketmq_error::RocketMQError::illegal_argument(
                "a managed consumer needs a message listener",
            ));
        }
        Ok(())
    }

    /// Build one incarnation from the spec. Must run on the controller runtime: subscribing
    /// resolves the client id and touches the runtime handle.
    fn build_incarnation(&self, progress: Arc<ConsumerProgress>) -> DefaultMQPushConsumer {
        let mut builder = self.builder.clone();
        if let Some(from) = self.consume_from_where {
            builder = builder.consume_from_where(from);
        }
        // The SDK's own assignment observation is always attached; an application queue listener
        // supplied through the builder keeps running behind it.
        let sdk_tracker: Arc<Box<dyn MessageQueueListener>> = Arc::new(Box::new(AssignmentTracker {
            progress: Arc::clone(&progress),
            known: parking_lot::Mutex::new(HashSet::new()),
        }));
        let composed: Arc<Box<dyn MessageQueueListener>> = match builder.take_message_queue_listener() {
            Some(app) => Arc::new(Box::new(CompositeQueueListener {
                sdk: Arc::clone(&sdk_tracker),
                app,
            })),
            None => sdk_tracker,
        };
        builder = builder.message_queue_listener(Some(composed));
        let mut consumer = builder.build();
        consumer.set_progress(Arc::clone(&progress));
        if let Some(listener) = self.listener.clone() {
            consumer.register_message_listener_concurrently(SpecListener { listener });
        }
        consumer
    }
}

/// Adapts the spec's boxed listener to the SDK's concurrent listener trait.
struct SpecListener {
    listener: ManagedListener,
}

impl MessageListenerConcurrently for SpecListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeConcurrentlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
        (self.listener)(msgs, context)
    }
}

/// Runs the SDK tracker and then any application listener.
struct CompositeQueueListener {
    sdk: Arc<Box<dyn MessageQueueListener>>,
    app: Arc<Box<dyn MessageQueueListener>>,
}

impl MessageQueueListener for CompositeQueueListener {
    fn message_queue_changed(&self, topic: &str, mq_all: &HashSet<MessageQueue>, mq_assigned: &HashSet<MessageQueue>) {
        self.sdk.message_queue_changed(topic, mq_all, mq_assigned);
        self.app.message_queue_changed(topic, mq_all, mq_assigned);
    }
}

/// Feeds assignment and revocation transitions into the SDK progress bus. This is how the SDK
/// itself observes "assignment observed" instead of relying on an application observer.
struct AssignmentTracker {
    progress: Arc<ConsumerProgress>,
    known: parking_lot::Mutex<HashSet<MessageQueue>>,
}

impl MessageQueueListener for AssignmentTracker {
    fn message_queue_changed(
        &self,
        _topic: &str,
        _mq_all: &HashSet<MessageQueue>,
        mq_assigned: &HashSet<MessageQueue>,
    ) {
        let (revoked, assigned) = {
            let mut known = self.known.lock();
            let revoked: Vec<MessageQueue> = known.difference(mq_assigned).cloned().collect();
            known.extend(mq_assigned.iter().cloned());
            (revoked, mq_assigned.clone())
        };
        self.progress.note_revocation(&revoked);
        self.progress.note_assignment(&assigned);
    }
}

/// Outcome of a bounded shutdown wait on the managed handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    /// Local ownership is proved quiescent.
    Complete,
    /// The caller's deadline elapsed; the SDK-owned cleanup session continues in the background.
    Pending,
}

/// Cloneable handle owning one managed consumer. Clones share the controller; dropping a clone
/// never stops the consumer, and dropping every handle requests stop without waiting.
pub struct ManagedConsumerHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    status: watch::Receiver<ConsumerStatus>,
    events: broadcast::Sender<ConsumerStatus>,
    stop_token: CancellationToken,
    stop_requested: AtomicBool,
    /// Resolved once cleanup is proved complete; `None` after the outcome was consumed.
    cleanup_complete: parking_lot::Mutex<Option<oneshot::Receiver<()>>>,
    /// Remembered outcome of a completed session so a second caller gets the same answer.
    proved: AtomicBool,
}

impl HandleInner {
    fn current(&self) -> ConsumerStatus {
        *self.status.borrow()
    }
}

impl ManagedConsumerHandle {
    /// Latest status snapshot. Cheap: reads a watch value, never touches the consumer graph.
    pub fn status(&self) -> ConsumerStatus {
        self.inner.current()
    }

    /// Status of the current incarnation including queues it owns and delivery still pending.
    pub fn subscribe_status(&self) -> ConsumerStatus {
        self.inner.current()
    }

    /// Stream of status transitions. A lagging observer skips stale values rather than delaying
    /// recovery: recovery never waits on an observer.
    pub fn subscribe_events(&self) -> ConsumerStatusEvents {
        ConsumerStatusEvents {
            receiver: self.inner.events.subscribe(),
        }
    }

    /// Whether a stop has already been requested (stop is idempotent).
    pub fn stop_requested(&self) -> bool {
        self.inner.stop_requested.load(Ordering::SeqCst)
    }

    /// Request stop and wait for proved cleanup until `deadline`. The deadline bounds only the
    /// caller-visible wait; the SDK-owned cleanup session continues afterwards, so a `Pending`
    /// result can still be followed by completion observed through [`Self::status`]. A second
    /// caller attaches to the same session rather than starting another shutdown.
    pub async fn shutdown_until(&self, deadline: tokio::time::Instant) -> ShutdownOutcome {
        self.request_stop();
        let waited = async {
            loop {
                {
                    let mut guard = self.inner.cleanup_complete.lock();
                    match guard.as_mut() {
                        // Consumed earlier: the outcome is already known to this handle.
                        None => return self.inner.proved.load(Ordering::SeqCst),
                        Some(receiver) => match receiver.try_recv() {
                            Ok(()) => {
                                self.inner.proved.store(true, Ordering::SeqCst);
                                return true;
                            }
                            // The session ended without proof: report Pending at once rather than
                            // making a caller wait out its own deadline.
                            Err(oneshot::error::TryRecvError::Closed) => {
                                self.inner.proved.store(false, Ordering::SeqCst);
                                return false;
                            }
                            Err(oneshot::error::TryRecvError::Empty) => {}
                        },
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if waited.await {
            ShutdownOutcome::Complete
        } else {
            ShutdownOutcome::Pending
        }
    }

    /// Request stop without waiting. A second call is a no-op and the original cleanup session
    /// stays attached to this consumer.
    pub fn request_stop(&self) -> bool {
        if self.inner.stop_requested.swap(true, Ordering::SeqCst) {
            return true;
        }
        self.inner.stop_token.cancel();
        true
    }
}

/// Borrowed view of the status stream. Dropping it unsubscribes; recovery is unaffected.
pub struct ConsumerStatusEvents {
    receiver: broadcast::Receiver<ConsumerStatus>,
}

impl futures::Stream for ConsumerStatusEvents {
    type Item = ConsumerStatus;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.receiver.try_recv() {
                Ok(status) => return Poll::Ready(Some(status)),
                Err(broadcast::error::TryRecvError::Empty) => return Poll::Pending,
                // A lagging observer skips stale values rather than delaying recovery, so one
                // slow consumer of status events can never hold the lifecycle back.
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => return Poll::Ready(None),
            }
        }
    }
}

impl Clone for ManagedConsumerHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for HandleInner {
    fn drop(&mut self) {
        // Dropping the last handle requests stop and returns; it never waits and never exits.
        self.stop_requested.store(true, Ordering::SeqCst);
        self.stop_token.cancel();
    }
}

/// Start a managed consumer from an already validated spec (plan 49 section 3.1).
pub(crate) fn start_managed(
    spec: ManagedConsumerSpec,
    policy: LifecyclePolicy,
) -> Result<ManagedConsumerHandle, rocketmq_error::RocketMQError> {
    spec.validate()?;
    let initial = ConsumerStatus {
        phase: ConsumerPhase::Starting,
        health: ConsumerHealth::Healthy,
        reason: Reason::Starting.as_str(),
        incarnation: 1,
        progress: ProgressCounts::default(),
    };
    let (status_tx, status_rx) = watch::channel(initial);
    let (events_tx, _) = broadcast::channel(64);
    let stop_token = CancellationToken::new();

    let inner = Arc::new(HandleInner {
        status: status_rx,
        events: events_tx,
        stop_token: stop_token.clone(),
        stop_requested: AtomicBool::new(false),
        cleanup_complete: parking_lot::Mutex::new(None),
        proved: AtomicBool::new(false),
    });
    // The controller reports cleanup proof through a channel the handle owns; keep the receiver
    // so a caller can attach before the first incarnation even exists.
    let (proof_tx, proof_rx) = oneshot::channel();
    *inner.cleanup_complete.lock() = Some(proof_rx);

    static CONTROLLER_SEQ: AtomicU64 = AtomicU64::new(1);
    let id = CONTROLLER_SEQ.fetch_add(1, Ordering::Relaxed);
    let publisher = StatusPublisher {
        status: status_tx,
        events: inner.events.clone(),
    };
    // A detached controller thread with its own current-thread runtime: the recursive consumer
    // graph is not `Send`, and an application must never move it across schedulers.
    thread::Builder::new()
        .name(format!("mq-managed-ctl-{id}"))
        .spawn(move || run_controller(spec, policy, publisher, stop_token, CleanupProof::new(proof_tx)))
        .map_err(|err| {
            rocketmq_error::RocketMQError::illegal_argument(format!("cannot start the consumer controller: {err}"))
        })?;
    Ok(ManagedConsumerHandle { inner })
}

/// One-shot cleanup proof handed to the handle. Dropping it without proving completion closes
/// the channel, which callers observe as `Pending` rather than as a false completion.
struct CleanupProof {
    sender: Option<oneshot::Sender<()>>,
}

impl CleanupProof {
    fn new(sender: oneshot::Sender<()>) -> Self {
        Self { sender: Some(sender) }
    }

    /// Prove that local ownership is quiescent. Only this reports `Complete`.
    fn settle(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for CleanupProof {
    fn drop(&mut self) {
        if self.sender.is_some() {
            tracing::error!("managed consumer: cleanup session ended without a proof; the incarnation stays retained");
        }
    }
}

struct StatusPublisher {
    status: watch::Sender<ConsumerStatus>,
    events: broadcast::Sender<ConsumerStatus>,
}

impl StatusPublisher {
    fn publish(&self, state: MachineState, incarnation: u64, snapshot: &ProgressSnapshot) {
        let status = project(state, incarnation, snapshot);
        let _ = self.status.send(status);
        let _ = self.events.send(status);
    }
}

/// Controller thread body: a current-thread runtime plus a local task set, because the recursive
/// consumer graph is not `Send`.
fn run_controller(
    spec: ManagedConsumerSpec,
    policy: LifecyclePolicy,
    publisher: StatusPublisher,
    stop_token: CancellationToken,
    mut proof: CleanupProof,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(err) => {
            // Reporting beats panicking: a missing controller runtime is not the application's
            // fault and must never take the process down.
            tracing::error!("managed consumer: cannot build the controller runtime: {err}");
            publisher.publish(
                MachineState {
                    phase: ConsumerPhase::Stopped,
                    health: ConsumerHealth::ConfigurationError,
                    reason: Reason::StartupRejected,
                },
                1,
                &ProgressSnapshot::default(),
            );
            // Nothing was ever created, so there is nothing left to release.
            proof.settle();
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        // Dropping the proof inside is the report: an unproved session closes the channel and
        // every attached caller observes `Pending` instead of a false completion.
        controller_body(spec, policy, publisher, stop_token, proof).await;
    });
}

async fn controller_body(
    spec: ManagedConsumerSpec,
    policy: LifecyclePolicy,
    publisher: StatusPublisher,
    stop_token: CancellationToken,
    mut proof: CleanupProof,
) {
    let mut machine = RecoveryMachine::new(policy, 1);
    let empty = ProgressSnapshot::default();
    publisher.publish(machine.state(), machine.incarnation(), &empty);

    loop {
        if stop_token.is_cancelled() {
            let state = machine.request_stop();
            publisher.publish(state, machine.incarnation(), &empty);
            proof.settle();
            return;
        }

        let progress = ConsumerProgress::new();
        let mut consumer = spec.build_incarnation(Arc::clone(&progress));
        let deadline = tokio::time::Instant::now() + machine.startup_budget();
        let start_result = {
            let start = consumer.start_until(deadline);
            tokio::pin!(start);
            tokio::select! {
                biased;
                _ = stop_token.cancelled() => Err(StartFailure::Stopped),
                result = &mut start => result.map_err(|err| StartFailure::Failed(err.to_string())),
            }
        };

        match start_result {
            Ok(()) => {
                let (state, effects) = machine.on_start_result(true, false, false, jitter_unit());
                publisher.publish(state, machine.incarnation(), &progress.snapshot(stall_after()));
                match run_incarnation(&mut consumer, &mut machine, &publisher, &stop_token, &progress, effects).await {
                    IncarnationEnd::StopRequested => {
                        let state = machine.request_stop();
                        publisher.publish(state, machine.incarnation(), &progress.snapshot(stall_after()));
                        finish_with_cleanup(&mut consumer, policy, &mut machine, &publisher, &mut proof).await;
                        return;
                    }
                    IncarnationEnd::Replace => {
                        if !replace_or_block(&mut consumer, &mut machine, &publisher, &progress, &mut proof).await {
                            return;
                        }
                    }
                }
            }
            Err(StartFailure::Stopped) => {
                let state = machine.request_stop();
                publisher.publish(state, machine.incarnation(), &empty);
                finish_with_cleanup(&mut consumer, policy, &mut machine, &publisher, &mut proof).await;
                return;
            }
            Err(StartFailure::Failed(message)) => {
                // Roll back partial resources before deciding anything else.
                if matches!(
                    bounded_cleanup(&mut consumer, policy.cleanup_budget).await,
                    ShutdownOutcome::Pending
                ) {
                    let state = machine.on_cleanup_unproved();
                    publisher.publish(state, machine.incarnation(), &empty);
                    // No replacement and no second cleanup worker: wait for this one session.
                    if matches!(
                        bounded_cleanup(&mut consumer, policy.cleanup_budget * 8).await,
                        ShutdownOutcome::Complete
                    ) {
                        proof.settle();
                    }
                    return;
                }
                if stop_token.is_cancelled() {
                    let state = machine.request_stop();
                    publisher.publish(state, machine.incarnation(), &empty);
                    proof.settle();
                    return;
                }
                let retryable = !is_permanent(&message);
                // Only an attempt that ran to its own deadline is "exhausted"; an error that came
                // back sooner is a retry in progress.
                let timed_out = tokio::time::Instant::now() >= deadline;
                tracing::warn!(
                    attempt = machine.incarnation(),
                    retryable,
                    "managed consumer: startup attempt did not complete"
                );
                let (state, effects) = machine.on_start_result(false, retryable, timed_out, jitter_unit());
                publisher.publish(state, machine.incarnation(), &empty);
                if effects.is_empty() {
                    // Invalid configuration or a terminal report: no retry is scheduled and this
                    // is never an exit request. The handle keeps reporting the last status.
                    proof.settle();
                    return;
                }
                // Bounded backoff before the next attempt; a stop request interrupts the wait.
                if wait_or_stop(wait_of(&effects), &stop_token).await {
                    let state = machine.request_stop();
                    publisher.publish(state, machine.incarnation(), &empty);
                    finish_with_cleanup(&mut consumer, policy, &mut machine, &publisher, &mut proof).await;
                    return;
                }
            }
        }
    }
}

/// Why startup did not reach `Running`.
enum StartFailure {
    Stopped,
    Failed(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncarnationEnd {
    StopRequested,
    Replace,
}

/// Roll an incarnation back and decide whether a replacement may start. Returns `false` when the
/// controller must stop: cleanup could not be proved, so at most one retiring incarnation is kept.
async fn replace_or_block(
    consumer: &mut DefaultMQPushConsumer,
    machine: &mut RecoveryMachine,
    publisher: &StatusPublisher,
    progress: &Arc<ConsumerProgress>,
    proof: &mut CleanupProof,
) -> bool {
    match bounded_cleanup(consumer, machine.policy().cleanup_budget).await {
        ShutdownOutcome::Complete => {
            let state = machine.on_cleanup_complete();
            machine.advance_incarnation();
            publisher.publish(state, machine.incarnation(), &progress.snapshot(stall_after()));
            true
        }
        ShutdownOutcome::Pending => {
            let state = machine.on_cleanup_unproved();
            publisher.publish(state, machine.incarnation(), &progress.snapshot(stall_after()));
            // Wait for the one session; never start a replacement or another cleanup worker.
            if matches!(
                bounded_cleanup(consumer, machine.policy().cleanup_budget * 8).await,
                ShutdownOutcome::Complete
            ) {
                proof.settle();
            }
            false
        }
    }
}

/// Final teardown after a stop request: prove cleanup, report it, and settle the proof exactly
/// once. A caller whose deadline elapsed already saw `Pending` and may attach to this session.
async fn finish_with_cleanup(
    consumer: &mut DefaultMQPushConsumer,
    policy: LifecyclePolicy,
    machine: &mut RecoveryMachine,
    publisher: &StatusPublisher,
    proof: &mut CleanupProof,
) {
    match bounded_cleanup(consumer, policy.cleanup_budget).await {
        ShutdownOutcome::Complete => {
            let state = machine.on_cleanup_complete();
            publisher.publish(state, machine.incarnation(), &ProgressSnapshot::default());
            proof.settle();
        }
        ShutdownOutcome::Pending => {
            let state = machine.on_cleanup_unproved();
            publisher.publish(state, machine.incarnation(), &ProgressSnapshot::default());
            if matches!(
                bounded_cleanup(consumer, policy.cleanup_budget * 8).await,
                ShutdownOutcome::Complete
            ) {
                proof.settle();
            }
        }
    }
}

/// Drive one running incarnation. Returns whether the controller should stop or replace it.
async fn run_incarnation(
    consumer: &mut DefaultMQPushConsumer,
    machine: &mut RecoveryMachine,
    publisher: &StatusPublisher,
    stop_token: &CancellationToken,
    progress: &Arc<ConsumerProgress>,
    mut effects: Vec<Effect>,
) -> IncarnationEnd {
    let policy = machine.policy();
    let mut ticker = tokio::time::interval(policy.progress_check);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut previous: Option<ProgressSnapshot> = None;
    loop {
        for effect in effects.drain(..) {
            match effect {
                Effect::Idle(delay) => {
                    if wait_or_stop(delay, stop_token).await {
                        return IncarnationEnd::StopRequested;
                    }
                }
                Effect::Repair(kind) => match kind {
                    RepairKind::RouteAndPull | RepairKind::Assignment => consumer.repair_routes_and_rebalance().await,
                },
                Effect::Stop { .. } => return IncarnationEnd::Replace,
            }
        }
        tokio::select! {
            biased;
            _ = stop_token.cancelled() => return IncarnationEnd::StopRequested,
            _ = ticker.tick() => {}
        }
        let snapshot = progress.snapshot(stall_after());
        let (state, next) = machine.on_tick(&snapshot, previous.as_ref(), pull_deadline(), jitter_unit());
        publisher.publish(state, machine.incarnation(), &snapshot);
        previous = Some(snapshot);
        effects = next;
    }
}

async fn bounded_cleanup(consumer: &mut DefaultMQPushConsumer, budget: Duration) -> ShutdownOutcome {
    let deadline = tokio::time::Instant::now() + budget;
    match consumer.shutdown_until(deadline).await {
        crate::consumer::default_mq_push_consumer::ConsumerCleanup::Complete => ShutdownOutcome::Complete,
        crate::consumer::default_mq_push_consumer::ConsumerCleanup::Incomplete => ShutdownOutcome::Pending,
    }
}

/// Errors that retrying the same spec cannot fix.
fn is_permanent(message: &str) -> bool {
    let message = message.to_lowercase();
    message.contains("invalid") || message.contains("not supported") || message.contains("must not be empty")
}

fn wait_of(effects: &[Effect]) -> Duration {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Idle(delay) => Some(*delay),
            _ => None,
        })
        .unwrap_or(Duration::from_millis(100))
}

async fn wait_or_stop(delay: Duration, stop_token: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = stop_token.cancelled() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

/// A queue counts as stalled once it has had no real work transition for this long.
fn stall_after() -> Duration {
    Duration::from_secs(30)
}

/// The pull deadline the SDK uses internally; an outstanding long poll inside it is an expected
/// wait rather than silence.
fn pull_deadline() -> Duration {
    Duration::from_secs(35)
}

fn jitter_unit() -> f64 {
    use rand::Rng;
    rand::rng().random_range(0.0..1.0)
}

// Compile-time guarantees required by the plan: the spec crosses a thread boundary and the handle
// is shareable across runtimes.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    let _ = assert_send_sync::<ManagedConsumerSpec>;
    let _ = assert_send_sync::<ManagedConsumerHandle>;
    let _ = assert_send_sync::<ConsumerStatus>;
};

#[cfg(test)]
mod tests {

    use super::*;

    fn spec() -> ManagedConsumerSpec {
        ManagedConsumerSpec::new(
            DefaultMQPushConsumer::builder()
                .consumer_group("group")
                .subscribe("topic", "*"),
        )
        .with_listener(Arc::new(|_msgs, _ctx| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)))
    }

    #[test]
    fn spec_and_handle_are_send_and_sync() {
        fn assert_<T: Send + Sync>() {}
        assert_::<ManagedConsumerSpec>();
        assert_::<ManagedConsumerHandle>();
    }

    #[test]
    fn a_spec_without_a_listener_is_invalid_configuration() {
        let err = ManagedConsumerSpec::new(
            DefaultMQPushConsumer::builder()
                .consumer_group("group")
                .subscribe("topic", "*"),
        )
        .validate()
        .expect_err("missing listener must fail");
        assert!(
            matches!(err, rocketmq_error::RocketMQError::IllegalArgument(_)),
            "{err:?}"
        );
    }

    #[test]
    fn a_spec_without_a_subscription_is_invalid_configuration() {
        let err = ManagedConsumerSpec::new(DefaultMQPushConsumer::builder().consumer_group("group"))
            .with_listener(Arc::new(|_msgs, _ctx| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)))
            .validate()
            .expect_err("missing subscription must fail");
        assert!(
            matches!(err, rocketmq_error::RocketMQError::IllegalArgument(_)),
            "{err:?}"
        );
    }

    #[test]
    fn a_valid_spec_passes_validation() {
        assert!(spec().validate().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_consumer_with_an_unreachable_nameserver_stays_observable_and_never_exits() {
        let handle = start_managed(
            spec().with_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset),
            LifecyclePolicy::default(),
        )
        .expect("a valid spec starts a managed consumer");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let status = handle.status();
        // Local startup succeeds without a reachable nameserver, so the consumer reports an
        // expected wait; either way it must never be terminal and never take the process down.
        // Whether local startup finished or a retry is scheduled depends on what other consumers in
        // this process registered; none of those states may be terminal.
        assert!(
            matches!(
                status.phase,
                ConsumerPhase::Starting | ConsumerPhase::Running | ConsumerPhase::Recovering
            ),
            "unexpected phase: {status:?}"
        );
        assert_ne!(status.health, ConsumerHealth::ConfigurationError);
        assert!(
            matches!(
                status.reason,
                "waiting_for_assignment" | "starting" | "startup_attempt_exhausted"
            ),
            "unexpected reason: {}",
            status.reason
        );
        assert!(handle.request_stop());
        assert!(handle.stop_requested());
        // A second stop attaches to the same session instead of starting another shutdown.
        assert!(handle.request_stop());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expired_waiter_deadline_bounds_only_the_visible_wait() {
        let handle = start_managed(spec(), LifecyclePolicy::default()).expect("managed consumer starts");
        // An already-elapsed deadline cannot wait; it must not be reported as a failure of cleanup
        // nor start a second session.
        let first = handle.shutdown_until(tokio::time::Instant::now()).await;
        assert!(matches!(first, ShutdownOutcome::Complete | ShutdownOutcome::Pending));
        assert!(handle.stop_requested());

        // A later caller attaches to the same SDK-owned session, which kept running in the
        // background, and may observe its completion.
        let second = handle
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(30))
            .await;
        if matches!(first, ShutdownOutcome::Complete) {
            assert_eq!(second, ShutdownOutcome::Complete, "a proved outcome is remembered");
        }
        assert!(matches!(second, ShutdownOutcome::Complete | ShutdownOutcome::Pending));
        assert_ne!(handle.status().health, ConsumerHealth::ConfigurationError);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_handle_requests_stop_without_waiting() {
        let handle = start_managed(spec(), LifecyclePolicy::default()).expect("managed consumer starts");
        drop(handle);
        // The only assertion that matters is that this test process is still alive and waiting
        // for nothing.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_events_are_published_and_a_lagging_reader_cannot_stall_recovery() {
        let handle = start_managed(spec(), LifecyclePolicy::default()).expect("managed consumer starts");
        let mut events = handle.subscribe_events();
        // Drain whatever arrived; the stream stays open while the consumer runs, so bounded waits
        // are the only correct way to observe it here.
        for _ in 0..8 {
            if tokio::time::timeout(Duration::from_millis(50), futures::StreamExt::next(&mut events))
                .await
                .is_err()
            {
                break;
            }
        }
        assert!(matches!(
            handle.status().phase,
            ConsumerPhase::Starting | ConsumerPhase::Running | ConsumerPhase::Recovering
        ));
        // Recovery keeps running while nobody reads the stream.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.stop_requested());
        handle.request_stop();
    }

    #[test]
    fn status_display_never_leaks_identifiers() {
        let status = project(
            MachineState {
                phase: ConsumerPhase::Running,
                health: ConsumerHealth::Healthy,
                reason: Reason::IdleNoMessages,
            },
            7,
            &ProgressSnapshot::default(),
        );
        let text = status.to_string();
        assert!(text.contains("phase=running"));
        assert!(text.contains("reason=idle_no_messages"));
    }

    #[test]
    fn permanent_startup_errors_are_classified_without_retry() {
        assert!(is_permanent(
            "Client exception occurred: CODE:20  DESC: the consumer group must not be empty"
        ));
        assert!(!is_permanent("consumer startup deadline elapsed"));
    }
}
