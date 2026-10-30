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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use std::sync::Weak;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::ReusableBoxFuture;

use crate::consumer::consumer_impl::consumer_lifecycle::ConsumerProgress;
use crate::consumer::consumer_impl::consumer_lifecycle::Effect;
use crate::consumer::consumer_impl::consumer_lifecycle::LifecyclePolicy;
use crate::consumer::consumer_impl::consumer_lifecycle::MachineState;
use crate::consumer::consumer_impl::consumer_lifecycle::ProgressSnapshot;
use crate::consumer::consumer_impl::consumer_lifecycle::Reason;
use crate::consumer::consumer_impl::consumer_lifecycle::RecoveryMachine;
use crate::consumer::consumer_impl::consumer_lifecycle::RepairKind;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::check_consumer_identity;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::check_subscription;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::validate_consumer_config;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::ListenerPresence;
use crate::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use crate::consumer::default_mq_push_consumer::LostPullRepairPass;
use crate::consumer::default_mq_push_consumer::RouteRepairPass;
use crate::consumer::default_mq_push_consumer_builder::DefaultMQPushConsumerBuilder;
use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use crate::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQError;

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

/// Health of the current phase. `Healthy` explicitly includes empty traffic, an assignment that was
/// observed as empty against a known route, and an expected long poll; only real faults or unproved
/// cleanup degrade it. A zero assignment with no route behind it is a failed assignment refresh,
/// which is a fault and reports `Degraded`.
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
    /// Releases that exceeded outstanding work. Non-zero means an ownership pairing is broken, so
    /// the delivery counts above cannot be trusted until it is understood (plan 49c C4).
    pub accounting_underflows: u64,
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
             active_callbacks={} accounting_underflows={}",
            self.phase.as_str(),
            self.health.as_str(),
            self.reason,
            self.incarnation,
            self.progress.assigned_queues,
            self.progress.stalled_queues,
            self.progress.pending_delivery_msgs,
            self.progress.active_callbacks,
            self.progress.accounting_underflows,
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
            accounting_underflows: snapshot.accounting_underflows,
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

    /// Side-effect-free validation run before the controller, its runtime or any client instance
    /// exists (plan 50 F3): identity and subscription rules are the ones `start` applies later, so
    /// a managed consumer cannot fail later than the SDK could have told it. Every failure is
    /// an `IllegalArgument`, which is how the controller recognises "never retry" by type.
    pub(crate) fn validate(&self) -> Result<(), rocketmq_error::RocketMQError> {
        // Tagged as configuration so the controller can classify it by type (plan 50 F3).
        let rejected =
            |err: rocketmq_error::RocketMQError| rocketmq_error::RocketMQError::configuration(err.to_string());
        if self.listener.is_none() {
            return Err(rocketmq_error::RocketMQError::configuration(
                "a managed consumer needs a message listener",
            ));
        }
        let group = self.builder.effective_group();
        check_consumer_identity(group.as_str()).map_err(rejected)?;
        let subscriptions = self.builder.subscriptions();
        if subscriptions.is_empty() {
            return Err(rocketmq_error::RocketMQError::configuration(
                "a managed consumer needs at least one subscription",
            ));
        }
        for (topic, expression) in &subscriptions {
            check_subscription(topic.as_str(), expression.as_str()).map_err(rejected)?;
        }
        // Thread bounds, batch sizes, flow-control thresholds and POP limits belong to the same
        // promise. They used to be decided only inside `check_config`, so a spec with
        // consume_thread_min = 0 passed here, built a controller plus incarnation graph,
        // and failed afterwards: reported as permanent, but never rejected synchronously as
        // Plan 49a requires (plan 49b R5). The managed listener is attached to the object
        // after construction, so presence is asserted above rather than read from config.
        let config = self.builder.clone().normalized_consumer_config();
        validate_consumer_config(&config, ListenerPresence::Provided).map_err(rejected)?;
        Ok(())
    }

    /// Build one incarnation from the spec. Must run on the controller runtime: subscribing
    /// resolves the client id and touches the runtime handle.
    fn build_incarnation(
        &self,
        progress: Arc<ConsumerProgress>,
        policy: &LifecyclePolicy,
    ) -> Result<DefaultMQPushConsumer, rocketmq_error::RocketMQError> {
        let mut builder = self.builder.clone();
        if let Some(from) = self.consume_from_where {
            builder = builder.consume_from_where(from);
        }
        // No assignment listener is injected to establish ownership. A queue's acquisition identity
        // begins when its process queue is installed and ends when it leaves the table, both inside
        // rebalance; an application listener - if any - only observes (plan 49c C1). Routing
        // ownership through this callback made it wait for a heartbeat, so a first pull scheduled
        // 500 ms after installation could arrive before its owner existed.
        // Subscriptions are detached from the builder and applied here: `build()` would apply them
        // through an `expect`, and a configuration problem on the managed path has to come back as
        // a reportable startup failure instead of a panic (plan 50 F3).
        let subscriptions = builder.take_subscriptions();
        let mut consumer = builder.build();
        // Cleanup finalization is sized by the lifecycle policy this controller runs under, never
        // by how long an observer happens to wait (plan 49b R2).
        consumer.mark_managed_lifecycle();
        consumer.set_cleanup_session_ceiling(policy.cleanup_session_ceiling());
        // The remote half of cleanup is policy-sized too: what bulk persistence may take once local
        // work drained, and what unregistration keeps back before it starts (plan 49c C5).
        consumer.set_remote_budgets(policy.remote_persist_opportunity, policy.finalization_reserve);
        consumer.set_progress(Arc::clone(&progress));
        if let Some(listener) = self.listener.clone() {
            consumer.register_message_listener_concurrently(SpecListener { listener });
        }
        for (topic, expression) in &subscriptions {
            consumer.subscribe(topic.as_str(), expression.as_str()).map_err(|err| {
                rocketmq_error::RocketMQError::configuration(format!(
                    "subscription {topic} ({expression}) is invalid: {err}"
                ))
            })?;
        }
        Ok(consumer)
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

/// Outcome of a bounded shutdown wait on the managed handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    /// Local ownership is proved quiescent.
    Complete,
    /// Cleanup was not proved by the time this caller stopped waiting - its deadline elapsed, or
    /// the session ended without a proof. The SDK keeps ownership of the cleanup session; a pending
    /// answer is never a licence to start a second owner, and it is never a process-exit reason.
    Pending,
}

/// Cloneable handle owning one managed consumer. Clones share the controller; dropping a clone
/// never stops the consumer, and dropping every handle requests stop without waiting.
pub struct ManagedConsumerHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    status: watch::Receiver<ConsumerStatus>,
    /// Weak on purpose: the controller alone owns a strong sender, so neither a remaining handle
    /// nor an observer can keep the event channel open and prevent terminal closure (plan 49a
    /// F9).
    events: Weak<broadcast::Sender<ConsumerStatus>>,
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

    /// Live status subscription. The receiver starts at the current value and `changed()` resolves
    /// on every later transition, so an observer never has to poll for a state it could have
    /// been told about (plan 49a F9). Reading the latest value stays available through
    /// [`Self::status`].
    pub fn subscribe_status(&self) -> watch::Receiver<ConsumerStatus> {
        self.inner.status.clone()
    }

    /// Stream of status transitions. A lagging observer skips stale values rather than delaying
    /// recovery: recovery never waits on an observer. Creating one borrows nothing that extends the
    /// consumer's life, and it closes once the controller has exited.
    pub fn subscribe_events(&self) -> ConsumerStatusEvents {
        match self.inner.events.upgrade() {
            Some(sender) => ConsumerStatusEvents::new(sender.subscribe()),
            None => {
                // The controller already finished: hand back a receiver whose sender is dropped, so
                // the stream reports terminal closure instead of waiting for a
                // publisher that is gone.
                let (gone, receiver) = broadcast::channel::<ConsumerStatus>(1);
                drop(gone);
                ConsumerStatusEvents::new(receiver)
            }
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
                            // Consume the channel on either terminal answer. A consumed oneshot
                            // answers neither Empty nor Closed reliably, so leaving it in place
                            // would make the next caller re-poll a dead channel: it could wait out
                            // a deadline that has already been answered, or report Pending for a
                            // cleanup this very handle already proved.
                            Ok(()) => {
                                *guard = None;
                                self.inner.proved.store(true, Ordering::SeqCst);
                                return true;
                            }
                            // The session ended without proof: report Pending at once rather than
                            // making a caller wait out its own deadline.
                            Err(oneshot::error::TryRecvError::Closed) => {
                                *guard = None;
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
    /// One retained receive step, reused after each item: `Pending` registers the caller's waker
    /// with the broadcast channel instead of returning Pending without one and never waking
    /// again (plan 49a F9). Reusing a boxed future keeps this allocation-free after
    /// construction.
    next: ReusableBoxFuture<
        'static,
        (
            Result<ConsumerStatus, broadcast::error::RecvError>,
            broadcast::Receiver<ConsumerStatus>,
        ),
    >,
}

async fn receive_status(
    mut receiver: broadcast::Receiver<ConsumerStatus>,
) -> (
    Result<ConsumerStatus, broadcast::error::RecvError>,
    broadcast::Receiver<ConsumerStatus>,
) {
    let result = receiver.recv().await;
    (result, receiver)
}

impl ConsumerStatusEvents {
    fn new(receiver: broadcast::Receiver<ConsumerStatus>) -> Self {
        Self {
            next: ReusableBoxFuture::new(receive_status(receiver)),
        }
    }
}

impl futures::Stream for ConsumerStatusEvents {
    type Item = ConsumerStatus;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let (result, receiver) = std::task::ready!(futures::future::FutureExt::poll_unpin(&mut this.next, cx));
            // Put the receiver back before deciding: a lagged answer still has to be polled again.
            this.next.set(receive_status(receiver));
            match result {
                Ok(status) => return Poll::Ready(Some(status)),
                // A lagging observer skips stale values rather than delaying recovery, so one
                // slow consumer of status events can never hold the lifecycle back.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return Poll::Ready(None),
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
    // The publisher below holds the only strong sender; handles see it weakly.
    let events_tx = Arc::new(broadcast::channel::<ConsumerStatus>(64).0);
    let stop_token = CancellationToken::new();

    let inner = Arc::new(HandleInner {
        status: status_rx,
        events: Arc::downgrade(&events_tx),
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
        events: events_tx,
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
    /// Strong owner of the event channel. When the controller returns, receivers observe closure; a
    /// handle or observer cannot manufacture that publisher's lifetime (plan 49a F9).
    events: Arc<broadcast::Sender<ConsumerStatus>>,
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
        let mut consumer = match spec.build_incarnation(Arc::clone(&progress), &policy) {
            Ok(consumer) => consumer,
            // Building the incarnation failed on configuration grounds. Nothing was started, so
            // there is nothing to clean up; report it as a permanent startup failure.
            Err(err) => {
                // Terminal report: the machine records a configuration rejection and schedules
                // nothing, exactly as it does for a permanent startup failure.
                let (state, effects) = machine.on_start_result(false, false, false, jitter_unit());
                debug_assert!(effects.is_empty(), "a rejected build must not schedule a retry");
                publisher.publish(state, machine.incarnation(), &empty);
                tracing::error!(error = %err, "managed consumer: incarnation could not be built");
                proof.settle();
                return;
            }
        };
        let deadline = tokio::time::Instant::now() + machine.startup_budget();
        let start_result = {
            let start = consumer.start_until(deadline);
            tokio::pin!(start);
            tokio::select! {
                biased;
                _ = stop_token.cancelled() => Err(StartFailure::Stopped),
                result = &mut start => result.map_err(StartFailure::Failed),
            }
        };

        match start_result {
            Ok(()) => {
                let (state, effects) = machine.on_start_result(true, false, false, jitter_unit());
                publisher.publish(
                    state,
                    machine.incarnation(),
                    &progress.snapshot(stall_after(), pull_deadline()),
                );
                match run_incarnation(&mut consumer, &mut machine, &publisher, &stop_token, &progress, effects).await {
                    IncarnationEnd::StopRequested => {
                        let state = machine.request_stop();
                        publisher.publish(
                            state,
                            machine.incarnation(),
                            &progress.snapshot(stall_after(), pull_deadline()),
                        );
                        // The terminal `stopped` state and the handle's proof belong to the
                        // requested stop. An unproved pass keeps both
                        // pending and stays owned: waiting for it is
                        // never a reason to exit (plan 49b R2).
                        if !matches!(
                            observe_cleanup(&mut consumer, &mut machine, &publisher, &stop_token, Some(&progress))
                                .await,
                            CleanupSettled::Proved
                        ) {
                            await_cleanup_proof(&mut consumer, &mut machine, &publisher, &stop_token, Some(&progress))
                                .await;
                        }
                        proof.settle();
                        return;
                    }
                    IncarnationEnd::Replace => {
                        // Cleanup could not be proved yet: at most one retiring incarnation is
                        // kept, no replacement starts, and the controller
                        // keeps observing instead of exiting.
                        if !matches!(
                            observe_cleanup(&mut consumer, &mut machine, &publisher, &stop_token, Some(&progress))
                                .await,
                            CleanupSettled::Proved
                        ) {
                            await_cleanup_proof(&mut consumer, &mut machine, &publisher, &stop_token, Some(&progress))
                                .await;
                        }
                        if stop_token.is_cancelled() {
                            // A stop raised while the old incarnation was being released wins over
                            // a replacement, and observe_cleanup
                            // already published the terminal state.
                            proof.settle();
                            return;
                        }
                        machine.advance_incarnation();
                        publisher.publish(
                            machine.state(),
                            machine.incarnation(),
                            &progress.snapshot(stall_after(), pull_deadline()),
                        );
                    }
                }
            }
            Err(StartFailure::Stopped) => {
                let state = machine.request_stop();
                publisher.publish(state, machine.incarnation(), &empty);
                if !matches!(
                    observe_cleanup(&mut consumer, &mut machine, &publisher, &stop_token, None).await,
                    CleanupSettled::Proved
                ) {
                    await_cleanup_proof(&mut consumer, &mut machine, &publisher, &stop_token, None).await;
                }
                proof.settle();
                return;
            }
            Err(StartFailure::Failed(error)) => {
                // Roll back partial resources before deciding anything else. An unproved rollback
                // keeps its session owned and observable: no replacement, no second cleanup worker,
                // and never an exit.
                if !matches!(
                    observe_cleanup(&mut consumer, &mut machine, &publisher, &stop_token, None).await,
                    CleanupSettled::Proved
                ) {
                    await_cleanup_proof(&mut consumer, &mut machine, &publisher, &stop_token, None).await;
                }
                if stop_token.is_cancelled() {
                    // The requested stop already won: observe_cleanup published `stopped`, so this
                    // settles the handle without turning a completed stop back into `stopping`.
                    proof.settle();
                    return;
                }
                let retryable = !is_permanent(&error);
                // Only an attempt that ran to its own deadline is "exhausted"; an error that came
                // back sooner is a retry in progress.
                let timed_out = tokio::time::Instant::now() >= deadline;
                tracing::warn!(
                    attempt = machine.incarnation(),
                    retryable,
                    error = %error,
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
                    if !matches!(
                        observe_cleanup(&mut consumer, &mut machine, &publisher, &stop_token, None).await,
                        CleanupSettled::Proved
                    ) {
                        await_cleanup_proof(&mut consumer, &mut machine, &publisher, &stop_token, None).await;
                    }
                    proof.settle();
                    return;
                }
            }
        }
    }
}

/// Why startup did not reach `Running`.
enum StartFailure {
    Stopped,
    /// The typed failure, kept as raised by the SDK so permanence is decided by error type and not
    /// by matching message text (plan 50 F3).
    Failed(RocketMQError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncarnationEnd {
    StopRequested,
    Replace,
}

/// What waiting for the one cleanup session produced.
enum CleanupSettled {
    /// Local ownership is quiescent; a replacement or a terminal stop may proceed.
    Proved,
    /// The pass ended without proving cleanup. Ownership stays retained and observable, which
    /// blocks a guarded replacement and is never an exit request.
    Unproved(&'static str),
}

/// Observe the consumer's one cleanup session until it settles, publishing status as it goes.
///
/// The SDK owns the pass; this only watches it. Admission and the pass itself are established
/// before anything is awaited, so no caller wait decides how much finalization the pass gets (plan
/// 49b R2). A stop request is reconciled into every transition - a pass that proves itself after a
/// stop can only publish `Stopped` - and status stays fresh at the policy cadence while the pass
/// runs. Nothing here cancels, restarts or duplicates a pass: an unproved result keeps its owner
/// and stays observable.
async fn observe_cleanup(
    consumer: &mut DefaultMQPushConsumer,
    machine: &mut RecoveryMachine,
    publisher: &StatusPublisher,
    stop_token: &CancellationToken,
    progress: Option<&Arc<ConsumerProgress>>,
) -> CleanupSettled {
    // Idempotent for an existing session: this closes admission and returns without waiting.
    consumer.begin_cleanup();
    let policy = machine.policy();
    let Some(mut changes) = consumer.cleanup_progress_changes() else {
        // No session means the pass could never be detached from a caller that has already gone.
        // Report it as unproved rather than inventing success.
        let state = if stop_token.is_cancelled() {
            machine.request_stop()
        } else {
            machine.on_cleanup_unproved()
        };
        publisher.publish(state, machine.incarnation(), &snapshot_of(progress));
        return CleanupSettled::Unproved("no_cleanup_session");
    };
    let mut ticker = tokio::time::interval(policy.progress_check);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let stopping = stop_token.is_cancelled();
        let session = changes.borrow_and_update().clone();
        if session.proved {
            // Reconcile the stop first so completion cannot publish a nonterminal state.
            if stopping {
                machine.request_stop();
            }
            let state = machine.on_cleanup_complete();
            publisher.publish(state, machine.incarnation(), &snapshot_of(progress));
            return CleanupSettled::Proved;
        }
        if !session.running {
            // The pass ended without proving cleanup. Publish the blocker and keep ownership: this
            // is never an exit request and never a second pass.
            //
            // The unproved fact is recorded even when a stop is already in flight (plan 49c C6):
            // reasserting `Stopping` alone tells an operator that shutdown is progressing, when
            // what it means is that ownership has not been released. Recording it first
            // lets the machine pair the sticky stop with the blocker - phase
            // `Stopping`, health `Blocked`, reason naming cleanup - instead of hiding
            // behind one or the other.
            if stopping {
                machine.request_stop();
            }
            let state = machine.on_cleanup_unproved();
            publisher.publish(state, machine.incarnation(), &snapshot_of(progress));
            return CleanupSettled::Unproved(session.phase);
        }
        // Still running: report where it is. A stop changes only what is published.
        let state = if stopping {
            machine.request_stop()
        } else {
            machine.on_cleanup_unproved()
        };
        publisher.publish(state, machine.incarnation(), &snapshot_of(progress));
        tokio::select! {
            biased;
            // Guarded so an already-cancelled stop cannot spin this loop.
            _ = stop_token.cancelled(), if !stopping => {}
            changed = changes.changed() => {
                if changed.is_err() {
                    // The pass stopped reporting; settle from the last known state next round.
                    tokio::time::sleep(policy.progress_check).await;
                }
            }
            _ = ticker.tick() => {}
        }
    }
}

/// Hold an unproved cleanup until it proves itself, publishing its blocker at the policy cadence.
/// Returning means local ownership is quiescent; a stop observed meanwhile stays visible in status
/// and leaves the handle's proof pending (plan 49b R2). The controller never exits because cleanup
/// is slow, and it never starts another pass or an overlapping incarnation.
///
/// Stop is sticky state that one observation consumes, so it is never a wake-up source here. An
/// already-cancelled token is permanently ready, and using it as this loop's timer re-observed an
/// immutable settled result over a hundred thousand times in a tenth of a second (plan 49c C6).
/// What can change the answer is the session reporting something new - a retry that finally wrote,
/// a phase that finished - so that is what this waits for, with a real timer bounding only how
/// often an unchanged blocker is restated.
async fn await_cleanup_proof(
    consumer: &mut DefaultMQPushConsumer,
    machine: &mut RecoveryMachine,
    publisher: &StatusPublisher,
    stop_token: &CancellationToken,
    progress: Option<&Arc<ConsumerProgress>>,
) {
    let cadence = machine.policy().progress_check;
    let mut blocker: Option<&'static str> = None;
    loop {
        match observe_cleanup(consumer, machine, publisher, stop_token, progress).await {
            CleanupSettled::Proved => return,
            CleanupSettled::Unproved(phase) => {
                // One warning per distinct blocker. Repeated observation of the same terminal
                // result says nothing new, and log volume has to stay bounded by
                // cadence rather than by how fast this loop can turn over (plan 49c
                // C6).
                if blocker != Some(phase) {
                    tracing::warn!(
                        phase,
                        "managed consumer: cleanup session ended unproved, ownership retained; waiting for it to \
                         change"
                    );
                    blocker = Some(phase);
                }
            }
        }
        wait_for_cleanup_change(consumer.cleanup_progress_changes(), cadence).await;
    }
}

/// Wait until the cleanup session reports something new, or `cadence` has passed. Only a phase
/// change and a real timer can wake this: neither a stop request nor an observer coming and going
/// is a state change, and treating one as such is what turned waiting into spinning (plan 49c C6).
async fn wait_for_cleanup_change(
    changes: Option<tokio::sync::watch::Receiver<crate::consumer::cleanup_session::CleanupProgress>>,
    cadence: Duration,
) {
    let Some(mut changes) = changes else {
        // Nothing exists that could report a change; the timer alone keeps this bounded.
        tokio::time::sleep(cadence).await;
        return;
    };
    // A phase change is what this waits for. The real timer beside it bounds how often an unchanged
    // blocker is restated, and also covers a change that landed between observing and subscribing -
    // without it, an observer could park on a value it had already read.
    tokio::select! {
        biased;
        changed = changes.changed() => {
            if changed.is_err() {
                // The session stopped reporting. Wait out the timer rather than re-observing a channel
                // that answers `Err` instantly forever.
                tokio::time::sleep(cadence).await;
            }
        }
        _ = tokio::time::sleep(cadence) => {}
    }
}

fn snapshot_of(progress: Option<&Arc<ConsumerProgress>>) -> ProgressSnapshot {
    match progress {
        Some(progress) => progress.snapshot(stall_after(), pull_deadline()),
        None => ProgressSnapshot::default(),
    }
}

/// Drive one running incarnation. Returns whether the controller should stop or replace it.
async fn run_incarnation(
    consumer: &mut DefaultMQPushConsumer,
    machine: &mut RecoveryMachine,
    publisher: &StatusPublisher,
    stop_token: &CancellationToken,
    progress: &Arc<ConsumerProgress>,
    effects: Vec<Effect>,
) -> IncarnationEnd {
    let policy = machine.policy();
    let mut ticker = tokio::time::interval(policy.progress_check);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut effects = std::collections::VecDeque::from(effects);
    // What is left of the repair the machine asked for. The cursor is plain data - it names real
    // remaining work - and each round turns one cursor into a **pinned operation** that the loop
    // owns (plan 49d R3): a status tick neither restarts it, duplicates it, nor abandons what
    // it prepared.
    let mut repair: Option<RepairCursor> = None;
    let mut pending: Option<PendingRepair> = None;
    loop {
        // 1. Start the next bounded pass only when none is in flight. Building a pass borrows the
        //    consumer for the length of this statement; what it returns holds cloned handles, so
        //    the operation stays alive beside later status work.
        if pending.is_none() {
            match repair.take() {
                Some(RepairCursor::LostPull { .. }) => {
                    pending = Some(PendingRepair::LostPull(consumer.lost_pull_repair_pass(
                        stall_after(),
                        pull_deadline(),
                        policy.progress_check / 2,
                    )));
                }
                Some(RepairCursor::Routes { after }) => {
                    match consumer.route_repair_pass(after, policy.progress_check / 2) {
                        Some(pass) => pending = Some(PendingRepair::Routes(pass)),
                        // No client instance to repair through: there is no remaining work to carry.
                        None => repair = None,
                    }
                }
                None => {}
            }
        }

        // 2. With no repair owned at all, take the effects the machine asked for. Anything queued
        //    behind an operation is re-derived from fresh state on the following round instead of
        //    executed blindly, and a second refresh is never started beside the first.
        if pending.is_none() && repair.is_none() {
            while let Some(effect) = effects.pop_front() {
                match effect {
                    Effect::Idle(delay) => {
                        if wait_or_stop(delay, stop_token).await {
                            return IncarnationEnd::StopRequested;
                        }
                    }
                    // The kind decides what is repaired: a lost pull goes straight back through the
                    // pull service, while route and assignment faults take the coalesced refresh path.
                    // Abandoning our own fetch only releases this consumer's ticket (as a failure,
                    // retried behind its backoff); another consumer's refresh belongs to its own flight
                    // owner and is never cancelled here.
                    Effect::Repair(kind) => {
                        repair = Some(match kind {
                            RepairKind::LostPull => RepairCursor::LostPull { after: None },
                            RepairKind::RouteAndPull | RepairKind::Assignment => RepairCursor::Routes { after: None },
                        });
                        break;
                    }
                    Effect::Stop { .. } => return IncarnationEnd::Replace,
                }
            }
        }

        // 3. Wait for whichever comes first: the pinned operation settling, the status cadence, or
        //    stop. Polling them in one select is what keeps publication on schedule behind a held
        //    nameserver or a held offset read - slicing alone could not, because the wait happens
        //    inside a single topic or queue (plan 49d R3).
        let mut settled = None;
        match pending.as_mut() {
            Some(op) => {
                tokio::select! {
                    biased;
                    _ = stop_token.cancelled() => return IncarnationEnd::StopRequested,
                    outcome = &mut *op => settled = Some(outcome),
                    _ = ticker.tick() => {}
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = stop_token.cancelled() => return IncarnationEnd::StopRequested,
                    _ = ticker.tick() => {}
                }
            }
        }

        // 4. An operation that finished is judged on its own evidence: the remaining cursor moves
        //    forward, and only a pass that completed reports to the replacement budget.
        let snapshot = progress.snapshot(stall_after(), pull_deadline());
        if let Some(settled) = settled {
            pending = None;
            match settled {
                RepairOutcome::Routes(next_topic) => {
                    repair = next_topic.map(|topic| RepairCursor::Routes { after: Some(topic) });
                    if repair.is_none() {
                        machine.note_repair_settled(route_repair_resolved(&snapshot));
                    }
                }
                RepairOutcome::LostPull(finished) => {
                    repair = (!finished).then_some(RepairCursor::LostPull { after: None });
                    if finished {
                        machine.note_repair_settled(snapshot.lost_pull_queues == 0);
                    }
                }
            }
        }

        let (state, next) = machine.on_tick(&snapshot, pull_deadline(), jitter_unit());
        publisher.publish(state, machine.incarnation(), &snapshot);
        effects = next.into();
        // A replacement decision takes effect at once rather than waiting for the running repair,
        // which belongs to this incarnation and ends with it.
        if effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })) {
            return IncarnationEnd::Replace;
        }
    }
}

/// Where a retained repair continues from (plan 49c C2, plan 49d R3). Plain data on purpose: it
/// says what work is still owed between one owned pass and the next, so nothing that already
/// succeeded is repeated and no future has to borrow the consumer across status ticks.
#[derive(Clone, Debug)]
enum RepairCursor {
    /// Re-dispatch lost pulls. Queues already repaired leave the lost set, so the next slice picks
    /// up what is still behind; `after` records the last queue reached for logging and forward
    /// proof.
    LostPull {
        after: Option<cheetah_string::CheetahString>,
    },
    /// Refresh subscribed topic routes, continuing after this topic in sorted order.
    Routes {
        after: Option<cheetah_string::CheetahString>,
    },
}

/// One repair pass the controller owns, pinned so a status tick can neither drop it nor start a
/// second one beside it (plan 49d R3). Each variant wraps an already-boxed owned future; polling
/// this type never allocates.
enum PendingRepair {
    Routes(RouteRepairPass),
    LostPull(LostPullRepairPass),
}

// Both fields are `Pin<Box<dyn Future>>`, which is `Unpin`, so the enum is: polling it through a
// bare mutable reference moves nothing, and holding one across status ticks needs no pinning
// ceremony.
impl Future for PendingRepair {
    type Output = RepairOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            PendingRepair::Routes(pass) => pass.as_mut().poll(cx).map(RepairOutcome::Routes),
            PendingRepair::LostPull(pass) => pass.as_mut().poll(cx).map(RepairOutcome::LostPull),
        }
    }
}

/// What a pinned repair pass reported when it settled.
enum RepairOutcome {
    /// The route pass ended; `Some(topic)` means its slice ran out and the next pass continues
    /// there.
    Routes(Option<cheetah_string::CheetahString>),
    /// The lost-pull pass ended; `true` once nothing more is queued for re-dispatch.
    LostPull(bool),
}

/// Whether a finished route repair cleared the faults it addresses (a failed allocation, a queue
/// with no route knowledge, an RPC that timed out, a lost pull loop). Anything still standing means
/// this attempt did not resolve what asked for it.
fn route_repair_resolved(snapshot: &ProgressSnapshot) -> bool {
    snapshot.lost_pull_queues == 0
        && snapshot.route_miss_queues == 0
        && snapshot.rpc_timeout_queues == 0
        && snapshot.failed_allocations().next().is_none()
}

/// Errors that retrying the same spec cannot fix.
/// Whether a startup failure could ever succeed if retried. Decided by the error's own type, never
/// by its message text: configuration, credential and message-shape failures are permanent for this
/// process, while anything a broker, network, route or clock could restore stays retryable. A wrong
/// "permanent" verdict ends recovery forever, so anything uncertain - including a group
/// registration still held by our own unproved previous incarnation - is retried (plan 50 F3).
fn is_permanent(error: &RocketMQError) -> bool {
    matches!(
        error,
        RocketMQError::Configuration(_)
            | RocketMQError::Serialization(_)
            | RocketMQError::Authentication(_)
            | RocketMQError::MissingRequiredMessageProperty { .. }
            | RocketMQError::InvalidVersionOrdinal(_)
    )
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

    /// Plan 49c C6: an already-requested stop used to be this loop's timer, and a cancelled token
    /// is permanently ready - the review fixture re-observed one immutable settled-unproved
    /// session 156,319 times in about a tenth of a second. Waiting is now bounded by cadence
    /// and woken only by real progress, so an idle observer costs one wake per cadence and
    /// nothing else.
    #[tokio::test(flavor = "current_thread")]
    async fn waiting_for_cleanup_progress_is_bounded_by_cadence() {
        use crate::consumer::cleanup_session::CleanupProgress;
        let cadence = Duration::from_millis(50);
        let settled = CleanupProgress {
            phase: "offset_file_writer_busy",
            running: false,
            proved: false,
            attempts: 3,
        };
        let (sender, receiver) = watch::channel(settled.clone());

        // Nothing has changed and stop may already be requested: the wait still ends, and only
        // after a full cadence. Returning immediately is what turned waiting into spinning.
        let began = tokio::time::Instant::now();
        wait_for_cleanup_change(Some(receiver), cadence).await;
        assert!(began.elapsed() >= cadence, "an idle wait returned early");

        // Actual progress - the session reporting a new phase - wakes it at once, long before the
        // fallback timer would have. The subscription is taken first because that is what an
        // observer does: watch for the next change, then read whatever it produced.
        let witness = sender.subscribe();
        sender.send_replace(CleanupProgress {
            phase: "complete",
            running: false,
            proved: true,
            attempts: 3,
        });
        let began = tokio::time::Instant::now();
        wait_for_cleanup_change(Some(witness), cadence * 20).await;
        assert!(began.elapsed() < cadence, "real progress must not wait for the timer");

        // A session that stopped reporting cannot wake anyone either; the timer bounds that case
        // too, instead of an immediately-answering error channel spinning the caller.
        drop(sender);
        let began = tokio::time::Instant::now();
        wait_for_cleanup_change(Some(sender_empty()), cadence).await;
        assert!(
            began.elapsed() >= cadence,
            "a closed channel must fall back to the timer"
        );
    }

    /// A receiver whose sender is already gone.
    fn sender_empty() -> watch::Receiver<crate::consumer::cleanup_session::CleanupProgress> {
        let (sender, receiver) = watch::channel(crate::consumer::cleanup_session::CleanupProgress {
            phase: "admission_closed",
            running: true,
            proved: false,
            attempts: 0,
        });
        drop(sender);
        receiver
    }

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

    /// Plan 49b R5: numeric limits belong to the same synchronous promise as identity and
    /// subscriptions. Each setting below used to pass spec validation, build a controller plus
    /// incarnation graph, and only then fail `check_config` - permanent, but never reported
    /// before resources existed.
    #[test]
    fn numeric_settings_are_rejected_before_any_resource_exists() {
        fn rejected(mutate: impl Fn(DefaultMQPushConsumerBuilder) -> DefaultMQPushConsumerBuilder) -> RocketMQError {
            let builder = DefaultMQPushConsumer::builder()
                .consumer_group("group")
                .subscribe("topic", "*");
            let err = ManagedConsumerSpec::new(mutate(builder))
                .with_listener(Arc::new(|_msgs, _ctx| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)))
                .validate()
                .expect_err("an invalid numeric setting must be rejected synchronously");
            assert!(matches!(err, RocketMQError::Configuration(_)), "{err:?}");
            err
        }

        for (describe, err) in [
            ("consume_thread_min = 0", rejected(|b| b.consume_thread_min(0))),
            (
                "consume_thread_max above range",
                rejected(|b| b.consume_thread_max(1001)),
            ),
            (
                "consume_thread_min above consume_thread_max",
                rejected(|b| b.consume_thread_min(20).consume_thread_max(10)),
            ),
            ("pull_batch_size = 0", rejected(|b| b.pull_batch_size(0))),
            (
                "consume_message_batch_max_size above range",
                rejected(|b| b.consume_message_batch_max_size(1025)),
            ),
            (
                "pull_threshold_for_queue = 0",
                rejected(|b| b.pull_threshold_for_queue(0)),
            ),
            (
                "pull_threshold_size_for_queue above range",
                rejected(|b| b.pull_threshold_size_for_queue(1025)),
            ),
            (
                "consume_concurrently_max_span = 0",
                rejected(|b| b.consume_concurrently_max_span(0)),
            ),
            ("pop_batch_nums above range", rejected(|b| b.pop_batch_nums(33))),
        ] {
            let message = err.to_string();
            assert!(
                message.contains("Out of range") || message.contains("larger than"),
                "{describe} must be reported as a range violation, got {message}"
            );
        }
    }

    /// The shared validator must still accept what the SDK configures by default; bounds tightened
    /// past the documented defaults would turn every ordinary managed consumer into a permanent
    /// rejection.
    #[test]
    fn documented_numeric_defaults_pass_preflight() {
        spec()
            .validate()
            .expect("the documented defaults are valid configuration");
        let config = DefaultMQPushConsumer::builder()
            .consumer_group("group")
            .subscribe("topic", "*")
            .normalized_consumer_config();
        validate_consumer_config(&config, ListenerPresence::Provided)
            .expect("the same projection build() uses must pass validation");
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
        assert!(matches!(err, RocketMQError::Configuration(_)), "{err:?}");
    }

    /// Plan 50 F3: the rules legacy `start` applies are applied here too - before a controller,
    /// runtime or client instance exists - and every rejection is typed so the controller can tell
    /// "never retry" from "not yet" without reading message text.
    #[test]
    fn identity_and_subscription_rules_are_rejected_before_any_resource() {
        fn listener() -> ManagedListener {
            Arc::new(|_msgs, _ctx| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess))
        }
        let rejected = |builder: DefaultMQPushConsumerBuilder| {
            let err = ManagedConsumerSpec::new(builder)
                .with_listener(listener())
                .validate()
                .expect_err("invalid configuration must fail");
            assert!(matches!(err, RocketMQError::Configuration(_)), "{err:?}");
        };

        rejected(DefaultMQPushConsumer::builder().subscribe("topic", "*"));
        rejected(
            DefaultMQPushConsumer::builder()
                .consumer_group("   ")
                .subscribe("topic", "*"),
        );
        rejected(
            DefaultMQPushConsumer::builder()
                .consumer_group(rocketmq_common::common::mix_all::DEFAULT_CONSUMER_GROUP)
                .subscribe("topic", "*"),
        );
        rejected(
            DefaultMQPushConsumer::builder()
                .consumer_group("group with spaces")
                .subscribe("topic", "*"),
        );
        rejected(
            DefaultMQPushConsumer::builder()
                .consumer_group("group")
                .subscribe("", "*"),
        );
        rejected(
            DefaultMQPushConsumer::builder()
                .consumer_group("group")
                .subscribe("bad topic!", "*"),
        );
    }

    #[test]
    fn a_spec_without_a_subscription_is_invalid_configuration() {
        let err = ManagedConsumerSpec::new(DefaultMQPushConsumer::builder().consumer_group("group"))
            .with_listener(Arc::new(|_msgs, _ctx| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)))
            .validate()
            .expect_err("missing subscription must fail");
        assert!(matches!(err, RocketMQError::Configuration(_)), "{err:?}");
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
    async fn one_session_answers_every_caller_the_same_way() {
        let handle = start_managed(spec(), LifecyclePolicy::default()).expect("managed consumer starts");
        // Generous enough that the answer comes from a settled session rather than a deadline.
        let first = handle
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(30))
            .await;
        let began = tokio::time::Instant::now();
        // A second caller, including one holding another clone of the same handle, attaches to the
        // session that already answered; it must repeat that answer instead of re-waiting on a
        // channel nobody will send on again.
        let second = handle
            .clone()
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(30))
            .await;
        assert_eq!(first, second, "a resolved session gives one answer per outcome");
        if matches!(first, ShutdownOutcome::Complete) {
            assert!(
                began.elapsed() < Duration::from_secs(1),
                "a proved cleanup must not be waited for twice"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_handle_requests_stop_without_waiting() {
        let handle = start_managed(spec(), LifecyclePolicy::default()).expect("managed consumer starts");
        drop(handle);
        // The only assertion that matters is that this test process is still alive and waiting
        // for nothing.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    fn sample_status(incarnation: u64) -> ConsumerStatus {
        ConsumerStatus {
            phase: ConsumerPhase::Running,
            health: ConsumerHealth::Healthy,
            reason: "assigned",
            incarnation,
            progress: ProgressCounts::default(),
        }
    }

    /// Plan 49a F9: an idle observer must be woken by a published status. Returning `Pending`
    /// without registering a wakeup leaves a quiet consumer's observer asleep forever, which no
    /// amount of later publishing can undo.
    #[tokio::test(flavor = "current_thread")]
    async fn an_idle_event_observer_is_woken_by_a_published_status() {
        let (sender, receiver) = broadcast::channel::<ConsumerStatus>(8);
        let mut events = ConsumerStatusEvents::new(receiver);
        let waiter = tokio::spawn(async move { futures::StreamExt::next(&mut events).await });
        // Park the reader on an empty buffer. Nothing else runs on this runtime until the awaited
        // item arrives, so a missing wakeup can only show up as the timeout below.
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender
            .send(sample_status(3))
            .expect("a live receiver keeps the channel open");
        let observed = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("publishing a status must wake the idle observer")
            .expect("observer task")
            .expect("the stream stays open while a sender lives");
        assert_eq!(observed.incarnation, 3);
        assert_eq!(observed.phase, ConsumerPhase::Running);
        assert_eq!(observed.reason, "assigned");
    }

    /// Plan 49a F9: only the publisher owns the event channel. A remaining handle must neither keep
    /// a stream open nor be able to manufacture a publisher, and creating a receiver borrows no
    /// control ownership of the consumer.
    #[tokio::test(flavor = "current_thread")]
    async fn a_handle_cannot_keep_the_event_channel_open_without_a_publisher() {
        let (status_tx, status_rx) = watch::channel(sample_status(1));
        let publisher = Arc::new(broadcast::channel::<ConsumerStatus>(8).0);
        let handle = ManagedConsumerHandle {
            inner: Arc::new(HandleInner {
                status: status_rx,
                events: Arc::downgrade(&publisher),
                stop_token: CancellationToken::new(),
                stop_requested: AtomicBool::new(false),
                cleanup_complete: parking_lot::Mutex::new(None),
                proved: AtomicBool::new(false),
            }),
        };
        // While the publisher lives, a receiver created from the handle sees its values.
        let mut events = handle.subscribe_events();
        publisher
            .send(sample_status(7))
            .expect("a subscribed receiver keeps the channel open");
        assert_eq!(
            futures::StreamExt::next(&mut events)
                .await
                .expect("open stream")
                .incarnation,
            7
        );
        drop(events);
        // The controller is gone: a fresh receiver reports terminal closure at once instead of
        // waiting for values that can never arrive, even though this handle is still alive.
        drop(publisher);
        let mut after = handle.subscribe_events();
        let closed = tokio::time::timeout(Duration::from_secs(2), futures::StreamExt::next(&mut after)).await;
        assert!(
            matches!(closed, Ok(None)),
            "a stream must close once its only publisher is gone, observed: {closed:?}"
        );
        // Latest-state access stays available and costs no ownership.
        assert_eq!(handle.status().incarnation, 1);
        drop(status_tx);
    }

    /// Plan 49a F9: `subscribe_status` hands out a live subscription, so an observer learns the
    /// next state by being told instead of by sampling on a timer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_status_subscription_delivers_the_next_transition_unprompted() {
        let handle = start_managed(spec(), LifecyclePolicy::default()).expect("managed consumer starts");
        let mut statuses = handle.subscribe_status();
        let initial = *statuses.borrow_and_update();
        handle.request_stop();
        let observed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if statuses.changed().await.is_err() {
                    // The publisher finished: the subscription ended on its own, which is also a
                    // delivery rather than a wait.
                    return None;
                }
                let next = *statuses.borrow_and_update();
                if next.phase != initial.phase || next.reason != initial.reason {
                    return Some(next);
                }
            }
        })
        .await
        .expect("a stop must reach a subscriber through the subscription, not by polling");
        if let Some(status) = observed {
            assert_ne!(status.reason, "");
        }
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
    fn permanent_startup_errors_are_classified_by_type() {
        // A configuration problem is tagged by the SDK that found it, and never retried.
        assert!(is_permanent(&RocketMQError::configuration(
            "the consumer group must not be empty"
        )));
        // A rejected credential is permanent for this process: retrying cannot mint a secret.
        assert!(is_permanent(&RocketMQError::authentication_failed("bad credentials")));
        // A malformed message property is permanent too - the payload will not repair itself.
        assert!(is_permanent(&RocketMQError::MissingRequiredMessageProperty {
            property: "KEYS"
        }));
        // Everything a broker, network or clock could restore stays retryable, including a timeout
        // raised by the startup budget and a group still registered to an unproved predecessor.
        // `IllegalArgument` in particular cannot be trusted as permanent: this codebase raises it
        // for transient conditions too, which is why rejections carry their own variant.
        assert!(!is_permanent(&mq_client_err!("consumer startup deadline elapsed")));
        assert!(!is_permanent(&mq_client_err!("the consumer group[g] exist already.")));
        assert!(!is_permanent(
            &rocketmq_error::RocketMQError::network_connection_failed("127.0.0.1:9876", "connection refused")
        ));
    }

    /// Plan 49b R2: every incarnation is built under the controller's policy, and that policy - not
    /// any observer's patience - decides how long its cleanup pass may finalize.
    #[tokio::test(flavor = "current_thread")]
    async fn an_incarnation_carries_the_policy_finalization_ceiling() {
        let policy = LifecyclePolicy {
            cleanup_budget: Duration::from_secs(4),
            ..LifecyclePolicy::default()
        };
        let consumer = spec()
            .build_incarnation(Arc::new(ConsumerProgress::default()), &policy)
            .expect("a valid spec builds an incarnation");
        assert_eq!(consumer.cleanup_session_ceiling(), Duration::from_secs(12));
    }

    /// An observer whose wait has already elapsed must not shrink the pass it just joined: giving
    /// up on watching cannot take away the consumer's chance to persist offsets and unregister.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_elapsed_observer_does_not_resize_the_finalization_ceiling() {
        let policy = LifecyclePolicy {
            cleanup_budget: Duration::from_secs(4),
            ..LifecyclePolicy::default()
        };
        let mut consumer = spec()
            .build_incarnation(Arc::new(ConsumerProgress::default()), &policy)
            .expect("a valid spec builds an incarnation");
        let settled = consumer.shutdown_until(tokio::time::Instant::now()).await;
        assert!(
            matches!(
                settled,
                crate::consumer::default_mq_push_consumer::ConsumerCleanup::Incomplete
            ),
            "an elapsed wait cannot prove cleanup"
        );
        assert_eq!(consumer.cleanup_session_ceiling(), Duration::from_secs(12));
    }

    /// Admission closes at once and the pass is detached from the caller that asked for it, so
    /// status can follow its phases without anybody waiting on it or starting a second pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observers_follow_the_one_pass_without_starting_another() {
        let mut consumer = spec()
            .build_incarnation(Arc::new(ConsumerProgress::default()), &LifecyclePolicy::default())
            .expect("a valid spec builds an incarnation");
        assert!(consumer.cleanup_progress_changes().is_none(), "no stop requested yet");
        assert!(consumer.begin_cleanup(), "the pass is owned by the SDK");
        let mut first = consumer
            .cleanup_progress_changes()
            .expect("admission created a session");
        let second = consumer
            .cleanup_progress_changes()
            .expect("the same session, not a new one");
        let progress = first.borrow_and_update().clone();
        assert!(progress.running && !progress.proved, "{progress:?}");
        assert_eq!(*second.borrow(), *first.borrow());
        // Asking again joins what exists; it never spawns a second pass or writer.
        assert!(consumer.begin_cleanup());
        let progress = consumer.cleanup_progress().expect("still the same session");
        assert!(progress.running, "{progress:?}");
    }

    /// A managed incarnation built through the unmodified spec: subscriptions live only in the
    /// runtime table, exactly as after `start`, while configuration stays empty (plan 49d R2).
    fn managed_incarnation(group: &str, topic: &str) -> DefaultMQPushConsumer {
        let spec = ManagedConsumerSpec::new(
            DefaultMQPushConsumer::builder()
                .consumer_group(group)
                .subscribe(topic, "*"),
        )
        .with_listener(Arc::new(|_, _| Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)));
        spec.build_incarnation(ConsumerProgress::new(), &LifecyclePolicy::default())
            .expect("a valid managed spec builds an incarnation")
    }

    /// Plan 49d R2: route repair must read the runtime subscription set. Reading configuration made
    /// every managed `Assignment` and `RouteAndPull` repair finish without visiting a single
    /// subscribed topic, so the controller's requested repair refreshed nothing and only later
    /// periodic SDK refresh could help.
    #[tokio::test(flavor = "current_thread")]
    async fn route_repair_visits_the_runtime_subscription_set() {
        use crate::factory::mq_client_instance::MQClientInstance;
        use cheetah_string::CheetahString;
        use rocketmq_common::common::mix_all;
        use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;

        let group = "49d-r2-repair-scope";
        let mut consumer = managed_incarnation(group, "T-A");
        let (subscriptions, client, retry_topic) = {
            let inner = consumer.default_mqpush_consumer_impl.as_mut().unwrap();
            let subscriptions = inner.rebalance_impl.get_subscription_inner();
            assert_eq!(
                subscriptions.read().await.len(),
                1,
                "the managed constructor subscribed at runtime"
            );
            assert!(
                inner.consumer_config.subscription.is_empty(),
                "managed construction leaves configuration empty - that is what repair must not read"
            );
            let client = MQClientInstance::new_arc(
                crate::base::client_config::ClientConfig {
                    namesrv_addr: Some(CheetahString::from_static_str("127.0.0.1:1")),
                    ..Default::default()
                },
                0,
                "49d-r2-repair-scope-client",
                None,
            );
            inner.client_instance = Some(client.clone());
            // What `start` does for the SDK retry topic: a runtime subscription nothing in
            // configuration mentions.
            let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(group));
            let retry_data =
                FilterAPI::build_subscription_data(&retry_topic, &CheetahString::from_static_str("*")).unwrap();
            inner
                .rebalance_impl
                .put_subscription_data(retry_topic.clone(), retry_data)
                .await;
            (subscriptions, client, retry_topic)
        };

        // One topic per round: a zero slice must stop between topics and hand back where it
        // stopped.
        let cursor = consumer
            .route_repair_pass(None, Duration::ZERO)
            .expect("a client instance is attached")
            .await
            .expect("two subscriptions cannot both fit in a zero-length slice");
        assert_eq!(
            cursor, retry_topic,
            "the SDK retry topic is part of the runtime set and sorts first"
        );
        assert_eq!(client.route_refresh_guard.len(), 1, "one real refresh request");

        // Membership is re-checked on a resumed slice: an unsubscribed topic gets no route work,
        // while one subscribed after the previous round is picked up.
        subscriptions
            .write()
            .await
            .remove(&CheetahString::from_static_str("T-A"));
        let late = FilterAPI::build_subscription_data(
            &CheetahString::from_static_str("T-B"),
            &CheetahString::from_static_str("*"),
        )
        .unwrap();
        consumer
            .default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .rebalance_impl
            .put_subscription_data(CheetahString::from_static_str("T-B"), late)
            .await;
        let finished = consumer
            .route_repair_pass(Some(cursor.clone()), Duration::from_secs(30))
            .expect("a client instance is attached")
            .await;
        assert!(finished.is_none(), "the cursor pass finishes the repair");
        assert_eq!(
            client.route_refresh_guard.len(),
            2,
            "only the still-subscribed topics are refreshed: the retry topic and T-B, never T-A"
        );
        drop(subscriptions);
        client.clone().shutdown().await;
    }

    fn status_sink() -> (StatusPublisher, broadcast::Receiver<ConsumerStatus>) {
        let initial = ConsumerStatus {
            phase: ConsumerPhase::Starting,
            health: ConsumerHealth::Healthy,
            reason: Reason::None.as_str(),
            incarnation: 1,
            progress: ProgressCounts::default(),
        };
        let (status, _observed) = watch::channel(initial);
        let (events, receiver) = broadcast::channel(512);
        (
            StatusPublisher {
                status,
                events: Arc::new(events),
            },
            receiver,
        )
    }

    /// Plan 49d R3: a repair pass is pinned and polled **beside** the status cadence. Holding the
    /// nameserver lock makes one route refresh wait inside that pass; publications must keep their
    /// cadence, elapsed ticks may not be read as further failed attempts (which used to escalate a
    /// still-running repair into incarnation replacement), and stop must still be answered at once.
    #[tokio::test(flavor = "current_thread")]
    async fn status_keeps_its_cadence_while_a_route_repair_waits_on_the_nameserver() {
        use crate::factory::mq_client_instance::MQClientInstance;
        use cheetah_string::CheetahString;

        let group = "49d-r3-route-repair";
        let mut consumer = managed_incarnation(group, "T-A");
        let client = MQClientInstance::new_arc(
            crate::base::client_config::ClientConfig {
                namesrv_addr: Some(CheetahString::from_static_str("127.0.0.1:1")),
                ..Default::default()
            },
            0,
            "49d-r3-route-repair-client",
            None,
        );
        consumer.default_mqpush_consumer_impl.as_mut().unwrap().client_instance = Some(client.clone());
        let progress = consumer.progress();
        let mut machine = RecoveryMachine::new(
            LifecyclePolicy {
                progress_check: Duration::from_millis(20),
                ..LifecyclePolicy::default()
            },
            1,
        );
        let (publisher, mut events) = status_sink();

        // One route refresh waits on this lock for the whole observation window.
        let hold = client.nameserver_lock();
        let held = tokio::spawn(async move {
            let _guard = hold.lock().await;
            tokio::time::sleep(Duration::from_millis(400)).await;
        });

        let stop = CancellationToken::new();
        let stopper = {
            let stop = stop.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                stop.cancel();
            })
        };

        let began = std::time::Instant::now();
        let end = run_incarnation(
            &mut consumer,
            &mut machine,
            &publisher,
            &stop,
            &progress,
            vec![Effect::Repair(RepairKind::Assignment)],
        )
        .await;
        let elapsed = began.elapsed();

        // Stop was requested while the same pass was still waiting, and answered without waiting
        // for it - and never by an escalation that replaced a healthy incarnation.
        assert_eq!(end, IncarnationEnd::StopRequested);
        assert!(
            (Duration::from_millis(140)..Duration::from_millis(320)).contains(&elapsed),
            "stop must be answered promptly beside a waiting repair: {elapsed:?}"
        );

        let mut publications = 0usize;
        while events.try_recv().is_ok() {
            publications += 1;
        }
        assert!(
            publications >= 4,
            "a waiting nameserver delayed status: {publications} publications in {elapsed:?}"
        );

        stopper.abort();
        held.abort();
    }

    /// Plan 49d R3, offset side: the same guarantee while a lost-pull repair waits inside its own
    /// preparation. The process-queue table is write-locked, so the pass blocks where real offset
    /// preparation blocks; status and stop must be unaffected.
    #[tokio::test(flavor = "current_thread")]
    async fn status_keeps_its_cadence_while_a_lost_pull_repair_waits_on_preparation() {
        use rocketmq_common::common::message::message_queue::MessageQueue;

        let mut consumer = managed_incarnation("49d-r3-lost-pull", "T-A");
        let progress = consumer.progress();
        let table = consumer
            .default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .clone();

        // One acquisition whose own RPC window has already passed: lost, and repairable.
        let mq = MessageQueue::from_parts("T-A", "broker-a", 0);
        progress.note_queue_installed(&mq);
        let lease = progress.acquire_pull_lease(&mq).expect("an installed queue is owned");
        let epoch = lease.epoch();
        progress.begin_pull_attempt(&mq, epoch, Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            progress.lost_pulls(stall_after(), pull_deadline()).len(),
            1,
            "fixture: one overdue attempt"
        );

        let mut machine = RecoveryMachine::new(
            LifecyclePolicy {
                progress_check: Duration::from_millis(20),
                ..LifecyclePolicy::default()
            },
            1,
        );
        let (publisher, mut events) = status_sink();

        let hold = table.clone();
        let held = tokio::spawn(async move {
            let _guard = hold.write().await;
            tokio::time::sleep(Duration::from_millis(400)).await;
        });
        // Let the write guard be taken before the controller starts, so its first repair pass is
        // the one that waits.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let stop = CancellationToken::new();
        let stopper = {
            let stop = stop.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                stop.cancel();
            })
        };

        let began = std::time::Instant::now();
        let end = run_incarnation(
            &mut consumer,
            &mut machine,
            &publisher,
            &stop,
            &progress,
            vec![Effect::Repair(RepairKind::LostPull)],
        )
        .await;
        let elapsed = began.elapsed();

        assert_eq!(end, IncarnationEnd::StopRequested);
        assert!(
            (Duration::from_millis(140)..Duration::from_millis(320)).contains(&elapsed),
            "stop must be answered promptly beside a waiting repair: {elapsed:?}"
        );
        let mut publications = 0usize;
        while events.try_recv().is_ok() {
            publications += 1;
        }
        assert!(
            publications >= 4,
            "held offset preparation delayed status: {publications} publications in {elapsed:?}"
        );

        drop(lease);
        stopper.abort();
        held.abort();
    }
}
