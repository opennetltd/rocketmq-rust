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

//! Progress tracking and recovery policy owned by the SDK (plan 49 sections 3.1/3.2).
//!
//! Everything here is deliberately free of I/O: [`ConsumerProgress`] is typed shared state
//! updated at real work transitions, and [`RecoveryMachine`] turns snapshots plus explicit time
//! into effects. That keeps startup/backoff/replacement/blocking decisions deterministic and
//! testable without a broker, and it means recovery never depends on an application observer,
//! metric or callback being alive.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::TimeUtils::get_current_millis;

/// SDK-owned recovery policy (plan 49 section 3.2). These are explicit SDK policy choices, not
/// application configuration: the managed entry point intentionally exposes no copies of them.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LifecyclePolicy {
    /// How often the controller evaluates progress.
    pub progress_check: Duration,
    /// Startup budget for the very first incarnation (cold start allowance).
    pub initial_startup_attempt: Duration,
    /// Startup budget for every later attempt once the consumer has run at least once.
    pub replacement_startup_attempt: Duration,
    /// First retry delay before jitter.
    pub retry_min: Duration,
    /// Retry ceiling; delays grow exponentially towards it.
    pub retry_max: Duration,
    /// Symmetric jitter fraction applied to each delay (±20% by default).
    pub jitter_ratio: f64,
    /// Extra one-off spread for the first retry so many consumers do not align.
    pub initial_spread_max: Duration,
    /// Pending-delivery silence that produces a warning, applied only with real pending work.
    pub pending_delivery_warn: Duration,
    /// Budget for one rollback/cleanup pass around an incarnation boundary. A caller's own
    /// deadline still bounds only the caller-visible wait; the session continues afterwards.
    pub cleanup_budget: Duration,
    /// Failed in-place repairs tolerated before one guarded incarnation replacement.
    pub max_repairs_before_replace: u32,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            progress_check: Duration::from_secs(1),
            initial_startup_attempt: Duration::from_secs(1_200),
            replacement_startup_attempt: Duration::from_secs(30),
            retry_min: Duration::from_secs(3),
            retry_max: Duration::from_secs(120),
            jitter_ratio: 0.2,
            initial_spread_max: Duration::from_secs(60),
            pending_delivery_warn: Duration::from_secs(60),
            cleanup_budget: Duration::from_secs(30),
            max_repairs_before_replace: 3,
        }
    }
}

/// Exponential delay for `attempt` (1-based) with symmetric jitter. `unit` is a value in
/// `[0, 1)`; passing it explicitly keeps the arithmetic testable without a clock or RNG.
pub(crate) fn backoff_delay(policy: &LifecyclePolicy, attempt: u32, unit: f64) -> Duration {
    let unit = unit.clamp(0.0, 1.0);
    let base = policy.retry_min.as_secs_f64() * 2f64.powi(attempt.saturating_sub(1) as i32);
    let base = Duration::from_secs_f64(base).min(policy.retry_max);
    let factor = 1.0 + policy.jitter_ratio * (2.0 * unit - 1.0);
    let jittered = Duration::from_secs_f64(base.as_secs_f64() * factor);
    jittered.clamp(policy.retry_min / 2, policy.retry_max)
}

/// Delay before the first retry after a failure: bounded backoff plus a one-off spread so a
/// fleet of consumers does not retry in lockstep.
pub(crate) fn first_retry_delay(policy: &LifecyclePolicy, unit: f64) -> Duration {
    let unit = unit.clamp(0.0, 1.0);
    backoff_delay(policy, 1, unit).saturating_add(millis_of(policy.initial_spread_max, unit))
}

fn millis_of(total: Duration, unit: f64) -> Duration {
    Duration::from_secs_f64(total.as_secs_f64() * unit.clamp(0.0, 1.0))
}

/// Bounded reason carried by a status snapshot. Kept as a fixed enum so it can never grow
/// unbounded identifiers; queue/broker names and incarnations stay out.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Reason {
    #[default]
    None,
    Starting,
    WaitingForAssignment,
    IdleNoMessages,
    LongPollPending,
    FlowControl,
    RouteOrBrokerMiss,
    RpcTimeout,
    AssignmentRefreshFailed,
    ExpectedPullMissing,
    CallbacksStalled,
    ListenerActiveOverBudget,
    StartupAttemptExhausted,
    StartupRejected,
    CleanupUnproved,
    Stopping,
    Stopped,
}

impl Reason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Starting => "starting",
            Self::WaitingForAssignment => "waiting_for_assignment",
            Self::IdleNoMessages => "idle_no_messages",
            Self::LongPollPending => "long_poll_pending",
            Self::FlowControl => "flow_control",
            Self::RouteOrBrokerMiss => "route_or_broker_miss",
            Self::RpcTimeout => "rpc_timeout",
            Self::AssignmentRefreshFailed => "assignment_refresh_failed",
            Self::ExpectedPullMissing => "expected_pull_missing",
            Self::CallbacksStalled => "callbacks_stalled",
            Self::ListenerActiveOverBudget => "listener_active_over_budget",
            Self::StartupAttemptExhausted => "startup_attempt_exhausted",
            Self::StartupRejected => "startup_rejected",
            Self::CleanupUnproved => "cleanup_unproved",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }

    /// Whether this reason describes something an operator must act on.
    pub(crate) const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::CleanupUnproved | Self::ListenerActiveOverBudget | Self::StartupRejected
        )
    }
}

/// Progress counters and timestamps read at the SDK's own work transitions.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ProgressSnapshot {
    pub now_ms: u64,
    pub started_at_ms: u64,
    pub assigned_queues: u64,
    pub stalled_queues: u64,
    pub pulls_started: u64,
    pub pulls_completed: u64,
    pub pulls_empty: u64,
    pub pull_failures: u64,
    pub route_misses: u64,
    pub rpc_timeouts: u64,
    pub retries_scheduled: u64,
    pub flow_controls: u64,
    pub assignments: u64,
    pub revocations: u64,
    pub pending_delivery_msgs: u64,
    pub active_callbacks: u64,
    pub callbacks_started: u64,
    pub callbacks_finished: u64,
    pub offsets_persisted: u64,
    pub last_pull_ms: u64,
    pub last_callback_progress_ms: u64,
    pub last_assignment_ms: u64,
    pub last_offset_ms: u64,
    /// Timestamps of the latest rebalance observation that could see a topic route, and of the
    /// latest one that could not. Which is newer decides whether a zero assignment is explicit.
    pub last_route_ok_ms: u64,
    pub last_route_miss_ms: u64,
}

impl ProgressSnapshot {
    fn age_ms(&self, since_ms: u64) -> Duration {
        Duration::from_millis(self.now_ms.saturating_sub(since_ms))
    }

    /// True when work is outstanding: messages are waiting for or inside a listener.
    pub(crate) fn has_pending_work(&self) -> bool {
        self.pending_delivery_msgs > 0 || self.active_callbacks > 0
    }

    /// A long poll that is still inside its own deadline is an expected wait, not silence.
    pub(crate) fn pull_within_deadline(&self, deadline: Duration) -> bool {
        self.pulls_started > self.pulls_completed && self.age_ms(self.last_pull_ms) < deadline
    }

    /// Counters that moved since `previous`; used to prove progress rather than trust a timer.
    pub(crate) fn progressed_since(&self, previous: &Self) -> bool {
        self.pulls_completed > previous.pulls_completed
            || self.callbacks_finished > previous.callbacks_finished
            || self.offsets_persisted > previous.offsets_persisted
            || self.assignments > previous.assignments
    }
}

#[derive(Clone, Copy, Debug)]
struct QueueProgress {
    epoch: u64,
    last_progress_ms: u64,
    pull_leased: bool,
}

/// Why a pull failed, used only to choose the repair action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PullFailureKind {
    RouteOrBrokerMiss,
    RpcTimeout,
    Other,
}

/// Typed shared progress state for one consumer incarnation.
///
/// Counters are atomics and the per-queue table is a plain lock held only for a few
/// instructions; nothing here is awaited while a lock is held, so a stalled queue can never
/// block reporting, and one active queue cannot hide thousands of stalled ones.
#[derive(Default)]
pub(crate) struct ConsumerProgress {
    started_at_ms: AtomicU64,
    pulls_started: AtomicU64,
    pulls_completed: AtomicU64,
    pulls_empty: AtomicU64,
    pull_failures: AtomicU64,
    route_misses: AtomicU64,
    rpc_timeouts: AtomicU64,
    retries_scheduled: AtomicU64,
    flow_controls: AtomicU64,
    assignments: AtomicU64,
    revocations: AtomicU64,
    pending_delivery_msgs: AtomicU64,
    active_callbacks: AtomicU64,
    callbacks_started: AtomicU64,
    callbacks_finished: AtomicU64,
    offsets_persisted: AtomicU64,
    last_pull_ms: AtomicU64,
    last_callback_progress_ms: AtomicU64,
    last_assignment_ms: AtomicU64,
    last_offset_ms: AtomicU64,
    last_route_ok_ms: AtomicU64,
    last_route_miss_ms: AtomicU64,
    queues: parking_lot::RwLock<HashMap<MessageQueue, QueueProgress>>,
}

impl ConsumerProgress {
    pub(crate) fn new() -> Arc<Self> {
        let now = get_current_millis();
        Arc::new(Self {
            started_at_ms: AtomicU64::new(now),
            last_pull_ms: AtomicU64::new(now),
            last_callback_progress_ms: AtomicU64::new(now),
            last_assignment_ms: AtomicU64::new(0),
            last_offset_ms: AtomicU64::new(now),
            ..Default::default()
        })
    }

    fn bump(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn stamp(&self, counter: &AtomicU64) {
        counter.store(get_current_millis(), Ordering::Relaxed);
    }

    /// Claims the single in-flight pull lease for `queue` under its current epoch. A second
    /// concurrent pull for the same queue epoch is rejected, which is what keeps repair from
    /// stacking duplicate work per queue.
    pub(crate) fn claim_pull(&self, queue: &MessageQueue) -> Option<u64> {
        let mut queues = self.queues.write();
        let entry = queues.get_mut(queue)?;
        if entry.pull_leased {
            return None;
        }
        entry.pull_leased = true;
        Some(entry.epoch)
    }

    /// Releases the lease after a pull completes, fails or is cancelled. `epoch` rejects a
    /// release from a stale assignment generation.
    pub(crate) fn release_pull(&self, queue: &MessageQueue, epoch: u64) {
        let mut queues = self.queues.write();
        if let Some(entry) = queues.get_mut(queue) {
            if entry.epoch == epoch {
                entry.pull_leased = false;
            }
        }
    }

    pub(crate) fn note_pull_started(&self, queue: &MessageQueue) {
        self.bump(&self.pulls_started);
        self.touch(queue);
    }

    /// Refresh one queue's progress timestamp; the lock is held for a single store.
    pub(crate) fn touch(&self, queue: &MessageQueue) {
        let now = get_current_millis();
        if let Some(entry) = self.queues.write().get_mut(queue) {
            entry.last_progress_ms = now;
        }
    }

    /// A successful pull, including an empty one, is progress for that queue.
    pub(crate) fn note_pull_completed(&self, queue: &MessageQueue, found_message: bool) {
        self.bump(&self.pulls_completed);
        if !found_message {
            self.bump(&self.pulls_empty);
        }
        let now = get_current_millis();
        self.last_pull_ms.store(now, Ordering::Relaxed);
        if let Some(entry) = self.queues.write().get_mut(queue) {
            entry.last_progress_ms = now;
        }
    }

    pub(crate) fn note_pull_failed(&self, queue: &MessageQueue, kind: PullFailureKind) {
        self.bump(&self.pull_failures);
        match kind {
            PullFailureKind::RouteOrBrokerMiss => self.bump(&self.route_misses),
            PullFailureKind::RpcTimeout => self.bump(&self.rpc_timeouts),
            PullFailureKind::Other => {}
        }
    }

    pub(crate) fn note_retry_scheduled(&self) {
        self.bump(&self.retries_scheduled);
    }

    pub(crate) fn note_flow_control(&self) {
        self.bump(&self.flow_controls);
    }

    /// Records what the latest rebalance pass could actually see. A pass with no route for this
    /// consumer's own subscriptions is a failed assignment refresh, which section 3.2 treats as a
    /// fault to repair; only a pass that saw routes can prove that a zero assignment was explicit.
    pub(crate) fn note_route_observation(&self, routed: bool) {
        if routed {
            self.stamp(&self.last_route_ok_ms);
        } else {
            self.stamp(&self.last_route_miss_ms);
        }
    }

    /// Publishes a new assignment. Queues seen for the first time, or again after revocation,
    /// start a fresh epoch; retained queues keep theirs.
    pub(crate) fn note_assignment(&self, assigned: &HashSet<MessageQueue>) {
        let now = get_current_millis();
        self.bump(&self.assignments);
        self.last_assignment_ms.store(now, Ordering::Relaxed);
        let mut queues = self.queues.write();
        for queue in assigned {
            match queues.get_mut(queue) {
                Some(entry) => entry.pull_leased = false,
                None => {
                    queues.insert(
                        queue.clone(),
                        QueueProgress {
                            epoch: 1,
                            last_progress_ms: now,
                            pull_leased: false,
                        },
                    );
                }
            }
        }
    }

    /// Drops revoked queues so their epochs can never publish again.
    pub(crate) fn note_revocation(&self, revoked: &[MessageQueue]) {
        if revoked.is_empty() {
            return;
        }
        self.bump(&self.revocations);
        let mut queues = self.queues.write();
        for queue in revoked {
            queues.remove(queue);
        }
    }

    /// Messages handed to the delivery layer but not yet acknowledged by a listener.
    pub(crate) fn note_delivery_pending(&self, messages: u64) {
        self.pending_delivery_msgs.fetch_add(messages, Ordering::Relaxed);
    }

    pub(crate) fn note_callback_started(&self, messages: u64) {
        self.bump(&self.callbacks_started);
        self.active_callbacks.fetch_add(1, Ordering::Relaxed);
        let now = get_current_millis();
        self.last_callback_progress_ms.store(now, Ordering::Relaxed);
    }

    pub(crate) fn note_callback_finished(&self, messages: u64) {
        self.bump(&self.callbacks_finished);
        self.active_callbacks.fetch_sub(1, Ordering::Relaxed);
        self.pending_delivery_msgs.fetch_sub(
            messages.min(self.pending_delivery_msgs.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
        let now = get_current_millis();
        self.last_callback_progress_ms.store(now, Ordering::Relaxed);
    }

    /// A batch that will never reach a listener (its queue was revoked) stops counting as work.
    pub(crate) fn note_delivery_dropped(&self, messages: u64) {
        self.pending_delivery_msgs.fetch_sub(
            messages.min(self.pending_delivery_msgs.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn note_offset_persisted(&self) {
        self.bump(&self.offsets_persisted);
        self.stamp(&self.last_offset_ms);
    }

    /// Whether `queue` still belongs to this incarnation's assignment. Used to reject stale
    /// assignment publication and late pulls from a previous epoch.
    pub(crate) fn owns(&self, queue: &MessageQueue) -> bool {
        self.queues.read().contains_key(queue)
    }

    pub(crate) fn assigned_queues(&self) -> u64 {
        self.queues.read().len() as u64
    }

    pub(crate) fn snapshot(&self, stall_after: Duration) -> ProgressSnapshot {
        let now_ms = get_current_millis();
        let queues = self.queues.read();
        let stall = u64::try_from(stall_after.as_millis()).unwrap_or(u64::MAX);
        let stalled_queues = queues
            .values()
            .filter(|entry| now_ms.saturating_sub(entry.last_progress_ms) >= stall)
            .count() as u64;
        ProgressSnapshot {
            now_ms,
            started_at_ms: self.started_at_ms.load(Ordering::Relaxed),
            assigned_queues: queues.len() as u64,
            stalled_queues,
            pulls_started: self.pulls_started.load(Ordering::Relaxed),
            pulls_completed: self.pulls_completed.load(Ordering::Relaxed),
            pulls_empty: self.pulls_empty.load(Ordering::Relaxed),
            pull_failures: self.pull_failures.load(Ordering::Relaxed),
            route_misses: self.route_misses.load(Ordering::Relaxed),
            rpc_timeouts: self.rpc_timeouts.load(Ordering::Relaxed),
            retries_scheduled: self.retries_scheduled.load(Ordering::Relaxed),
            flow_controls: self.flow_controls.load(Ordering::Relaxed),
            assignments: self.assignments.load(Ordering::Relaxed),
            revocations: self.revocations.load(Ordering::Relaxed),
            pending_delivery_msgs: self.pending_delivery_msgs.load(Ordering::Relaxed),
            active_callbacks: self.active_callbacks.load(Ordering::Relaxed),
            callbacks_started: self.callbacks_started.load(Ordering::Relaxed),
            callbacks_finished: self.callbacks_finished.load(Ordering::Relaxed),
            offsets_persisted: self.offsets_persisted.load(Ordering::Relaxed),
            last_pull_ms: self.last_pull_ms.load(Ordering::Relaxed),
            last_callback_progress_ms: self.last_callback_progress_ms.load(Ordering::Relaxed),
            last_assignment_ms: self.last_assignment_ms.load(Ordering::Relaxed),
            last_offset_ms: self.last_offset_ms.load(Ordering::Relaxed),
            last_route_ok_ms: self.last_route_ok_ms.load(Ordering::Relaxed),
            last_route_miss_ms: self.last_route_miss_ms.load(Ordering::Relaxed),
        }
    }
}

/// What the controller should do next. Effects are executed by the async shell; the machine
/// itself never performs I/O and never holds a resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Effect {
    /// Ask the running incarnation to repair itself in place first.
    Repair(RepairKind),
    /// Stop the current incarnation; `grace` bounds only the caller-visible wait.
    Stop { grace: Duration },
    /// Wait before the next evaluation.
    Idle(Duration),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RepairKind {
    /// Coalesced route/broker repair plus one rescheduled pull per queue epoch.
    RouteAndPull,
    /// Refresh assignment through the existing rebalance path.
    Assignment,
}

/// Phase/health pair derived by the machine; `managed_consumer` projects it into the public
/// status snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MachineState {
    pub phase: crate::consumer::managed_consumer::ConsumerPhase,
    pub health: crate::consumer::managed_consumer::ConsumerHealth,
    pub reason: Reason,
}

/// Pairs delivery accounting for one batch of messages handed to a listener.
///
/// The guard is created when the batch enters the consume path and releases its pending count on
/// drop, so an early return or an unwind inside a user listener cannot leave `active_callbacks` or
/// `pending_delivery_msgs` inflated - the machine would read that as stalled delivery forever. A
/// queue whose callbacks keep finishing is progressing even when its pulls are quiet.
pub(crate) struct DeliveryGuard {
    progress: Arc<ConsumerProgress>,
    queue: MessageQueue,
    messages: u64,
    listener_started: bool,
}

impl DeliveryGuard {
    pub(crate) fn new(progress: &Arc<ConsumerProgress>, queue: &MessageQueue, messages: usize) -> Self {
        Self {
            progress: Arc::clone(progress),
            queue: queue.clone(),
            messages: u64::try_from(messages).unwrap_or(0),
            listener_started: false,
        }
    }

    /// Marks the entry into the application listener; only then is a callback "active".
    pub(crate) fn enter_listener(&mut self) {
        if !self.listener_started {
            self.listener_started = true;
            self.progress.note_callback_started(self.messages);
        }
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.listener_started {
            self.progress.note_callback_finished(self.messages);
        } else {
            self.progress.note_delivery_dropped(self.messages);
        }
        self.progress.touch(&self.queue);
    }
}

/// One serialized lifecycle decision function per managed consumer.
#[derive(Debug)]
pub(crate) struct RecoveryMachine {
    policy: LifecyclePolicy,
    state: MachineState,
    incarnation: u64,
    attempt: u32,
    /// Startup budget for the next start call.
    startup_budget: Duration,
    stop_requested: bool,
    /// Cleanup of a retiring incarnation is not proved complete; no replacement may start.
    cleanup_unproved: bool,
    first_attempt_done: bool,
    /// Consecutive repair passes that produced no observable progress.
    stalled_repairs: u32,
}

impl RecoveryMachine {
    pub(crate) fn new(policy: LifecyclePolicy, initial_incarnation: u64) -> Self {
        Self {
            policy,
            state: MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Starting,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::Starting,
            },
            incarnation: initial_incarnation,
            attempt: 0,
            startup_budget: policy.initial_startup_attempt,
            stop_requested: false,
            cleanup_unproved: false,
            first_attempt_done: false,
            stalled_repairs: 0,
        }
    }

    /// Startup budget for the next start call: the cold-start allowance first, then the shorter
    /// replacement budget.
    pub(crate) fn startup_budget(&self) -> Duration {
        self.startup_budget
    }

    pub(crate) fn state(&self) -> MachineState {
        self.state
    }

    pub(crate) fn incarnation(&self) -> u64 {
        self.incarnation
    }

    pub(crate) fn policy(&self) -> LifecyclePolicy {
        self.policy
    }

    pub(crate) fn stop_requested(&self) -> bool {
        self.stop_requested
    }

    /// Records the sticky stop request. Returns the state to publish immediately.
    pub(crate) fn request_stop(&mut self) -> MachineState {
        self.stop_requested = true;
        self.state.phase = crate::consumer::managed_consumer::ConsumerPhase::Stopping;
        self.state.reason = Reason::Stopping;
        if self.cleanup_unproved {
            self.state.health = crate::consumer::managed_consumer::ConsumerHealth::Blocked;
        }
        self.state
    }

    /// A start attempt finished. `Ok` means the incarnation is running and consuming work may
    /// be observed; `Err(retryable)` means rollback already happened locally.
    pub(crate) fn on_start_result(
        &mut self,
        ok: bool,
        retryable: bool,
        timed_out: bool,
        unit: f64,
    ) -> (MachineState, Vec<Effect>) {
        self.attempt = self.attempt.saturating_add(1);
        self.first_attempt_done = true;
        if ok {
            self.stalled_repairs = 0;
            self.startup_budget = self.policy.replacement_startup_attempt;
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::WaitingForAssignment,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }
        if !retryable {
            // Invalid local configuration or an unrecoverable runtime error: report it and stop
            // spinning. Never a process-exit request.
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Stopped,
                health: crate::consumer::managed_consumer::ConsumerHealth::ConfigurationError,
                reason: Reason::StartupRejected,
            };
            return (self.state, Vec::new());
        }
        if self.stop_requested {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Stopped,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::Stopped,
            };
            return (self.state, Vec::new());
        }
        self.startup_budget = self.policy.replacement_startup_attempt;
        self.state = MachineState {
            phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
            health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
            // A failed attempt that returned early is just a retry in progress; only an attempt
            // that ran out of its own budget is reported as exhausted.
            reason: if timed_out {
                Reason::StartupAttemptExhausted
            } else {
                Reason::Starting
            },
        };
        let delay = if self.attempt == 1 {
            first_retry_delay(&self.policy, unit)
        } else {
            backoff_delay(&self.policy, self.attempt, unit)
        };
        (self.state, vec![Effect::Idle(delay)])
    }

    /// The next incarnation identity; only called after teardown is proved complete.
    pub(crate) fn advance_incarnation(&mut self) -> u64 {
        self.incarnation = self.incarnation.saturating_add(1);
        self.incarnation
    }

    /// Cleanup could not be proved complete: retain the retiring incarnation, report Blocked
    /// and stop creating further incarnations or cleanup workers.
    pub(crate) fn on_cleanup_unproved(&mut self) -> MachineState {
        self.cleanup_unproved = true;
        self.state = MachineState {
            phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
            health: crate::consumer::managed_consumer::ConsumerHealth::Blocked,
            reason: Reason::CleanupUnproved,
        };
        if self.stop_requested {
            self.state.phase = crate::consumer::managed_consumer::ConsumerPhase::Stopping;
            self.state.reason = Reason::CleanupUnproved;
        }
        self.state
    }

    /// Cleanup completed. A pending stop ends the machine; otherwise recovery may continue.
    pub(crate) fn on_cleanup_complete(&mut self) -> MachineState {
        self.cleanup_unproved = false;
        if self.stop_requested {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Stopped,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::Stopped,
            };
        } else {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                reason: Reason::Starting,
            };
        }
        self.state
    }

    /// Periodic evaluation of a running incarnation against the section 3.2 condition table.
    ///
    /// `previous` is the snapshot from the last tick so progress is proven by counters moving,
    /// not by silence alone.
    pub(crate) fn on_tick(
        &mut self,
        snapshot: &ProgressSnapshot,
        previous: Option<&ProgressSnapshot>,
        pull_deadline: Duration,
        unit: f64,
    ) -> (MachineState, Vec<Effect>) {
        if self.stop_requested || self.cleanup_unproved {
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }
        let progressed = previous.is_none_or(|before| snapshot.progressed_since(before));
        if progressed {
            self.stalled_repairs = 0;
        }

        // Healthy idle: explicit zero assignment in CLUSTERING, or successful empty pulls.
        if snapshot.assigned_queues == 0 && !snapshot.has_pending_work() {
            // "Explicitly assigned zero queues" needs evidence: the newest rebalance pass has to
            // have seen a topic route. When it could not, this is a failed assignment refresh and
            // is reported as such. It repairs in place and deliberately does not spend
            // the replacement budget - replacing an incarnation cannot fix a
            // control-plane outage.
            if snapshot.last_route_miss_ms > snapshot.last_route_ok_ms {
                self.state = MachineState {
                    phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                    health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                    reason: Reason::AssignmentRefreshFailed,
                };
                return (
                    self.state,
                    vec![
                        Effect::Repair(RepairKind::Assignment),
                        Effect::Idle(self.policy.progress_check),
                    ],
                );
            }
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::WaitingForAssignment,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }
        if snapshot.pulls_empty > 0 && !snapshot.has_pending_work() && snapshot.stalled_queues == 0 {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::IdleNoMessages,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        // Expected waits: an outstanding long poll inside its deadline, or deliberate flow control.
        if snapshot.pull_within_deadline(pull_deadline) {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::LongPollPending,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        // Pending messages whose callbacks do not advance.
        if snapshot.has_pending_work() {
            let silent = snapshot.age_ms(snapshot.last_callback_progress_ms);
            if silent >= self.policy.pending_delivery_warn && !progressed {
                if snapshot.active_callbacks > 0 {
                    // A synchronous listener is still running: it stays owned and blocked, we
                    // never cancel it, replace the incarnation or ask the app to restart.
                    self.state = MachineState {
                        phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                        health: crate::consumer::managed_consumer::ConsumerHealth::Blocked,
                        reason: Reason::ListenerActiveOverBudget,
                    };
                    return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
                }
                self.state = MachineState {
                    phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                    health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                    reason: Reason::CallbacksStalled,
                };
                return (self.state, vec![Effect::Repair(RepairKind::RouteAndPull)]);
            }
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::None,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        // Transport/route problems with no pending work: repair in place first.
        let transport_reason = if snapshot.route_misses > 0 && !progressed {
            Some(Reason::RouteOrBrokerMiss)
        } else if snapshot.rpc_timeouts > 0 && !progressed {
            Some(Reason::RpcTimeout)
        } else if snapshot.revocations > 0 && snapshot.assigned_queues == 0 && !progressed {
            Some(Reason::AssignmentRefreshFailed)
        } else if snapshot.pulls_started > snapshot.pulls_completed && !progressed {
            Some(Reason::ExpectedPullMissing)
        } else {
            None
        };
        if let Some(reason) = transport_reason {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                reason,
            };
            let repair = match reason {
                Reason::AssignmentRefreshFailed => RepairKind::Assignment,
                _ => RepairKind::RouteAndPull,
            };
            self.stalled_repairs = self.stalled_repairs.saturating_add(1);
            // In-place repair is always tried first. Only after repeated repairs produce no
            // progress does the shell roll one guarded incarnation replacement.
            if self.stalled_repairs > self.policy.max_repairs_before_replace {
                self.stalled_repairs = 0;
                return (self.state, vec![Effect::Stop { grace: Duration::ZERO }]);
            }
            let _ = unit;
            return (
                self.state,
                vec![Effect::Repair(repair), Effect::Idle(self.policy.progress_check)],
            );
        }

        if snapshot.flow_controls > 0 && !progressed {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::FlowControl,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        self.state = MachineState {
            phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
            health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
            reason: Reason::None,
        };
        (self.state, vec![Effect::Idle(self.policy.progress_check)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(topic: &str, id: i32) -> MessageQueue {
        MessageQueue::from_parts(topic, "broker-a", id)
    }

    fn assigned(progress: &ConsumerProgress, count: usize) {
        let set: HashSet<MessageQueue> = (0..count).map(|id| queue("T", id as i32)).collect();
        progress.note_assignment(&set);
    }

    fn snapshot(progress: &ConsumerProgress) -> ProgressSnapshot {
        progress.snapshot(Duration::from_secs(5))
    }

    #[test]
    fn backoff_grows_exponentially_and_stays_within_the_ceiling() {
        let policy = LifecyclePolicy::default();
        // unit = 0.5 means zero jitter, so the arithmetic is exact.
        assert_eq!(backoff_delay(&policy, 1, 0.5), Duration::from_secs(3));
        assert_eq!(backoff_delay(&policy, 2, 0.5), Duration::from_secs(6));
        assert_eq!(backoff_delay(&policy, 3, 0.5), Duration::from_secs(12));
        assert_eq!(backoff_delay(&policy, 40, 0.5), Duration::from_secs(120));
    }

    #[test]
    fn jitter_stays_inside_its_fraction_and_never_goes_negative() {
        let policy = LifecyclePolicy::default();
        for unit in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let delay = backoff_delay(&policy, 3, unit);
            assert!(
                delay >= Duration::from_secs(9) && delay <= Duration::from_secs(15),
                "{unit}: {delay:?}"
            );
        }
    }

    #[test]
    fn first_retry_adds_a_one_off_spread() {
        let policy = LifecyclePolicy::default();
        // unit = 0.5 is the no-jitter midpoint, so the spread shows up exactly.
        assert_eq!(
            first_retry_delay(&policy, 0.5),
            Duration::from_secs(3) + Duration::from_secs(30)
        );
        for unit in [0.0, 0.25, 0.75, 1.0] {
            let delay = first_retry_delay(&policy, unit);
            assert!(delay >= backoff_delay(&policy, 1, unit), "{unit}: {delay:?}");
            assert!(
                delay <= backoff_delay(&policy, 1, unit) + policy.initial_spread_max,
                "{unit}: {delay:?}"
            );
        }
    }

    #[test]
    fn zero_assignment_in_clustering_is_healthy_idle_not_a_restart() {
        let progress = ConsumerProgress::new();
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_tick(&snapshot(&progress), None, Duration::from_secs(30), 0.5);
        assert_eq!(
            (state.phase, state.health, state.reason),
            (
                crate::consumer::managed_consumer::ConsumerPhase::Running,
                crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                Reason::WaitingForAssignment
            )
        );
        // Idle only: no replacement, no repeated registration.
        assert_eq!(effects, vec![Effect::Idle(LifecyclePolicy::default().progress_check)]);
    }

    #[test]
    fn successful_empty_pulls_are_healthy_idle() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 3);
        for id in 0..3 {
            progress.note_pull_completed(&queue("T", id), false);
        }
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_tick(&snapshot(&progress), None, Duration::from_secs(30), 0.5);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Healthy);
        assert_eq!(state.reason, Reason::IdleNoMessages);
        assert_eq!(effects.len(), 1);
    }

    #[test]
    fn an_outstanding_long_poll_inside_its_deadline_is_not_silence() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        for id in 0..2 {
            let q = queue("T", id);
            progress.claim_pull(&q);
            progress.note_pull_started(&q);
        }
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, _) = machine.on_tick(&snapshot(&progress), None, Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::LongPollPending);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Healthy);
    }

    #[test]
    fn one_active_queue_does_not_hide_thousands_of_stalled_queues() {
        let progress = ConsumerProgress::new();
        let many: HashSet<MessageQueue> = (0..5_408).map(|id| queue("T", id)).collect();
        progress.note_assignment(&many);
        // A single queue makes progress; the rest stay silent.
        progress.note_pull_completed(&queue("T", 0), true);
        let snap = progress.snapshot(Duration::from_millis(1));
        assert_eq!(snap.assigned_queues, 5_408);
        assert_eq!(snap.stalled_queues, 5_407);
    }

    /// Section 3.2: only an assignment observed against a known route is "explicitly zero". With no
    /// route evidence the consumer reports a failed assignment refresh and repairs in place.
    #[test]
    fn zero_assignment_without_route_evidence_is_a_fault_not_healthy_idle() {
        let progress = ConsumerProgress::new();
        progress.note_route_observation(false);
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (started, _) = machine.on_start_result(true, true, false, 0.5);
        assert_eq!(started.phase, crate::consumer::managed_consumer::ConsumerPhase::Running);
        let snap = snapshot(&progress);
        let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
        assert_eq!(
            state.phase,
            crate::consumer::managed_consumer::ConsumerPhase::Recovering
        );
        assert_eq!(
            state.health,
            crate::consumer::managed_consumer::ConsumerHealth::Degraded
        );
        assert_eq!(state.reason, Reason::AssignmentRefreshFailed);
        assert!(matches!(effects.first(), Some(Effect::Repair(RepairKind::Assignment))));
    }

    #[test]
    fn an_explicitly_empty_assignment_stays_healthy_idle() {
        let policy = LifecyclePolicy::default();
        let progress = ConsumerProgress::new();
        progress.note_route_observation(true);
        assigned(&progress, 0);
        let mut machine = RecoveryMachine::new(policy, 1);
        machine.on_start_result(true, true, false, 0.5);
        let snap = snapshot(&progress);
        let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
        assert_eq!(state.phase, crate::consumer::managed_consumer::ConsumerPhase::Running);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Healthy);
        assert_eq!(state.reason, Reason::WaitingForAssignment);
        assert_eq!(effects, vec![Effect::Idle(policy.progress_check)]);
    }

    /// A control-plane outage is not fixed by rolling an incarnation, so repeated fruitless repairs
    /// on an unroutable consumer must never reach the replacement effect.
    #[test]
    fn an_unroutable_consumer_never_spends_the_replacement_budget() {
        let policy = LifecyclePolicy::default();
        let progress = ConsumerProgress::new();
        progress.note_route_observation(false);
        let mut machine = RecoveryMachine::new(policy, 1);
        machine.on_start_result(true, true, false, 0.5);
        let snap = snapshot(&progress);
        for _ in 0..policy.max_repairs_before_replace + 8 {
            let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
            assert_eq!(state.reason, Reason::AssignmentRefreshFailed);
            assert!(
                !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
                "replacement would not restore a missing route: {effects:?}"
            );
        }
    }

    #[test]
    fn route_miss_without_progress_repairs_in_place() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        for id in 0..2 {
            progress.note_pull_failed(&queue("T", id), PullFailureKind::RouteOrBrokerMiss);
        }
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let snap = snapshot(&progress);
        let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::RouteOrBrokerMiss);
        assert_eq!(
            state.phase,
            crate::consumer::managed_consumer::ConsumerPhase::Recovering
        );
        assert!(matches!(
            effects.first(),
            Some(Effect::Repair(RepairKind::RouteAndPull))
        ));
    }

    #[test]
    fn stalled_pending_delivery_with_no_active_callback_repairs() {
        let policy = LifecyclePolicy {
            pending_delivery_warn: Duration::ZERO,
            ..Default::default()
        };
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        progress.note_delivery_pending(4);
        let mut machine = RecoveryMachine::new(policy, 1);
        let snap = snapshot(&progress);
        let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::CallbacksStalled);
        assert!(matches!(effects.first(), Some(Effect::Repair(_))));
    }

    #[test]
    fn an_over_budget_active_listener_becomes_blocked_and_is_never_replaced() {
        let policy = LifecyclePolicy {
            pending_delivery_warn: Duration::ZERO,
            ..Default::default()
        };
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        progress.note_delivery_pending(4);
        progress.note_callback_started(4);
        let mut machine = RecoveryMachine::new(policy, 1);
        let snap = snapshot(&progress);
        let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Blocked);
        assert_eq!(state.reason, Reason::ListenerActiveOverBudget);
        // Only an idle tick: the listener stays owned until it returns.
        assert_eq!(effects, vec![Effect::Idle(policy.progress_check)]);
    }

    #[test]
    fn failed_startup_retries_with_bounded_backoff_and_never_exits() {
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_start_result(false, true, true, 0.5);
        assert_eq!(
            state.phase,
            crate::consumer::managed_consumer::ConsumerPhase::Recovering
        );
        assert_eq!(state.reason, Reason::StartupAttemptExhausted);
        assert_eq!(effects.len(), 1);
        let Effect::Idle(first) = effects[0] else {
            panic!("expected a delay, got {:?}", effects[0])
        };
        assert!(first >= Duration::from_secs(3) && first <= Duration::from_secs(63));
        for _ in 0..12 {
            let (_, effects) = machine.on_start_result(false, true, true, 0.5);
            let Effect::Idle(delay) = effects[0] else {
                panic!("expected a delay")
            };
            assert!(delay <= LifecyclePolicy::default().retry_max * 2);
        }
    }

    #[test]
    fn invalid_configuration_reports_without_retry_spin() {
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_start_result(false, false, true, 0.5);
        assert_eq!(
            state.health,
            crate::consumer::managed_consumer::ConsumerHealth::ConfigurationError
        );
        assert_eq!(state.reason, Reason::StartupRejected);
        assert!(effects.is_empty(), "no retry effects may be scheduled: {effects:?}");
    }

    #[test]
    fn unproved_cleanup_blocks_replacement_until_complete() {
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.on_start_result(true, false, false, 0.5);
        let blocked = machine.on_cleanup_unproved();
        assert_eq!(
            blocked.health,
            crate::consumer::managed_consumer::ConsumerHealth::Blocked
        );
        assert_eq!(blocked.reason, Reason::CleanupUnproved);
        // While cleanup is unproved a tick may not schedule anything but an idle wait.
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        let snap = snapshot(&progress);
        let (_, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
        assert_eq!(effects, vec![Effect::Idle(LifecyclePolicy::default().progress_check)]);

        let done = machine.on_cleanup_complete();
        assert_eq!(done.phase, crate::consumer::managed_consumer::ConsumerPhase::Recovering);
    }

    #[test]
    fn stop_is_sticky_across_start_results_and_completes_only_when_clean() {
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.request_stop();
        let (state, _) = machine.on_start_result(false, true, true, 0.5);
        assert_eq!(state.phase, crate::consumer::managed_consumer::ConsumerPhase::Stopped);
        assert_eq!(machine.on_cleanup_complete().reason, Reason::Stopped);

        // A stop during unproved cleanup stays blocked rather than claiming success.
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.request_stop();
        let state = machine.on_cleanup_unproved();
        assert_eq!(state.phase, crate::consumer::managed_consumer::ConsumerPhase::Stopping);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Blocked);
    }

    #[test]
    fn pull_leases_allow_one_in_flight_pull_per_queue_epoch() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        let q = queue("T", 0);
        let epoch = progress.claim_pull(&q).expect("first claim");
        assert!(progress.claim_pull(&q).is_none(), "duplicate pull must be rejected");
        progress.release_pull(&q, epoch + 99);
        assert!(
            progress.claim_pull(&q).is_none(),
            "a stale release may not free the lease"
        );
        progress.release_pull(&q, epoch);
        assert_eq!(progress.claim_pull(&q), Some(epoch));
    }

    #[test]
    fn revocation_drops_queue_ownership_and_bumps_the_epoch_on_reassignment() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        let q = queue("T", 0);
        assert!(progress.owns(&q));
        progress.note_revocation(std::slice::from_ref(&q));
        assert!(!progress.owns(&q));
        assert_eq!(progress.claim_pull(&q), None, "a revoked queue may not pull");
        assigned(&progress, 1);
        assert_eq!(progress.claim_pull(&q), Some(1), "reassignment starts a fresh epoch");
    }

    #[test]
    fn callback_bookkeeping_never_underflows_pending_counts() {
        let progress = ConsumerProgress::new();
        progress.note_delivery_pending(2);
        progress.note_callback_started(2);
        progress.note_callback_finished(2);
        let snap = snapshot(&progress);
        assert_eq!(snap.pending_delivery_msgs, 0);
        assert_eq!(snap.active_callbacks, 0);
        assert_eq!(snap.callbacks_started, 1);
        assert_eq!(snap.callbacks_finished, 1);
    }

    #[test]
    fn a_startup_attempt_that_fails_early_is_a_retry_not_an_exhausted_budget() {
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_start_result(false, true, false, 0.5);
        assert_eq!(state.reason, Reason::Starting);
        assert!(!effects.is_empty(), "a retryable failure schedules one bounded retry");

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, _) = machine.on_start_result(false, true, true, 0.5);
        assert_eq!(state.reason, Reason::StartupAttemptExhausted);
    }

    #[test]
    fn a_delivery_guard_releases_a_batch_that_never_reached_a_listener() {
        let progress = ConsumerProgress::new();
        let queue = queue("t", 0);
        progress.note_assignment(&HashSet::from([queue.clone()]));
        progress.note_delivery_pending(3);
        {
            // A revoked queue returns before the listener runs; Drop must still release the batch.
            let _guard = DeliveryGuard::new(&progress, &queue, 3);
        }
        let snap = snapshot(&progress);
        assert_eq!(snap.pending_delivery_msgs, 0);
        assert_eq!(snap.active_callbacks, 0);
        assert_eq!(snap.callbacks_started, 0, "a batch never delivered is not a callback");
        assert_eq!(snap.callbacks_finished, 0);
    }

    #[test]
    fn a_delivery_guard_pairs_an_unwinding_listener() {
        let progress = ConsumerProgress::new();
        let queue = queue("t", 0);
        progress.note_assignment(&HashSet::from([queue.clone()]));
        progress.note_delivery_pending(2);
        // Drop runs on unwind exactly as it does on return, so an exception inside a user listener
        // cannot leave active callbacks behind.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut guard = DeliveryGuard::new(&progress, &queue, 2);
            guard.enter_listener();
            panic!("listener unwound");
        }));
        assert!(result.is_err());
        let snap = snapshot(&progress);
        assert_eq!(snap.pending_delivery_msgs, 0);
        assert_eq!(snap.active_callbacks, 0);
        assert_eq!(snap.callbacks_started, 1);
        assert_eq!(snap.callbacks_finished, 1);
        // The queue stamped progress on the way out, so it is not counted as stalled.
        assert_eq!(snapshot(&progress).stalled_queues, 0);
    }

    #[test]
    fn reasons_are_bounded_and_never_carry_identifiers() {
        // Reasons are a closed enum; the string form must stay label-safe.
        for reason in [
            Reason::None,
            Reason::RouteOrBrokerMiss,
            Reason::RpcTimeout,
            Reason::CallbacksStalled,
            Reason::CleanupUnproved,
        ] {
            assert!(!reason.as_str().contains(' '), "{}", reason.as_str());
            assert!(reason.as_str().len() <= 32);
        }
    }
}
