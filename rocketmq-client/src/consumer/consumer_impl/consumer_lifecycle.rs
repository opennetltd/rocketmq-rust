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

impl LifecyclePolicy {
    /// How long the SDK allows one cleanup pass for its own finalization work - local quiescence,
    /// offset persistence and unregistration. It is deliberately unrelated to how long the first
    /// observer happens to wait: an observer that gives up early must not decide how much remote
    /// opportunity a consumer gets (plan 49b R2). `cleanup_budget` targets local quiescence; the
    /// remaining allowance is for the remote steps behind it.
    pub(crate) fn cleanup_session_ceiling(&self) -> std::time::Duration {
        self.cleanup_budget.saturating_mul(3)
    }
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
    // Growth happens in integer milliseconds with a saturating shift, and the ceiling is applied
    // before any floating-point number appears. Computing `retry_min * 2^attempt` in seconds and
    // converting afterwards panics inside `Duration::from_secs_f64` once the product overflows (at
    // attempt 64 with a three-second floor), and narrowing the attempt to `i32` turns large counts
    // into negative exponents. Saturating at the ceiling is also the only honest result: this
    // policy cannot express a delay above `retry_max`, so growth beyond it is invisible.
    let base_ms = millis_bounded(policy.retry_min).saturating_mul(1u64 << attempt.saturating_sub(1).min(63));
    let ceiling_ms = millis_bounded(policy.retry_max);
    let base_ms = base_ms.min(ceiling_ms);
    // Symmetric jitter around a value already inside the ceiling; clamped again so a wide ratio can
    // never push a delay past it or below half the floor.
    let factor = 1.0 + policy.jitter_ratio.clamp(0.0, 1.0) * (2.0 * unit - 1.0);
    let jittered_ms = (base_ms as f64 * factor).clamp(0.0, ceiling_ms as f64).ceil() as u64;
    // Ordered by hand: `Ord::clamp` panics when the bounds are inverted, and a policy with
    // retry_max below half of retry_min must produce a delay rather than take the process down.
    let floor_ms = millis_bounded(policy.retry_min / 2).min(ceiling_ms);
    Duration::from_millis(jittered_ms.max(floor_ms).min(ceiling_ms))
}

/// Milliseconds without ever truncating a duration that outlives `u64`.
fn millis_bounded(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
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
    /// Queues this incarnation owns whose pull loop is not running and is not advancing.
    pub lost_pull_queues: u64,
    /// Queues with a pull request in flight right now; the only honest evidence of an outstanding
    /// long poll.
    pub leased_queues: u64,
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

    /// A long poll that is still inside its own deadline is an expected wait, not silence. Judged
    /// by a queue actually holding its pull lease now, not by lifetime counters that stay
    /// unequal forever (plan 49a F4 section 4.3 item 4).
    pub(crate) fn pull_within_deadline(&self, deadline: Duration) -> bool {
        self.leased_queues > 0 && self.age_ms(self.last_pull_ms) < deadline
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

/// One in-flight pull of one queue acquisition (plan 49a F4 section 4.3 item 3).
///
/// The token *is* the lease: it releases when the last reference to the pull loop goes away, so a
/// request that ends for any reason - answer arrived and was not re-dispatched, queue revoked,
/// consumer stopped, request dropped while still queued - cannot leave its queue permanently
/// leased. A retry of the same loop carries the same token and therefore keeps holding it, which is
/// what makes "one pull per queue" hold across delayed retries instead of only within a single RPC.
pub(crate) struct PullLease {
    progress: Arc<ConsumerProgress>,
    queue: MessageQueue,
    epoch: u64,
}

impl PullLease {
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Drop for PullLease {
    fn drop(&mut self) {
        self.progress.release_pull(&self.queue, self.epoch);
    }
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
    /// Source of queue epochs for this incarnation. Epochs are never reused: a revoked queue and
    /// the same queue assigned again must carry different numbers, or a lease claim, retry or
    /// completion captured under the old assignment would be accepted by the new one (plan 49a
    /// F4).
    next_epoch: AtomicU64,
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

    /// The epoch this incarnation's current acquisition of `queue` carries, if it owns the queue at
    /// all.
    pub(crate) fn current_epoch(&self, queue: &MessageQueue) -> Option<u64> {
        self.queues.read().get(queue).map(|entry| entry.epoch)
    }

    /// Whether `epoch` still names the acquisition this token was taken under (plan 49a F4 section
    /// 4.3 item 2: an answer or retry must publish only into the acquisition that issued it).
    pub(crate) fn is_current(&self, queue: &MessageQueue, epoch: u64) -> bool {
        self.current_epoch(queue) == Some(epoch)
    }

    /// Takes the single in-flight lease for `queue` under its current epoch. Returns `None` when
    /// another pull loop already holds it or the queue is not assigned to this incarnation.
    pub(crate) fn acquire_pull_lease(self: &Arc<Self>, queue: &MessageQueue) -> Option<Arc<PullLease>> {
        let epoch = self.claim_pull(queue)?;
        Some(Arc::new(PullLease {
            progress: Arc::clone(self),
            queue: queue.clone(),
            epoch,
        }))
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

    /// Publishes a new assignment. Queues seen for the first time, or again after revocation, start
    /// a fresh (strictly newer) epoch. A retained queue is left exactly as it was - same epoch,
    /// same in-flight lease: publishing an unchanged answer is not evidence that the pull
    /// already running on that queue finished, and clearing its lease would let a second pull
    /// stack onto it (plan 49a F4).
    pub(crate) fn note_assignment(&self, assigned: &HashSet<MessageQueue>) {
        let now = get_current_millis();
        self.bump(&self.assignments);
        self.last_assignment_ms.store(now, Ordering::Relaxed);
        let mut queues = self.queues.write();
        for queue in assigned {
            match queues.get_mut(queue) {
                // Retained: identity and lease are the running work's, not this pass's to reset.
                Some(_) => {}
                None => {
                    // A fresh acquisition always receives a strictly larger epoch than any this
                    // incarnation handed out before, including the one this queue held when it was
                    // revoked. Retained queues keep theirs (plan 49a F4 section 4.3 item 2).
                    let epoch = self.next_epoch.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                    queues.insert(
                        queue.clone(),
                        QueueProgress {
                            epoch,
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

    /// How long a queue may stay silent before its pull loop counts as lost or as stalled. Derived
    /// from the consumer's own long-poll reserve so it adapts when an application tunes that
    /// timeout.
    /// Queues this incarnation owns whose pull loop is neither running nor advancing: the request
    /// was dropped, its delayed retry never arrived, or a callback finished without
    /// re-dispatching (plan 49a F4 section 4.3 item 5). A queue holding its lease is an
    /// outstanding pull and not a lost one, however old its last answer looks: the RPC timeout
    /// bounds it.
    pub(crate) fn lost_pulls(&self, stall_after: Duration) -> Vec<MessageQueue> {
        let now_ms = get_current_millis();
        let window = u64::try_from(stall_after.as_millis()).unwrap_or(u64::MAX);
        self.queues
            .read()
            .iter()
            .filter(|(_, entry)| !entry.pull_leased && now_ms.saturating_sub(entry.last_progress_ms) >= window)
            .map(|(queue, _)| queue.clone())
            .collect()
    }

    pub(crate) fn assigned_queues(&self) -> u64 {
        self.queues.read().len() as u64
    }

    pub(crate) fn snapshot(&self, stall_after: Duration) -> ProgressSnapshot {
        let now_ms = get_current_millis();
        let queues = self.queues.read();
        let stall = u64::try_from(stall_after.as_millis()).unwrap_or(u64::MAX);
        let mut stalled_queues = 0u64;
        let mut lost_pull_queues = 0u64;
        let mut leased_queues = 0u64;
        for entry in queues.values() {
            let silent = now_ms.saturating_sub(entry.last_progress_ms) >= stall;
            if entry.pull_leased {
                leased_queues += 1;
            } else if silent {
                lost_pull_queues += 1;
            }
            if silent {
                stalled_queues += 1;
            }
        }
        ProgressSnapshot {
            now_ms,
            started_at_ms: self.started_at_ms.load(Ordering::Relaxed),
            assigned_queues: queues.len() as u64,
            stalled_queues,
            lost_pull_queues,
            leased_queues,
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

/// Outcome of one synchronous user-code invocation.
#[derive(Debug)]
pub(crate) enum ListenerCall<T> {
    Returned(T),
    /// The listener unwound instead of returning. Nothing downstream may assume the messages or the
    /// consume context were left untouched, so callers treat it as failed consumption.
    ThrewPanic,
}

/// Run a synchronous listener call with an unwinding panic converted into a value instead of
/// letting it reach the task root (plan 49a F5 section 4.4 item 2). User code may abort through a
/// foreign frame or simply panic; either way the batch must still be recorded as failed consumption
/// and released, which is what the caller's existing retry/send-back path and delivery guard do
/// once the call returns normally.
pub(crate) fn call_listener_catching<T, F>(call: F) -> ListenerCall<T>
where
    F: FnOnce() -> T,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)) {
        Ok(value) => ListenerCall::Returned(value),
        Err(_) => ListenerCall::ThrewPanic,
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
    /// Re-dispatch the pull of a queue whose loop was lost, at this consumer's committed offset,
    /// under that acquisition's lease (plan 49a F4 section 4.3 item 5).
    LostPull,
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
        // Per-queue faults are evaluated before any aggregate "everything is fine" answer, so that
        // a live long poll on one queue cannot hide a stalled callback or a lost pull loop
        // on another (plan 49a F4 section 4.3 item 4).

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
        }

        // A queue this incarnation owns whose pull loop is neither running nor advancing. Judged
        // per queue and by current state rather than by lifetime counters, so one busy
        // queue cannot excuse an idle neighbour and a healthy past cannot excuse a stopped
        // present (plan 49a F4 section 4.3 item 5).
        if snapshot.lost_pull_queues > 0 && !progressed {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                reason: Reason::ExpectedPullMissing,
            };
            self.stalled_repairs = self.stalled_repairs.saturating_add(1);
            if self.stalled_repairs > self.policy.max_repairs_before_replace {
                self.stalled_repairs = 0;
                return (self.state, vec![Effect::Stop { grace: Duration::ZERO }]);
            }
            return (
                self.state,
                vec![
                    Effect::Repair(RepairKind::LostPull),
                    Effect::Idle(self.policy.progress_check),
                ],
            );
        }

        // Expected waits: successful empty pulls, or an outstanding long poll inside its deadline.
        if snapshot.pulls_empty > 0 && !snapshot.has_pending_work() && snapshot.stalled_queues == 0 {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::IdleNoMessages,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }
        if snapshot.pull_within_deadline(pull_deadline) {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::LongPollPending,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        // Pending work that is advancing: busy rather than blocked.
        if snapshot.has_pending_work() {
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

    /// Plan 50 F3: the attempt counter is a lifetime total and may reach any `u32`. Growing the
    /// delay must saturate at the ceiling for every one of them - never panic, never exceed it -
    /// whatever jitter draw accompanies it.
    #[test]
    fn backoff_saturates_for_every_attempt_without_panicking() {
        let policy = LifecyclePolicy::default();
        for attempt in [0, 1, 2, 31, 32, 62, 63, 64, 65, 100, 1_024, u32::MAX - 1, u32::MAX] {
            for unit in [0.0, 0.25, 0.5, 0.75, 1.0] {
                let delay = backoff_delay(&policy, attempt, unit);
                assert!(
                    delay >= policy.retry_min / 2 && delay <= policy.retry_max,
                    "attempt {attempt} unit {unit}: {delay:?}"
                );
            }
        }
        // Once the exponent passes what the ceiling can absorb, the answer is the ceiling itself
        // (modulo jitter), not a wrapped or negative value.
        assert_eq!(backoff_delay(&policy, u32::MAX, 0.5), policy.retry_max);
        assert_eq!(backoff_delay(&policy, 64, 0.5), policy.retry_max);
    }

    /// Degenerate policies must not be able to panic the arithmetic either.
    #[test]
    fn backoff_survives_degenerate_policies() {
        let zero_floor = LifecyclePolicy {
            retry_min: Duration::ZERO,
            ..LifecyclePolicy::default()
        };
        assert_eq!(backoff_delay(&zero_floor, 7, 0.5), Duration::ZERO);

        let huge_ceiling = LifecyclePolicy {
            retry_max: Duration::from_secs(u64::MAX / 2),
            ..LifecyclePolicy::default()
        };
        assert!(backoff_delay(&huge_ceiling, u32::MAX, 1.0) <= huge_ceiling.retry_max);

        // An inverted policy (ceiling below the floor) still yields a bounded delay.
        let inverted = LifecyclePolicy {
            retry_min: Duration::from_secs(60),
            retry_max: Duration::from_secs(5),
            ..LifecyclePolicy::default()
        };
        assert!(backoff_delay(&inverted, 3, 0.5) <= Duration::from_secs(5));

        // A jitter ratio outside [0, 1] is clamped rather than trusted.
        let wild = LifecyclePolicy {
            jitter_ratio: -1.0,
            ..LifecyclePolicy::default()
        };
        assert!(backoff_delay(&wild, 3, 0.0) <= wild.retry_max);
        let wild = LifecyclePolicy {
            jitter_ratio: 9.0,
            ..LifecyclePolicy::default()
        };
        assert!(backoff_delay(&wild, 3, 1.0) <= wild.retry_max);
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
        let first_epoch = progress.claim_pull(&q).expect("an assigned queue may pull");
        progress.note_revocation(std::slice::from_ref(&q));
        assert!(!progress.owns(&q));
        assert_eq!(progress.claim_pull(&q), None, "a revoked queue may not pull");
        assigned(&progress, 1);

        // Re-acquisition must be distinguishable from the assignment it replaced: an epoch is the
        // only identity a queued retry or an in-flight completion carries back.
        let current_epoch = progress.claim_pull(&q).expect("a reassigned queue may pull again");
        assert!(
            current_epoch > first_epoch,
            "an epoch must never be reused within an incarnation, saw {current_epoch} after {first_epoch}"
        );

        // A release arriving with the revoked epoch cannot unlock the new lease.
        progress.release_pull(&q, first_epoch);
        assert_eq!(
            progress.claim_pull(&q),
            None,
            "a stale release must not free a live lease"
        );
        progress.release_pull(&q, current_epoch);
        assert_eq!(progress.claim_pull(&q), Some(current_epoch));
    }

    /// Plan 49a F4 section 4.3 item 2: an unchanged rebalance pass must not disturb what a queue
    /// already owns - its epoch and its in-flight lease - while every later acquisition is
    /// newer than the last.
    /// Plan 49a F5 section 4.4 item 2: a listener that unwinds must be data, not a dead task, and
    /// the batch accounting must still unwind with it.
    /// Plan 49a F4 section 4.3 items 2 and 3: one in-flight pull per queue, held for the whole loop
    /// rather than one RPC, released by the last reference, and unable to publish once its
    /// acquisition is gone.
    #[test]
    fn a_pull_lease_covers_the_loop_and_dies_with_it() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        let q = queue("T", 0);

        let lease = progress.acquire_pull_lease(&q).expect("an assigned queue may pull");
        let epoch = lease.epoch();
        assert!(
            progress.acquire_pull_lease(&q).is_none(),
            "a second request must not stack behind a live pull"
        );

        // A delayed retry is the same loop: it keeps holding the token it already has, and dropping
        // one reference to that token must not open the queue to a duplicate.
        let retry = Arc::clone(&lease);
        drop(lease);
        assert!(
            progress.acquire_pull_lease(&q).is_none(),
            "the loop still owns its lease"
        );

        // Only the end of the loop releases it, and releasing does not change the acquisition's
        // identity.
        drop(retry);
        let again = progress
            .acquire_pull_lease(&q)
            .expect("a released queue may be pulled again");
        assert_eq!(epoch, again.epoch(), "release must not renumber the acquisition");

        // Once revoked, nothing that was issued under the old epoch may publish into it.
        progress.note_revocation(std::slice::from_ref(&q));
        assert!(!progress.is_current(&q, epoch), "a revoked queue is not current");
        drop(again); // releasing a lease whose queue is gone must be harmless

        assigned(&progress, 1);
        let fresh = progress
            .acquire_pull_lease(&q)
            .expect("a reassigned queue may pull again");
        assert!(fresh.epoch() > epoch, "the new acquisition has its own identity");
        assert!(!progress.is_current(&q, epoch), "an expired lease cannot publish");
    }

    /// Plan 49a F4 section 4.3 item 4: a live long poll on one queue must not aggregate away a
    /// neighbour whose pull loop is gone, and the fault has to name the missing pull instead of
    /// reporting health.
    #[test]
    fn a_live_long_poll_does_not_hide_a_lost_pull_on_another_queue() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        // Queue 0 holds an outstanding long poll; queue 1 has no request in flight and is not
        // advancing.
        let live = queue("T", 0);
        assert!(progress.claim_pull(&live).is_some());
        progress.note_pull_started(&live);

        // Let a one-millisecond silence window pass: both queues are now silent, and what tells
        // them apart is that one still has its request in flight. Ageing only moves one
        // way, so the expectation cannot depend on how long the scheduler takes.
        std::thread::sleep(Duration::from_millis(5));
        let before = progress.snapshot(Duration::from_millis(1));
        let snap = progress.snapshot(Duration::from_millis(1));
        assert_eq!(snap.leased_queues, 1, "one queue is actively pulling");
        assert_eq!(snap.lost_pull_queues, 1, "the other queue lost its pull loop");

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        // Two passes with identical counters: nothing moved between them, so silence here is
        // evidence rather than the absence of an observation.
        assert!(!snap.progressed_since(&before));
        let (state, effects) = machine.on_tick(&snap, Some(&before), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::ExpectedPullMissing);
        assert_eq!(
            state.health,
            crate::consumer::managed_consumer::ConsumerHealth::Degraded
        );
        assert!(
            matches!(effects.first(), Some(Effect::Repair(RepairKind::LostPull))),
            "the repair must re-dispatch the lost pull rather than refresh routes: {effects:?}"
        );
    }

    /// A queue holding its lease is an outstanding pull, not a lost one, however old its last
    /// answer looks; otherwise every slow long poll would look like a dead loop.
    #[test]
    fn an_outstanding_pull_is_never_counted_as_lost() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 1);
        let q = queue("T", 0);
        assert!(progress.claim_pull(&q).is_some());
        progress.note_pull_started(&q);
        // Let the silence window pass while the request stays in flight: what keeps this queue from
        // being called lost is the held lease and nothing else.
        std::thread::sleep(Duration::from_millis(5));
        let snap = progress.snapshot(Duration::from_millis(1));
        assert_eq!(snap.stalled_queues, 1, "it has been silent for a while");
        assert_eq!(snap.lost_pull_queues, 0, "but the request is still in flight");

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, _) = machine.on_tick(&snap, None, Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::LongPollPending);
    }

    #[test]
    fn a_panicking_listener_is_returned_as_a_failed_call() {
        let quiet = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = call_listener_catching(|| -> u32 { panic!("listener exploded") });
        std::panic::set_hook(quiet);
        assert!(matches!(outcome, ListenerCall::ThrewPanic));

        // Accounting attached to the frame is released by the same unwind.
        struct Released(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Released {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let quiet = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        call_listener_catching(|| {
            let _guard = Released(released.clone());
            panic!("listener exploded");
        });
        std::panic::set_hook(quiet);
        assert!(released.load(Ordering::SeqCst), "the batch guard must unwind too");

        // A listener that returns normally is untouched by the wrapper.
        assert!(matches!(call_listener_catching(|| 7u32), ListenerCall::Returned(7)));
    }

    #[test]
    fn retained_queues_keep_their_epoch_and_lease_across_unchanged_passes() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        let q0 = queue("T", 0);
        let q1 = queue("T", 1);
        let epoch = progress.claim_pull(&q0).expect("assigned queue may pull");

        // The same assignment published again: the lease is still held by its owner.
        assigned(&progress, 2);
        assert_eq!(
            progress.claim_pull(&q0),
            None,
            "an unchanged pass must not steal a lease"
        );
        progress.release_pull(&q0, epoch);
        assert_eq!(
            progress.claim_pull(&q0),
            Some(epoch),
            "an unchanged pass must keep the queue's epoch"
        );

        // Revoking one neighbour cannot change the other queue's identity.
        progress.note_revocation(std::slice::from_ref(&q1));
        assert_eq!(
            progress.claim_pull(&q0),
            None,
            "unrelated revocation must not free a lease"
        );
        let mut previous = epoch;
        for _ in 0..4 {
            progress.note_revocation(std::slice::from_ref(&q0));
            assigned(&progress, 2);
            let next = progress.claim_pull(&q0).expect("re-acquired queue may pull");
            assert!(
                next > previous,
                "epochs must increase monotonically: {next} <= {previous}"
            );
            previous = next;
        }
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

    /// Plan 49 section 5: a hundred repair/replacement cycles may not accumulate per-consumer
    /// bookkeeping. Fruitless repairs roll guarded incarnations; the shared progress state they
    /// hand to the next incarnation has to come back to baseline - no leaked pull leases, no
    /// queue entries added or lost by rolling, and a bounded repair counter.
    #[test]
    fn a_hundred_replacement_cycles_return_resources_to_baseline() {
        let policy = LifecyclePolicy::default();
        let progress = ConsumerProgress::new();
        assigned(&progress, 8);
        let mut machine = RecoveryMachine::new(policy, 1);
        machine.on_start_result(true, true, false, 0.5);

        let mut replacements = 0u32;
        let mut ticks = 0u32;
        while replacements < 100 {
            ticks += 1;
            assert!(ticks < 10_000, "replacements are not being reached");
            for id in 0..8 {
                let mq = queue("T", id);
                // One pull attempt per queue per cycle, taken and given back like the real pull
                // service does, so a leaked lease would show up as a failed claim next cycle.
                if let Some(epoch) = progress.claim_pull(&mq) {
                    progress.release_pull(&mq, epoch);
                }
                progress.note_pull_failed(&mq, PullFailureKind::RouteOrBrokerMiss);
            }
            let snap = snapshot(&progress);
            let (state, effects) = machine.on_tick(&snap, Some(&snap), Duration::from_secs(30), 0.5);
            assert_eq!(
                state.phase,
                crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                "a fruitless repair never becomes terminal: {state:?}"
            );
            assert!(machine.stalled_repairs <= policy.max_repairs_before_replace);
            if effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })) {
                replacements += 1;
                // The controller proves cleanup before the next incarnation is built.
                machine.on_cleanup_complete();
            }
        }

        assert_eq!(8, progress.assigned_queues(), "rolling incarnations changed ownership");
        for id in 0..8 {
            let mq = queue("T", id);
            assert!(
                progress.claim_pull(&mq).is_some(),
                "pull lease for queue {id} leaked across {} replacements",
                replacements
            );
        }
        // A retiring incarnation leaves nothing behind: revoking the assignment empties the table.
        let all: Vec<MessageQueue> = (0..8).map(|id| queue("T", id)).collect();
        progress.note_revocation(&all);
        assert_eq!(0, progress.assigned_queues());
    }

    /// Plan 49b R2: a stop that arrives while cleanup is running has to be visible before the pass
    /// completes, because completion is what settles a handle's proof. The controller therefore
    /// reconciles the stop into the machine ahead of every cleanup-complete transition, and
    /// completion can only end the machine - never resume recovery behind a requested stop.
    #[test]
    fn a_stop_reconciled_before_cleanup_completion_ends_the_machine() {
        use crate::consumer::managed_consumer::ConsumerHealth;
        use crate::consumer::managed_consumer::ConsumerPhase;
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        // While the pass runs the stop is already reported, and an unproved pass shows why nothing
        // has been released.
        let stopping = machine.request_stop();
        assert_eq!(stopping.phase, ConsumerPhase::Stopping);
        let blocked = machine.on_cleanup_unproved();
        assert_eq!(blocked.phase, ConsumerPhase::Stopping);
        assert_eq!(blocked.health, ConsumerHealth::Blocked);
        // Completion after that can only be terminal.
        let settled = machine.on_cleanup_complete();
        assert_eq!(settled.phase, ConsumerPhase::Stopped);
        assert_eq!(settled.reason, Reason::Stopped);
    }

    /// Without a requested stop the same completion leaves recovery available; reconciliation must
    /// not turn an ordinary repair into a shutdown.
    #[test]
    fn cleanup_completion_without_a_stop_keeps_recovery_available() {
        use crate::consumer::managed_consumer::ConsumerHealth;
        use crate::consumer::managed_consumer::ConsumerPhase;
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.on_cleanup_unproved();
        let settled = machine.on_cleanup_complete();
        assert_eq!(settled.phase, ConsumerPhase::Recovering);
        assert_eq!(settled.health, ConsumerHealth::Degraded);
    }

    /// Plan 49b R2: how much finalization a cleanup pass gets is a property of the lifecycle
    /// policy. An observer's wait must not be able to size it, so nothing here reads a
    /// deadline.
    #[test]
    fn the_finalization_ceiling_follows_the_cleanup_budget() {
        let policy = LifecyclePolicy {
            cleanup_budget: Duration::from_secs(7),
            ..LifecyclePolicy::default()
        };
        assert_eq!(policy.cleanup_session_ceiling(), Duration::from_secs(21));
        let default = LifecyclePolicy::default();
        assert_eq!(
            default.cleanup_session_ceiling(),
            default.cleanup_budget * 3,
            "the ceiling must exceed the local quiescence target so remote steps keep an opportunity"
        );
    }
}
