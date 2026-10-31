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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::TimeUtils::get_current_millis;
use tracing::debug;
use tracing::error;

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
    /// Remote opportunity granted to bulk offset persistence, counted from the moment local work
    /// has drained. A listener that releases late therefore cannot spend it, and a fast drain
    /// does not let persistence run past this bound either (plan 49c C5).
    pub remote_persist_opportunity: Duration,
    /// What unregistration and client teardown keep for themselves. It is set aside before bulk
    /// persistence starts, so the last steps of cleanup cannot be starved by the writes before them
    /// (plan 49c C5).
    pub finalization_reserve: Duration,
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
            remote_persist_opportunity: Duration::from_secs(10),
            finalization_reserve: Duration::from_secs(7),
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
#[derive(Clone, Debug, Default)]
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
    /// Queues sitting in a scheduled retry or a deliberate flow control wait (plan 49c C2). These
    /// wait on purpose and stay healthy until their own expectation fails.
    pub waiting_queues: u64,
    /// Queues whose newest outcome is a route miss / RPC timeout rather than completed work.
    pub route_miss_queues: u64,
    pub rpc_timeout_queues: u64,
    /// When the longest-running listener call entered user code, `0` when none is active. One
    /// blocked callback therefore stays visible while other callbacks keep churning (plan 49c C2).
    pub oldest_callback_ms: u64,
    /// Releases that asked for more outstanding work than there was. Any non-zero value means the
    /// delivery ownership pairing is broken somewhere, and every other delivery count should be
    /// read with that in mind (plan 49c C4).
    pub accounting_underflows: u64,
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
    /// Messages whose listener call already returned but whose offsets are not committed yet. Kept
    /// apart from `pending_delivery_msgs` because the two phases need different remedies: queued or
    /// in-listener work can be slow in user code, while commit-phase work is SDK bookkeeping that
    /// cleanup must still drain (plan 49c C4).
    pub committing_delivery_msgs: u64,
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
    /// Newest completed pull, and newest failed pull outcome by kind. Compared with each other, so
    /// they describe the present rather than a lifetime tally (plan 49c C2).
    pub last_pull_ok_ms: u64,
    pub pull_route_miss_ms: u64,
    pub pull_rpc_timeout_ms: u64,
    /// What the latest completed allocation pass concluded **for each topic**, ordered by topic.
    /// A zero assignment is only "explicitly idle" when a pass actually answered for that
    /// subscription; which answer it was decides whether health repairs in place or reports a
    /// failed refresh, and one topic's answer never stands in for another's (plan 49c C3).
    /// Empty means no pass has reported yet, so health falls back to route observations alone.
    pub allocations: Vec<(CheetahString, AllocationOutcome, u64)>,
}

impl ProgressSnapshot {
    /// Subscriptions whose latest completed pass did not get what it asked for, in topic order.
    pub(crate) fn failed_allocations(&self) -> impl Iterator<Item = &(CheetahString, AllocationOutcome, u64)> {
        self.allocations.iter().filter(|(_, outcome, _)| outcome.is_failure())
    }

    /// Whether every reported subscription was explicitly given nothing. Used only to decide
    /// whether a zero-assignment consumer is idle; a single failed record makes it false.
    pub(crate) fn all_allocations_explicit_zero(&self) -> bool {
        !self.allocations.is_empty()
            && self
                .allocations
                .iter()
                .all(|(_, outcome, _)| *outcome == AllocationOutcome::ExplicitZero)
    }

    fn age_ms(&self, since_ms: u64) -> Duration {
        Duration::from_millis(self.now_ms.saturating_sub(since_ms))
    }

    /// True when work is outstanding: messages are queued for, inside, or just past a listener
    /// whose offsets are not committed yet.
    pub(crate) fn has_pending_work(&self) -> bool {
        self.pending_delivery_msgs > 0 || self.committing_delivery_msgs > 0 || self.active_callbacks > 0
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
    /// When this acquisition last **completed** work. Only completion refreshes it, so a queue that
    /// keeps launching retries or waiting on flow control still reads as silent (plan 49c C2).
    last_progress_ms: u64,
    /// Whether a pull loop currently holds this queue's single in-flight lease. The lease belongs
    /// to the **whole loop** - it survives successful answers and delayed continuations - so it
    /// says nothing by itself about whether an RPC is outstanding (plan 49d R1).
    pull_leased: bool,
    /// Identity of the pull attempt currently handed to the transport, `0` while the loop owns the
    /// queue with no RPC in flight. Kept apart from the lease so completing a pull can retire its
    /// own attempt without touching loop ownership, and so a late answer for an older attempt
    /// cannot finish the one now running.
    attempt_id: u64,
    /// When **this attempt** was handed over and when its own answer is due, `0`/`0` while none is
    /// in flight. An outstanding long poll inside its own deadline is an expected wait; only
    /// that deadline passing means the answer is never coming (plan 49d R1).
    attempt_started_ms: u64,
    attempt_deadline_ms: u64,
    /// Not before this instant may the next attempt run: a scheduled retry or a deliberate flow
    /// control wait. Waiting is healthy until its own expectation fails (plan 49c C2).
    wait_until_ms: u64,
    /// When this acquisition last **answered a pull successfully**. Kept apart from
    /// `last_progress_ms`, which any local activity refreshes, because only a newer successful pull
    /// says the route or transport that failed has recovered (plan 49e E2).
    last_pull_ok_ms: u64,
    /// Newest failed outcome for this acquisition, compared against `last_pull_ok_ms` so an old
    /// failure cannot stay current forever and no unrelated activity can retire it.
    failure_ms: u64,
    failure_kind: PullFailureKind,
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

/// What one completed allocation pass concluded (plan 49a F4 section 4.3 item 1). Only consumers
/// that track assignment through the SDK record these; other consumers keep inferring from route
/// observations alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub(crate) enum AllocationOutcome {
    /// The pass read the control plane and this consumer was explicitly assigned no queues.
    ExplicitZero,
    /// The pass read the control plane and this consumer was assigned at least one queue, and that
    /// queue's preparation succeeded well enough to keep working on it.
    QueuesOwned,
    /// The topic has no route to allocate from: there is nothing to consume yet, and no pull loop
    /// exists. Reported apart from a lookup failure because the two need different repairs.
    RouteAbsent,
    /// The pass could not read what it needed - typically the consumer list of the topic.
    LookupFailed,
    /// Queues were allocated but a newly acquired queue's starting offset could not be prepared, so
    /// no pull loop was started for it.
    OffsetPreparationFailed,
}

impl AllocationOutcome {
    /// Whether this conclusion means the subscription did not get what it asked for. A sibling
    /// topic's explicit zero must never certify a failed topic as idle (plan 49c C3), so health
    /// asks each record this question instead of reading one shared answer.
    pub(crate) fn is_failure(self) -> bool {
        matches!(
            self,
            Self::RouteAbsent | Self::LookupFailed | Self::OffsetPreparationFailed
        )
    }
}

/// One topic's latest completed allocation pass, kept so that evidence belongs to the subscription
/// it describes.
pub(crate) struct AllocationRecord {
    pub(crate) outcome: AllocationOutcome,
    /// Millisecond timestamp of the pass that produced this conclusion.
    pub(crate) at_ms: u64,
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
    /// Messages in the post-listener commit phase, held by a [`DeliveryOwner`] until its batch is
    /// committed or discarded.
    committing_delivery_msgs: AtomicU64,
    /// Times a delivery counter was released more than it was admitted - evidence of two owners for
    /// one batch, never a condition to average away.
    accounting_underflows: AtomicU64,
    active_callbacks: AtomicU64,
    /// Every listener call currently running, by id and the moment it entered the listener. One
    /// blocked callback therefore stays visible while other callbacks keep starting and finishing;
    /// a single counter would let that churn hide it (plan 49c C2).
    callback_started_at: parking_lot::Mutex<HashMap<u64, u64>>,
    next_callback_id: AtomicU64,
    /// Newest completed pull anywhere and newest failed pull outcome by kind, compared with each
    /// other instead of counted over a lifetime, so a fault stops being evidence as soon as work
    /// completes after it.
    last_pull_ok_ms: AtomicU64,
    pull_route_miss_ms: AtomicU64,
    pull_rpc_timeout_ms: AtomicU64,
    callbacks_started: AtomicU64,
    callbacks_finished: AtomicU64,
    offsets_persisted: AtomicU64,
    last_pull_ms: AtomicU64,
    last_callback_progress_ms: AtomicU64,
    last_assignment_ms: AtomicU64,
    last_offset_ms: AtomicU64,
    /// Latest completed allocation pass **per topic**. One consumer-wide answer cannot describe a
    /// consumer with several subscriptions: an explicit zero for the retry topic would certify a
    /// business topic whose lookup failed, and reversing iteration order would reverse the
    /// diagnosis (plan 49c C3). Bounded by the subscription set; entries are dropped on
    /// unsubscribe.
    allocations: parking_lot::Mutex<HashMap<CheetahString, AllocationRecord>>,
    last_route_ok_ms: AtomicU64,
    last_route_miss_ms: AtomicU64,
    /// Source of queue epochs for this incarnation. Epochs are never reused: a revoked queue and
    /// the same queue assigned again must carry different numbers, or a lease claim, retry or
    /// completion captured under the old assignment would be accepted by the new one (plan 49a
    /// F4).
    next_epoch: AtomicU64,
    /// Identity source for pull attempts. Identities are never reused within an incarnation so a
    /// callback can only ever retire the attempt it answered (plan 49d R1).
    next_attempt_id: AtomicU64,
    queues: parking_lot::RwLock<HashMap<MessageQueue, QueueProgress>>,
}

/// Whether a report about `queue` may become the evidence health reads for it (plan 49e E1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueEvidence {
    /// The reporting request belongs to the acquisition this incarnation holds now.
    Current,
    /// Nothing tracks this queue, so only the aggregate fallback can describe it.
    Untracked,
    /// The reporting request outlived its acquisition: history, never current evidence.
    Expired,
}

/// One queue's standing right now, judged from that queue alone (plan 49c C2).
#[derive(Clone, Copy, Debug)]
struct QueueStanding {
    silent: bool,
    waiting: bool,
    leased: bool,
    /// An admitted RPC whose **own** deadline passed without an answer.
    overdue: bool,
    /// The loop still holds the queue's lease while nothing is admitted and no continuation is due:
    /// work stopped without ending the loop, which stays diagnosable instead of looking healthy
    /// forever (plan 49d R1).
    idle_lease: bool,
}

impl QueueStanding {
    /// A queue with a request in flight inside its own deadline, or with a continuation still due
    /// in the future, is not lost however old its last answer looks - only an expired attempt
    /// deadline or a failed wait expectation says the work is gone (plan 49c C2, plan 49d R1).
    fn is_lost(&self) -> bool {
        self.overdue || self.idle_lease || (!self.leased && !self.waiting && self.silent)
    }
}

impl QueueProgress {
    fn standing(&self, now_ms: u64, stall: u64, rpc_grace: u64) -> QueueStanding {
        let silent = now_ms.saturating_sub(self.last_progress_ms) >= stall;
        let waiting = self.wait_until_ms > now_ms;
        // Judged from the attempt's own record: a loop that keeps completing pulls reopens it per
        // attempt, so a lease held for hours never ages into a fault on its own (plan 49d R1). The
        // caller's allowance is only a fallback for an attempt that recorded no window of its own.
        let overdue = self.attempt_id != 0
            && now_ms
                >= if self.attempt_deadline_ms > 0 {
                    self.attempt_deadline_ms
                } else {
                    self.attempt_started_ms.saturating_add(rpc_grace)
                };
        QueueStanding {
            silent,
            waiting,
            leased: self.pull_leased,
            overdue,
            idle_lease: self.pull_leased && self.attempt_id == 0 && !waiting && silent,
        }
    }
}

impl ConsumerProgress {
    pub(crate) fn new() -> Arc<Self> {
        let now = get_current_millis();
        Arc::new(Self {
            started_at_ms: AtomicU64::new(now),
            last_pull_ms: AtomicU64::new(now),
            last_callback_progress_ms: AtomicU64::new(now),
            callback_started_at: parking_lot::Mutex::new(HashMap::new()),
            next_callback_id: AtomicU64::new(1),
            last_pull_ok_ms: AtomicU64::new(now),
            pull_route_miss_ms: AtomicU64::new(0),
            pull_rpc_timeout_ms: AtomicU64::new(0),
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
        // Taking the lease says one loop owns this queue; it starts no RPC timer. The attempt that
        // actually reaches the transport records its own window (plan 49d R1), and a wait scheduled
        // before the claim is no longer an expectation anyone owes.
        entry.wait_until_ms = 0;
        Some(entry.epoch)
    }

    /// Records that **this** pull attempt reached the transport, with the answer window the request
    /// was issued under. Returns the attempt identity its callback must hand back to
    /// [`Self::finish_pull_attempt`], or `None` when this queue is no longer owned under `epoch`
    /// (revoked, or assigned again under a newer epoch) - in which case the caller must not treat
    /// what it issued as owned work.
    ///
    /// The loop lease is untouched: an attempt is a phase inside that lease, never the lease
    /// itself.
    pub(crate) fn begin_pull_attempt(&self, queue: &MessageQueue, epoch: u64, window: Duration) -> Option<u64> {
        let mut queues = self.queues.write();
        let entry = queues.get_mut(queue)?;
        if entry.epoch != epoch || !entry.pull_leased {
            return None;
        }
        let now = get_current_millis();
        // Identities are offset by one because `0` means "no attempt in flight", which keeps a
        // freshly defaulted counter from handing out the sentinel.
        let attempt_id = self.next_attempt_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        entry.attempt_id = attempt_id;
        entry.attempt_started_ms = now;
        entry.attempt_deadline_ms = now.saturating_add(u64::try_from(window.as_millis()).unwrap_or(u64::MAX));
        // An admitted RPC supersedes any wait expectation recorded for this queue.
        entry.wait_until_ms = 0;
        Some(attempt_id)
    }

    /// Ends one pull attempt - answered, failed or cancelled - and leaves the loop lease alone.
    /// Only the currently recorded attempt can end itself, so a late callback for an older
    /// attempt cannot retire work a newer one owns (plan 49d R1).
    pub(crate) fn finish_pull_attempt(&self, queue: &MessageQueue, epoch: u64, attempt_id: u64) {
        let mut queues = self.queues.write();
        if let Some(entry) = queues.get_mut(queue) {
            if entry.epoch == epoch && entry.attempt_id == attempt_id {
                entry.attempt_id = 0;
                entry.attempt_started_ms = 0;
                entry.attempt_deadline_ms = 0;
            }
        }
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
                entry.attempt_id = 0;
                entry.attempt_started_ms = 0;
                entry.attempt_deadline_ms = 0;
            }
        }
    }

    /// Counts an issued pull. It deliberately refreshes **no** progress timestamp: issuing work is
    /// not completing it, and a queue that only ever launches retries must stay diagnosable
    /// (plan 49c C2).
    pub(crate) fn note_pull_started(&self, _queue: &MessageQueue) {
        self.bump(&self.pulls_started);
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
        self.last_pull_ok_ms.store(now, Ordering::Relaxed);
        if let Some(entry) = self.queues.write().get_mut(queue) {
            entry.last_progress_ms = now;
            // The one event that retires a route or transport fault for this acquisition (plan 49e
            // E2): the pull itself answered successfully.
            entry.last_pull_ok_ms = now;
        }
    }

    pub(crate) fn note_pull_failed(&self, queue: &MessageQueue, kind: PullFailureKind) {
        self.record_pull_failure(queue, None, kind);
    }

    /// Record a pull failure **for the acquisition that issued it** (plan 49e E1). The epoch is
    /// checked while holding the same queue-map write lock that mutates the evidence, so an answer
    /// for an expired acquisition cannot stamp the queue its current owner holds. Returns whether
    /// this queue's newest outcome was recorded.
    /// `None` means the reporter carries no acquisition identity (a legacy consumer that does not
    /// use SDK-owned assignment); such a report is recorded as before rather than refused.
    pub(crate) fn note_pull_failed_for(&self, queue: &MessageQueue, epoch: Option<u64>, kind: PullFailureKind) -> bool {
        self.record_pull_failure(queue, epoch, kind)
    }

    fn record_pull_failure(&self, queue: &MessageQueue, epoch: Option<u64>, kind: PullFailureKind) -> bool {
        let now = get_current_millis();
        // Identity is decided **inside** the write lock that mutates the evidence. A pre-check with
        // a separate mutation would let a revoke and reinstall land between them, which is
        // the race plan 49e E1 exists to close.
        let evidence = {
            let mut queues = self.queues.write();
            match queues.get_mut(queue) {
                // The acquisition that asked is still the current one: this becomes the queue's
                // newest outcome and the aggregate fallback for untracked queues.
                Some(entry) if epoch.is_none_or(|epoch| entry.epoch == epoch) => {
                    entry.failure_ms = now;
                    entry.failure_kind = kind;
                    QueueEvidence::Current
                }
                Some(_) => QueueEvidence::Expired,
                // Nothing tracks this queue, so only the aggregate fallback can describe it.
                None => QueueEvidence::Untracked,
            }
        };
        // Work issued under an expired acquisition may still be counted as history, but it can
        // neither stamp this queue's evidence nor renew the aggregate fault timestamp health reads
        // for queues nobody tracks (plan 49e E1).
        self.record_failure_tally(kind, !matches!(evidence, QueueEvidence::Expired));
        matches!(evidence, QueueEvidence::Current)
    }

    fn record_failure_tally(&self, kind: PullFailureKind, stamp_aggregate: bool) {
        let now = get_current_millis();
        self.bump(&self.pull_failures);
        match kind {
            PullFailureKind::RouteOrBrokerMiss => {
                self.bump(&self.route_misses);
                if stamp_aggregate {
                    self.stamp(&self.pull_route_miss_ms);
                }
            }
            PullFailureKind::RpcTimeout => {
                self.bump(&self.rpc_timeouts);
                if stamp_aggregate {
                    self.stamp(&self.pull_rpc_timeout_ms);
                }
            }
            PullFailureKind::Other => {}
        }
    }

    /// A retry or a deliberate flow control wait for `queue`, with the delay actually scheduled.
    /// While that wait lies in the future the queue is waiting on purpose; once it has passed with
    /// no work admitted, the queue's own expectation has failed and it becomes repairable
    /// (plan 49c C2).
    pub(crate) fn note_queue_wait(&self, queue: &MessageQueue, delay: Duration) {
        self.record_queue_wait(queue, None, delay);
    }

    /// Schedule a wait **for the acquisition that owns the request** (plan 49e E1). Returns whether
    /// it was recorded; `false` means this continuation belongs to an expired acquisition and must
    /// not replace the current owner's expectation.
    /// `None` means the continuation carries no acquisition identity (legacy path): recorded as
    /// before rather than refused.
    pub(crate) fn note_queue_wait_for(&self, queue: &MessageQueue, epoch: Option<u64>, delay: Duration) -> bool {
        self.record_queue_wait(queue, epoch, delay)
    }

    fn record_queue_wait(&self, queue: &MessageQueue, epoch: Option<u64>, delay: Duration) -> bool {
        let wait_until = get_current_millis().saturating_add(delay.as_millis() as u64);
        // Same rule as a failure: identity and mutation share one write lock, so an expired
        // continuation can never replace the current owner's expectation (plan 49e E1).
        let mut queues = self.queues.write();
        let Some(entry) = queues.get_mut(queue) else {
            return false;
        };
        if !epoch.is_none_or(|epoch| entry.epoch == epoch) {
            return false;
        }
        entry.wait_until_ms = entry.wait_until_ms.max(wait_until);
        // A scheduled continuation is the loop saying what it expects to happen next; nothing
        // is in flight while that timer runs, so no attempt may be judged
        // outstanding (plan 49d R1).
        entry.attempt_id = 0;
        entry.attempt_started_ms = 0;
        entry.attempt_deadline_ms = 0;
        true
    }

    pub(crate) fn note_retry_scheduled(&self) {
        self.bump(&self.retries_scheduled);
    }

    pub(crate) fn note_flow_control(&self) {
        self.bump(&self.flow_controls);
    }

    /// Records what the latest completed rebalance pass concluded **for one topic**. Only that
    /// topic's previous answer is replaced, so a failed subscription stays visible until its own
    /// next successful pass (plan 49c C3).
    pub(crate) fn note_allocation_pass(&self, topic: &str, outcome: AllocationOutcome) {
        self.allocations.lock().insert(
            CheetahString::from_slice(topic),
            AllocationRecord {
                outcome,
                at_ms: get_current_millis(),
            },
        );
    }

    /// Drops evidence for topics this consumer no longer subscribes to, so the record set stays
    /// bounded by the subscription table instead of remembering every topic it ever served.
    pub(crate) fn retain_allocation_passes(&self, topics: &[CheetahString]) {
        let mut allocations = self.allocations.lock();
        allocations.retain(|topic, _| topics.iter().any(|subscribed| subscribed == topic));
    }

    /// Every completed pass, ordered by topic so aggregation cannot depend on map iteration order.
    pub(crate) fn allocation_records(&self) -> Vec<(CheetahString, AllocationOutcome, u64)> {
        let mut records = self
            .allocations
            .lock()
            .iter()
            .map(|(topic, record)| (topic.clone(), record.outcome, record.at_ms))
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.0.cmp(&right.0));
        records
    }

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
            Self::acquire_queue(&mut queues, &self.next_epoch, queue, now);
        }
    }

    /// Publishes the acquisition of one queue whose process queue has just been installed. This is
    /// the authoritative moment ownership begins - before any pull is dispatched for it (plan 49c
    /// C1)
    /// - and a queue that could not be prepared never reaches it.
    pub(crate) fn note_queue_installed(&self, queue: &MessageQueue) {
        let now = get_current_millis();
        self.bump(&self.assignments);
        self.last_assignment_ms.store(now, Ordering::Relaxed);
        let mut queues = self.queues.write();
        Self::acquire_queue(&mut queues, &self.next_epoch, queue, now);
    }

    /// Ends one queue's acquisition because its process queue left the table. Its epoch is retired
    /// with it, so a replacement installation receives a strictly newer identity even when the
    /// desired allocation never changed.
    pub(crate) fn note_queue_revoked(&self, queue: &MessageQueue) {
        self.note_revocation(std::slice::from_ref(queue));
    }

    fn acquire_queue(
        queues: &mut HashMap<MessageQueue, QueueProgress>,
        next_epoch: &AtomicU64,
        queue: &MessageQueue,
        now: u64,
    ) {
        match queues.get_mut(queue) {
            // Retained: identity and lease are the running work's, not this pass's to reset.
            Some(_) => {}
            None => {
                // A fresh acquisition always receives a strictly larger epoch than any this
                // incarnation handed out before, including the one this queue held when it was
                // revoked. Retained queues keep theirs (plan 49a F4 section 4.3 item 2).
                let epoch = next_epoch.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                queues.insert(
                    queue.clone(),
                    QueueProgress {
                        epoch,
                        last_progress_ms: now,
                        last_pull_ok_ms: now,
                        pull_leased: false,
                        attempt_id: 0,
                        attempt_started_ms: 0,
                        attempt_deadline_ms: 0,
                        wait_until_ms: 0,
                        failure_ms: 0,
                        failure_kind: PullFailureKind::Other,
                    },
                );
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

    /// Take accounting ownership of one admitted batch (plan 49c C4). The returned owner is the
    /// only thing that may release these messages, it releases them exactly once, and it can
    /// hand part of its count to a delayed retry without ever letting the count fall to zero in
    /// between.
    pub(crate) fn admit_batch(self: &Arc<Self>, queue: &MessageQueue, messages: usize) -> DeliveryOwner {
        let messages = u64::try_from(messages).unwrap_or(0);
        self.pending_delivery_msgs.fetch_add(messages, Ordering::Relaxed);
        DeliveryOwner {
            progress: Arc::clone(self),
            queue: queue.clone(),
            messages,
            phase: OwnerPhase::Scheduled,
            callback_id: 0,
        }
    }

    /// Messages handed to the delivery layer but not yet acknowledged by a listener.
    pub(crate) fn note_delivery_pending(&self, messages: u64) {
        self.pending_delivery_msgs.fetch_add(messages, Ordering::Relaxed);
    }

    /// Registers a listener call that has entered user code and returns the id its owner hands back
    /// when it finishes.
    pub(crate) fn note_callback_started(&self, messages: u64) -> u64 {
        let _ = messages;
        let id = self.next_callback_id.fetch_add(1, Ordering::Relaxed);
        self.bump(&self.callbacks_started);
        self.active_callbacks.fetch_add(1, Ordering::Relaxed);
        let now = get_current_millis();
        self.callback_started_at.lock().insert(id, now);
        self.last_callback_progress_ms.store(now, Ordering::Relaxed);
        id
    }

    pub(crate) fn note_callback_finished(&self, id: u64, messages: u64) {
        self.bump(&self.callbacks_finished);
        self.release(&self.active_callbacks, 1, "active callback count");
        self.callback_started_at.lock().remove(&id);
        self.release_pending(messages);
        let now = get_current_millis();
        self.last_callback_progress_ms.store(now, Ordering::Relaxed);
    }

    /// A batch that will never reach a listener (its queue was revoked) stops counting as work.
    pub(crate) fn note_delivery_dropped(&self, messages: u64) {
        self.release_pending(messages);
    }

    /// Move a released batch's count into the post-listener commit phase. Held until the batch's
    /// offsets are committed or it is discarded.
    pub(crate) fn note_commit_started(&self, messages: u64) {
        self.committing_delivery_msgs.fetch_add(messages, Ordering::Relaxed);
    }

    pub(crate) fn note_commit_finished(&self, messages: u64) {
        self.release(&self.committing_delivery_msgs, messages, "committing delivery count");
    }

    fn release_pending(&self, messages: u64) {
        self.release(&self.pending_delivery_msgs, messages, "pending delivery count");
    }

    /// Release counted work in one atomic step. A release larger than what is outstanding is a
    /// broken ownership pairing: it is reported and counted as an underflow, and the counter is
    /// left alone - clamping to zero would erase other batches' still-outstanding counts and
    /// hide which pairing broke. Leaving it means the over-release is a no-op and every
    /// legitimately admitted batch stays counted until its own owner releases it (plan 49c C4).
    fn release(&self, counter: &AtomicU64, messages: u64, what: &'static str) {
        if messages == 0 {
            return;
        }
        if Self::subtract(counter, messages).is_err() {
            self.note_accounting_underflow(what);
        }
    }

    /// Atomic compare-and-subtract. `fetch_update` was renamed to `try_update` on newer toolchains
    /// only; keeping the old spelling here so the crate's minimum supported compiler still builds.
    #[allow(deprecated)]
    fn subtract(counter: &AtomicU64, amount: u64) -> Result<u64, u64> {
        counter.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |v| v.checked_sub(amount))
    }

    fn note_accounting_underflow(&self, what: &'static str) {
        let seen = self.accounting_underflows.fetch_add(1, Ordering::Relaxed) + 1;
        error!(
            counter = what,
            times = seen,
            "delivery accounting released more than it was given - a batch has two owners"
        );
    }

    /// How many times delivery accounting was released more than it was admitted. Non-zero means an
    /// ownership bug somewhere in the consume path, not a transient condition.
    pub(crate) fn accounting_underflows(&self) -> u64 {
        self.accounting_underflows.load(Ordering::Relaxed)
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
    /// Queues this incarnation owns whose own expectation has failed: neither running nor waiting
    /// with no completed work for the stall window, or holding a lease whose RPC deadline passed
    /// without an answer (plan 49a F4 section 4.3 item 5, plan 49c C2). Health and repair share
    /// this classification so they cannot disagree about which queues are affected.
    pub(crate) fn lost_pulls(&self, stall_after: Duration, pull_deadline: Duration) -> Vec<MessageQueue> {
        let now_ms = get_current_millis();
        let window = u64::try_from(stall_after.as_millis()).unwrap_or(u64::MAX);
        let grace = u64::try_from(pull_deadline.as_millis()).unwrap_or(u64::MAX);
        self.queues
            .read()
            .iter()
            .filter(|(_, entry)| entry.standing(now_ms, window, grace).is_lost())
            .map(|(queue, _)| queue.clone())
            .collect()
    }

    pub(crate) fn assigned_queues(&self) -> u64 {
        self.queues.read().len() as u64
    }

    pub(crate) fn snapshot(&self, stall_after: Duration, pull_deadline: Duration) -> ProgressSnapshot {
        let now_ms = get_current_millis();
        let queues = self.queues.read();
        let stall = u64::try_from(stall_after.as_millis()).unwrap_or(u64::MAX);
        let grace = u64::try_from(pull_deadline.as_millis()).unwrap_or(u64::MAX);
        let mut stalled_queues = 0u64;
        let mut lost_pull_queues = 0u64;
        let mut leased_queues = 0u64;
        let mut waiting_queues = 0u64;
        let mut route_miss_queues = 0u64;
        let mut rpc_timeout_queues = 0u64;
        for entry in queues.values() {
            let standing = entry.standing(now_ms, stall, grace);
            if standing.waiting {
                waiting_queues += 1;
            }
            if entry.pull_leased {
                leased_queues += 1;
            }
            if standing.is_lost() {
                lost_pull_queues += 1;
            }
            if standing.silent && !standing.waiting {
                stalled_queues += 1;
            }
            // A failure is current evidence only while no **successful pull** has answered this
            // acquisition since (plan 49c C2, plan 49e E2): releasing delivery ownership, touching
            // a queue, or another queue's response says nothing about whether pull
            // routing recovered. `>=` on purpose: a failure stamped in the same
            // millisecond as the last successful pull is ambiguous, and guessing "fine"
            // would drop a live fault for a whole tick.
            if entry.failure_ms >= entry.last_pull_ok_ms {
                match entry.failure_kind {
                    PullFailureKind::RouteOrBrokerMiss => route_miss_queues += 1,
                    PullFailureKind::RpcTimeout => rpc_timeout_queues += 1,
                    PullFailureKind::Other => {}
                }
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
            committing_delivery_msgs: self.committing_delivery_msgs.load(Ordering::Relaxed),
            active_callbacks: self.active_callbacks.load(Ordering::Relaxed),
            callbacks_started: self.callbacks_started.load(Ordering::Relaxed),
            callbacks_finished: self.callbacks_finished.load(Ordering::Relaxed),
            offsets_persisted: self.offsets_persisted.load(Ordering::Relaxed),
            last_pull_ms: self.last_pull_ms.load(Ordering::Relaxed),
            last_callback_progress_ms: self.last_callback_progress_ms.load(Ordering::Relaxed),
            last_assignment_ms: self.last_assignment_ms.load(Ordering::Relaxed),
            last_offset_ms: self.last_offset_ms.load(Ordering::Relaxed),
            allocations: self.allocation_records(),
            waiting_queues,
            route_miss_queues,
            rpc_timeout_queues,
            oldest_callback_ms: self.callback_started_at.lock().values().copied().min().unwrap_or(0),
            accounting_underflows: self.accounting_underflows.load(Ordering::Relaxed),
            last_route_ok_ms: self.last_route_ok_ms.load(Ordering::Relaxed),
            last_route_miss_ms: self.last_route_miss_ms.load(Ordering::Relaxed),
            last_pull_ok_ms: self.last_pull_ok_ms.load(Ordering::Relaxed),
            // The aggregate timestamps are the fallback for faults no tracked queue describes. Once
            // every tracked queue that held one has gone - revoked, or reacquired under a new epoch -
            // the timestamp is history and would keep escalating a replacement over work this
            // incarnation no longer owns and cannot repair (plan 49f F1). The lifetime tallies above
            // still report what happened.
            pull_route_miss_ms: if route_miss_queues == 0 {
                0
            } else {
                self.pull_route_miss_ms.load(Ordering::Relaxed)
            },
            pull_rpc_timeout_ms: if rpc_timeout_queues == 0 {
                0
            } else {
                self.pull_rpc_timeout_ms.load(Ordering::Relaxed)
            },
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

/// Where the batch an [`DeliveryOwner`] holds is right now. The phases are what make queued work, a
/// running listener, a delayed retry and a stuck commit distinguishable (plan 49c C4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OwnerPhase {
    /// Admitted and waiting for its listener.
    Scheduled,
    /// Inside application code.
    Listener,
    /// The listener returned; offsets are being committed or the batch is being retried.
    Committing,
}

/// Accounting ownership of one admitted batch of messages.
///
/// Ownership is taken when a batch is accepted for delivery - not inside whichever task eventually
/// runs it - and travels with those messages through queueing, a delayed retry, listener execution
/// and final commit or discard, releasing its counts exactly once on drop. Part of a batch can be
/// handed to a retry with [`Self::split_off`] without the outstanding count ever dropping to zero
/// in between, which is what used to make a sleeping retry invisible and let the next attempt
/// subtract messages it never added. An early return or an unwind inside a user listener likewise
/// cannot leave `active_callbacks` or `pending_delivery_msgs` inflated.
pub(crate) struct DeliveryOwner {
    progress: Arc<ConsumerProgress>,
    queue: MessageQueue,
    messages: u64,
    phase: OwnerPhase,
    callback_id: u64,
}

impl DeliveryOwner {
    /// Adopt a batch whose messages were already admitted as pending work. Production handover uses
    /// [`ConsumerProgress::admit_batch`], which counts them once here.
    pub(crate) fn new(progress: &Arc<ConsumerProgress>, queue: &MessageQueue, messages: usize) -> Self {
        Self {
            progress: Arc::clone(progress),
            queue: queue.clone(),
            messages: u64::try_from(messages).unwrap_or(0),
            phase: OwnerPhase::Scheduled,
            callback_id: 0,
        }
    }

    /// How many messages this owner is responsible for.
    pub(crate) fn messages(&self) -> usize {
        usize::try_from(self.messages).unwrap_or(usize::MAX)
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        &self.queue
    }

    /// Move ownership of `count` of this batch's messages into a new owner - the same messages, so
    /// no counter is added or subtracted. Used when part of a batch goes to a delayed retry
    /// while the rest is committed (plan 49c C4).
    pub(crate) fn split_off(&mut self, count: usize) -> Option<Self> {
        let count = u64::try_from(count).unwrap_or(0);
        if count == 0 {
            return None;
        }
        // A batch cannot hand out more messages than it owns; that is a broken pairing and must be
        // loud rather than absorbed by a clamp.
        debug_assert!(
            count <= self.messages,
            "delivery ownership split {} down to {count}",
            self.messages
        );
        let count = count.min(self.messages);
        self.messages -= count;
        // A batch handed on from the commit phase is queued work again, so those messages stop
        // being counted as committing and start being counted as pending once more. From
        // any earlier phase the messages simply change hands with no counter movement,
        // which is what keeps a sleeping retry visible instead of double-counted (plan 49c
        // C4).
        if self.phase == OwnerPhase::Committing {
            self.progress.note_commit_finished(count);
            self.progress.note_delivery_pending(count);
        }
        Some(Self {
            progress: Arc::clone(&self.progress),
            queue: self.queue.clone(),
            messages: count,
            phase: OwnerPhase::Scheduled,
            callback_id: 0,
        })
    }

    /// Marks the entry into the application listener; only then is a callback "active", and it is
    /// registered individually so no other callback's completion can retire it (plan 49c C2).
    pub(crate) fn enter_listener(&mut self) {
        if self.phase == OwnerPhase::Scheduled {
            self.phase = OwnerPhase::Listener;
            self.callback_id = self.progress.note_callback_started(self.messages);
        }
    }

    /// The listener call returned. Its callback stops counting as running user code and the batch
    /// moves to the commit phase, which cleanup still has to drain before it may finish.
    pub(crate) fn leave_listener(&mut self) {
        if self.phase == OwnerPhase::Listener {
            self.progress.note_callback_finished(self.callback_id, self.messages);
            self.progress.note_commit_started(self.messages);
            self.phase = OwnerPhase::Committing;
        }
    }
}

impl Drop for DeliveryOwner {
    fn drop(&mut self) {
        match self.phase {
            // Never reached a listener: queue revoked, work closed, or cancelled before entry.
            OwnerPhase::Scheduled => self.progress.note_delivery_dropped(self.messages),
            // Unwound or abandoned inside user code: end the callback and release its messages.
            OwnerPhase::Listener => {
                self.progress.note_callback_finished(self.callback_id, self.messages);
            }
            // The commit phase owns these counts now; closing the owner ends it.
            OwnerPhase::Committing => self.progress.note_commit_finished(self.messages),
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

    /// The controller reports a repair attempt that **finished** (plan 49d R3). Only a settled
    /// attempt speaks for the replacement budget: while a pass is still owned - waiting on a
    /// nameserver, on an offset read, or simply working through its time slice - elapsed status
    /// ticks measure wall clock, not failed work, and charging them escalated repairs that had
    /// not been judged yet.
    pub(crate) fn note_repair_settled(&mut self, resolved: bool) {
        if resolved {
            self.stalled_repairs = 0;
        } else {
            self.stalled_repairs = self.stalled_repairs.saturating_add(1);
        }
    }

    /// The controller reports a repair attempt that finished **without an answer to charge**: the
    /// route work it pinned was deferred (a success owns the cooldown window, or a failed fetch has
    /// already scheduled its retry) or is still owned by another caller's flight, and no flight of
    /// this round said it failed (plan 49e E3).
    ///
    /// Such an attempt neither resolves a fault nor proves recovery impossible, so the allowance
    /// stays exactly as it was. Charging deferred checks would turn a cooldown window or one slow
    /// shared refresh into a guarded incarnation replacement with no evidence behind it; the next
    /// eligibility instant asks again.
    pub(crate) fn note_repair_deferred(&mut self) {}

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
        pull_deadline: Duration,
        unit: f64,
    ) -> (MachineState, Vec<Effect>) {
        if self.stop_requested || self.cleanup_unproved {
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }
        // Nothing here asks whether unrelated counters moved since the last tick. A fault is a
        // fault while its own evidence stands, and progress somewhere else neither creates
        // nor retires it; the repair budget is cleared only by a conclusion that found
        // nothing wrong (plan 49c C2).

        // Healthy idle: explicit zero assignment in CLUSTERING, or successful empty pulls.
        if snapshot.assigned_queues == 0 && !snapshot.has_pending_work() {
            // "Explicitly assigned zero queues" needs evidence: the newest rebalance pass has to
            // have seen a topic route. When it could not, this is a failed assignment refresh and
            // is reported as such. It repairs in place and deliberately does not spend
            // the replacement budget - replacing an incarnation cannot fix a
            // control-plane outage.
            // The completed passes answer this directly when any have been recorded: only "you get
            // nothing" from every subscription makes idleness explicit, and any other conclusion -
            // including queues allocated that this incarnation does not actually own, or a sibling
            // topic's failure - is a refresh that did not finish. Without any record there is
            // nothing better than the route observation itself.
            let refresh_failed = if snapshot.allocations.is_empty() {
                snapshot.last_route_miss_ms > snapshot.last_route_ok_ms
            } else {
                !snapshot.all_allocations_explicit_zero()
            };
            if refresh_failed {
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
            self.stalled_repairs = 0;
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

        // Pending messages whose callbacks do not advance. Each running listener call is judged by
        // its own elapsed time: other callbacks starting and finishing, pulls returning or offsets
        // persisting cannot retire an over-budget call, because that call still owns the batch and
        // nothing else can hand it back (plan 49c C2).
        if snapshot.has_pending_work() {
            if snapshot.active_callbacks > 0
                && snapshot.age_ms(snapshot.oldest_callback_ms) >= self.policy.pending_delivery_warn
            {
                // A synchronous listener is still running: it stays owned and blocked, we never
                // cancel it, replace the incarnation or ask the app to restart.
                self.state = MachineState {
                    phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                    health: crate::consumer::managed_consumer::ConsumerHealth::Blocked,
                    reason: Reason::ListenerActiveOverBudget,
                };
                return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
            }
            if snapshot.age_ms(snapshot.last_callback_progress_ms) >= self.policy.pending_delivery_warn {
                self.state = MachineState {
                    phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                    health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                    reason: Reason::CallbacksStalled,
                };
                return (self.state, vec![Effect::Repair(RepairKind::RouteAndPull)]);
            }
        }

        // A queue this incarnation owns whose own expectation has failed - no request in flight,
        // nothing scheduled and no completed work for the stall window; or a lease whose RPC
        // deadline passed without an answer. Judged per queue, so one busy queue cannot
        // excuse an idle neighbour and a healthy past cannot excuse a stopped present (plan
        // 49a F4 section 4.3 item 5, plan 49c C2).
        if snapshot.lost_pull_queues > 0 {
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                health: crate::consumer::managed_consumer::ConsumerHealth::Degraded,
                reason: Reason::ExpectedPullMissing,
            };
            // The budget is spent by repair attempts that finished without resolving the fault,
            // never by status ticks elapsed while one pass is still running (plan 49d
            // R3).
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

        // A subscription whose latest completed pass failed remains a fault while other
        // subscriptions keep working: owning some queues does not certify every one of
        // them, and looking only at the zero-assignment case hides a partially failed
        // acquisition behind the queues that did install (plan 49c C3). It repairs in place
        // and spends no replacement budget, because a new incarnation cannot change what
        // the control plane answered.
        if let Some((topic, outcome, _)) = snapshot.failed_allocations().next() {
            debug!(
                topic = %topic,
                ?outcome,
                "latest allocation pass did not obtain this subscription; repairing the assignment in place"
            );
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

        // Transport and route faults come from what each acquisition's newest outcome was - a route
        // or broker miss is repaired in place, an RPC timeout is only watched - and they are judged
        // **before** any aggregate "everything is fine" answer below (plan 49e E2). A queue sitting
        // in a scheduled retry keeps `stalled_queues` at zero, so judging these later let another
        // queue's old empty pull, a live long poll elsewhere, or callbacks advancing on a different
        // queue certify a consumer whose own newest answer was a failure. For queues this
        // incarnation does not track at all, the aggregate timestamps remain as the
        // fallback: the newest failure anywhere measured against the newest completed pull.
        // Lifetime tallies are never evidence - a consumer that recovered a second ago must
        // not stay faulty, and one unrelated counter moving must not hide a fault that is
        // still standing (plan 49c C2).
        let transport_reason =
            if snapshot.route_miss_queues > 0 || snapshot.pull_route_miss_ms > snapshot.last_pull_ok_ms {
                Some(Reason::RouteOrBrokerMiss)
            } else if snapshot.rpc_timeout_queues > 0 || snapshot.pull_rpc_timeout_ms > snapshot.last_pull_ok_ms {
                Some(Reason::RpcTimeout)
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
            // In-place repair is always tried first. Only after repeated repairs - each one
            // reported by the controller once it actually settled - produce no progress
            // does the shell roll one guarded incarnation replacement.
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

        // Expected waits: successful empty pulls, or an outstanding long poll inside its deadline.
        if snapshot.pulls_empty > 0 && !snapshot.has_pending_work() && snapshot.stalled_queues == 0 {
            self.stalled_repairs = 0;
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::IdleNoMessages,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }
        if snapshot.pull_within_deadline(pull_deadline) {
            self.stalled_repairs = 0;
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::LongPollPending,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        // Pending work that is advancing: busy rather than blocked.
        if snapshot.has_pending_work() {
            self.stalled_repairs = 0;
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::None,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        // A queue sitting in a scheduled retry or a deliberate flow control wait is healthy while
        // that wait still holds; once it lapses without work being admitted, the queue's own
        // expectation has failed and the lost-pull branch above handles it (plan 49c C2).
        if snapshot.waiting_queues > 0 {
            self.stalled_repairs = 0;
            self.state = MachineState {
                phase: crate::consumer::managed_consumer::ConsumerPhase::Running,
                health: crate::consumer::managed_consumer::ConsumerHealth::Healthy,
                reason: Reason::FlowControl,
            };
            return (self.state, vec![Effect::Idle(self.policy.progress_check)]);
        }

        self.stalled_repairs = 0;
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
        progress.snapshot(Duration::from_secs(5), Duration::from_secs(30))
    }

    /// A queue that went quiet `age` ago with no request in flight and nothing scheduled: the shape
    /// of a lost pull loop.
    fn silent_queue(progress: &ConsumerProgress, mq: &MessageQueue, age: Duration) {
        progress.note_queue_installed(mq);
        let stamp = get_current_millis().saturating_sub(u64::try_from(age.as_millis()).unwrap_or(0));
        progress.queues.write().get_mut(mq).unwrap().last_progress_ms = stamp;
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
        let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
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
        let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
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
        let (state, _) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
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
        let snap = progress.snapshot(Duration::from_millis(1), Duration::from_secs(30));
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
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
            let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
            assert_eq!(state.reason, Reason::AssignmentRefreshFailed);
            assert!(
                !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
                "replacement would not restore a missing route: {effects:?}"
            );
        }
    }

    /// Plan 49d R3: ticks that pass while one repair is still running say nothing about whether
    /// that attempt worked. Only an attempt the controller reports as **settled** spends the
    /// replacement budget, and a settled attempt that resolved its fault clears what earlier
    /// ones accumulated.
    #[test]
    fn elapsed_ticks_do_not_spend_the_replacement_budget() {
        let policy = LifecyclePolicy::default();
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        for id in 0..2 {
            progress.note_pull_failed(&queue("T", id), PullFailureKind::RouteOrBrokerMiss);
        }
        let mut machine = RecoveryMachine::new(policy, 1);
        let snap = snapshot(&progress);

        // The same fault observed across many cadence ticks while one pass is still working: it
        // stays a repair in place, however long that pass takes.
        for _ in 0..policy.max_repairs_before_replace + 4 {
            let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
            assert_eq!(state.reason, Reason::RouteOrBrokerMiss);
            assert!(
                !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
                "elapsed ticks escalated a repair that had not settled: {effects:?}"
            );
        }

        // Attempts that finish without resolving anything do spend it - one at a time.
        for _ in 0..policy.max_repairs_before_replace {
            machine.note_repair_settled(false);
            let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
            assert!(
                !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
                "replacement comes only after the allowance is spent: {effects:?}"
            );
        }
        machine.note_repair_settled(false);
        let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
        assert!(
            matches!(effects.first(), Some(Effect::Stop { .. })),
            "repeated settled-but-fruitless attempts must roll one guarded replacement: {effects:?}"
        );

        // And progress pays the debt back: a full allowance of fruitless attempts followed by one
        // that resolved its fault starts counting again from nothing.
        let mut machine = RecoveryMachine::new(policy, 1);
        for _ in 0..policy.max_repairs_before_replace {
            machine.note_repair_settled(false);
        }
        machine.note_repair_settled(true);
        for _ in 0..policy.max_repairs_before_replace {
            machine.note_repair_settled(false);
            let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
            assert!(
                !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
                "a resolved attempt cleared the accumulated failures: {effects:?}"
            );
        }
    }

    /// Plan 49e E3: a repair round whose route work was deferred - a success owns the cooldown
    /// window, a failed fetch already scheduled its retry, or another caller's flight still runs -
    /// finished with no answer to charge. Those rounds must never spend the replacement allowance,
    /// while genuinely failed flights keep exhausting it exactly as before, deferrals interleaved
    /// or not.
    #[test]
    fn deferred_repair_rounds_never_spend_the_replacement_budget() {
        let policy = LifecyclePolicy::default();
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        for id in 0..2 {
            progress.note_pull_failed(&queue("T", id), PullFailureKind::RouteOrBrokerMiss);
        }
        let snap = snapshot(&progress);

        // Cooldown checks and unfinished joins, at any length: nothing accumulates.
        let mut machine = RecoveryMachine::new(policy, 1);
        for _ in 0..policy.max_repairs_before_replace * 20 {
            machine.note_repair_deferred();
            let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
            assert_eq!(state.reason, Reason::RouteOrBrokerMiss);
            assert!(
                matches!(effects.first(), Some(Effect::Repair(RepairKind::RouteAndPull))),
                "the fault still asks to be repaired in place: {effects:?}"
            );
        }

        // Failed flights still spend it one at a time, and deferrals interleaved between them
        // neither add to nor pay off the debt.
        let mut machine = RecoveryMachine::new(policy, 1);
        for spent in 0..policy.max_repairs_before_replace {
            machine.note_repair_settled(false);
            for _ in 0..4 {
                machine.note_repair_deferred();
                let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
                assert!(
                    !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
                    "replacement came after only {spent} failed flights: {effects:?}"
                );
            }
        }
        let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
        assert!(
            !effects.iter().any(|effect| matches!(effect, Effect::Stop { .. })),
            "the allowance is spent by failed flights only, not by the deferrals between them"
        );
        machine.note_repair_settled(false);
        let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
        assert!(
            matches!(effects.first(), Some(Effect::Stop { .. })),
            "the failed flight that exhausts the allowance still rolls one guarded replacement: {effects:?}"
        );
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
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
        let (_, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
    /// Plan 49a F4 section 4.3 item 1: an explicit zero assignment is idleness, not a failed
    /// refresh, even though the newest route observation was a miss.
    #[test]
    fn an_explicit_zero_assignment_is_idle_rather_than_a_failed_refresh() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 0);
        progress.note_route_observation(false);
        progress.note_allocation_pass("TopicA", AllocationOutcome::ExplicitZero);

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::WaitingForAssignment);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Healthy);
        assert!(matches!(effects.first(), Some(Effect::Idle(_))));
    }

    /// A pass that could not read the consumer list is a failed assignment refresh even when routes
    /// were readable - the route timestamp alone cannot tell the two apart.
    #[test]
    fn a_consumer_id_lookup_failure_is_a_failed_assignment_refresh() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 0);
        progress.note_route_observation(true);
        progress.note_allocation_pass("TopicA", AllocationOutcome::LookupFailed);

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::AssignmentRefreshFailed);
        assert!(matches!(effects.first(), Some(Effect::Repair(RepairKind::Assignment))));
    }

    /// Plan 49c C3: allocation evidence belongs to the topic it describes. The consumer's automatic
    /// retry subscription is routinely assigned nothing, and that explicit zero must never be read
    /// as proof that a business topic whose consumer-list lookup failed is simply idle - in either
    /// recording order.
    #[test]
    fn another_topics_explicit_zero_does_not_certify_a_failed_topic() {
        for business_first in [true, false] {
            let progress = ConsumerProgress::new();
            assigned(&progress, 0);
            progress.note_route_observation(true);
            let record = |topic: &str, outcome| progress.note_allocation_pass(topic, outcome);
            if business_first {
                record("TopicA", AllocationOutcome::LookupFailed);
                record("%RETRY%group", AllocationOutcome::ExplicitZero);
            } else {
                record("%RETRY%group", AllocationOutcome::ExplicitZero);
                record("TopicA", AllocationOutcome::LookupFailed);
            }

            let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
            let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
            assert_eq!(
                state.reason,
                Reason::AssignmentRefreshFailed,
                "iteration order must not decide the diagnosis"
            );
            assert!(matches!(effects.first(), Some(Effect::Repair(RepairKind::Assignment))));
        }
    }

    /// Only a completed pass for the topic itself clears that topic's failure; every subscription
    /// has to answer before an idle consumer is healthy.
    #[test]
    fn a_successful_pass_clears_only_its_own_topics_failure() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 0);
        progress.note_allocation_pass("TopicA", AllocationOutcome::LookupFailed);
        progress.note_allocation_pass("TopicB", AllocationOutcome::RouteAbsent);

        progress.note_allocation_pass("TopicA", AllocationOutcome::ExplicitZero);
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, _) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
        assert_eq!(
            state.reason,
            Reason::AssignmentRefreshFailed,
            "TopicB has still not answered for itself"
        );

        progress.note_allocation_pass("TopicB", AllocationOutcome::ExplicitZero);
        let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::WaitingForAssignment);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Healthy);
        assert!(matches!(effects.first(), Some(Effect::Idle(_))));
    }

    /// Owning some queues does not certify every subscription: a partially failed acquisition stays
    /// visible while the installed queues keep working (plan 49c C3).
    #[test]
    fn a_partially_failed_subscription_is_visible_while_other_queues_are_owned() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        progress.note_allocation_pass("TopicA", AllocationOutcome::QueuesOwned);
        progress.note_allocation_pass("TopicB", AllocationOutcome::OffsetPreparationFailed);

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
        assert_eq!(
            state.reason,
            Reason::AssignmentRefreshFailed,
            "a queue that could not be prepared must not hide behind the ones that were"
        );
        assert!(matches!(effects.first(), Some(Effect::Repair(RepairKind::Assignment))));
    }

    /// Evidence is bounded by the subscription table: a topic that is no longer subscribed stops
    /// contributing its conclusion (plan 49c C3).
    #[test]
    fn allocation_evidence_stops_with_the_subscription() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 0);
        progress.note_allocation_pass("TopicA", AllocationOutcome::LookupFailed);
        assert_eq!(1, progress.allocation_records().len());

        progress.retain_allocation_passes(&[CheetahString::from_slice("%RETRY%group")]);
        assert!(
            progress.allocation_records().is_empty(),
            "an unsubscribed topic is forgotten"
        );

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, _) = machine.on_tick(&snapshot(&progress), Duration::from_secs(30), 0.5);
        assert_eq!(state.reason, Reason::WaitingForAssignment);
    }

    /// Plan 49c C1: a queue can only be dispatched once its installation has published an owner,
    /// and the acquisition ends with the process queue that held it.
    #[test]
    fn a_queue_can_pull_only_after_its_installation_published_it() {
        let progress = ConsumerProgress::new();
        let mq = queue("TopicA", 0);
        assert!(
            progress.acquire_pull_lease(&mq).is_none(),
            "an uninstalled queue has no acquisition to dispatch under"
        );

        progress.note_queue_installed(&mq);
        let lease = progress
            .acquire_pull_lease(&mq)
            .expect("installation publishes ownership before any dispatch");
        assert!(
            progress.acquire_pull_lease(&mq).is_none(),
            "the running loop keeps the single in-flight pull"
        );
        drop(lease);

        progress.note_queue_revoked(&mq);
        assert!(
            progress.acquire_pull_lease(&mq).is_none(),
            "removal ends the acquisition with the table entry"
        );
    }

    /// Replacing a process queue under an unchanged desired allocation must not inherit the old
    /// loop's identity, or its outstanding lease would block the replacement forever.
    #[test]
    fn a_replaced_process_queue_starts_a_new_acquisition() {
        let progress = ConsumerProgress::new();
        let mq = queue("TopicA", 0);
        progress.note_queue_installed(&mq);
        let stale = progress
            .acquire_pull_lease(&mq)
            .expect("the installed queue is owned by this incarnation");
        let epoch = stale.epoch();

        progress.note_queue_revoked(&mq);
        progress.note_queue_installed(&mq);
        drop(stale);

        let replacement = progress
            .acquire_pull_lease(&mq)
            .expect("a replacement installation pulls under its own identity");
        assert!(replacement.epoch() > epoch, "identities are never reused");
        assert!(!progress.is_current(&mq, epoch), "the old loop cannot publish");
    }

    #[test]
    fn a_live_long_poll_does_not_hide_a_lost_pull_on_another_queue() {
        let progress = ConsumerProgress::new();
        assigned(&progress, 2);
        // Queue 0 holds an outstanding long poll; queue 1 has no request in flight and is not
        // advancing.
        let live = queue("T", 0);
        let epoch = progress.claim_pull(&live).expect("assigned queue");
        progress.note_pull_started(&live);
        progress
            .begin_pull_attempt(&live, epoch, Duration::from_secs(30))
            .expect("the admitted attempt belongs to this acquisition");

        // Let a one-millisecond silence window pass: both queues are now silent, and what tells
        // them apart is that one still has its request in flight. Ageing only moves one
        // way, so the expectation cannot depend on how long the scheduler takes.
        std::thread::sleep(Duration::from_millis(5));
        let before = progress.snapshot(Duration::from_millis(1), Duration::from_secs(30));
        let snap = progress.snapshot(Duration::from_millis(1), Duration::from_secs(30));
        assert_eq!(snap.leased_queues, 1, "one queue is actively pulling");
        assert_eq!(snap.lost_pull_queues, 1, "the other queue lost its pull loop");

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        // Two passes with identical counters: nothing moved between them, so silence here is
        // evidence rather than the absence of an observation.
        assert!(!snap.progressed_since(&before));
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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
        let epoch = progress.claim_pull(&q).expect("assigned queue");
        progress.note_pull_started(&q);
        progress
            .begin_pull_attempt(&q, epoch, Duration::from_secs(30))
            .expect("the attempt was admitted under this acquisition");
        // Let the silence window pass while the request stays in flight: what keeps this queue from
        // being called lost is its own unexpired answer deadline and nothing else.
        std::thread::sleep(Duration::from_millis(5));
        let snap = progress.snapshot(Duration::from_millis(1), Duration::from_secs(30));
        assert_eq!(snap.stalled_queues, 1, "it has been silent for a while");
        assert_eq!(snap.lost_pull_queues, 0, "but the request is still in flight");

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, _) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
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

    /// Plan 49c C2: progress somewhere else must not excuse a queue whose own expectation failed.
    /// One queue lost its pull loop while a sibling keeps completing pulls and offsets keep
    /// persisting; the old aggregate "nothing moved" prerequisite reported that as healthy
    /// idleness.
    #[test]
    fn unrelated_pull_progress_does_not_hide_a_lost_queue() {
        let progress = ConsumerProgress::new();
        let busy = queue("T", 0);
        let lost = queue("T", 1);
        progress.note_queue_installed(&busy);
        silent_queue(&progress, &lost, Duration::from_secs(60));

        // The sibling is doing real work at this very moment.
        progress.note_pull_completed(&busy, true);
        progress.note_offset_persisted();

        let snap = snapshot(&progress);
        assert_eq!(
            snap.lost_pull_queues, 1,
            "one queue has no request in flight and nothing scheduled"
        );
        assert_eq!(snap.pulls_completed, 1, "and the other one is advancing");

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
        assert_eq!(
            state.reason,
            Reason::ExpectedPullMissing,
            "sibling progress cannot make a lost queue healthy"
        );
        assert!(
            matches!(effects.first(), Some(Effect::Repair(RepairKind::LostPull))),
            "the affected queue is repaired, not the whole consumer: {effects:?}"
        );
    }

    /// Plan 49c C2: one listener call over its budget stays blocked while other callbacks start and
    /// finish and pulls keep returning - that churn is exactly what used to hide it.
    #[test]
    fn unrelated_progress_does_not_hide_a_blocked_listener_call() {
        let policy = LifecyclePolicy::default();
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_queue_installed(&mq);

        // A batch enters a listener that never returns, and that call is aged past the budget by
        // moving only its own start time.
        progress.note_delivery_pending(4);
        let stuck = progress.note_callback_started(4);
        {
            let mut active = progress.callback_started_at.lock();
            let started = active.get_mut(&stuck).unwrap();
            *started = started.saturating_sub(policy.pending_delivery_warn.as_millis() as u64 + 1);
        }

        // Everything else advances: another callback runs to completion, a pull comes back with
        // work, so the aggregate "callback progress" timestamp is fresh.
        progress.note_delivery_pending(3);
        let other = progress.note_callback_started(3);
        progress.note_callback_finished(other, 3);
        progress.note_pull_completed(&mq, true);

        let snap = snapshot(&progress);
        assert_eq!(snap.active_callbacks, 1, "only the stuck call is still running");
        assert!(
            snap.age_ms(snap.last_callback_progress_ms) < policy.pending_delivery_warn,
            "callback bookkeeping as a whole looks like it just progressed"
        );

        let mut machine = RecoveryMachine::new(policy, 1);
        let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
        assert_eq!(state.health, crate::consumer::managed_consumer::ConsumerHealth::Blocked);
        assert_eq!(state.reason, Reason::ListenerActiveOverBudget);
        assert!(
            !matches!(effects.first(), Some(Effect::Stop { .. })),
            "a blocked listener never asks for replacement or restart: {effects:?}"
        );
    }

    /// A deliberate wait is healthy until its own expectation fails, and becomes repairable once it
    /// lapses (plan 49c C2).
    #[test]
    fn a_wait_that_has_not_lapsed_is_healthy_and_a_lapsed_one_is_repairable() {
        let progress = ConsumerProgress::new();
        let waiting = queue("T", 0);
        let lapsed = queue("T", 1);
        progress.note_queue_installed(&waiting);
        silent_queue(&progress, &lapsed, Duration::from_secs(60));
        progress.note_queue_wait(&waiting, Duration::from_secs(5));
        progress.note_queue_wait(&lapsed, Duration::ZERO);

        let snap = snapshot(&progress);
        assert_eq!(snap.waiting_queues, 1, "one queue is waiting on purpose");
        assert_eq!(
            snap.lost_pull_queues, 1,
            "the queue whose wait already lapsed with no work has failed its own expectation"
        );
    }

    #[test]
    fn callback_bookkeeping_never_underflows_pending_counts() {
        let progress = ConsumerProgress::new();
        progress.note_delivery_pending(2);
        let id = progress.note_callback_started(2);
        progress.note_callback_finished(id, 2);
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
            let _guard = DeliveryOwner::new(&progress, &queue, 3);
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
            let mut guard = DeliveryOwner::new(&progress, &queue, 2);
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

    /// Plan 49c C4: a batch whose retry is still sleeping must stay counted as outstanding work
    /// while the attempt that scheduled it finishes. Before this, the finished attempt
    /// subtracted the whole batch - including the messages it had just handed to the delayed
    /// task - so the sleeping retry was invisible and its own later release underflowed.
    #[test]
    fn a_sleeping_retry_keeps_its_messages_pending_until_it_runs() {
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_assignment(&HashSet::from([mq.clone()]));

        // One admitted batch of three; two are committed now, one goes to a delayed retry.
        let mut batch = progress.admit_batch(&mq, 3);
        batch.enter_listener();
        batch.leave_listener();
        let mut retry = batch.split_off(1).expect("the batch owns the retried message");

        let snap = snapshot(&progress);
        assert_eq!(snap.committing_delivery_msgs, 2, "two messages are being committed now");
        assert_eq!(snap.pending_delivery_msgs, 1, "and one is queued for its retry");

        drop(batch);

        let snap = snapshot(&progress);
        assert_eq!(
            snap.pending_delivery_msgs + snap.committing_delivery_msgs,
            1,
            "the sleeping retry is still outstanding work"
        );
        assert_eq!(
            snap.committing_delivery_msgs, 0,
            "the attempt that handed it off kept only its own messages"
        );
        assert_eq!(progress.accounting_underflows(), 0);

        // The retry runs and finishes; that releases exactly the one message it owns.
        retry.enter_listener();
        retry.leave_listener();
        drop(retry);
        let snap = snapshot(&progress);
        assert_eq!(snap.pending_delivery_msgs, 0);
        assert_eq!(snap.committing_delivery_msgs, 0);
        assert_eq!(snap.active_callbacks, 0);
        assert_eq!(
            progress.accounting_underflows(),
            0,
            "one batch was released once, not twice"
        );
    }

    /// Ownership that never reaches a listener - rejected admission or cancellation before entry -
    /// still releases exactly what it was given (plan 49c C4).
    #[test]
    fn ownership_taken_at_admission_is_released_even_when_nothing_runs() {
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_assignment(&HashSet::from([mq.clone()]));
        {
            let batch = progress.admit_batch(&mq, 4);
            assert_eq!(snapshot(&progress).pending_delivery_msgs, 4);
            assert_eq!(batch.messages(), 4);
        }
        let snap = snapshot(&progress);
        assert_eq!(snap.pending_delivery_msgs, 0);
        assert_eq!(snap.callbacks_started, 0, "a batch that never ran is not a callback");
        assert_eq!(progress.accounting_underflows(), 0);

        // Releasing more than is outstanding is a broken ownership pairing. It is reported and the
        // counter is left alone rather than clamped, so other batches keep their counts (plan 49c
        // C4).
        progress.note_delivery_dropped(3);
        let snap = snapshot(&progress);
        assert_eq!(snap.pending_delivery_msgs, 0, "the counter never went negative");
        assert_eq!(
            snap.accounting_underflows, 1,
            "and the status projection carries the broken pairing where operators can see it"
        );
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
            let (state, effects) = machine.on_tick(&snap, Duration::from_secs(30), 0.5);
            // The repair this tick asked for runs and settles without resolving anything: attempts
            // are charged when they finish, never per status tick (plan 49d R3).
            machine.note_repair_settled(false);
            assert_eq!(
                state.phase,
                crate::consumer::managed_consumer::ConsumerPhase::Recovering,
                "a fruitless repair never becomes terminal: {state:?}"
            );
            // Bounded either way: an attempt charges itself when it settles, and the following
            // status tick is what escalates, so the counter may sit one over the
            // allowance in between.
            assert!(machine.stalled_repairs <= policy.max_repairs_before_replace + 1);
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

    /// Plan 49c C6: with a sticky stop, an unproved pass is reported as blocked *because of
    /// cleanup*, not merely as "still stopping" - and re-observing the same settled result
    /// restates that state without changing it. An observer must never have to invent progress
    /// by asking again.
    #[test]
    fn an_unproved_pass_under_a_sticky_stop_is_blocked_on_cleanup_and_stays_that_way() {
        use crate::consumer::managed_consumer::ConsumerHealth;
        use crate::consumer::managed_consumer::ConsumerPhase;
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.request_stop();
        let first = machine.on_cleanup_unproved();
        assert_eq!(first.phase, ConsumerPhase::Stopping);
        assert_eq!(first.health, ConsumerHealth::Blocked);
        assert_eq!(
            first.reason,
            Reason::CleanupUnproved,
            "reasserting `stopping` alone hides the fact that ownership was never released"
        );
        // A second observation of an unchanged result yields exactly what the first one did.
        let again = machine.on_cleanup_unproved();
        assert_eq!(again, first);
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

    /// Plan 49d R1: a pull loop keeps one lease across successful answers and delayed
    /// continuations, so owning that lease may never be read as "one RPC has been outstanding
    /// since the loop started". A healthy queue reaching the answer deadline used to declare
    /// every retained lease lost, which no repair could claim back, and escalated a working
    /// consumer into replacement.
    #[test]
    fn healthy_continuations_do_not_expire_the_loop_lease() {
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_queue_installed(&mq);
        let _lease = progress
            .acquire_pull_lease(&mq)
            .expect("an installed queue is owned by this incarnation");
        // One long poll completes and the same loop admits the next attempt, far past the answer
        // window of any single RPC.
        std::thread::sleep(Duration::from_millis(25));
        progress.note_pull_completed(&mq, false);
        let epoch = progress.current_epoch(&mq).expect("still owned");
        assert!(progress
            .begin_pull_attempt(&mq, epoch, Duration::from_secs(30))
            .is_some());

        let snap = progress.snapshot(Duration::from_secs(30), Duration::from_millis(10));
        assert_eq!(
            snap.lost_pull_queues, 0,
            "the lease belongs to the whole loop, not one RPC"
        );
        assert_eq!(snap.leased_queues, 1);

        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.on_start_result(true, false, false, 0.5);
        let mut replacement = false;
        for _ in 0..4 {
            let (_, effects) = machine.on_tick(&snap, Duration::from_millis(10), 0.5);
            replacement |= effects.iter().any(|effect| matches!(effect, Effect::Stop { .. }));
        }
        assert!(
            !replacement,
            "a loop that keeps completing pulls must not spend replacement allowance"
        );
    }

    /// Plan 49d R1: a deliberately scheduled wait - suspension, flow control, a retry backoff - is
    /// the queue's own expectation. While it lies in the future the queue is waiting on purpose
    /// and is not
    // an outstanding RPC, whatever its lease age looks like.
    #[test]
    fn an_explicit_wait_is_not_an_outstanding_rpc() {
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_queue_installed(&mq);
        let _lease = progress
            .acquire_pull_lease(&mq)
            .expect("an installed queue is owned by this incarnation");
        std::thread::sleep(Duration::from_millis(25));
        progress.note_pull_completed(&mq, true);
        progress.note_queue_wait(&mq, Duration::from_secs(5));

        let snap = progress.snapshot(Duration::from_secs(30), Duration::from_millis(10));
        assert_eq!(snap.waiting_queues, 1, "the scheduled wait is visible");
        assert_eq!(
            snap.lost_pull_queues, 0,
            "an intentional wait is not an outstanding RPC"
        );
    }

    /// Plan 49d R1: attempt identity is what makes the record safe. Only the attempt that is
    /// currently recorded may retire it, so a late answer for an older attempt cannot make a
    /// newer one look finished - and cannot make a genuinely unanswered one look healthy.
    #[test]
    fn only_the_live_attempt_can_end_its_own_window() {
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_queue_installed(&mq);
        let _lease = progress
            .acquire_pull_lease(&mq)
            .expect("an installed queue is owned by this incarnation");
        let epoch = progress.current_epoch(&mq).expect("still owned");
        let first = progress
            .begin_pull_attempt(&mq, epoch, Duration::from_secs(30))
            .expect("the attempt belongs to this acquisition");
        // The loop reopens its record: the second attempt is now the live work.
        let second = progress
            .begin_pull_attempt(&mq, epoch, Duration::from_millis(10))
            .expect("the same loop may admit its next attempt");
        assert_ne!(first, second, "attempt identities are never reused");

        // A callback for the first attempt must leave the second one's window standing.
        progress.finish_pull_attempt(&mq, epoch, first);
        std::thread::sleep(Duration::from_millis(25));
        assert_eq!(
            progress
                .lost_pulls(Duration::from_secs(30), Duration::from_secs(30))
                .len(),
            1,
            "the live attempt's own window passed and a stale answer cannot retire it"
        );

        progress.finish_pull_attempt(&mq, epoch, second);
        assert_eq!(
            progress
                .lost_pulls(Duration::from_secs(30), Duration::from_secs(30))
                .len(),
            0,
            "the attempt that was actually admitted can end its own window"
        );
    }

    /// Plan 49d R1: taking the lease starts no RPC timer, so a loop that holds its queue while
    /// admitting nothing and scheduling no continuation stays diagnosable instead of reading as
    /// an eternally healthy long poll. Repair still may not take that lease away.
    #[test]
    fn a_leased_queue_with_no_live_attempt_or_wait_stays_diagnosable() {
        let progress = ConsumerProgress::new();
        let mq = queue("T", 0);
        progress.note_queue_installed(&mq);
        let _lease = progress
            .acquire_pull_lease(&mq)
            .expect("an installed queue is owned by this incarnation");
        // Age the acquisition: nothing has completed for a while, and no attempt or wait was
        // recorded.
        silent_queue(&progress, &mq, Duration::from_millis(25));

        let snap = progress.snapshot(Duration::from_millis(5), Duration::from_secs(30));
        assert_eq!(snap.leased_queues, 1, "the loop still owns the queue");
        assert_eq!(
            snap.lost_pull_queues, 1,
            "a lease with no live attempt and no scheduled wait is a lost pull"
        );
        assert!(
            progress.acquire_pull_lease(&mq).is_none(),
            "health never releases a live loop's lease to allow a duplicate pull"
        );
    }
}

#[cfg(test)]
mod review49e {
    use super::*;
    #[test]
    fn a_siblings_empty_pull_must_not_hide_a_current_route_failure() {
        let p = ConsumerProgress::new();
        let good = MessageQueue::from_parts("T", "good", 0);
        let bad = MessageQueue::from_parts("T", "bad", 0);
        p.note_queue_installed(&good);
        p.note_queue_installed(&bad);
        p.note_pull_completed(&good, false);
        p.note_pull_failed(&bad, PullFailureKind::RouteOrBrokerMiss);
        p.note_queue_wait(&bad, Duration::from_secs(3));
        let s = p.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        let mut m = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        m.on_start_result(true, false, false, 0.5);
        let (state, effects) = m.on_tick(&s, Duration::from_secs(35), 0.5);
        eprintln!(
            "current queue fault: route_miss_queues={} state={state:?} effects={effects:?}",
            s.route_miss_queues
        );
        assert_eq!(s.route_miss_queues, 1);
        assert_eq!(
            state.reason,
            Reason::RouteOrBrokerMiss,
            "a historical empty pull certified another queue's current failure"
        );
    }
}

#[cfg(test)]
mod review49e_delivery {
    use super::*;
    #[test]
    fn releasing_delivery_ownership_must_not_clear_a_pull_route_failure() {
        let p = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "broker", 0);
        p.note_queue_installed(&q);
        p.note_pull_failed(&q, PullFailureKind::RouteOrBrokerMiss);
        assert_eq!(
            p.snapshot(Duration::from_secs(30), Duration::from_secs(35))
                .route_miss_queues,
            1
        );
        std::thread::sleep(Duration::from_millis(2));
        drop(p.admit_batch(&q, 1));
        let s = p.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!(
            "delivery release after a route failure: route_miss_queues={} pulls_completed={}",
            s.route_miss_queues, s.pulls_completed
        );
        assert_eq!(
            s.route_miss_queues, 1,
            "delivery bookkeeping is not a successful pull response"
        );
    }
}

/// Plan 49e E1: whose evidence a report may become is decided under the same lock that writes it,
/// so no interleaving between a revoke and a reinstall can let retired work stamp the replacement.
#[cfg(test)]
mod review49e_acquisition {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn a_stale_report_cannot_interleave_with_reacquisition() {
        let progress = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "broker", 0);
        progress.note_queue_installed(&q);
        // The identity an in-flight request would carry, plus the lease that request holds.
        let stale_epoch = progress.current_epoch(&q).unwrap();
        let _stale_lease = progress.acquire_pull_lease(&q).expect("leased once");

        let barrier = Arc::new(Barrier::new(5));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let progress = Arc::clone(&progress);
                let q = q.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..500 {
                        progress.note_pull_failed_for(&q, Some(stale_epoch), PullFailureKind::RouteOrBrokerMiss);
                        progress.note_queue_wait_for(&q, Some(stale_epoch), Duration::from_secs(5));
                    }
                })
            })
            .collect();
        barrier.wait();
        // The acquisition is replaced while those reports are in flight.
        progress.note_queue_revoked(&q);
        progress.note_queue_installed(&q);
        for worker in workers {
            let _ = worker.join();
        }

        let s = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!(
            "stale reports against a replacement: route_miss={} waiting={} assigned={}",
            s.route_miss_queues, s.waiting_queues, s.assigned_queues
        );
        assert_eq!(s.assigned_queues, 1);
        assert_eq!(
            s.route_miss_queues, 0,
            "a report for a retired epoch stamped the acquisition that replaced it"
        );
        assert_eq!(
            s.waiting_queues, 0,
            "a report for a retired epoch scheduled work for the acquisition that replaced it"
        );
        // The replacement is left exactly as its owner installed it: leasable, and only once.
        let lease = progress
            .acquire_pull_lease(&q)
            .expect("the current acquisition takes its single lease");
        assert!(
            progress.acquire_pull_lease(&q).is_none(),
            "exactly one pull loop may hold this queue"
        );
        drop(lease);
    }
}

/// Plan 49e E2: a queue's own current transport fault is judged before any aggregate healthy
/// answer, and only a successful pull of the same acquisition retires it.
#[cfg(test)]
mod review49e_evidence {
    use super::*;

    fn new_machine() -> RecoveryMachine {
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.on_start_result(true, false, false, 0.5);
        machine
    }

    #[test]
    fn a_current_route_failure_stands_beside_a_live_long_poll() {
        let progress = ConsumerProgress::new();
        let good = MessageQueue::from_parts("T", "good", 0);
        let bad = MessageQueue::from_parts("T", "bad", 0);
        progress.note_queue_installed(&good);
        progress.note_queue_installed(&bad);
        // A sibling that answered empty and is now sitting in a long poll inside its own deadline.
        progress.note_pull_completed(&good, false);
        let lease = progress.acquire_pull_lease(&good).unwrap();
        progress.begin_pull_attempt(&good, lease.epoch(), Duration::from_secs(30));
        // This queue's newest answer was a route miss and it is waiting on a scheduled retry.
        progress.note_pull_failed(&bad, PullFailureKind::RouteOrBrokerMiss);
        progress.note_queue_wait(&bad, Duration::from_secs(3));

        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.route_miss_queues, 1);
        assert!(snapshot.waiting_queues > 0, "the failed queue waits on purpose");
        let mut machine = new_machine();
        let (state, effects) = machine.on_tick(&snapshot, Duration::from_secs(35), 0.5);
        eprintln!("mixed queues: reason={:?} effects={:?}", state.reason, effects);
        assert_eq!(state.reason, Reason::RouteOrBrokerMiss);
        assert!(matches!(
            effects.first(),
            Some(Effect::Repair(RepairKind::RouteAndPull))
        ));
    }

    #[test]
    fn an_rpc_timeout_stands_beside_advancing_callbacks() {
        let progress = ConsumerProgress::new();
        let bad = MessageQueue::from_parts("T", "bad", 0);
        let busy = MessageQueue::from_parts("T", "busy", 0);
        progress.note_queue_installed(&bad);
        progress.note_queue_installed(&busy);
        // Work is outstanding on this queue while its callbacks keep advancing: the listener call
        // returned and the batch sits in the commit phase.
        let mut owner = progress.admit_batch(&busy, 3);
        owner.enter_listener();
        owner.leave_listener();
        // Another queue's newest answer was an RPC timeout.
        progress.note_pull_failed(&bad, PullFailureKind::RpcTimeout);

        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert!(snapshot.has_pending_work());
        let mut machine = new_machine();
        let (state, effects) = machine.on_tick(&snapshot, Duration::from_secs(35), 0.5);
        eprintln!("advancing callbacks: reason={:?} effects={:?}", state.reason, effects);
        assert_eq!(state.reason, Reason::RpcTimeout);
        assert!(matches!(
            effects.first(),
            Some(Effect::Repair(RepairKind::RouteAndPull))
        ));
        drop(owner);
    }

    #[test]
    fn only_a_successful_pull_or_a_revocation_retires_the_fault() {
        let progress = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "broker", 0);
        progress.note_queue_installed(&q);
        progress.note_pull_failed(&q, PullFailureKind::RouteOrBrokerMiss);
        assert_eq!(
            progress
                .snapshot(Duration::from_secs(30), Duration::from_secs(35))
                .route_miss_queues,
            1
        );

        // Local activity of every kind leaves the fault standing (plan 49e E2).
        std::thread::sleep(Duration::from_millis(2));
        progress.touch(&q);
        drop(progress.admit_batch(&q, 1));
        let mut owner = progress.admit_batch(&q, 1);
        owner.enter_listener();
        owner.leave_listener();
        drop(owner);
        progress.note_queue_installed(&q);
        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.route_miss_queues, 1, "none of this answered a pull");

        // The same acquisition answering a pull successfully retires it.
        std::thread::sleep(Duration::from_millis(2));
        progress.note_pull_completed(&q, true);
        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.route_miss_queues, 0);

        // And a revoked queue stops being evidence the moment it leaves this incarnation.
        progress.note_pull_failed(&q, PullFailureKind::RpcTimeout);
        assert_eq!(
            progress
                .snapshot(Duration::from_secs(30), Duration::from_secs(35))
                .rpc_timeout_queues,
            1
        );
        progress.note_queue_revoked(&q);
        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.rpc_timeout_queues, 0);
        assert_eq!(snapshot.assigned_queues, 0);
    }

    #[test]
    fn healthy_idle_and_deliberate_waits_stay_quiet() {
        // Successful empty pulls and nothing outstanding: idle, with no repair requested.
        let progress = ConsumerProgress::new();
        for id in 0..2 {
            let q = MessageQueue::from_parts("T", "broker", id);
            progress.note_queue_installed(&q);
            progress.note_pull_completed(&q, false);
        }
        let mut machine = new_machine();
        let (state, effects) = machine.on_tick(
            &progress.snapshot(Duration::from_secs(30), Duration::from_secs(35)),
            Duration::from_secs(35),
            0.5,
        );
        assert_eq!(state.reason, Reason::IdleNoMessages);
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Repair(_))));

        // Queues sitting in scheduled retries with no failure of their own: waiting, not faulty.
        let progress = ConsumerProgress::new();
        for id in 0..2 {
            let q = MessageQueue::from_parts("T", "broker", id);
            progress.note_queue_installed(&q);
            progress.note_pull_completed(&q, false);
            progress.note_queue_wait(&q, Duration::from_secs(3));
        }
        let mut machine = new_machine();
        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.route_miss_queues, 0);
        let (state, effects) = machine.on_tick(&snapshot, Duration::from_secs(35), 0.5);
        eprintln!("quiet waits: reason={:?} effects={:?}", state.reason, effects);
        assert!(!matches!(
            state.health,
            crate::consumer::managed_consumer::ConsumerHealth::Degraded
        ));
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Repair(_))));
    }
}

/// Plan 49f F1: a fault that no tracked queue still holds must stop being evidence.
#[cfg(test)]
mod review49f {
    use super::*;

    #[test]
    fn revocation_must_retire_current_managed_failure_from_the_fallback() {
        let p = ConsumerProgress::new();
        let old = MessageQueue::from_parts("T", "old", 0);
        let good = MessageQueue::from_parts("T", "good", 0);
        p.note_queue_installed(&old);
        let epoch = p.current_epoch(&old).unwrap();
        std::thread::sleep(Duration::from_millis(3));
        assert!(p.note_pull_failed_for(&old, Some(epoch), PullFailureKind::RouteOrBrokerMiss));
        p.note_queue_revoked(&old);
        p.note_queue_installed(&good);
        let s = p.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.on_start_result(true, false, false, 0.5);
        let (state, _) = machine.on_tick(&s, Duration::from_secs(35), 0.5);
        eprintln!(
            "revoked managed fault: tracked_faults={} aggregate_miss={} last_ok={} state={state:?}",
            s.route_miss_queues, s.pull_route_miss_ms, s.last_pull_ok_ms
        );
        assert_eq!(s.route_miss_queues, 0);
        assert_eq!(
            state.reason,
            Reason::None,
            "revocation removed the queue fault but left it live in the aggregate fallback"
        );
    }
}
