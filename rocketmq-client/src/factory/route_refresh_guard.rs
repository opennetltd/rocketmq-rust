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

//! Single-flight/cooldown guard for route refreshes triggered by a broker-address miss.
//!
//! A missing broker address used to make every queued pull issue its own nameserver
//! fetch. Thousands of queues on one topic therefore produced thousands of sequential
//! round trips, each serialized behind the nameserver lock (plan 49 section 4.1.4).
//! Only the first miss performs a fetch; concurrent misses join it and later misses are
//! answered from the cooldown window. Ordinary scheduled route refreshes deliberately
//! bypass this guard.
//!
//! Ownership of a flight is a [`RefreshTicket`], not a convention (plan 50 F8): the slot
//! is released when the ticket is completed *or* dropped, so a leader whose future is
//! cancelled or whose task panics cannot strand the topic in "in flight" forever. Each
//! flight carries a generation, so a late completion can never clear a newer one.
//!
//! A ticket belongs to one exact entry and generation (plan 49b R3): releasing looks the
//! topic up without creating state, validates the generation under the same lock that holds
//! the cooldown timestamp, and does nothing at all when either has moved on. A completion
//! from a topic that was pruned meanwhile therefore cannot resurrect its map entry, record a
//! cooldown, or take another flight's result - generations are never reused, so there is
//! nothing for a late arrival to match. Joining reports the outcome of the flight that was
//! joined rather than whatever finished last, and pruning wakes the waiters of what it removes.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

/// How much of a cooldown window is reused as the retry window after a failed flight, and its
/// floor: enough that a stream of queue misses coalesces into few fetches, short enough that a real
/// fix is not held hostage by one bad answer (plan 49b R3).
const FAILURE_BACKOFF_DIVISOR: u32 = 4;
const FAILURE_BACKOFF_FLOOR: Duration = Duration::from_millis(50);

use cheetah_string::CheetahString;
use tokio::sync::Notify;

/// Outcome of asking for a miss-triggered route refresh.
pub(crate) enum BeginRefresh {
    /// This caller owns the refresh and holds its ticket.
    Fetch(RefreshTicket),
    /// A refresh is already running; wait on it instead of issuing another fetch.
    Joining,
    /// A recent successful refresh makes this refresh redundant.
    Cooldown,
    /// The last attempt failed and its bounded retry window has not elapsed; re-check local tables
    /// instead of issuing another fetch per queue (plan 49b R3).
    Backoff(Instant),
    /// The client is stopping; no refresh may start or wait any more.
    Closed,
}

impl BeginRefresh {
    /// Variant name for assertions and logs without moving a ticket out of the result.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            BeginRefresh::Fetch(_) => "fetch",
            BeginRefresh::Joining => "joining",
            BeginRefresh::Cooldown => "cooldown",
            BeginRefresh::Backoff(_) => "backoff",
            BeginRefresh::Closed => "closed",
        }
    }
}

/// Everything the guard decides about one topic, held behind a single lock so an outcome, its
/// cooldown/retry timestamps and the idle transition are published together. Splitting them let a
/// stale completion award another flight's cooldown (plan 49b R3).
struct FlightState {
    /// `0` means idle, otherwise it is the generation owning the flight.
    generation: u64,
    /// When a nameserver last answered for this topic; drives the success cooldown.
    last_success: Option<Instant>,
    /// Until when a further attempt is deferred because the last one failed.
    retry_after: Option<Instant>,
    /// The outcome of the most recent completed flight, with its generation, so a joiner can be
    /// told what the flight it waited on achieved.
    completed: Option<(u64, bool)>,
    /// Set when the topic was pruned out from under this state: anyone still waiting on it is
    /// waiting on nothing and must stop (plan 49b R3).
    forgotten: bool,
}

impl FlightState {
    fn new() -> Self {
        Self {
            generation: 0,
            last_success: None,
            retry_after: None,
            completed: None,
            forgotten: false,
        }
    }
}

#[derive(Clone)]
struct Entry {
    flight: Arc<Mutex<FlightState>>,
    notify: Arc<Notify>,
}

/// Shared state behind the guard and every ticket it issued, so a ticket can release its
/// slot on drop without borrowing the guard (the caller holds `&mut self` meanwhile).
struct State {
    cooldown: Duration,
    retry_backoff: Duration,
    closed: AtomicBool,
    next_generation: AtomicU64,
    entries: Mutex<HashMap<CheetahString, Entry>>,
}

impl State {
    /// The entry for `topic`, creating it if this is the first attempt. Only a caller that intends
    /// to claim or join a flight may create state.
    fn entry(&self, topic: &CheetahString) -> Option<Entry> {
        let mut guard = self.entries.lock().ok()?;
        Some(
            guard
                .entry(topic.clone())
                .or_insert_with(|| Entry {
                    flight: Arc::new(Mutex::new(FlightState::new())),
                    notify: Arc::new(Notify::new()),
                })
                .clone(),
        )
    }

    /// Look `topic` up without creating anything. A completion for a topic that has been pruned
    /// finds nothing and is dropped, which is what keeps pruning final (plan 49b R3).
    fn lookup(&self, topic: &CheetahString) -> Option<Entry> {
        let guard = self.entries.lock().ok()?;
        guard.get(topic).cloned()
    }

    /// Release `topic`'s flight if `generation` still owns it. A completion from an older
    /// generation, or for a topic that no longer exists, changes nothing at all: it cannot
    /// clear a refresh that started after it, record a cooldown, or schedule its retry (plans
    /// 50 F8 and 49b R3).
    fn release(&self, topic: &CheetahString, generation: u64, success: bool) {
        let Some(entry) = self.lookup(topic) else {
            return;
        };
        let Ok(mut flight) = entry.flight.lock() else {
            return;
        };
        if flight.generation != generation {
            // Not ours any more. Leave the newer flight's state exactly as it is.
            drop(flight);
            return;
        }
        let now = Instant::now();
        flight.generation = 0;
        flight.completed = Some((generation, success));
        if success {
            flight.last_success = Some(now);
            flight.retry_after = None;
        } else if !self.retry_backoff.is_zero() {
            flight.retry_after = Some(now + self.retry_backoff);
        }
        drop(flight);
        Self::wake_entry(&entry.notify);
    }

    /// Releases every waiter now and retains one permit for a caller that has not
    /// re-registered yet, so completion is never lost.
    fn wake_entry(notify: &Notify) {
        notify.notify_waiters();
        notify.notify_one();
    }
}

/// What waiting on a flight told the joiner. A joiner acts on the flight it joined, not on
/// whichever completion happened to land last (plan 49b R3).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlightAnswer {
    /// The flight we joined answered a nameserver result successfully.
    Succeeded,
    /// The flight we joined finished without an answer; the next attempt waits out its retry
    /// window.
    Failed,
    /// Our flight ended and another one owns the topic now, or the state was pruned: re-check local
    /// tables, but do not read this as a success.
    Superseded,
    /// The client is stopping.
    Closed,
    /// Our own budget elapsed while the flight still runs.
    Timeout,
}

impl FlightAnswer {
    /// Variant name for assertions and logs.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            FlightAnswer::Succeeded => "succeeded",
            FlightAnswer::Failed => "failed",
            FlightAnswer::Superseded => "superseded",
            FlightAnswer::Closed => "closed",
            FlightAnswer::Timeout => "timeout",
        }
    }

    /// Whether the caller should re-check its own tables afterwards.
    pub(crate) fn recheck(&self) -> bool {
        !matches!(self, FlightAnswer::Closed | FlightAnswer::Timeout)
    }
}

/// Bounded per-topic single-flight state shared by one `MQClientInstance`.
pub(crate) struct RouteRefreshGuard {
    state: Arc<State>,
}

impl RouteRefreshGuard {
    pub(crate) fn new(cooldown: Duration) -> Self {
        // A failed flight is retried sooner than a success is repeated, but never per queue miss.
        let retry_backoff = (cooldown / FAILURE_BACKOFF_DIVISOR).max(FAILURE_BACKOFF_FLOOR);
        Self::with_retry_backoff(cooldown, retry_backoff)
    }

    /// The same guard with an explicit failure retry window.
    fn with_retry_backoff(cooldown: Duration, retry_backoff: Duration) -> Self {
        Self {
            state: Arc::new(State {
                cooldown,
                retry_backoff,
                closed: AtomicBool::new(false),
                next_generation: AtomicU64::new(0),
                entries: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// How many flights this guard has ever claimed (plan 49e E3, test-only). A deferred cooldown
    /// or backoff check must claim none, and `len()` cannot show that because the topic keeps
    /// its entry either way.
    #[cfg(test)]
    pub(crate) fn claimed_flights(&self) -> u64 {
        self.state.next_generation.load(Ordering::Acquire)
    }

    /// Number of topics holding refresh state; diagnostics and tests only.
    pub(crate) fn len(&self) -> usize {
        self.state.entries.lock().map(|guard| guard.len()).unwrap_or(0)
    }

    fn entry(&self, topic: &CheetahString) -> Option<Entry> {
        self.state.entry(topic)
    }

    /// Claims the right to refresh `topic`, or explains why the caller must not. On
    /// [`BeginRefresh::Fetch`] the returned ticket owns the slot for as long as it lives.
    pub(crate) fn begin(&self, topic: &CheetahString) -> BeginRefresh {
        if self.state.closed.load(Ordering::Acquire) {
            return BeginRefresh::Closed;
        }
        let Some(entry) = self.entry(topic) else {
            // A poisoned guard must not start unbounded network work.
            return BeginRefresh::Closed;
        };
        // One lock covers the running check, both windows and the claim, so no other completion can
        // slip a result in between them.
        let Ok(mut flight) = entry.flight.lock() else {
            return BeginRefresh::Closed;
        };
        if flight.generation != 0 {
            return BeginRefresh::Joining;
        }
        if !self.state.cooldown.is_zero() && flight.last_success.is_some_and(|at| at.elapsed() < self.state.cooldown) {
            return BeginRefresh::Cooldown;
        }
        if let Some(until) = flight.retry_after {
            if until > Instant::now() {
                return BeginRefresh::Backoff(until);
            }
        }
        let generation = self.state.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        flight.generation = generation;
        flight.retry_after = None;
        drop(flight);
        BeginRefresh::Fetch(RefreshTicket {
            state: Arc::clone(&self.state),
            topic: topic.clone(),
            generation,
            settled: false,
        })
    }

    /// Waits until the running refresh for `topic` finishes or `budget` elapses.
    /// Returns `false` when closed or out of budget; callers then re-check their own
    /// tables without issuing another fetch.
    pub(crate) async fn await_in_flight(&self, topic: &CheetahString, budget: Duration) -> bool {
        self.await_flight(topic, budget).await.recheck()
    }

    /// Wait for the flight that is running when this starts, and report what *that* flight
    /// achieved. A completion belonging to a newer generation, or pruning of the topic, is
    /// reported as [`FlightAnswer::Superseded`] rather than borrowed as someone else's success
    /// (plan 49b R3).
    pub(crate) async fn await_flight(&self, topic: &CheetahString, budget: Duration) -> FlightAnswer {
        let deadline = tokio::time::Instant::now() + budget;
        // Join the flight as it is now; `0` means nothing is running to join.
        let Some(entry) = self.state.lookup(topic) else {
            return FlightAnswer::Superseded;
        };
        let joined = match entry.flight.lock() {
            Ok(flight) => flight.generation,
            Err(_) => return FlightAnswer::Closed,
        };
        if joined == 0 {
            return FlightAnswer::Superseded;
        }
        loop {
            if self.state.closed.load(Ordering::Acquire) {
                return FlightAnswer::Closed;
            }
            // Register interest before reading the state so a completion between the
            // check and the wait cannot be missed; notify_one retains one permit.
            let notified = Box::pin(entry.notify.notified());
            let observed = entry.flight.lock().ok().and_then(|flight| {
                if flight.forgotten {
                    // Pruned away: our flight is not being tracked any more, and it is nobody's
                    // success.
                    return Some(FlightAnswer::Superseded);
                }
                if flight.generation != 0 {
                    // Still running (possibly a newer flight after pruning and a re-begin).
                    None
                } else {
                    match flight.completed {
                        Some((generation, success)) if generation == joined => Some(if success {
                            FlightAnswer::Succeeded
                        } else {
                            FlightAnswer::Failed
                        }),
                        // Our flight ended but something newer owns the result, or the entry was
                        // pruned and recreated: never claim another flight's outcome.
                        _ => Some(FlightAnswer::Superseded),
                    }
                }
            });
            if let Some(answer) = observed {
                return answer;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return FlightAnswer::Timeout;
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// Forgets state for topics that are no longer subscribed or cached.
    ///
    /// Removal is real: the entry is deleted, so a completion arriving afterwards finds nothing and
    /// is dropped rather than recreating state or recording a cooldown nobody asked for -
    /// generations are never reused, so there is nothing for a late arrival to match and no
    /// tombstone table is needed. Whatever is removed has its waiters released immediately as
    /// superseded: pruning must not strand a caller behind a flight it just stopped tracking
    /// (plan 49b R3).
    pub(crate) fn retain<F: Fn(&str) -> bool>(&self, keep: F) {
        let removed: Vec<Entry> = match self.state.entries.lock() {
            Ok(mut guard) => {
                let dropped: Vec<Entry> = guard
                    .iter()
                    .filter(|(topic, _)| !keep(topic.as_str()))
                    .map(|(_, entry)| entry.clone())
                    .collect();
                guard.retain(|topic, _| keep(topic.as_str()));
                dropped
            }
            Err(_) => return,
        };
        for entry in removed {
            // Settle the flight as superseded so a waiter cannot read it as somebody's success, and
            // stop anyone already parked on it from waiting for a topic that no longer
            // exists.
            if let Ok(mut flight) = entry.flight.lock() {
                flight.completed = None;
                flight.forgotten = true;
            }
            State::wake_entry(&entry.notify);
        }
    }

    /// Cancels waiters and refuses new refreshes during shutdown.
    pub(crate) fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
        if let Ok(guard) = self.state.entries.lock() {
            for entry in guard.values() {
                State::wake_entry(&entry.notify);
            }
        }
    }

    /// The generation currently owning `topic`'s flight, or `0`; tests only.
    #[cfg(test)]
    fn current_generation(&self, topic: &CheetahString) -> u64 {
        self.state
            .lookup(topic)
            .and_then(|entry| entry.flight.lock().ok().map(|flight| flight.generation))
            .unwrap_or(0)
    }

    /// Claim a flight directly, bypassing single-flight; used only to build the stale
    /// completion scenario in tests.
    #[cfg(test)]
    fn claim_for_test(&self, topic: &CheetahString) -> u64 {
        let generation = self.state.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(entry) = self.entry(topic) {
            if let Ok(mut flight) = entry.flight.lock() {
                flight.generation = generation;
            }
        }
        generation
    }

    #[cfg(test)]
    fn release_for_test(&self, topic: &CheetahString, generation: u64, success: bool) {
        self.state.release(topic, generation, success);
    }
}

/// Proof that the caller owns one refresh of one topic. Completing or dropping the ticket
/// releases the slot; only completing successfully starts the cooldown (plan 50 F8).
pub(crate) struct RefreshTicket {
    state: Arc<State>,
    topic: CheetahString,
    generation: u64,
    settled: bool,
}

impl RefreshTicket {
    /// Finish the flight. `success` is what a nameserver answered (plan 50 F7), and only it
    /// starts the cooldown window.
    pub(crate) fn complete(&mut self, success: bool) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.state.release(&self.topic, self.generation, success);
    }
}

impl Drop for RefreshTicket {
    fn drop(&mut self) {
        // A cancelled or panicking leader leaves through here: the slot is released as a
        // failure so the next miss can own the refresh instead of joining a dead flight.
        if !self.settled {
            self.settled = true;
            self.state.release(&self.topic, self.generation, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(name: &str) -> CheetahString {
        CheetahString::from_string(name.to_string())
    }

    #[test]
    fn a_completed_successful_flight_starts_the_cooldown() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        let mut ticket = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        ticket.complete(true);
        assert_eq!(guard.begin(&t).kind(), "cooldown");
    }

    /// Plan 49b R3: a failure no longer licenses one fetch per queue miss. The next attempt is
    /// deferred behind a bounded window, and then it really does run - deferral must not become
    /// suppression, or a transient nameserver problem would end route recovery for the topic.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_flight_defers_the_next_attempt_without_suppressing_it() {
        let guard = RouteRefreshGuard::with_retry_backoff(Duration::from_secs(30), Duration::from_millis(60));
        let t = topic("T");
        let mut ticket = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        ticket.complete(false);
        assert_eq!(guard.begin(&t).kind(), "backoff", "a stream of misses coalesces");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(guard.begin(&t).kind(), "fetch", "the retry window is bounded");
    }

    #[test]
    fn concurrent_misses_produce_exactly_one_fetch() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        let leader = guard.begin(&t);
        assert_eq!(leader.kind(), "fetch");
        let mut joining = 0;
        for _ in 0..5_407 {
            match guard.begin(&t) {
                BeginRefresh::Joining => joining += 1,
                other => panic!("unexpected {}", other.kind()),
            }
        }
        assert_eq!(joining, 5_407);
    }

    /// Plan 50 F8: a leader that never reaches its completion - cancelled future, panicking
    /// task - must not strand the topic in "in flight".
    #[test]
    fn an_abandoned_leader_releases_the_slot() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        {
            let _leader = match guard.begin(&t) {
                BeginRefresh::Fetch(ticket) => ticket,
                other => panic!("expected a fetch, got {}", other.kind()),
            };
            assert_eq!(guard.begin(&t).kind(), "joining");
        }
        assert_eq!(guard.current_generation(&t), 0, "the slot is idle again");
        // The abandoned attempt counts as a failure, so its retry window applies before the next
        // fetch.
        assert_ne!(guard.begin(&t).kind(), "joining", "nobody is left holding the slot");
    }

    /// Plan 50 F8: an abandoned leader must not look like a success either - no cooldown may be
    /// recorded from a flight that never answered.
    #[tokio::test(flavor = "current_thread")]
    async fn an_abandoned_leader_does_not_start_the_cooldown() {
        let guard = RouteRefreshGuard::with_retry_backoff(Duration::from_secs(30), Duration::from_millis(60));
        let t = topic("T");
        drop(match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        });
        assert_ne!(guard.begin(&t).kind(), "cooldown", "an abandoned flight earned nothing");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(guard.begin(&t).kind(), "fetch");
    }

    /// Plan 50 F8: a completion from an older generation cannot clear a newer flight.
    #[test]
    fn an_old_completion_cannot_clear_a_newer_flight() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        let stale = guard.claim_for_test(&t);
        let current = guard.claim_for_test(&t);

        guard.release_for_test(&t, stale, true);
        assert_eq!(guard.current_generation(&t), current);
        // The stale completion also must not have recorded a success.
        assert_eq!(guard.begin(&t).kind(), "joining");

        guard.release_for_test(&t, current, false);
        assert_eq!(guard.current_generation(&t), 0);
    }

    #[tokio::test]
    async fn waiters_terminate_when_the_leader_is_abandoned() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_secs(30)));
        let t = topic("T");
        let leader = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };

        let waiter_guard = Arc::clone(&guard);
        let waiter_topic = t.clone();
        let waiter = tokio::spawn(async move {
            // A generous budget that must not be consumed by a dead leader.
            waiter_guard
                .await_in_flight(&waiter_topic, Duration::from_secs(30))
                .await
        });

        drop(leader);
        let woke = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("a waiter must not outlive the leader it joined")
            .expect("waiter task");
        assert!(woke, "the waiter is released, not timed out");
    }

    #[tokio::test]
    async fn waiters_terminate_within_budget_when_the_leader_hangs() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_secs(30)));
        let t = topic("T");
        let _leader = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        let woke = tokio::time::timeout(
            Duration::from_secs(2),
            guard.await_in_flight(&t, Duration::from_millis(100)),
        )
        .await
        .expect("a waiter must return within its own budget");
        assert!(!woke, "an unfinished flight reports a budget miss, not a hang");
    }

    #[tokio::test]
    async fn closing_wakes_every_waiter() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_secs(30)));
        let t = topic("T");
        let _leader = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };

        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let guard = Arc::clone(&guard);
                let topic = t.clone();
                tokio::spawn(async move { guard.await_in_flight(&topic, Duration::from_secs(30)).await })
            })
            .collect();

        // Close from a blocking task so waiters are already parked on the notify.
        let closer = Arc::clone(&guard);
        tokio::task::spawn_blocking(move || closer.close())
            .await
            .expect("closer");

        for waiter in waiters {
            let woke = tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("close must wake every waiter")
                .expect("waiter task");
            assert!(!woke, "a closed guard reports no successful refresh");
        }
    }

    /// Plan 49b R3 test 1: a completion for state that has been forgotten must not put it back. The
    /// topic was pruned while its leader was fetching; publishing its result afterwards used to
    /// recreate the entry and hand out cooldown nobody asked for.
    #[test]
    fn a_forgotten_topic_is_not_resurrected_by_a_late_completion() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        let mut ticket = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        guard.retain(|_| false);
        assert_eq!(guard.len(), 0, "pruning removed the state");
        ticket.complete(true);
        assert_eq!(guard.len(), 0, "a late completion may not recreate it");
    }

    /// Plan 49b R3 test 2: a stale success must not award another flight's cooldown. The topic is
    /// pruned and re-added, so the old ticket belongs to nothing; its success may neither
    /// create state nor make the new flight's failure look like a success.
    #[test]
    fn a_stale_success_cannot_give_another_flight_its_cooldown() {
        let guard = RouteRefreshGuard::with_retry_backoff(Duration::from_secs(30), Duration::ZERO);
        let t = topic("T");
        let mut stale = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        // Forget the topic and re-add it: the new entry knows nothing of the old flight.
        guard.retain(|_| false);
        let mut current = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch for the re-added topic, got {}", other.kind()),
        };
        // The stale leader answers successfully; the new flight fails.
        stale.complete(true);
        current.complete(false);
        assert_ne!(
            guard.begin(&t).kind(),
            "cooldown",
            "the forgotten success must not suppress a needed repair"
        );
    }

    /// Plan 49b R3: joiners are told what the flight they joined achieved. A newer flight's outcome
    /// is never borrowed, and an abandoned or failed flight does not read as success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_joiner_receives_the_result_of_the_flight_it_joined() {
        let guard = Arc::new(RouteRefreshGuard::with_retry_backoff(
            Duration::from_secs(30),
            Duration::ZERO,
        ));
        let t = topic("T");
        let mut leader = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        let waiter_guard = Arc::clone(&guard);
        let waiter_topic = t.clone();
        let joined =
            tokio::spawn(async move { waiter_guard.await_flight(&waiter_topic, Duration::from_secs(5)).await });
        // Let the joiner register before the flight answers.
        tokio::time::sleep(Duration::from_millis(20)).await;
        leader.complete(true);
        let answer = tokio::time::timeout(Duration::from_secs(5), joined)
            .await
            .expect("the joiner must not outlive its flight")
            .expect("joiner task");
        assert_eq!(answer, FlightAnswer::Succeeded);

        // A failed flight reports itself as failed rather than leaving the joiner guessing. A fresh
        // guard, because the success above legitimately holds its cooldown.
        let guard = Arc::new(RouteRefreshGuard::with_retry_backoff(Duration::ZERO, Duration::ZERO));
        let mut leader = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        let waiter_guard = Arc::clone(&guard);
        let waiter_topic = t.clone();
        let joined =
            tokio::spawn(async move { waiter_guard.await_flight(&waiter_topic, Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        leader.complete(false);
        let answer = tokio::time::timeout(Duration::from_secs(5), joined)
            .await
            .expect("the joiner must not outlive its flight")
            .expect("joiner task");
        assert_eq!(answer, FlightAnswer::Failed);
    }

    /// Plan 49b R3: pruning may not strand the waiters of what it removes, and a leader that
    /// finishes afterwards must not disturb whatever flight the topic has since acquired.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pruning_wakes_waiters_and_a_late_leader_cannot_disturb_a_new_flight() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_secs(30)));
        let t = topic("T");
        let leader = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        let waiter_guard = Arc::clone(&guard);
        let waiter_topic = t.clone();
        let waiter =
            tokio::spawn(async move { waiter_guard.await_flight(&waiter_topic, Duration::from_secs(30)).await });
        // Give the waiter time to park on the notification.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let closer = Arc::clone(&guard);
        tokio::task::spawn_blocking(move || closer.retain(|_| false))
            .await
            .expect("pruning runs");
        let answer = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("pruning must wake the waiters of what it removed")
            .expect("waiter task");
        assert_eq!(answer, FlightAnswer::Superseded, "a pruned flight is nobody's success");

        // The topic is tracked again from scratch; the old leader finishing late must leave this
        // flight alone - neither clearing its slot nor recording a cooldown for it.
        let mut current = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch after pruning, got {}", other.kind()),
        };
        drop(leader);
        assert_ne!(guard.current_generation(&t), 0, "the new flight still owns its slot");
        current.complete(false);
        assert_eq!(guard.current_generation(&t), 0, "the new flight released its own slot");
        assert_ne!(
            guard.begin(&t).kind(),
            "cooldown",
            "the abandoned flight recorded no success for this one"
        );
    }

    /// Plan 49b R3: many misses behind one failure produce one retry per window, not one fetch
    /// each.
    #[test]
    fn a_stream_of_misses_behind_a_failure_issues_one_retry() {
        let guard = RouteRefreshGuard::with_retry_backoff(Duration::from_secs(30), Duration::from_secs(5));
        let t = topic("T");
        match guard.begin(&t) {
            BeginRefresh::Fetch(mut ticket) => ticket.complete(false),
            other => panic!("expected a fetch, got {}", other.kind()),
        }
        let mut fetched = 0;
        let mut deferred = 0;
        for _ in 0..5_407 {
            match guard.begin(&t) {
                BeginRefresh::Fetch(_) => fetched += 1,
                BeginRefresh::Backoff(_) => deferred += 1,
                other => panic!("unexpected {}", other.kind()),
            }
        }
        assert_eq!(fetched, 0, "the window absorbs every miss");
        assert_eq!(deferred, 5_407);
    }

    #[test]
    fn retain_bounds_the_state_map() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        for name in ["A", "B", "C"] {
            match guard.begin(&topic(name)) {
                BeginRefresh::Fetch(ticket) => drop(ticket),
                other => panic!("expected a fetch, got {}", other.kind()),
            }
        }
        assert_eq!(guard.len(), 3);
        guard.retain(|name| name == "A");
        assert_eq!(guard.len(), 1);
    }
}
