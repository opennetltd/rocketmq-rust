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

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

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
            BeginRefresh::Closed => "closed",
        }
    }
}

#[derive(Clone)]
struct Entry {
    /// `0` means idle, otherwise it is the generation owning the flight.
    flight: Arc<Mutex<u64>>,
    notify: Arc<Notify>,
}

/// Shared state behind the guard and every ticket it issued, so a ticket can release its
/// slot on drop without borrowing the guard (the caller holds `&mut self` meanwhile).
struct State {
    cooldown: Duration,
    closed: AtomicBool,
    next_generation: AtomicU64,
    entries: Mutex<HashMap<CheetahString, Entry>>,
    last_success: Mutex<HashMap<CheetahString, Instant>>,
}

impl State {
    fn entry(&self, topic: &CheetahString) -> Option<Entry> {
        let mut guard = self.entries.lock().ok()?;
        Some(
            guard
                .entry(topic.clone())
                .or_insert_with(|| Entry {
                    flight: Arc::new(Mutex::new(0)),
                    notify: Arc::new(Notify::new()),
                })
                .clone(),
        )
    }

    fn within_cooldown(&self, topic: &CheetahString) -> bool {
        if self.cooldown.is_zero() {
            return false;
        }
        let Ok(last_success) = self.last_success.lock() else {
            return false;
        };
        last_success.get(topic).is_some_and(|at| at.elapsed() < self.cooldown)
    }

    /// Release `topic`'s flight if `generation` still owns it. A completion from an older
    /// generation is dropped: it must not clear a refresh that started after it (plan 50 F8).
    fn release(&self, topic: &CheetahString, generation: u64, success: bool) {
        let entry = match self.entry(topic) {
            Some(entry) => entry,
            None => return,
        };
        let released = match entry.flight.lock() {
            Ok(mut flight) => {
                if *flight == generation {
                    *flight = 0;
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        };
        if success {
            if let Ok(mut last_success) = self.last_success.lock() {
                last_success.insert(topic.clone(), Instant::now());
            }
        }
        if released {
            Self::wake_entry(&entry.notify);
        }
    }

    /// Releases every waiter now and retains one permit for a caller that has not
    /// re-registered yet, so completion is never lost.
    fn wake_entry(notify: &Notify) {
        notify.notify_waiters();
        notify.notify_one();
    }
}

/// Bounded per-topic single-flight state shared by one `MQClientInstance`.
pub(crate) struct RouteRefreshGuard {
    state: Arc<State>,
}

impl RouteRefreshGuard {
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            state: Arc::new(State {
                cooldown,
                closed: AtomicBool::new(false),
                next_generation: AtomicU64::new(0),
                entries: Mutex::new(HashMap::new()),
                last_success: Mutex::new(HashMap::new()),
            }),
        }
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
        let Ok(mut flight) = entry.flight.lock() else {
            return BeginRefresh::Closed;
        };
        if *flight != 0 {
            return BeginRefresh::Joining;
        }
        if self.state.within_cooldown(topic) {
            return BeginRefresh::Cooldown;
        }
        let generation = self.state.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        *flight = generation;
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
        let deadline = tokio::time::Instant::now() + budget;
        let Some(entry) = self.entry(topic) else {
            return false;
        };
        loop {
            if self.state.closed.load(Ordering::Acquire) {
                return false;
            }
            // Register interest before reading the flag so a completion between the
            // check and the wait cannot be missed; notify_one retains one permit.
            let notified = Box::pin(entry.notify.notified());
            let running = entry.flight.lock().map(|flight| *flight != 0).unwrap_or(false);
            if !running {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// Forgets state for topics that are no longer subscribed or cached.
    pub(crate) fn retain<F: Fn(&str) -> bool>(&self, keep: F) {
        if let Ok(mut guard) = self.state.entries.lock() {
            guard.retain(|topic, _| keep(topic.as_str()));
        }
        if let Ok(mut guard) = self.state.last_success.lock() {
            guard.retain(|topic, _| keep(topic.as_str()));
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
        self.entry(topic)
            .and_then(|entry| entry.flight.lock().ok().map(|flight| *flight))
            .unwrap_or(0)
    }

    /// Claim a flight directly, bypassing single-flight; used only to build the stale
    /// completion scenario in tests.
    #[cfg(test)]
    fn claim_for_test(&self, topic: &CheetahString) -> u64 {
        let generation = self.state.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(entry) = self.entry(topic) {
            if let Ok(mut flight) = entry.flight.lock() {
                *flight = generation;
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

    #[test]
    fn a_failed_flight_does_not_suppress_the_next_attempt() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        let mut ticket = match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        };
        ticket.complete(false);
        assert_eq!(guard.begin(&t).kind(), "fetch");
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
        assert_eq!(guard.begin(&t).kind(), "fetch", "a new flight can start");
    }

    /// Plan 50 F8: an abandoned leader must not look like a success either.
    #[test]
    fn an_abandoned_leader_does_not_start_the_cooldown() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        drop(match guard.begin(&t) {
            BeginRefresh::Fetch(ticket) => ticket,
            other => panic!("expected a fetch, got {}", other.kind()),
        });
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
