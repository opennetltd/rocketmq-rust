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

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use tokio::sync::Notify;

/// Outcome of asking for a miss-triggered route refresh.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeginRefresh {
    /// This caller owns the refresh and must call [`RouteRefreshGuard::finish`].
    Fetch,
    /// A refresh is already running; wait on it instead of issuing another fetch.
    Joining,
    /// A recent successful refresh makes this refresh redundant.
    Cooldown,
    /// The client is stopping; no refresh may start or wait any more.
    Closed,
}

#[derive(Clone)]
struct Entry {
    in_flight: Arc<Mutex<bool>>,
    notify: Arc<Notify>,
}

/// Bounded per-topic single-flight state shared by one `MQClientInstance`.
pub(crate) struct RouteRefreshGuard {
    cooldown: Duration,
    closed: AtomicBool,
    entries: Mutex<HashMap<CheetahString, Entry>>,
    last_success: Mutex<HashMap<CheetahString, Instant>>,
}

impl RouteRefreshGuard {
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            closed: AtomicBool::new(false),
            entries: Mutex::new(HashMap::new()),
            last_success: Mutex::new(HashMap::new()),
        }
    }

    /// Number of topics holding refresh state; diagnostics and tests only.
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().map(|guard| guard.len()).unwrap_or(0)
    }

    fn entry(&self, topic: &CheetahString) -> Option<Entry> {
        let mut guard = self.entries.lock().ok()?;
        Some(
            guard
                .entry(topic.clone())
                .or_insert_with(|| Entry {
                    in_flight: Arc::new(Mutex::new(false)),
                    notify: Arc::new(Notify::new()),
                })
                .clone(),
        )
    }

    /// Claims the right to refresh `topic`, or explains why the caller must not.
    pub(crate) fn begin(&self, topic: &CheetahString) -> BeginRefresh {
        if self.closed.load(Ordering::Acquire) {
            return BeginRefresh::Closed;
        }
        let Some(entry) = self.entry(topic) else {
            // A poisoned guard must not start unbounded network work.
            return BeginRefresh::Closed;
        };
        let Ok(mut in_flight) = entry.in_flight.lock() else {
            return BeginRefresh::Closed;
        };
        if *in_flight {
            return BeginRefresh::Joining;
        }
        if self.within_cooldown(topic) {
            return BeginRefresh::Cooldown;
        }
        *in_flight = true;
        BeginRefresh::Fetch
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

    /// Releases the single-flight slot. Only a successful fetch starts the cooldown, so
    /// a failed refresh never suppresses the next scheduled repair attempt beyond it.
    pub(crate) fn finish(&self, topic: &CheetahString, success: bool) {
        if let Some(entry) = self.entry(topic) {
            if let Ok(mut in_flight) = entry.in_flight.lock() {
                *in_flight = false;
            }
        }
        if success {
            if let Ok(mut last_success) = self.last_success.lock() {
                last_success.insert(topic.clone(), Instant::now());
            }
        }
        self.wake(topic);
    }

    fn wake(&self, topic: &CheetahString) {
        if let Some(entry) = self.entry(topic) {
            Self::wake_entry(&entry.notify);
        }
    }

    /// Releases every waiter now and retains one permit for a caller that has not
    /// re-registered yet, so completion is never lost.
    fn wake_entry(notify: &Notify) {
        notify.notify_waiters();
        notify.notify_one();
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
            if self.closed.load(Ordering::Acquire) {
                return false;
            }
            // Register interest before reading the flag so a completion between the
            // check and the wait cannot be missed; notify_one retains one permit.
            let notified = Box::pin(entry.notify.notified());
            let running = entry.in_flight.lock().map(|in_flight| *in_flight).unwrap_or(false);
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
        if let Ok(mut guard) = self.entries.lock() {
            guard.retain(|topic, _| keep(topic.as_str()));
        }
        if let Ok(mut guard) = self.last_success.lock() {
            guard.retain(|topic, _| keep(topic.as_str()));
        }
    }

    /// Cancels waiters and refuses new refreshes during shutdown.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(guard) = self.entries.lock() {
            for entry in guard.values() {
                Self::wake_entry(&entry.notify);
            }
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
    fn first_miss_fetches_and_a_second_miss_within_cooldown_is_skipped() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
        guard.finish(&t, true);
        assert_eq!(guard.begin(&t), BeginRefresh::Cooldown);
    }

    #[test]
    fn failed_refresh_does_not_suppress_the_next_attempt() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
        guard.finish(&t, false);
        assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
    }

    #[test]
    fn concurrent_misses_produce_exactly_one_fetch() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        let t = topic("T");
        let mut fetches = 0;
        let mut joining = 0;
        for _ in 0..5_408 {
            match guard.begin(&t) {
                BeginRefresh::Fetch => fetches += 1,
                BeginRefresh::Joining => joining += 1,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(fetches, 1);
        assert_eq!(joining, 5_407);
    }

    #[tokio::test]
    async fn waiters_are_released_when_the_refresh_finishes() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_secs(30)));
        let t = topic("T");
        assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
        let waiter_guard = guard.clone();
        let waiter_topic = t.clone();
        let waiter = tokio::spawn(async move {
            waiter_guard
                .await_in_flight(&waiter_topic, Duration::from_secs(5))
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        guard.finish(&t, true);
        assert!(waiter.await.unwrap());
    }

    #[tokio::test]
    async fn stop_cancels_waiters_and_new_refreshes() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_secs(30)));
        let t = topic("T");
        assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
        let waiter_guard = guard.clone();
        let waiter_topic = t.clone();
        let waiter = tokio::spawn(async move {
            waiter_guard
                .await_in_flight(&waiter_topic, Duration::from_secs(30))
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        guard.close();
        assert!(!tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("waiter must not outlive stop")
            .unwrap());
        assert_eq!(guard.begin(&t), BeginRefresh::Closed);
    }

    #[test]
    fn state_is_bounded_to_retained_topics() {
        let guard = RouteRefreshGuard::new(Duration::from_secs(30));
        for id in 0..64 {
            let t = topic(&format!("T{id}"));
            assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
            guard.finish(&t, true);
        }
        assert_eq!(guard.len(), 64);
        guard.retain(|name| name == "T0");
        assert_eq!(guard.len(), 1);
    }

    #[tokio::test]
    async fn joiner_retries_after_the_owner_finishes() {
        let guard = Arc::new(RouteRefreshGuard::new(Duration::from_millis(20)));
        let t = topic("T");
        assert_eq!(guard.begin(&t), BeginRefresh::Fetch);
        let owner_guard = guard.clone();
        let owner_topic = t.clone();
        let owner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            owner_guard.finish(&owner_topic, true);
        });
        assert!(guard.await_in_flight(&t, Duration::from_secs(5)).await);
        owner.await.unwrap();
        // The cooldown now answers the joiner without another fetch.
        assert_eq!(guard.begin(&t), BeginRefresh::Cooldown);
    }
}
