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

//! Bounded emission gate for high-frequency INFO logs (plan 49 section 4.1.6).
//!
//! Route churn used to turn every notification, route comparison and per-target
//! success into an INFO line. Failures stay observable; repetitive detail is either
//! moved behind DEBUG or summarized at a bounded rate with the suppressed count.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

/// Allows one emission per `interval` per key and reports how many requests were
/// suppressed in between so a summary line stays truthful.
pub(crate) struct LogGate {
    interval: Duration,
    entries: Mutex<HashMap<String, Entry>>,
}

#[derive(Clone, Copy)]
struct Entry {
    last: Instant,
    suppressed: u64,
}

impl LogGate {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Returns `Some(suppressed)` when the caller may emit for `key`, where
    /// `suppressed` counts requests since the previous emission. `None` means the
    /// caller must not format or log anything for this request.
    pub(crate) fn try_acquire(&self, key: &str) -> Option<u64> {
        self.try_acquire_at(key, Instant::now())
    }

    fn try_acquire_at(&self, key: &str, now: Instant) -> Option<u64> {
        let Ok(mut guard) = self.entries.lock() else {
            // A poisoned gate must not amplify logs.
            return None;
        };
        match guard.get(key).copied() {
            Some(entry) if now.duration_since(entry.last) < self.interval => {
                let suppressed = entry.suppressed.saturating_add(1);
                guard.insert(
                    key.to_string(),
                    Entry {
                        last: entry.last,
                        suppressed,
                    },
                );
                None
            }
            Some(entry) => {
                guard.insert(
                    key.to_string(),
                    Entry {
                        last: now,
                        suppressed: 0,
                    },
                );
                Some(entry.suppressed)
            }
            None => {
                guard.insert(
                    key.to_string(),
                    Entry {
                        last: now,
                        suppressed: 0,
                    },
                );
                Some(0)
            }
        }
    }

    /// Requests suppressed so far for `key`; diagnostics and tests only.
    pub(crate) fn suppressed(&self, key: &str) -> u64 {
        self.entries
            .lock()
            .ok()
            .and_then(|guard| guard.get(key).map(|entry| entry.suppressed))
            .unwrap_or(0)
    }

    /// Drops keys no longer relevant so a long-lived client cannot grow this map with
    /// transient topic or group names.
    pub(crate) fn retain<F: Fn(&str) -> bool>(&self, keep: F) {
        let Ok(mut guard) = self.entries.lock() else {
            return;
        };
        guard.retain(|key, _| keep(key.as_str()));
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().map(|guard| guard.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> LogGate {
        LogGate::new(Duration::from_secs(10))
    }

    #[test]
    fn first_request_emits_immediately() {
        let now = Instant::now();
        assert_eq!(gate().try_acquire_at("topic-a", now), Some(0));
    }

    #[test]
    fn repeated_requests_within_window_are_suppressed_and_counted() {
        let gate = gate();
        let now = Instant::now();
        assert_eq!(gate.try_acquire_at("topic-a", now), Some(0));
        for _ in 0..999 {
            assert_eq!(gate.try_acquire_at("topic-a", now + Duration::from_millis(1)), None);
        }
        assert_eq!(gate.suppressed("topic-a"), 999);
    }

    #[test]
    fn summary_after_the_window_reports_the_suppressed_count_and_resets() {
        let gate = gate();
        let now = Instant::now();
        assert_eq!(gate.try_acquire_at("topic-a", now), Some(0));
        assert_eq!(gate.try_acquire_at("topic-a", now + Duration::from_secs(1)), None);
        assert_eq!(gate.try_acquire_at("topic-a", now + Duration::from_secs(10)), Some(1));
        assert_eq!(gate.suppressed("topic-a"), 0);
    }

    #[test]
    fn keys_are_independent_and_retain_bounds_state() {
        let gate = gate();
        let now = Instant::now();
        assert_eq!(gate.try_acquire_at("a", now), Some(0));
        assert_eq!(gate.try_acquire_at("b", now), Some(0));
        assert_eq!(gate.try_acquire_at("a", now), None);
        gate.retain(|key| key == "a");
        assert_eq!(gate.len(), 1);
    }
}
