// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Mutex;

use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tokio_util::task::TaskTracker;

/// Admission and drain share one lock: shutdown cannot observe zero work while
/// a producer is between checking admission and registering a task/callback.
#[derive(Default)]
pub(crate) struct ConsumerWork {
    closed: Mutex<bool>,
    tasks: TaskTracker,
    cancelled: CancellationToken,
}

impl ConsumerWork {
    pub(crate) fn enter(&self) -> Option<TaskTrackerToken> {
        let closed = self.closed.lock().unwrap();
        (!*closed).then(|| self.tasks.token())
    }

    pub(crate) fn spawn(&self, handle: &Handle, future: impl Future<Output = ()> + Send + 'static) {
        let Some(token) = self.enter() else { return };
        let cancelled = self.cancelled.clone();
        handle.spawn(async move {
            let _token = token;
            tokio::select! {
                biased;
                _ = cancelled.cancelled() => {}
                _ = future => {}
            }
        });
    }

    /// Started delivery must finish its ack/offset commit; only scheduled work is cancelled.
    pub(crate) fn spawn_draining(&self, handle: &Handle, future: impl Future<Output = ()> + Send + 'static) {
        let Some(token) = self.enter() else { return };
        handle.spawn(async move {
            let _token = token;
            future.await;
        });
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancelled.clone()
    }

    pub(crate) fn close(&self) {
        *self.closed.lock().unwrap() = true;
        self.tasks.close();
        self.cancelled.cancel();
    }

    pub(crate) async fn wait(&self) {
        self.tasks.wait().await;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.cancelled.is_cancelled()
    }
}

/// Listener admission, deliberately separate from scheduled batch tracking.
///
/// Scheduled work (pull dispatch, retries, periodic tasks) may be cancelled at any moment, but a
/// listener invocation that already started owns an offset commit and must drain through it. The
/// gate is shared between the push consumer and its consume service so one `close()` stops both
/// admission paths while each side keeps its own tracker for what it owns.
#[derive(Default)]
pub(crate) struct DeliveryGate {
    closed: Mutex<bool>,
    started: TaskTracker,
}

impl DeliveryGate {
    /// Atomically check and claim admission. The lock is held only for the flag test and the
    /// tracker registration - never across listener execution or I/O - so shutdown can neither
    /// admit a batch after it closed the gate nor miss one that was already claimed.
    pub(crate) fn admit(&self) -> Option<DeliveryPermit> {
        let closed = self.closed.lock().unwrap();
        (!*closed).then(|| DeliveryPermit {
            _token: self.started.token(),
        })
    }

    /// Idempotent; safe to call from every cleanup entry point.
    pub(crate) fn close(&self) {
        let mut closed = self.closed.lock().unwrap();
        *closed = true;
        drop(closed);
        self.started.close();
    }

    pub(crate) fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }

    /// Wait for claimed deliveries to finish. Only meaningful after [`Self::close`].
    pub(crate) async fn wait(&self) {
        self.started.wait().await;
    }
}

/// A claimed listener slot. Held for the duration of one batch, released when the batch's offsets
/// are committed or the batch is discarded.
pub(crate) struct DeliveryPermit {
    _token: TaskTrackerToken,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_closed_gate_admits_no_further_delivery() {
        let gate = Arc::new(DeliveryGate::default());
        let first = gate.admit().expect("open gate admits");
        assert!(!gate.is_closed());
        gate.close();
        gate.close();
        assert!(gate.admit().is_none(), "a closed gate never admits");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.wait())
                .await
                .is_err(),
            "an admitted batch keeps the drain open"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), gate.wait())
            .await
            .expect("drain completes once permits are released");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admission_and_close_never_overlap() {
        // Either a batch is claimed before close and must drain, or it is rejected outright; the
        // window between checking admission and claiming it is exactly where a lost batch hides.
        for round in 0..64 {
            let gate = Arc::new(DeliveryGate::default());
            let admitted = Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::new();
            for _ in 0..8 {
                let gate = Arc::clone(&gate);
                let admitted = Arc::clone(&admitted);
                handles.push(tokio::spawn(async move {
                    if gate.admit().is_some() {
                        admitted.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }
            gate.close();
            for handle in handles {
                handle.await.unwrap();
            }
            tokio::time::timeout(Duration::from_secs(1), gate.wait())
                .await
                .unwrap_or_else(|_| panic!("round {round}: drain must finish once permits drop"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_sleeping_retry_dies_at_close_while_started_delivery_drains() {
        // Cleanup depends on exactly this split: a retry that was only waiting is cancelled, while
        // a delivery that already began runs to its offset commit.
        let work = Arc::new(ConsumerWork::default());
        let slept = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(AtomicUsize::new(0));

        let slept_flag = Arc::clone(&slept);
        work.spawn(&Handle::current(), async move {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            slept_flag.fetch_add(1, Ordering::SeqCst);
        });

        let delivered_flag = Arc::clone(&delivered);
        work.spawn_draining(&Handle::current(), async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            delivered_flag.fetch_add(1, Ordering::SeqCst);
        });

        // Give both tasks a chance to start before closing.
        tokio::time::sleep(Duration::from_millis(10)).await;
        work.close();
        assert!(work.enter().is_none(), "closed admission rejects new work");
        work.wait().await;

        assert_eq!(
            delivered.load(Ordering::SeqCst),
            1,
            "started delivery drains to completion"
        );
        assert_eq!(slept.load(Ordering::SeqCst), 0, "a sleeping retry is cancelled");
    }

    #[tokio::test]
    async fn shutdown_cancels_scheduled_work_but_waits_for_admitted_callbacks() {
        let work = Arc::new(ConsumerWork::default());
        let callback = work.enter().unwrap();
        work.spawn(&Handle::current(), std::future::pending());
        work.close();
        assert!(work.enter().is_none());
        assert!(tokio::time::timeout(Duration::from_millis(10), work.wait())
            .await
            .is_err());
        drop(callback);
        tokio::time::timeout(Duration::from_secs(1), work.wait()).await.unwrap();
    }
}
