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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

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
