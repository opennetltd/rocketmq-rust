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

//! One SDK-owned cleanup session per consumer incarnation (plan 50 section 4.1).
//!
//! Cleanup used to be a future that each caller awaited inside a `timeout_at`. When that timeout
//! fired - or when the waiting task's runtime was torn down - the pass was cancelled between
//! phases, leaving pull leases, offset writes and unregistration undone while everyone who could
//! finish them had already walked away.
//!
//! A session inverts that: the pass runs to its own end on a thread the SDK owns, and callers only
//! ever *observe* it through their own deadline. Dropping every observer cannot stop or destroy
//! the operation; a later caller joins the same session and gets the same answer.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::{self};
use std::sync::Arc;

use tokio::sync::watch;

/// Where the single cleanup pass of one consumer stands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CleanupProgress {
    /// Name of the phase the pass owns right now, or the terminal reason once it has ended.
    pub(crate) phase: &'static str,
    /// `true` while the pass is still running.
    pub(crate) running: bool,
    /// Only meaningful once `running` is false: local ownership was proved quiescent.
    pub(crate) proved: bool,
}

impl CleanupProgress {
    fn started() -> Self {
        Self {
            phase: "admission_closed",
            running: true,
            proved: false,
        }
    }
}

/// Joinable handle on the one cleanup session of a consumer. Cloning and joining never starts a
/// second pass, and dropping every clone never stops the running one.
#[derive(Clone)]
pub(crate) struct CleanupSession {
    progress: Arc<watch::Sender<CleanupProgress>>,
}

impl CleanupSession {
    /// Observe the session without waiting.
    /// A stream of this pass's phase changes. Subscribing cannot start, restart or cancel anything;
    /// it is how a controller stays responsive without polling (plan 49b R2).
    pub(crate) fn subscribe(&self) -> watch::Receiver<CleanupProgress> {
        self.progress.subscribe()
    }

    pub(crate) fn progress(&self) -> CleanupProgress {
        self.progress.subscribe().borrow().clone()
    }

    /// Whether local ownership has been proved quiescent, without waiting.
    pub(crate) fn is_proved(&self) -> bool {
        let progress = self.progress();
        !progress.running && progress.proved
    }

    /// Wait for the session to settle, at most until `deadline`. The deadline bounds only this
    /// wait: a `false` answer means "not proved yet", never "cleanup was cancelled".
    pub(crate) async fn join(&self, deadline: tokio::time::Instant) -> bool {
        let mut receiver = self.progress.subscribe();
        loop {
            if !receiver.borrow().running {
                return receiver.borrow().proved;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            // Woken by every phase change; a timeout here only means "check the deadline again".
            let _ = tokio::time::timeout_at(deadline, receiver.changed()).await;
        }
    }
}

/// Report progress from inside the pass. The session's completion result comes from exactly this
/// object, so no observer can manufacture a proof.
#[derive(Clone)]
pub(crate) struct CleanupDriver {
    progress: Arc<watch::Sender<CleanupProgress>>,
}

impl CleanupDriver {
    /// A driver that reports nowhere, for the degenerate case where a pass cannot be detached from
    /// its caller and progress therefore has no observer.
    pub(crate) fn detached() -> Self {
        let (sender, _receiver) = watch::channel(CleanupProgress::started());
        Self {
            progress: Arc::new(sender),
        }
    }

    /// Announce the phase currently owned. Purely observational.
    pub(crate) fn enter(&self, phase: &'static str) {
        self.progress.send_modify(|progress| {
            if progress.running {
                progress.phase = phase;
            }
        });
    }

    /// Settle the session with the pass' own verdict.
    pub(crate) fn finish(&self, proved: bool) {
        self.progress.send_if_modified(|progress| {
            if !progress.running {
                return false;
            }
            progress.running = false;
            progress.proved = proved;
            progress.phase = if proved { "complete" } else { progress.phase };
            true
        });
    }
}

/// Run `pass` on a thread the SDK owns and return its joinable session.
///
/// The dedicated runtime is what makes cleanup independent of caller liveness: neither an elapsed
/// caller deadline nor a stopped consumer controller can abort it, and the pass keeps whatever
/// leases and writers it holds until it releases them itself.
pub(crate) fn spawn_cleanup_session<Build, Fut>(group: &str, build: Build) -> CleanupSession
where
    Build: FnOnce(CleanupDriver) -> Fut + Send + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let (sender, _receiver) = watch::channel(CleanupProgress::started());
    let progress = Arc::new(sender);
    let driver_progress = Arc::clone(&progress);
    let thread_name = format!("mq-cleanup-{}", group.trim());
    let spawned = std::thread::Builder::new().name(thread_name).spawn(move || {
        let driver = CleanupDriver {
            progress: driver_progress,
        };
        let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime,
            Err(err) => {
                // Reporting beats panicking: an unproved session blocks replacement and never
                // exits.
                tracing::error!("cleanup session: cannot build the cleanup runtime: {err}");
                driver.finish(false);
                return;
            }
        };
        // A panic must not read as success and must not destroy the resource owner silently: the
        // session ends unproved with a bounded reason, and the incarnation stays retained.
        let verdict = panic::catch_unwind(AssertUnwindSafe(|| runtime.block_on(build(driver.clone()))));
        match verdict {
            Ok(proved) => driver.finish(proved),
            Err(_) => {
                tracing::error!("cleanup session: cleanup panicked; ownership is retained and unproved");
                driver.enter("cleanup_panicked");
                driver.finish(false);
            }
        }
    });
    if let Err(err) = spawned {
        tracing::error!("cleanup session: cannot start the cleanup thread: {err}");
        // Nothing can run; settle the session unproved so callers get an immediate honest answer.
        Arc::clone(&progress).send_if_modified(|current| {
            current.running = false;
            current.proved = false;
            current.phase = "cleanup_thread_unavailable";
            true
        });
    }
    CleanupSession { progress }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_elapsed_join_never_cancels_the_session() {
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let session = spawn_cleanup_session("elapsed-join-test", move |_driver| async move {
            // Signal that the pass is running, then take longer than any caller is willing to wait.
            let _ = started_tx.send(());
            tokio::time::sleep(Duration::from_millis(400)).await;
            true
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        // A caller whose deadline has passed gets "not proved yet" and nothing more.
        let now = tokio::time::Instant::now();
        assert!(
            !session.join(now).await,
            "an elapsed deadline must not claim completion"
        );

        // The pass kept running anyway and proves cleanup on its own schedule.
        let later = tokio::time::Instant::now() + Duration::from_secs(10);
        assert!(session.join(later).await, "the session must outlive the caller's wait");
        assert!(session.is_proved());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_joiner_gets_the_same_answer_and_only_one_pass_runs() {
        static RUNS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let session = spawn_cleanup_session("joiners-test", move |_driver| async move {
            RUNS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            false
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let first = session.join(deadline).await;
        let second = session.join(deadline).await;
        assert!(!first && !second, "an unproved pass answers every joiner the same way");
        assert_eq!(session.progress().phase, "admission_closed");
        assert_eq!(
            RUNS.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one pass per session"
        );

        // A clone joining afterwards is answered immediately from the remembered state.
        let clone = session.clone();
        assert!(!clone.join(tokio::time::Instant::now()).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_pass_is_reported_unproved_and_not_complete() {
        let session = spawn_cleanup_session("panic-test", move |_driver| async move {
            panic!("cleanup blew up halfway");
        });
        let proved = session
            .join(tokio::time::Instant::now() + Duration::from_secs(10))
            .await;
        assert!(!proved, "a panicked pass is never a proof");
        assert_eq!(session.progress().phase, "cleanup_panicked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn phases_are_visible_while_the_pass_runs() {
        let (first_tx, first_rx) = std::sync::mpsc::channel::<()>();
        let session = spawn_cleanup_session("phase-test", move |driver| async move {
            driver.enter("offset_persist");
            let _ = first_tx.send(());
            tokio::time::sleep(Duration::from_millis(200)).await;
            true
        });
        first_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(session.progress().phase, "offset_persist");
        assert!(session.progress().running);
        assert!(
            session
                .join(tokio::time::Instant::now() + Duration::from_secs(10))
                .await
        );
        assert!(!session.progress().running);
    }
}
