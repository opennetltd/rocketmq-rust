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

use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_rust::ArcMut;
use rocketmq_rust::Shutdown;
use tracing::info;
use tracing::warn;

use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::factory::mq_client_instance::MQClientInstance;

/// Whether a request made it into the shared pull service (plan 49f F2 and F4).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    /// Capacity was reserved, its queued expectation recorded, and the receiver can see it.
    Accepted,
    /// Room did not open inside the caller's step; the queue keeps its fault and its own lease is
    /// released with the dropped request.
    TimedOut,
    /// The service or this consumer ended.
    Closed,
    /// The task that owned the request stopped first.
    Cancelled,
    /// Its acquisition ended while it waited for room; recording anything for the queue's current
    /// owner would be wrong.
    Superseded,
}

#[derive(Clone)]
pub struct PullMessageService {
    tx: Option<tokio::sync::mpsc::Sender<Box<dyn MessageRequest + Send + 'static>>>,
    tx_shutdown: Option<tokio::sync::broadcast::Sender<()>>,
    /// Runtime owned by the client instance that runs this shared pull loop. Delayed requests are
    /// dispatched here rather than on whatever consumer scheduled them, so one controller's
    /// teardown cannot strand work that belongs to the shared service.
    runtime: Option<tokio::runtime::Handle>,
}

impl PullMessageService {
    pub fn new() -> Self {
        PullMessageService {
            tx: None,
            tx_shutdown: None,
            runtime: None,
        }
    }
    pub async fn start(&mut self, mut instance: ArcMut<MQClientInstance>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Box<dyn MessageRequest + Send + 'static>>(1024 * 4);
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);
        self.tx = Some(tx);
        self.tx_shutdown = Some(tx_shutdown);
        let work = instance.work.clone();
        let runtime = instance.runtime_handle();
        self.runtime = Some(runtime.clone());
        work.spawn(&runtime, async move {
            info!(">>>>>>>>>>>>>>>>>>>>>>>PullMessageService  started<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            /*while let Some(request) = rx.recv().await {
                if request.get_message_request_mode() == MessageRequestMode::Pull {
                    let pull_request =
                        unsafe { *Box::from_raw(Box::into_raw(request) as *mut PullRequest) };
                    PullMessageService::pull_message(pull_request, instance.as_mut()).await;
                } else {
                    let pop_request =
                        unsafe { *Box::from_raw(Box::into_raw(request) as *mut PopRequest) };
                    PullMessageService::pop_message(pop_request, instance.as_mut()).await;
                }
            }*/
            if shutdown.is_shutdown() {
                info!("PullMessageService shutdown");
                return;
            }
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("PullMessageService shutdown");
                    }
                    Some(request) = rx.recv() => {
                        if request.get_message_request_mode() == MessageRequestMode::Pull {
                            let pull_request =
                                unsafe { *Box::from_raw(Box::into_raw(request) as *mut PullRequest) };
                            PullMessageService::pull_message(pull_request, instance.as_mut()).await;
                        } else {
                            let pop_request =
                                unsafe { *Box::from_raw(Box::into_raw(request) as *mut PopRequest) };
                            PullMessageService::pop_message(pop_request, instance.as_mut()).await;
                        }
                    }
                }
                if shutdown.is_shutdown() {
                    info!("PullMessageService shutdown");
                    break;
                }
            }
        });
    }

    async fn pull_message(request: PullRequest, instance: &mut MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pull_message(request).await;
        } else {
            warn!("No matched consumer for the PullRequest {},drop it", request)
        }
    }

    async fn pop_message(request: PopRequest, instance: &mut MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pop_message(request).await;
        } else {
            warn!("No matched consumer for the PopRequest {}, drop it", request)
        }
    }

    pub fn execute_pull_request_later(&self, pull_request: PullRequest, time_delay: u64) {
        let Some(runtime) = self.runtime.clone() else {
            warn!("PullMessageService is not running; dropping a delayed pull request");
            return;
        };
        let this = self.clone();
        runtime.spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(time_delay)).await;
            if let Err(e) = this.tx.as_ref().unwrap().send(Box::new(pull_request)).await {
                warn!("Failed to send pull request to pull_tx, error: {:?}", e);
            }
        });
    }

    pub async fn execute_pull_request_immediately(&self, pull_request: PullRequest) {
        if let Err(e) = self.tx.as_ref().unwrap().send(Box::new(pull_request)).await {
            warn!("Failed to send pull request to pull_tx, error: {:?}", e);
        }
    }

    /// Hand a request to the shared pull service **under a deadline and a cancellation token**
    /// (plan 49f F2 and F4).
    ///
    /// Owning a queue's lease and finding room in this process-wide channel are separate facts, and
    /// an unbounded `send` conflated them: the task holding one queue's lease waited behind
    /// thousands of requests for other queues while health read the still-held lease as work in
    /// progress. Room is reserved first, so the hand-off itself cannot wait:
    ///
    /// 1. record that this submission is waiting for room, under `deadline` itself (plan 49g G1),
    ///    so the wait has an owner and an expiry instead of reading as a loop that stopped working;
    /// 2. reserve capacity, bounded by `deadline` and ended by `cancelled`;
    /// 3. turn that submitting expectation into the queued one while nothing can observe the
    ///    request yet - both moves happen in one progress write, so no window describes a visible
    ///    request as owed nothing;
    /// 4. make it visible with no await in between.
    ///
    /// Anything other than [`Admission::Accepted`] drops the request and retires its phase
    /// expectation, releasing only **its own** lease: the queue stays lost and a later round
    /// owns it again (plan 49f F4, plan 49g G1).
    pub(crate) async fn admit_pull_request(
        &self,
        pull_request: PullRequest,
        deadline: tokio::time::Instant,
        cancelled: Option<&tokio_util::sync::CancellationToken>,
    ) -> Admission {
        let Some(tx) = self.tx.as_ref() else {
            warn!("PullMessageService is not running; dropping a pull request that was never admitted");
            return Admission::Closed;
        };
        if tx.is_closed() {
            return Admission::Closed;
        }
        // Step 1: the wait for capacity is owned work, bounded by the deadline its caller issued
        // and by nothing else. Status polling cannot renew it, and once it passes the queue
        // is diagnosable again on its own (plan 49g G1 item 1).
        let submitting = pull_request.lease().map(|lease| {
            lease.note_submitting({
                let now = tokio::time::Instant::now();
                if deadline > now {
                    deadline - now
                } else {
                    std::time::Duration::ZERO
                }
            })
        });
        if matches!(submitting, Some(false)) {
            // Its acquisition ended before this request could even start waiting for room.
            return Admission::Superseded;
        }
        let retiring = pull_request.lease().cloned();
        let not_accepted = |admission: Admission| {
            if let Some(lease) = retiring {
                lease.forget_submitting();
            }
            admission
        };
        let stopping = cancelled.cloned();
        let watching = stopping.is_some();
        match tokio::select! {
            biased;
            _ = async {
                match &stopping {
                    Some(token) => token.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            }, if watching => return not_accepted(Admission::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return not_accepted(Admission::TimedOut),
            reserved = tx.clone().reserve_owned() => reserved,
        } {
            Err(_) => not_accepted(Admission::Closed),
            Ok(permit) => {
                // Steps 3 and 4: the expectation belongs to the acquisition that issued the request
                // and is recorded before its receiver can see it; `send` on a held permit never
                // waits.
                if let Some(lease) = pull_request.lease() {
                    if !lease.note_queued() {
                        // Its acquisition ended while this request waited for room: making it
                        // visible would put work on the queue's current owner.
                        return Admission::Superseded;
                    }
                }
                // Infallible: the permit is the reserved capacity, and holding it means nothing
                // between reservation and visibility can wait or fail.
                permit.send(Box::new(pull_request));
                Admission::Accepted
            }
        }
    }

    pub fn execute_pop_pull_request_later(&self, pop_request: PopRequest, time_delay: u64) {
        let Some(runtime) = self.runtime.clone() else {
            warn!("PullMessageService is not running; dropping a delayed pop request");
            return;
        };
        let this = self.clone();
        runtime.spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(time_delay)).await;
            if let Err(e) = this.tx.as_ref().unwrap().send(Box::new(pop_request)).await {
                warn!("Failed to send pull request to pull_tx, error: {:?}", e);
            }
        });
    }

    pub async fn execute_pop_pull_request_immediately(&self, pop_request: PopRequest) {
        if let Err(e) = self.tx.as_ref().unwrap().send(Box::new(pop_request)).await {
            warn!("Failed to send pull request to pull_tx, error: {:?}", e);
        }
    }

    pub fn shutdown(&self) {
        if let Some(tx_shutdown) = &self.tx_shutdown {
            if let Err(e) = tx_shutdown.send(()) {
                warn!("Failed to send shutdown signal to pull_tx, error: {:?}", e);
            }
        } else {
            warn!("Attempted to shutdown but tx_shutdown is None. Ensure `start` is called before `shutdown`.");
        }
    }
}

#[cfg(test)]
impl PullMessageService {
    /// Test seam (plan 49e E3): capture what a caller dispatches instead of running it, so a test
    /// can see which queues a bounded repair reached. Never compiled into anything that ships.
    pub(crate) fn review49e_capture(
        &mut self,
    ) -> tokio::sync::mpsc::Receiver<Box<dyn MessageRequest + Send + 'static>> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        self.tx = Some(tx);
        rx
    }
}

#[cfg(test)]
mod review49f {
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;
    use crate::consumer::consumer_impl::consumer_lifecycle::ConsumerProgress;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;

    fn request(queue: MessageQueue) -> PullRequest {
        PullRequest::new(
            CheetahString::from_static_str("review49f-admission"),
            queue,
            Arc::new(ProcessQueue::new()),
            0,
        )
    }

    /// A service whose channel is full answers inside the caller's step instead of holding a task
    /// that owns another queue's lease (plan 49f F4).
    #[tokio::test(flavor = "current_thread")]
    async fn a_full_pull_service_answers_within_the_callers_step() {
        let mut service = PullMessageService::new();
        let capture = service.review49e_capture();
        let dummy = MessageQueue::from_parts("T", "dummy", 0);
        for _ in 0..32 {
            service.execute_pull_request_immediately(request(dummy.clone())).await;
        }
        assert_eq!(capture.len(), 32, "the capture channel is full");

        let progress = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "ready", 0);
        progress.note_queue_installed(&q);
        let lease = progress.acquire_pull_lease(&q).unwrap();
        progress.review49f_age_activity(&q, Duration::from_secs(31));

        let started = tokio::time::Instant::now();
        let outcome = service
            .admit_pull_request(
                request_with_lease(q.clone(), lease),
                tokio::time::Instant::now() + Duration::from_millis(20),
                None,
            )
            .await;
        let elapsed = started.elapsed();
        eprintln!("full channel admission: {outcome:?} after {elapsed:?}");
        assert_eq!(outcome, Admission::TimedOut);
        assert!(elapsed < Duration::from_millis(100), "admission waited {elapsed:?}");

        // Nothing was recorded for a request that never became visible, so the queue is still lost
        // and remains repairable rather than looking cared for.
        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.lost_pull_queues, 1);
        assert_eq!(capture.len(), 32, "a refused request must not reach the receiver");
    }

    /// The consumer's own stop ends the wait for room at once (plan 49f F4 item 2).
    #[tokio::test(flavor = "current_thread")]
    async fn a_stopped_consumer_does_not_wait_for_room() {
        let mut service = PullMessageService::new();
        let _capture = service.review49e_capture();
        for _ in 0..32 {
            service
                .execute_pull_request_immediately(request(MessageQueue::from_parts("T", "dummy", 0)))
                .await;
        }
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let started = tokio::time::Instant::now();
        let outcome = service
            .admit_pull_request(
                request(MessageQueue::from_parts("T", "ready", 0)),
                tokio::time::Instant::now() + Duration::from_secs(60),
                Some(&token),
            )
            .await;
        eprintln!("cancelled admission: {outcome:?} after {:?}", started.elapsed());
        assert_eq!(outcome, Admission::Cancelled);
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    /// A service that never ran - or already stopped - says so instead of swallowing the request.
    #[tokio::test(flavor = "current_thread")]
    async fn a_service_without_a_channel_reports_closure() {
        let service = PullMessageService::new();
        let outcome = service
            .admit_pull_request(
                request(MessageQueue::from_parts("T", "ready", 0)),
                tokio::time::Instant::now() + Duration::from_millis(20),
                None,
            )
            .await;
        assert_eq!(outcome, Admission::Closed);

        let mut service = PullMessageService::new();
        let capture = service.review49e_capture();
        drop(capture); // The receiver is gone: the channel is closed.
        let outcome = service
            .admit_pull_request(
                request(MessageQueue::from_parts("T", "ready", 0)),
                tokio::time::Instant::now() + Duration::from_millis(20),
                None,
            )
            .await;
        assert_eq!(outcome, Admission::Closed);
    }

    /// Acceptance and the queued expectation are one step: the receiver cannot see a request that
    /// has not been recorded as owed an attempt (plan 49f F2).
    #[tokio::test(flavor = "current_thread")]
    async fn an_accepted_request_owes_an_attempt_not_a_fault() {
        let mut service = PullMessageService::new();
        let capture = service.review49e_capture();
        let progress = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "ready", 0);
        progress.note_queue_installed(&q);
        let lease = progress.acquire_pull_lease(&q).unwrap();
        // History says this queue went quiet a long time ago; only the admission may say otherwise.
        progress.review49f_age_activity(&q, Duration::from_secs(31));
        assert_eq!(
            progress
                .snapshot(Duration::from_secs(30), Duration::from_secs(35))
                .lost_pull_queues,
            1,
            "the fixture should start lost"
        );

        let outcome = service
            .admit_pull_request(
                request_with_lease(q.clone(), lease),
                tokio::time::Instant::now() + Duration::from_secs(5),
                None,
            )
            .await;
        assert_eq!(outcome, Admission::Accepted);
        assert_eq!(capture.len(), 1, "the receiver can see the request");
        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(snapshot.lost_pull_queues, 0, "queued work is not a lost pull");
        assert_eq!(snapshot.leased_queues, 1);
    }

    fn request_with_lease(
        queue: MessageQueue,
        lease: Arc<crate::consumer::consumer_impl::consumer_lifecycle::PullLease>,
    ) -> PullRequest {
        let mut pull_request = request(queue);
        pull_request.hold_lease(lease);
        pull_request
    }
}

#[cfg(test)]
mod review49g {
    use super::*;

    /// A request for a queue nothing else tracks, used to fill the capture channel.
    fn filler() -> PullRequest {
        PullRequest::new(
            CheetahString::from_static_str("review49g-filler"),
            MessageQueue::from_parts("T", "dummy", 0),
            Arc::new(ProcessQueue::new()),
            0,
        )
    }
    use crate::consumer::consumer_impl::consumer_lifecycle::ConsumerProgress;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use std::sync::Arc;
    use std::time::Duration;
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_the_last_queued_request_must_retire_its_expectation() {
        let p = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "ready", 0);
        p.note_queue_installed(&q);
        p.review49f_age_activity(&q, Duration::from_secs(31));
        let mut service = PullMessageService::new();
        let mut capture = service.review49e_capture();
        let mut request = PullRequest::new(
            CheetahString::from_static_str("review49g-drop"),
            q.clone(),
            Arc::new(ProcessQueue::new()),
            7,
        );
        request.hold_lease(p.acquire_pull_lease(&q).unwrap());
        assert_eq!(
            service
                .admit_pull_request(request, tokio::time::Instant::now() + Duration::from_secs(1), None)
                .await,
            Admission::Accepted
        );
        let live = p.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!((live.lost_pull_queues, live.leased_queues), (0, 1));
        drop(capture.try_recv().unwrap());
        let s = p.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!(
            "last queued owner dropped: lost={} leased={}",
            s.lost_pull_queues, s.leased_queues
        );
        assert_eq!(s.leased_queues, 0);
        assert_eq!(
            s.lost_pull_queues, 1,
            "queued health outlived the only task that owed the next pull"
        );
    }

    /// A submission that is waiting for room is owned work with an expiry of its own, and it
    /// becomes exactly one queued expectation the moment room appears - never two expectations,
    /// and never nothing in between (plan 49g G1 items 2-3).
    #[tokio::test(flavor = "current_thread")]
    async fn a_capacity_wait_becomes_one_queued_expectation_when_room_appears() {
        let progress = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "ready", 0);
        progress.note_queue_installed(&q);
        progress.review49f_age_activity(&q, Duration::from_secs(31));
        let mut service = PullMessageService::new();
        let mut capture = service.review49e_capture();
        for _ in 0..32 {
            service.execute_pull_request_immediately(filler()).await;
        }

        let lease = progress.acquire_pull_lease(&q).unwrap();
        let epoch = lease.epoch();
        let mut pull_request = PullRequest::new(
            CheetahString::from_static_str("review49g-wait"),
            q.clone(),
            Arc::new(ProcessQueue::new()),
            7,
        );
        pull_request.hold_lease(Arc::clone(&lease));

        let admit =
            service.admit_pull_request(pull_request, tokio::time::Instant::now() + Duration::from_secs(5), None);
        tokio::pin!(admit);
        // One poll without consuming it: no room means the hand-off has to wait.
        if let Ok(admission) = tokio::time::timeout(Duration::ZERO, admit.as_mut()).await {
            panic!("the request was admitted with no room in the channel ({admission:?})");
        }

        // While it waits, its queue is owned work: not lost, whatever its history says.
        let waiting = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!(
            "mid-wait: lost={} leased={}",
            waiting.lost_pull_queues, waiting.leased_queues
        );
        assert_eq!((waiting.lost_pull_queues, waiting.leased_queues), (0, 1));

        // What vouches for it is the submission's own window and nothing else: once that passes,
        // the queue is diagnosable again even though the wait has not been cancelled (plan
        // 49g G1 item 1).
        progress.review49g_expire_submitting(&q);
        let expired = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(
            (expired.lost_pull_queues, expired.leased_queues),
            (1, 1),
            "an expired wait for capacity stayed invisible to repair"
        );

        // Room appears: the same request becomes visible and its expectation moves to queued in one
        // move - exactly one buffered request, no second lease, no gap where it owed nothing.
        drop(capture.try_recv().unwrap());
        assert_eq!(admit.await, Admission::Accepted);
        assert_eq!(capture.len(), 32, "the submission was published twice");
        let queued = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!((queued.lost_pull_queues, queued.leased_queues), (0, 1));
        assert_eq!(
            lease.epoch(),
            epoch,
            "acceptance changed the acquisition that owns the queue"
        );

        // And when the loop's last reference goes with its request destroyed, nothing is left owed.
        drop(lease);
        while let Ok(request) = capture.try_recv() {
            drop(request);
        }
        let gone = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!((gone.lost_pull_queues, gone.leased_queues), (1, 0));
    }

    /// Revocation while a request waits for capacity: the wait ends as superseded, its request is
    /// never published, and it leaves no expectation on the queue - which now belongs to
    /// whomever the next allocation assigns it to (plan 49g G1 item 2).
    #[tokio::test(flavor = "current_thread")]
    async fn a_revoked_acquisition_leaves_nothing_after_its_wait() {
        let progress = ConsumerProgress::new();
        let q = MessageQueue::from_parts("T", "ready", 0);
        progress.note_queue_installed(&q);
        let mut service = PullMessageService::new();
        let mut capture = service.review49e_capture();
        for _ in 0..32 {
            service.execute_pull_request_immediately(filler()).await;
        }

        let lease = progress.acquire_pull_lease(&q).unwrap();
        let mut pull_request = PullRequest::new(
            CheetahString::from_static_str("review49g-revoked"),
            q.clone(),
            Arc::new(ProcessQueue::new()),
            7,
        );
        pull_request.hold_lease(lease);

        let admit =
            service.admit_pull_request(pull_request, tokio::time::Instant::now() + Duration::from_secs(5), None);
        tokio::pin!(admit);
        if let Ok(admission) = tokio::time::timeout(Duration::ZERO, admit.as_mut()).await {
            panic!("the request was admitted with no room ({admission:?})");
        }

        // The assignment is taken away while the request waits for room, and then capacity appears.
        progress.note_queue_revoked(&q);
        drop(capture.try_recv().unwrap());
        assert_eq!(admit.await, Admission::Superseded);
        assert_eq!(capture.len(), 31, "work for an ended acquisition was published");

        let snapshot = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!(
            "after revocation during the wait: assigned={} leased={}",
            snapshot.assigned_queues, snapshot.leased_queues
        );
        assert_eq!((snapshot.assigned_queues, snapshot.leased_queues), (0, 0));

        // A queue handed to a later acquisition starts clean: it inherits no submitting or queued
        // window from the request that was dropped (plan 49g G3).
        progress.note_queue_installed(&q);
        progress.review49f_age_activity(&q, Duration::from_secs(31));
        let fresh = progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(
            (fresh.lost_pull_queues, fresh.leased_queues),
            (1, 0),
            "the new acquisition was vouched for by a phase expectation it never opened"
        );
    }
}
