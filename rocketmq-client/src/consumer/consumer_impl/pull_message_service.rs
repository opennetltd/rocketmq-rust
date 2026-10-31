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
    /// 1. reserve capacity, bounded by `deadline` and ended by `cancelled`;
    /// 2. record this request's queued expectation while nothing can observe it yet;
    /// 3. make it visible with no await in between.
    ///
    /// Anything other than [`Admission::Accepted`] drops the request, which releases only **its
    /// own** lease: the queue stays lost and a later round owns it again (plan 49f F4).
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
        let stopping = cancelled.cloned();
        let watching = stopping.is_some();
        match tokio::select! {
            biased;
            _ = async {
                match &stopping {
                    Some(token) => token.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            }, if watching => return Admission::Cancelled,
            _ = tokio::time::sleep_until(deadline) => return Admission::TimedOut,
            reserved = tx.clone().reserve_owned() => reserved,
        } {
            Err(_) => Admission::Closed,
            Ok(permit) => {
                // Steps 2 and 3: the expectation belongs to the acquisition that issued the request
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
