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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::Lazy;
use rocketmq_rust::ArcMut;
use rocketmq_rust::Shutdown;
use tokio::select;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use crate::factory::mq_client_instance::MQClientInstance;

static WAIT_INTERVAL: Lazy<Duration> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.waitInterval")
        .unwrap_or_else(|_| "20000".into())
        .parse::<u64>()
        .map_or(Duration::from_millis(20000), Duration::from_millis)
});

static MIN_INTERVAL: Lazy<Duration> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.minInterval")
        .unwrap_or_else(|_| "1000".into())
        .parse::<u64>()
        .map_or(Duration::from_millis(1000), Duration::from_millis)
});

///`RebalanceService` is a crucial struct in Apache RocketMQ-Rust, responsible for coordinating
/// load balancing among the consumers of a message queue. Its primary function is to ensure that
/// consumer instances can reasonably distribute message queues among multiple consumers, achieving
/// efficient message processing and balanced load. Specifically, the role of `RebalanceService`
/// includes the following aspects:
///
/// 1. **Consumer Load Balancing** When consumers in a consumer group start, stop, or fail,
///    `RebalanceService` dynamically adjusts the message queue distribution between consumers to
///    ensure each consumer processes a reasonable number of queues, avoiding situations where some
///    consumers are overburdened or underutilized.
/// 2. **Queue Allocation and Revocation** `RebalanceService` triggers reallocation of consumer
///    queues periodically or when certain events occur. It decides which queues should be processed
///    by which consumers based on the number of consumers, the state of consumer instances, and the
///    number of message queues.
/// 3. **Consumer Failure Recovery** If a consumer instance fails or goes offline,
///    `RebalanceService` triggers a rebalancing operation, redistributing the queues it was
///    responsible for to other online consumers, ensuring that messages are not lost and the load
///    is evenly distributed.
/// 4. **Consumer Rejoining** When a new consumer joins the consumer group, `RebalanceService`
///    initiates a rebalancing process, adding the new consumer to the queue allocation and
///    adjusting the queues for each consumer to ensure the load is balanced across the entire
///    consumer group.
/// 5. **Listening for Queue State Changes** `RebalanceService` listens for changes in the state of
///    consumers and queues in RocketMQ, adjusting queue allocations based on these changes.
///
/// This service is integral to ensuring that the consumers in a RocketMQ cluster maintain high
/// availability, optimal resource utilization, and fault tolerance while processing messages
/// efficiently.

#[derive(Clone)]
pub struct RebalanceService {
    /// Single-waiter wake channel. `notify_one` stores at most one permit, so a
    /// notification raised while a pass is running is retained instead of lost
    /// (plan 49 section 4.1.5). Notification count is deliberately not a pass count.
    notify: Arc<Notify>,
    tx_shutdown: Option<tokio::sync::broadcast::Sender<()>>,
    wakeups: Arc<AtomicU64>,
    passes: Arc<AtomicU64>,
}

impl RebalanceService {
    pub fn new() -> Self {
        RebalanceService {
            notify: Arc::new(Notify::new()),
            tx_shutdown: None,
            wakeups: Arc::new(AtomicU64::new(0)),
            passes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Wakeups observed since construction. Diagnostics only; never a health signal.
    pub fn wakeup_count(&self) -> u64 {
        self.wakeups.load(Ordering::Relaxed)
    }

    /// Rebalance passes actually executed since construction.
    pub fn pass_count(&self) -> u64 {
        self.passes.load(Ordering::Relaxed)
    }

    pub async fn start(&mut self, mut instance: ArcMut<MQClientInstance>) {
        let notify = self.notify.clone();
        let passes = self.passes.clone();
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);
        self.tx_shutdown = Some(tx_shutdown);
        let work = instance.work.clone();
        work.spawn(&tokio::runtime::Handle::current(), async move {
            let mut last_rebalance_timestamp = Instant::now();
            let min_interval = *MIN_INTERVAL;
            let mut real_wait_interval = *WAIT_INTERVAL;
            info!(">>>>>>>>>RebalanceService started<<<<<<<<<");
            loop {
                select! {
                    _ = notify.notified() => {} // wakeup
                    _ = shutdown.recv() => {info!("RebalanceService shutdown");}
                    _ = tokio::time::sleep(real_wait_interval) => {}
                }
                if shutdown.is_shutdown() {
                    return;
                }
                let interval = Instant::now() - last_rebalance_timestamp;
                if interval < min_interval {
                    real_wait_interval = min_interval - interval;
                } else {
                    // Rebalance operation, the core of RebalanceService
                    passes.fetch_add(1, Ordering::Relaxed);
                    let balanced = instance.do_rebalance().await;
                    real_wait_interval = if balanced { *WAIT_INTERVAL } else { min_interval };
                    last_rebalance_timestamp = Instant::now();
                }
            }
        });
    }

    /// Requests one rebalance pass. Bursts coalesce into a single retained wake, but a
    /// wake raised during a running pass is never dropped.
    pub fn wakeup(&self) {
        self.wakeups.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
    }

    pub fn shutdown(&self) {
        if let Some(tx_shutdown) = &self.tx_shutdown {
            if let Err(e) = tx_shutdown.send(()) {
                warn!("Failed to send shutdown signal to RebalanceService, error: {:?}", e);
            }
        } else {
            warn!("Shutdown called before start; no shutdown signal sent");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn take_wake(service: &RebalanceService) -> bool {
        tokio::time::timeout(Duration::from_millis(50), service.notify.notified())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn notification_burst_retains_exactly_one_pending_wake() {
        let service = RebalanceService::new();
        for _ in 0..5_408 {
            service.wakeup();
        }
        assert_eq!(service.wakeup_count(), 5_408);
        // One retained wake means one pass, and a second wait has nothing left.
        assert!(take_wake(&service).await);
        assert!(!take_wake(&service).await);
    }

    #[tokio::test]
    async fn wake_raised_while_a_pass_runs_is_not_lost() {
        let service = RebalanceService::new();
        let waiter_service = service.clone();
        // Simulate the pass window: nobody is awaiting notified() yet.
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(10));
            waiter_service.wakeup();
        })
        .await
        .unwrap();
        assert!(take_wake(&service).await);
    }
}
