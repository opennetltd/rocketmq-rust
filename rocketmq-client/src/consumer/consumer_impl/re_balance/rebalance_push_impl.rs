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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use once_cell::sync::Lazy;
use rocketmq_common::common::constant::consume_init_mode::ConsumeInitMode;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::utils::util_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::local_persist_backoff;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::MAX_LOCAL_PERSIST_RETRIES;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::queue_transitions::RetirementGuard;
use crate::consumer::consumer_impl::re_balance::rebalance_impl::remove_process_queue_if_same;
use crate::consumer::consumer_impl::re_balance::rebalance_impl::table_holds_process_queue;
use crate::consumer::consumer_impl::re_balance::rebalance_impl::RebalanceImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::store::offset_store::OffsetWrite;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

static UNLOCK_DELAY_TIME_MILLS: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.unlockDelayTimeMills")
        .unwrap_or_else(|_| "20000".into())
        .parse::<u64>()
        .unwrap_or(20000)
});

pub struct RebalancePushImpl {
    pub(crate) client_config: ClientConfig,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) rebalance_impl_inner: RebalanceImpl<RebalancePushImpl>,
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
}

impl RebalancePushImpl {
    pub fn new(client_config: ClientConfig, consumer_config: ArcMut<ConsumerConfig>) -> Self {
        RebalancePushImpl {
            client_config,
            consumer_config,
            rebalance_impl_inner: RebalanceImpl::new(None, None, None, None),
            default_mqpush_consumer_impl: None,
        }
    }
}

impl RebalancePushImpl {
    pub(crate) fn invalidate_rebalance_generation(&self) {
        self.rebalance_impl_inner.invalidate_rebalance_generation();
    }

    pub fn get_subscription_inner(&self) -> Arc<RwLock<HashMap<CheetahString, SubscriptionData>>> {
        self.rebalance_impl_inner.subscription_inner.clone()
    }

    pub fn set_default_mqpush_consumer_impl(
        &mut self,
        default_mqpush_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    ) {
        self.default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl);
    }

    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.rebalance_impl_inner.consumer_group = Some(consumer_group);
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.rebalance_impl_inner.message_model = Some(message_model);
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        self.rebalance_impl_inner.allocate_message_queue_strategy = Some(allocate_message_queue_strategy);
    }

    pub fn set_mq_client_factory(&mut self, client_instance: ArcMut<MQClientInstance>) {
        self.rebalance_impl_inner.client_instance = Some(client_instance);
    }

    #[inline]
    pub async fn put_subscription_data(&mut self, topic: CheetahString, subscription_data: SubscriptionData) {
        let mut subscription_inner = self.rebalance_impl_inner.subscription_inner.write().await;
        subscription_inner.insert(topic, subscription_data);
    }

    pub fn set_rebalance_impl(&mut self, rebalance_impl: WeakArcMut<RebalancePushImpl>) {
        self.rebalance_impl_inner.sub_rebalance_impl = Some(rebalance_impl);
    }

    async fn try_remove_order_message_queue(&mut self, mq: &MessageQueue, pq: &Arc<ProcessQueue>) -> bool {
        let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_mut().unwrap();

        let force_unlock =
            pq.is_dropped() && (get_current_millis() > pq.get_last_lock_timestamp() + *UNLOCK_DELAY_TIME_MILLS);
        let consume_lock = pq.consume_lock.try_write_timeout(Duration::from_millis(500)).await;
        if force_unlock || consume_lock.is_some() {
            // Waiting for the consume lock is another window in which this key can be installed by
            // a newer acquisition (plan 49i I2 item 2), and its offsets are not this
            // retirement's to erase.
            if !default_mqpush_consumer_impl.owns_process_queue(mq, pq) {
                return false;
            }
            let offset_store = default_mqpush_consumer_impl.offset_store.as_mut().unwrap();
            offset_store.persist(mq).await;
            offset_store.remove_offset(mq).await;
            pq.set_locked(true);
            self.unlock(mq, true).await;
            return true;
        } else {
            pq.inc_try_unlock_times();
            warn!(
                "Failed to acquire consume_lock for {}, incrementing try_unlock_times.",
                mq
            );
        }

        false
    }
}

/// How many attempts one retirement makes at its own queue's offset write before it accepts that
/// the value is not going to land. The bulk persistence pass uses the same bound and the same
/// doubling backoff (plan 49k K2 item 4), so a transient volume fault is waited out by the one
/// owner that already holds the queue rather than reported as a finished retirement.
fn retirement_persist_attempts(mq: &MessageQueue) -> u32 {
    #[cfg(test)]
    {
        if let Some(attempts) = review49k_capped_attempts(mq) {
            return attempts;
        }
    }
    MAX_LOCAL_PERSIST_RETRIES
}

/// Test seam (plan 49k K2 item 5): cap one queue's retirement persistence attempts, so a permanent
/// regression can watch a retirement give up without waiting out the production backoff schedule.
/// Keyed by queue because these tests run concurrently; never compiled into anything that ships.
#[cfg(test)]
pub(crate) fn review49k_cap_persist_attempts(mq: &MessageQueue, attempts: u32) {
    let mut caps = REVIEW49K_PERSIST_ATTEMPTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    caps.insert(mq.clone(), attempts);
}

#[cfg(test)]
fn review49k_capped_attempts(mq: &MessageQueue) -> Option<u32> {
    let caps = REVIEW49K_PERSIST_ATTEMPTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    caps.get(mq).copied()
}

#[cfg(test)]
static REVIEW49K_PERSIST_ATTEMPTS: Lazy<std::sync::Mutex<HashMap<MessageQueue, u32>>> =
    Lazy::new(|| std::sync::Mutex::new(HashMap::new()));

/// Environment override for the watch budget below, in milliseconds.
const ENV_REBALANCE_RETIREMENT_WATCH_MS: &str = "rocketmq.client.rebalance.retirementWatchMs";
/// How long a retirement's caller watches the owned operation before leaving it to finish on its
/// own (plan 49k K3 item 1). Bounded on purpose: waiting for required persistence in a rebalance
/// loop would put every later queue in the same topic behind one slow local writer, which is what
/// plan 49i I2 ruled out. A caller that stops watching has cancelled nothing.
const DEFAULT_REBALANCE_RETIREMENT_WATCH_MS: u64 = 50;

fn retirement_watch_budget() -> Duration {
    static BUDGET: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var(ENV_REBALANCE_RETIREMENT_WATCH_MS)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            // A zero budget would defer every retirement, and an absurd one would put the stall back.
            .filter(|ms| (1..=5_000).contains(ms))
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(DEFAULT_REBALANCE_RETIREMENT_WATCH_MS))
    })
}

/// End one retirement's claim on its queue (plan 49i I2 item 1): an installation that was turned
/// away while it ran is asked for again rather than left to chance.
fn end_retirement(rebalance_impl: &mut RebalanceImpl<RebalancePushImpl>, guard: RetirementGuard) {
    if guard.release() {
        if let Some(client) = rebalance_impl.client_instance.as_mut() {
            client.rebalance_later(500);
        }
    }
}

/// The one retirement operation, performed on a task this SDK holds (plan 49j J2 item 1, plan 49k
/// K3 item 1) and gated on its offset write's evidence (plan 49k K2).
///
/// It is written against an owned handle rather than against whoever asked for the queue to go
/// away, because the caller's future may be cancelled halfway through: persistence for this queue
/// can wait on a local writer that outlives that future, and cancelling then would end the
/// transition claim - authorizing an installation to write the same offset file - while the
/// retirement's own erase is still coming.
///
/// The table entry leaves **after** the offset write is proven, not before. A queue whose offsets
/// could not be written stays exactly where it is - entry, acquisition identity and the memory copy
/// of the offset - so a later rebalance pass retries the retirement and this store keeps retrying
/// the value; removing it first would leave that offset with no owner anywhere (plan 49k K2 item
/// 2).
async fn retire_owned_process_queue(
    mut rebalance: ArcMut<RebalancePushImpl>,
    mq: MessageQueue,
    pq: Arc<ProcessQueue>,
    guard: RetirementGuard,
) -> bool {
    let table = rebalance.rebalance_impl_inner.process_queue_table.clone();
    // The entry is acted on only while it still names the object this retirement scheduled (plan
    // 49h H1): a replacement installed meanwhile keeps its table entry and everything hanging
    // off it.
    if !table_holds_process_queue(&table, &mq, &pq).await {
        end_retirement(&mut rebalance.rebalance_impl_inner, guard);
        return false;
    }
    let droped = pq.is_dropped();
    pq.set_dropped(true);

    // Concurrent PULL commits use this same per-queue barrier for their final dropped/acquisition
    // check and memory offset update. Keep it until the proven snapshot is no longer addressable
    // through the table or acquisition; otherwise an admitted commit can advance memory while this
    // write is pending and then have that newer value erased by the key-only removal below.
    // Orderly consumption has its own consume-lock retirement sequence and remains unchanged.
    let _commit_guard = if rebalance
        .default_mqpush_consumer_impl
        .as_ref()
        .is_some_and(|consumer| !consumer.is_consume_orderly())
    {
        Some(pq.commit_lock.lock().await)
    } else {
        None
    };

    // Side effects first, while this object still owns the queue; revocation afterwards, because it
    // is what stops further work on this identity. Each step revalidates against this exact process
    // queue (plan 49i I2 item 2), which is what makes it safe for the operation to outlive the
    // caller that started it.
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match rebalance.remove_unnecessary_message_queue(&mq, &pq).await {
            Some(OffsetWrite::Proven) | Some(OffsetWrite::NoLocalState) => break,
            Some(OffsetWrite::Deferred) if attempts < retirement_persist_attempts(&mq) => {
                let backoff = local_persist_backoff(attempts - 1);
                warn!(
                    "retiring {} waits on its offset write (attempt {attempts}), retrying in {backoff:?}",
                    mq
                );
                tokio::time::sleep(backoff).await;
                // Waiting on persistence is exactly the window in which a replacement installation
                // appears, and its offsets are not this retirement's to keep writing.
                if !table_holds_process_queue(&table, &mq, &pq).await {
                    debug!(
                        "leaving {} alone mid-retirement, its table entry answers to another process queue",
                        mq
                    );
                    end_retirement(&mut rebalance.rebalance_impl_inner, guard);
                    return false;
                }
            }
            _ => {
                // Either a newer installation owns those offsets, or this queue's write stayed
                // broken through its whole budget. Nothing is destroyed here: the
                // entry stays in the table, its acquisition stays published, and
                // the offset stays in the store carrying itself into every later
                // write. The next rebalance pass - and cleanup's own persistence -
                // find this queue exactly as it was left (plan 49k K2 item 2).
                error!(
                    "retiring {} is not proven after {attempts} offset write(s); leaving it installed",
                    mq
                );
                end_retirement(&mut rebalance.rebalance_impl_inner, guard);
                return false;
            }
        }
    }

    if !remove_process_queue_if_same(&table, &mq, &pq).await {
        end_retirement(&mut rebalance.rebalance_impl_inner, guard);
        return false;
    }
    if let Some(impl_) = rebalance.default_mqpush_consumer_impl.as_ref() {
        // The queue left the table, so its acquisition ends here - naming the object that just
        // left it, so a pull answer arriving later cannot publish into whoever installs next
        // (plan 49c C1) and a successor's own identity survives (plan 49i I2 item 3).
        impl_.note_queue_revoked_for(&mq, &pq);
    }
    info!(
        "Fix Offset, {}, remove unnecessary mq, {} Droped: {}",
        rebalance.rebalance_impl_inner.consumer_group.as_ref().unwrap(),
        mq,
        droped
    );
    // The claim ends with the last piece of bookkeeping (plan 49i I2 item 1).
    end_retirement(&mut rebalance.rebalance_impl_inner, guard);
    true
}

const UNLOCK_BATCH_MQ_TIMEOUT_MS: u64 = 1_000;

impl Rebalance for RebalancePushImpl {
    async fn note_allocation_pass(
        &mut self,
        topic: &str,
        outcome: crate::consumer::consumer_impl::consumer_lifecycle::AllocationOutcome,
    ) {
        if let Some(impl_) = self.default_mqpush_consumer_impl.as_ref() {
            impl_.note_allocation_pass(topic, outcome);
        }
    }

    async fn note_queue_installed(&mut self, mq: &MessageQueue, process_queue: &Arc<ProcessQueue>) {
        if let Some(impl_) = self.default_mqpush_consumer_impl.as_ref() {
            impl_.note_queue_installed(mq, process_queue);
        }
    }

    async fn note_queue_revoked(&mut self, mq: &MessageQueue, process_queue: &Arc<ProcessQueue>) {
        if let Some(impl_) = self.default_mqpush_consumer_impl.as_ref() {
            // Only the acquisition named by this retirement ends; a replacement installed while
            // this pass was at persistence keeps its own identity (plan 49i I2 item 3).
            impl_.note_queue_revoked_for(mq, process_queue);
        }
    }

    async fn note_subscription_set(&mut self, topics: &[cheetah_string::CheetahString]) {
        if let Some(impl_) = self.default_mqpush_consumer_impl.as_ref() {
            impl_.note_subscription_set(topics);
        }
    }

    async fn message_queue_changed(
        &mut self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    ) {
        let mut subscription_inner = self.rebalance_impl_inner.subscription_inner.write().await;
        let subscription_data = subscription_inner.get_mut(topic).unwrap();
        let new_version = get_current_millis() as i64;
        info!(
            "{} Rebalance changed, also update version: {}, {}",
            topic, subscription_data.sub_version, new_version
        );
        subscription_data.sub_version = new_version;
        drop(subscription_inner);

        let process_queue_table = self.rebalance_impl_inner.process_queue_table.read().await;
        let current_queue_count = process_queue_table.len();
        if current_queue_count != 0 {
            let pull_threshold_for_topic = self.consumer_config.pull_threshold_for_topic;
            if pull_threshold_for_topic != -1 {
                let new_val = 1.max(pull_threshold_for_topic / current_queue_count as i32);
                info!(
                    "The pullThresholdForQueue is changed from {} to {}",
                    pull_threshold_for_topic, new_val
                );
                self.consumer_config.pull_threshold_for_topic = new_val;
            }
            let pull_threshold_size_for_topic = self.consumer_config.pull_threshold_size_for_topic;
            if pull_threshold_size_for_topic != -1 {
                let new_val = 1.max(pull_threshold_size_for_topic / current_queue_count as i32);
                info!(
                    "The pullThresholdSizeForQueue is changed from {} to {}",
                    pull_threshold_size_for_topic, new_val
                );
                self.consumer_config.pull_threshold_size_for_topic = new_val;
            }
        }

        //notify broker
        let _ = self
            .rebalance_impl_inner
            .client_instance
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .send_heartbeat_to_all_broker_with_lock_v2(true)
            .await;
        if let Some(ref message_queue_listener) = self.consumer_config.message_queue_listener {
            message_queue_listener.message_queue_changed(topic, mq_all, mq_divided);
        }
    }

    async fn remove_unnecessary_message_queue(
        &mut self,
        mq: &MessageQueue,
        pq: &Arc<ProcessQueue>,
    ) -> Option<OffsetWrite> {
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        let consume_orderly = default_mqpush_consumer_impl.is_consume_orderly();

        // Offsets are stored by key, so they belong to whoever owns the queue when each step runs,
        // not to the pass that decided to retire it (plan 49i I2 item 2). Asking once at the top is
        // not enough: persisting this queue's offsets is exactly the wait during which a
        // replacement installation appears, and erasing by key afterwards would delete the
        // progress its owner has just prepared.
        if !default_mqpush_consumer_impl.owns_process_queue(mq, pq) {
            return None;
        }
        // Write first and keep the memory copy, because what this call is about to erase may be all
        // that can still be retried (plan 49k K2 item 1). The old shape called the void persist API
        // and erased whether or not anything landed.
        let written = default_mqpush_consumer_impl
            .offset_store
            .as_mut()
            .unwrap()
            .persist_for_retirement(mq)
            .await;
        if !default_mqpush_consumer_impl.owns_process_queue(mq, pq) {
            debug!(
                "leaving the offsets of {} alone, this retirement no longer owns that queue",
                mq
            );
            return None;
        }
        if consume_orderly && MessageModel::Clustering == self.consumer_config.message_model {
            // Orderly consumption keeps its own unlock-and-persist sequence unchanged; it is not a
            // concurrent-PULL retirement and this plan does not rewrite its policy.
            return match written {
                OffsetWrite::Deferred => Some(OffsetWrite::Deferred),
                _ => self.try_remove_order_message_queue(mq, pq).await.then_some(written),
            };
        }
        match written {
            // Proven on disk, or nothing local to prove: only now may this queue's offset go.
            OffsetWrite::Proven | OffsetWrite::NoLocalState => {
                default_mqpush_consumer_impl
                    .offset_store
                    .as_mut()
                    .unwrap()
                    .remove_offset(mq)
                    .await;
                Some(written)
            }
            // The write failed. The value stays in memory, carried by every later whole-table write,
            // and the retirement that asked for this call keeps its queue (plan 49k K2 item 1).
            OffsetWrite::Deferred => Some(OffsetWrite::Deferred),
        }
    }

    async fn retire_process_queue(
        &mut self,
        mq: &MessageQueue,
        pq: &Arc<ProcessQueue>,
        guard: RetirementGuard,
    ) -> bool {
        let Some(consumer) = self.default_mqpush_consumer_impl.clone() else {
            // Nothing could own this work. End the claim rather than strand the key, and do not
            // swallow an installation that was turned away while it was held.
            end_retirement(&mut self.rebalance_impl_inner, guard);
            return false;
        };

        // A retirement is this SDK's work rather than its caller's (plan 49j J2 item 1). The claim
        // moves into the task before this function's first await, so a caller that loses interest -
        // an observer whose deadline passed, a rebalance pass that moved on to its next queue -
        // leaves the operation running with its transition claim and its blocking writer instead of
        // ending both and authorizing an installation to write the same offset file. Cleanup joins
        // it because it belongs to the consumer's drain (plan 49j J2 item 3).
        // Admission is taken before the operation exists (plan 49l L1 item 2). A closed admission
        // starts no file work at all: the queue keeps its table entry, its offset and its
        // acquisition identity for whoever owns this consumer next, and only the claim goes back -
        // through the completion path, never onto a drain that has already been proved empty.
        let Some(permit) = consumer.work.admit_owned() else {
            info!(
                "retirement of {} found closed work and started nothing; its entry, offset and acquisition stay with \
                 the consumer for a later pass",
                mq
            );
            end_retirement(&mut self.rebalance_impl_inner, guard);
            return false;
        };
        let operation_owner = consumer.rebalance_impl.clone();
        let task_queue = mq.clone();
        let join = permit.spawn(&Handle::current(), move || {
            retire_owned_process_queue(operation_owner, task_queue, Arc::clone(pq), guard)
        });

        // A caller that is still there watches the bookkeeping it asked for - but only for a
        // bounded moment (plan 49k K3 item 1). Required persistence may wait on a local
        // writer that outlives this pass, and holding a rebalance loop to it would stall
        // every queue behind this one (plan 49i I2 item 1), so past the budget the caller
        // reports "not removed" and moves on. Dropping the watch cancels nothing: the task
        // belongs to the consumer's drain, keeps its transition claim, and finishes its
        // offset write, table removal and acquisition end on its own - which is also what a
        // cancelled observer leaves behind (plan 49j J2 item 1).
        match tokio::time::timeout(retirement_watch_budget(), join).await {
            Ok(Ok(removed)) => removed,
            Ok(Err(err)) => {
                warn!("retirement of {} was cut short by its runtime: {err}", mq);
                false
            }
            Err(_) => {
                debug!("retirement of {} is still running; its caller stops watching it", mq);
                false
            }
        }
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    async fn remove_dirty_offset(&mut self, mq: &MessageQueue) {
        let offset_store = self
            .default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .offset_store
            .as_mut()
            .unwrap();
        offset_store.remove_offset(mq).await;
    }

    #[allow(deprecated)]
    async fn compute_pull_from_where_with_exception(
        &mut self,
        mq: &MessageQueue,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let consume_from_where = self.consumer_config.consume_from_where;
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        let offset_store = default_mqpush_consumer_impl.offset_store.as_mut().unwrap();

        let result = match consume_from_where {
            ConsumeFromWhere::ConsumeFromLastOffset
            | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
            | ConsumeFromWhere::ConsumeFromMinOffset
            | ConsumeFromWhere::ConsumeFromMaxOffset => {
                let last_offset = offset_store.read_offset(mq, ReadOffsetType::ReadFromStore).await;
                if last_offset >= 0 {
                    last_offset
                } else if -1 == last_offset {
                    if mq.get_topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                        0
                    } else {
                        self.rebalance_impl_inner
                            .client_instance
                            .as_mut()
                            .unwrap()
                            .mq_admin_impl
                            .max_offset(mq)
                            .await?
                    }
                } else {
                    return Err(mq_client_err!(
                        ResponseCode::QueryNotFound as i32,
                        "Failed to query consume offset from offset store"
                    ));
                }
            }
            ConsumeFromWhere::ConsumeFromFirstOffset => {
                let last_offset = offset_store.read_offset(mq, ReadOffsetType::ReadFromStore).await;
                if last_offset >= 0 {
                    last_offset
                } else if -1 == last_offset {
                    0
                } else {
                    return Err(mq_client_err!(
                        ResponseCode::QueryNotFound as i32,
                        "Failed to query consume offset from offset store"
                    ));
                }
            }
            ConsumeFromWhere::ConsumeFromTimestamp => {
                let last_offset = offset_store.read_offset(mq, ReadOffsetType::ReadFromStore).await;
                if last_offset >= 0 {
                    last_offset
                } else if -1 == last_offset {
                    if mq.get_topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                        self.rebalance_impl_inner
                            .client_instance
                            .as_mut()
                            .unwrap()
                            .mq_admin_impl
                            .max_offset(mq)
                            .await?
                    } else {
                        let timestamp = util_all::parse_date(
                            self.consumer_config.consume_timestamp.as_ref().unwrap(),
                            util_all::YYYYMMDDHHMMSS,
                        )
                        .unwrap()
                        .and_utc()
                        .timestamp();
                        self.rebalance_impl_inner
                            .client_instance
                            .as_mut()
                            .unwrap()
                            .mq_admin_impl
                            .search_offset(mq, timestamp as u64)
                            .await?
                    }
                } else {
                    return Err(mq_client_err!(
                        ResponseCode::QueryNotFound as i32,
                        "Failed to query consume offset from offset store"
                    ));
                }
            }
        };
        if result < 0 {
            return Err(mq_client_err!(
                ResponseCode::SystemError as i32,
                "Failed to query consume offset from offset store"
            ));
        }
        Ok(result)
    }

    async fn compute_pull_from_where(&mut self, mq: &MessageQueue) -> i64 {
        self.compute_pull_from_where_with_exception(mq)
            .await
            .unwrap_or_else(|e| {
                warn!("Compute consume offset exception, mq={:?}", e);
                -1
            })
    }

    fn get_consume_init_mode(&self) -> i32 {
        let consume_from_where = self.consumer_config.consume_from_where;
        if consume_from_where == ConsumeFromWhere::ConsumeFromFirstOffset {
            ConsumeInitMode::MIN
        } else {
            ConsumeInitMode::MAX
        }
    }

    async fn dispatch_pull_request(&self, pull_request_list: Vec<PullRequest>, delay: u64) {
        if let Some(mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref() {
            let mqpush_consumer_impl = mqpush_consumer_impl.mut_from_ref();
            for pull_request in pull_request_list {
                if delay == 0 {
                    mqpush_consumer_impl
                        .execute_pull_request_immediately(pull_request)
                        .await;
                } else {
                    mqpush_consumer_impl.execute_pull_request_later(pull_request, delay);
                }
            }
        } else {
            error!("mqpush_consumer_impl is not initialized");
        }
    }

    async fn dispatch_pop_pull_request(&self, pop_request_list: Vec<PopRequest>, delay: u64) {
        if let Some(mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref() {
            let mqpush_consumer_impl = mqpush_consumer_impl.mut_from_ref();
            for pop_request in pop_request_list {
                if delay == 0 {
                    mqpush_consumer_impl.execute_pop_request_immediately(pop_request).await;
                } else {
                    mqpush_consumer_impl.execute_pop_request_later(pop_request, delay);
                }
            }
        } else {
            error!("mqpush_consumer_impl is not initialized");
        }
    }

    #[inline]
    fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    #[inline]
    fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    async fn remove_process_queue(&mut self, mq: &MessageQueue, expected: &Arc<ProcessQueue>) {
        // One retirement per queue (plan 49i I2 item 1): a caller reaching a key whose retirement
        // is already in flight does nothing, rather than stacking a second round of
        // persistence and offset removal - or ending an acquisition it never owned. The claim is
        // taken before the first await, so no later caller can join this one.
        let Some(guard) = self.rebalance_impl_inner.queue_transitions.try_retire(mq, expected) else {
            // The key is held by another transition - possibly an installation that is publishing
            // this very queue (plan 49j J1 item 2). Whoever holds it does not retire the queue on
            // this caller's behalf, so ask for a later pass instead of leaving the request dropped
            // on the floor.
            if let Some(client) = self.rebalance_impl_inner.client_instance.as_mut() {
                client.rebalance_later(500);
            }
            return;
        };
        // The retirement itself is the one operation every entry point uses (plan 49k K3 item 1),
        // so a direct request and a rebalance removal loop cannot drift apart in what they
        // guarantee.
        self.retire_process_queue(mq, expected, guard).await;
    }

    async fn unlock(&mut self, mq: &MessageQueue, oneway: bool) {
        let client = match self.rebalance_impl_inner.client_instance.as_mut() {
            Some(client) => client,
            None => {
                warn!("Client instance is not available.");
                return;
            }
        };
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let find_broker_result = client
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
            .await;
        if let Some(find_broker_result) = find_broker_result {
            let mut request_body = UnlockBatchRequestBody {
                consumer_group: Some(self.rebalance_impl_inner.consumer_group.clone().unwrap()),
                client_id: Some(client.client_id.clone()),
                ..Default::default()
            };
            request_body.mq_set.insert(mq.clone());
            let result = client
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .unlock_batch_mq(
                    find_broker_result.broker_addr.as_ref(),
                    request_body,
                    UNLOCK_BATCH_MQ_TIMEOUT_MS,
                    oneway,
                )
                .await;
            if let Err(e) = result {
                warn!("unlockBatchMQ exception, {},{}", mq, e);
            } else {
                warn!(
                    "unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    self.rebalance_impl_inner.consumer_group.as_ref().unwrap(),
                    client.client_id,
                    mq
                )
            }
        }
    }

    fn lock_all(&self) {
        todo!()
    }

    fn unlock_all(&self, oneway: bool) {
        todo!()
    }

    async fn do_rebalance(&mut self, is_order: bool) -> bool {
        self.rebalance_impl_inner.do_rebalance(is_order).await
    }

    /// Determines if the client should perform rebalancing for the given topic.
    ///
    /// This function checks the following conditions to decide if rebalancing is needed:
    /// 1. If `client_rebalance` is enabled in the consumer configuration.
    /// 2. If the message model is set to `Broadcasting`.
    /// 3. If the consumer is configured to consume messages in order.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string slice that holds the name of the topic.
    ///
    /// # Returns
    ///
    /// * `true` if the client should perform rebalancing.
    /// * `false` otherwise.
    fn client_rebalance(&mut self, topic: &str) -> bool {
        //Pop message mode, order message consumer not implement, it's use
        // ConsumeMessageOrderlyService to consume
        self.consumer_config.client_rebalance
            || self.rebalance_impl_inner.message_model.unwrap() == MessageModel::Broadcasting
            || self.default_mqpush_consumer_impl.as_ref().unwrap().is_consume_orderly()
    }

    fn destroy(&mut self) {
        // `destroy` is synchronous because it is called from consumer shutdown.
        // Mark queues dropped immediately so already scheduled pull/pop work is
        // discarded; the maps can then be reclaimed with the consumer instance.
        if let Ok(process_queues) = self.rebalance_impl_inner.process_queue_table.try_read() {
            for process_queue in process_queues.values() {
                process_queue.set_dropped(true);
            }
        }
        if let Ok(pop_process_queues) = self.rebalance_impl_inner.pop_process_queue_table.try_read() {
            for process_queue in pop_process_queues.values() {
                process_queue.set_dropped(true);
            }
        }
    }
}
