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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::consumer_lifecycle::AllocationOutcome;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::queue_transitions::QueueTransitions;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::factory::mq_client_instance::MQClientInstance;

const TIMEOUT_CHECK_TIMES: u32 = 3;
const QUERY_ASSIGNMENT_TIMEOUT: u64 = 3000;

/// Whether this queue's table entry is still exactly this process queue (plan 49h H1). Dropping a
/// queue is decided for one object; if rebalance installed another under the same key in the
/// meantime, that replacement - including its offsets - is somebody else's work.
pub(crate) async fn table_holds_process_queue(
    table: &Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    mq: &MessageQueue,
    pq: &Arc<ProcessQueue>,
) -> bool {
    let table = table.read().await;
    table.get(mq).is_some_and(|current| Arc::ptr_eq(current, pq))
}

/// Remove this queue's entry only while it is still exactly this process queue, reporting whether
/// anything was removed (plan 49h H1). Revocation then follows a removal that really happened, so
/// the pass that dropped a predecessor never retires an acquisition belonging to its replacement.
pub(crate) async fn remove_process_queue_if_same(
    table: &Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    mq: &MessageQueue,
    pq: &Arc<ProcessQueue>,
) -> bool {
    let mut table = table.write().await;
    match table.get(mq) {
        Some(current) if Arc::ptr_eq(current, pq) => table.remove(mq).is_some(),
        _ => false,
    }
}

pub(crate) struct RebalanceImpl<R> {
    pub(crate) process_queue_table: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    pub(crate) pop_process_queue_table: Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>>,
    pub(crate) topic_subscribe_info_table: Arc<RwLock<HashMap<CheetahString, HashSet<MessageQueue>>>>,
    pub(crate) subscription_inner: Arc<RwLock<HashMap<CheetahString, SubscriptionData>>>,
    pub(crate) consumer_group: Option<CheetahString>,
    pub(crate) message_model: Option<MessageModel>,
    pub(crate) allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
    pub(crate) client_instance: Option<ArcMut<MQClientInstance>>,
    pub(crate) sub_rebalance_impl: Option<WeakArcMut<R>>,
    pub(crate) topic_broker_rebalance: Arc<RwLock<HashMap<CheetahString, CheetahString>>>,
    pub(crate) topic_client_rebalance: Arc<RwLock<HashMap<CheetahString, CheetahString>>>,
    /// Which queue is in the middle of being retired (plan 49i I2 item 1). Installation and
    /// retirement of one key take turns here without either holding the process queue table
    /// across persistence.
    pub(crate) queue_transitions: Arc<QueueTransitions>,
    rebalance_generation: Arc<AtomicU64>,
}

impl<R> RebalanceImpl<R>
where
    R: Rebalance,
{
    pub fn new(
        consumer_group: Option<CheetahString>,
        message_model: Option<MessageModel>,
        allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
        mqclient_instance: Option<ArcMut<MQClientInstance>>,
    ) -> Self {
        RebalanceImpl {
            process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            pop_process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            topic_subscribe_info_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            subscription_inner: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            consumer_group,
            message_model,
            allocate_message_queue_strategy,
            client_instance: mqclient_instance,
            sub_rebalance_impl: None,
            topic_broker_rebalance: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            topic_client_rebalance: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            queue_transitions: QueueTransitions::new(),
            rebalance_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn invalidate_rebalance_generation(&self) {
        self.rebalance_generation.fetch_add(1, Ordering::AcqRel);
    }

    #[inline]
    fn rebalance_generation(&self) -> u64 {
        self.rebalance_generation.load(Ordering::Acquire)
    }

    #[inline]
    fn is_rebalance_generation_current(&self, generation: u64) -> bool {
        self.rebalance_generation() == generation
    }

    /// Forward one completed allocation pass to the consumer that owns this rebalance, if any. It
    /// carries the topic it describes: one answer for the whole consumer cannot separate a failed
    /// subscription from another subscription's explicit zero (plan 49c C3).
    #[inline]
    async fn note_allocation_pass(&mut self, topic: &str, outcome: AllocationOutcome) {
        let Some(inner) = self.sub_rebalance_impl.as_mut() else {
            return;
        };
        if let Some(mut sub_rebalance) = inner.upgrade() {
            sub_rebalance.note_allocation_pass(topic, outcome).await;
        }
    }

    /// Keep the consumer's allocation evidence bounded by this rebalance's subscription set.
    #[inline]
    async fn note_subscription_set(&mut self, topics: &[CheetahString]) {
        let Some(inner) = self.sub_rebalance_impl.as_mut() else {
            return;
        };
        if let Some(mut sub_rebalance) = inner.upgrade() {
            sub_rebalance.note_subscription_set(topics).await;
        }
    }

    #[inline]
    pub async fn put_subscription_data(&self, topic: &CheetahString, subscription_data: SubscriptionData) {
        let mut subscription_inner = self.subscription_inner.write().await;
        subscription_inner.insert(topic.clone(), subscription_data);
    }

    #[inline]
    pub async fn remove_subscription_data(&self, topic: &CheetahString) {
        let mut subscription_inner = self.subscription_inner.write().await;
        subscription_inner.remove(topic);
    }

    #[inline]
    pub async fn do_rebalance(&mut self, is_order: bool) -> bool {
        let generation = self.rebalance_generation();
        let mut balanced = true;
        let sub_table = self.subscription_inner.read().await;
        if !sub_table.is_empty() {
            let topics = sub_table.keys().cloned().collect::<Vec<CheetahString>>();
            drop(sub_table);
            // A topic this consumer no longer serves must stop contributing allocation evidence,
            // otherwise its last conclusion would be reported forever (plan 49c C3).
            self.note_subscription_set(&topics).await;
            for topic in &topics {
                if !self.is_rebalance_generation_current(generation) {
                    return false;
                }
                if !self.client_rebalance(topic) && self.try_query_assignment(topic).await {
                    //pop consumer
                    if !self.is_rebalance_generation_current(generation)
                        || !self.get_rebalance_result_from_broker(topic, is_order).await
                    {
                        balanced = false;
                    }
                } else if !self.is_rebalance_generation_current(generation)
                    || !self.rebalance_by_topic(topic, is_order).await
                {
                    balanced = false;
                }
            }
        }
        if self.is_rebalance_generation_current(generation) {
            self.truncate_message_queue_not_my_topic().await;
        }
        balanced
    }

    #[inline]
    pub fn client_rebalance(&mut self, topic: &str) -> bool {
        match self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
            None => true,
            Some(mut value) => value.client_rebalance(topic),
        }
    }

    /// Attempts to query the assignment for a given topic.
    ///
    /// This function checks if the topic is already present in the client or broker rebalance
    /// tables. If not, it queries the assignment from the broker using the allocation strategy.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to query.
    ///
    /// # Returns
    ///
    /// A `bool` indicating whether the assignment query was successful.
    ///
    /// # Errors
    ///
    /// This function logs errors if the allocation strategy is not set or if the query assignment
    /// fails.
    async fn try_query_assignment(&mut self, topic: &CheetahString) -> bool {
        let client_instance = match self.client_instance.as_mut() {
            Some(instance) => instance,
            None => {
                error!("tryQueryAssignment error, client_instance is None.");
                return false;
            }
        };

        // Check topic_client_rebalance
        {
            let topic_client_rebalance = self.topic_client_rebalance.read().await;
            if topic_client_rebalance.contains_key(topic) {
                return false;
            }
        }

        // Check topic_broker_rebalance
        {
            let topic_broker_rebalance = self.topic_broker_rebalance.read().await;
            if topic_broker_rebalance.contains_key(topic) {
                return true;
            }
        }

        // Get strategy name
        let strategy_name = if let Some(strategy) = &self.allocate_message_queue_strategy {
            CheetahString::from_static_str(strategy.get_name())
        } else {
            error!("tryQueryAssignment error, allocateMessageQueueStrategy is None.");
            return false;
        };

        // Retry query assignment
        for retry_times in 1..=TIMEOUT_CHECK_TIMES {
            let timeout = QUERY_ASSIGNMENT_TIMEOUT / (TIMEOUT_CHECK_TIMES as u64) * (retry_times as u64);
            match client_instance
                .query_assignment(
                    topic,
                    self.consumer_group.as_ref().unwrap(),
                    &strategy_name,
                    self.message_model.unwrap(),
                    timeout,
                )
                .await
            {
                Ok(_) => {
                    let mut topic_broker_rebalance = self.topic_broker_rebalance.write().await;
                    topic_broker_rebalance.insert(topic.clone(), topic.clone());
                    return true;
                }
                Err(e) => match e {
                    rocketmq_error::RocketMQError::Timeout { .. } => {
                        // Continue to retry on timeout errors
                    }
                    _ => {
                        error!("tryQueryAssignment error {}.", e);
                        let mut topic_client_rebalance = self.topic_client_rebalance.write().await;
                        topic_client_rebalance.insert(topic.clone(), topic.clone());
                        return false;
                    }
                },
            }
        }

        // Insert into topic_client_rebalance after all retries
        let mut topic_client_rebalance = self.topic_client_rebalance.write().await;
        topic_client_rebalance.insert(topic.clone(), topic.clone());
        false
    }

    async fn truncate_message_queue_not_my_topic(&self) {
        let sub_table = self.subscription_inner.read().await;

        let mut process_queue_table = self.process_queue_table.write().await;
        process_queue_table.retain(|mq, pq| {
            if !sub_table.contains_key(mq.get_topic()) {
                pq.set_dropped(true);
                info!(
                    "doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}",
                    self.consumer_group.as_ref().unwrap(),
                    mq.get_topic()
                );
                false
            } else {
                true
            }
        });
        let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
        pop_process_queue_table.retain(|mq, pq| {
            if !sub_table.contains_key(mq.get_topic()) {
                pq.set_dropped(true);
                info!(
                    "doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}",
                    self.consumer_group.as_ref().unwrap(),
                    mq.get_topic()
                );
                false
            } else {
                true
            }
        });

        let mut topic_client_rebalance = self.topic_client_rebalance.write().await;
        topic_client_rebalance.retain(|topic, _| sub_table.contains_key(topic));
        let mut topic_broker_rebalance = self.topic_broker_rebalance.write().await;
        topic_broker_rebalance.retain(|topic, _| sub_table.contains_key(topic));
    }

    /// Retrieves the rebalance result from the broker for a given topic.
    ///
    /// This function queries the broker for message queue assignments for the specified topic.
    /// If the assignments are successfully retrieved, it updates the message queue assignments
    /// and notifies the sub-rebalance implementation of any changes.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to query.
    /// * `is_order` - A boolean indicating whether the message queues should be ordered.
    ///
    /// # Returns
    ///
    /// A `bool` indicating whether the rebalance result matches the current working message queue.
    ///
    /// # Errors
    ///
    /// This function logs errors if the allocation strategy is not set or if the query assignment
    /// fails.
    async fn get_rebalance_result_from_broker(&mut self, topic: &CheetahString, is_order: bool) -> bool {
        let strategy_name = match self.allocate_message_queue_strategy.as_ref() {
            None => {
                error!("get_rebalance_result_from_broker error: allocate_message_queue_strategy is None.");
                return false;
            }
            Some(strategy) => strategy.get_name(),
        };
        let message_queue_assignments = self
            .client_instance
            .as_mut()
            .unwrap()
            .query_assignment(
                topic,
                self.consumer_group.as_ref().unwrap(),
                &CheetahString::from_slice(strategy_name),
                self.message_model.unwrap(),
                QUERY_ASSIGNMENT_TIMEOUT,
            )
            .await;
        let (mq_set, message_queue_assignments) = match message_queue_assignments {
            Ok(assignments) => {
                if assignments.is_none() {
                    // None means invalid result, we should skip the update logic
                    return false;
                }
                let assignments_inner = assignments.unwrap();
                let mut mq_set = HashSet::new();
                for assignment in &assignments_inner {
                    if let Some(ref mq) = assignment.message_queue {
                        mq_set.insert(mq.clone());
                    }
                }
                (mq_set, assignments_inner)
            }
            Err(e) => {
                error!(
                    "allocate message queue exception. strategy name: {}, {}.",
                    strategy_name, e
                );
                return false;
            }
        };

        let changed = self
            .update_message_queue_assignment(topic, &message_queue_assignments, is_order)
            .await;
        if changed {
            let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap().upgrade();
            if sub_rebalance_impl.is_none() {
                return false;
            }
            sub_rebalance_impl
                .unwrap()
                .message_queue_changed(topic, &HashSet::new(), &mq_set)
                .await;
        }
        let set = self.get_working_message_queue(topic).await;
        mq_set.eq(&set)
    }

    /// Updates the message queue assignments for a given topic.
    ///
    /// This function processes the provided message queue assignments, categorizing them into push
    /// and pop assignments. It handles the subscription and unsubscription of retry topics
    /// based on the assignments. It also removes unnecessary message queues and adds new
    /// message queues as needed.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to update.
    /// * `assignments` - A reference to a `HashSet` of `MessageQueueAssignment` representing the
    ///   new assignments.
    /// * `is_order` - A boolean indicating whether the message queues should be ordered.
    ///
    /// # Returns
    ///
    /// A `bool` indicating whether the message queue assignments were changed.
    async fn update_message_queue_assignment(
        &mut self,
        topic: &CheetahString,
        assignments: &HashSet<MessageQueueAssignment>,
        is_order: bool,
    ) -> bool {
        let generation = self.rebalance_generation();
        let mut changed = false;
        let mut mq2push_assignment = HashMap::new();
        let mut mq2pop_assignment = HashMap::new();
        for assignment in assignments {
            if let Some(ref mq) = assignment.message_queue {
                if MessageRequestMode::Pop == assignment.mode {
                    mq2pop_assignment.insert(mq, assignment);
                } else {
                    mq2push_assignment.insert(mq, assignment);
                }
            }
        }

        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if mq2pop_assignment.is_empty() && !mq2push_assignment.is_empty() {
                //pop switch to push
                //subscribe pop retry topic
                let retry_topic = CheetahString::from(KeyBuilder::build_pop_retry_topic(
                    topic,
                    self.consumer_group.as_ref().unwrap().as_ref(),
                    false,
                ));
                let subscription_data = FilterAPI::build_subscription_data(
                    &retry_topic,
                    &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                );
                if let Ok(subscription_data) = subscription_data {
                    self.put_subscription_data(&retry_topic, subscription_data).await;
                }
            } else if !mq2pop_assignment.is_empty() && mq2push_assignment.is_empty() {
                //push switch to pop
                //unsubscribe pop retry topic
                let retry_topic = CheetahString::from(KeyBuilder::build_pop_retry_topic(
                    topic,
                    self.consumer_group.as_ref().unwrap().as_ref(),
                    false,
                ));
                self.remove_subscription_data(&retry_topic).await;
            }
        }
        let mut remove_queue_map = HashMap::with_capacity(64);
        {
            // drop process queues no longer belong me

            let process_queue_table = self.process_queue_table.read().await;
            for (mq, pq) in process_queue_table.iter() {
                if mq.get_topic() == topic {
                    if !mq2push_assignment.contains_key(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, because pull is pause, so \
                                     try to fixed it",
                                    self.consumer_group,
                                    mq.get_topic()
                                );
                            }
                        }
                    }
                }
            }
        }

        // One queue is retired by one pass at a time (plan 49i I2 item 1): a pass that reaches a
        // key already retiring defers that queue and keeps working the others, because
        // waiting behind slow local persistence would stall every other queue in the same
        // pass. A retirement that turned an installation away asks for another rebalance
        // when it ends.
        let mut deferred_transition = false;
        for (mq, pq) in remove_queue_map {
            let Some(guard) = self.queue_transitions.try_retire(&mq, &pq) else {
                debug!(
                    "doRebalance, {:?}, leaving {} alone, another pass is already retiring it",
                    self.consumer_group, mq
                );
                continue;
            };
            // Plan 49h H1: this pass decided to drop that object. If the key now answers to a
            // different process queue, its offsets are not this pass's to erase and its acquisition
            // is not this pass's to end.
            if !table_holds_process_queue(&self.process_queue_table, &mq, &pq).await {
                debug!(
                    "doRebalance, {:?}, leaving {} alone, it answers to another process queue than the one being \
                     dropped",
                    self.consumer_group, mq
                );
                deferred_transition |= guard.release();
                continue;
            }
            let removed = if let Some(mut sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                sub_rebalance.remove_unnecessary_message_queue(&mq, &pq).await
            } else {
                false
            };
            if removed {
                let removed = remove_process_queue_if_same(&self.process_queue_table, &mq, &pq).await;
                if removed {
                    // Same rule as the plain rebalance path: leaving the table ends the acquisition
                    // (plan 49c C1) - and it names the object this pass retired, so a replacement
                    // installed while persistence ran is not ended by its predecessor (plan 49i I2
                    // item 3).
                    if let Some(mut sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                        sub_rebalance.note_queue_revoked(&mq, &pq).await;
                    }
                    changed = true;
                    info!(
                        "doRebalance, {:?}, remove unnecessary mq, {}",
                        self.consumer_group,
                        mq.get_topic()
                    );
                }
            }
            deferred_transition |= guard.release();
        }

        let mut remove_queue_map = HashMap::with_capacity(64);
        {
            // Drop process queues no longer belong to me
            let pop_process_queue_table = self.pop_process_queue_table.read().await;
            for (mq, pq) in pop_process_queue_table.iter() {
                if mq.get_topic() == topic {
                    if !mq2pop_assignment.contains_key(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, because pull is pause, so \
                                     try to fixed it",
                                    self.consumer_group,
                                    mq.get_topic()
                                );
                            }
                        }
                    }
                }
            }
        }

        for (mq, pq) in remove_queue_map {
            let removed = self
                .sub_rebalance_impl
                .as_mut()
                .unwrap()
                .upgrade()
                .map(|mut sub_rebalance| sub_rebalance.remove_unnecessary_pop_message_queue(&mq, &pq))
                .unwrap_or(false);
            if removed {
                let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
                if pop_process_queue_table.remove(&mq).is_some() {
                    changed = true;
                    info!(
                        "doRebalance, {:?}, remove unnecessary pop mq, {}",
                        self.consumer_group,
                        mq.get_topic()
                    );
                }
            }
        }

        {
            // add new message queue
            let mut all_mq_locked = true;
            let mut pull_request_list = Vec::new();
            let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap().upgrade();
            if sub_rebalance_impl.is_none() {
                return false;
            }
            let mut sub_rebalance_impl = sub_rebalance_impl.unwrap();
            let process_queue_table_clone = self.process_queue_table.clone();
            for (mq, assignment) in mq2push_assignment {
                let missing = {
                    let process_queue_table = process_queue_table_clone.read().await;
                    !process_queue_table.contains_key(mq)
                };
                if missing {
                    // A retirement of this very key may be waiting on persistence right now (plan
                    // 49i I2 item 1): publishing an installation into that
                    // window would let the pass still finishing erase its
                    // offset, so this queue waits for it and the retiring pass
                    // asks for another round. Other queues are untouched.
                    if self.queue_transitions.install_deferred(mq) {
                        debug!(
                            "doRebalance, {:?}, deferring {} until the retirement in flight ends",
                            self.consumer_group, mq
                        );
                        continue;
                    }
                    if is_order && !self.lock(mq).await {
                        warn!(
                            "doRebalance, {:?}, add a new mq failed, {}, because lock failed",
                            self.consumer_group,
                            mq.get_topic()
                        );
                        all_mq_locked = false;
                        continue;
                    }

                    sub_rebalance_impl.remove_dirty_offset(mq).await;
                    let pq = Arc::new(sub_rebalance_impl.create_process_queue());
                    pq.set_locked(true);
                    let prepared = sub_rebalance_impl.compute_pull_from_where_with_exception(mq).await;
                    let Ok(next_offset) = prepared else {
                        // Allocated but not preparable: recorded as this topic's conclusion, and
                        // never dispatched (plan 49c C1/C3).
                        sub_rebalance_impl
                            .note_allocation_pass(topic, AllocationOutcome::OffsetPreparationFailed)
                            .await;
                        continue;
                    };
                    if next_offset >= 0 && self.is_rebalance_generation_current(generation) {
                        let inserted = {
                            let mut process_queue_table = process_queue_table_clone.write().await;
                            process_queue_table.insert(mq.clone(), pq.clone()).is_none()
                        };
                        if inserted {
                            info!(
                                "doRebalance, {:?}, add a new mq, {}",
                                self.consumer_group,
                                mq.get_topic()
                            );
                            // Ownership is published before the request is queued for dispatch
                            // (plan 49c C1).
                            sub_rebalance_impl.note_queue_installed(mq, &pq).await;
                            pull_request_list.push(PullRequest::new(
                                self.consumer_group.as_ref().unwrap().clone(),
                                mq.clone(),
                                pq,
                                next_offset,
                            ));
                            changed = true;
                        } else {
                            info!(
                                "doRebalance, {:?}, mq already exists, {}",
                                self.consumer_group,
                                mq.get_topic()
                            );
                        }
                    } else {
                        warn!(
                            "doRebalance, {:?}, add new mq failed, {}",
                            self.consumer_group,
                            mq.get_topic()
                        );
                        if next_offset < 0 {
                            sub_rebalance_impl
                                .note_allocation_pass(topic, AllocationOutcome::OffsetPreparationFailed)
                                .await;
                        }
                    }
                }
            }

            if !all_mq_locked || deferred_transition {
                self.client_instance.as_mut().unwrap().rebalance_later(500);
            }
            sub_rebalance_impl.dispatch_pull_request(pull_request_list, 500).await;
        }

        {
            if let Some(rebalance_impl) = self.sub_rebalance_impl.as_ref().unwrap().upgrade() {
                let mut pop_request_list = Vec::new();
                let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
                for (mq, assignment) in mq2pop_assignment {
                    if !pop_process_queue_table.contains_key(mq) {
                        let pq = rebalance_impl.create_pop_process_queue();
                        let pre = pop_process_queue_table.insert(mq.clone(), Arc::new(pq.clone()));
                        if pre.is_none() {
                            info!("doRebalance, {:?}, mq already exists, {}", self.consumer_group, mq);
                        } else {
                            info!("doRebalance, {:?}, add a new pop mq, {}", self.consumer_group, mq);
                            let request = PopRequest::new(
                                topic.clone(),
                                self.consumer_group.as_ref().unwrap().clone(),
                                mq.clone(),
                                pq,
                                rebalance_impl.get_consume_init_mode(),
                            );
                            pop_request_list.push(request);
                            changed = true;
                        }
                    }
                }
                rebalance_impl.dispatch_pop_pull_request(pop_request_list, 500).await;
            }
        }

        if !self.is_rebalance_generation_current(generation) {
            return false;
        }
        changed
    }

    async fn update_process_queue_table_in_rebalance(
        &mut self,
        topic: &str,
        mq_set: &HashSet<MessageQueue>,
        is_order: bool,
    ) -> bool {
        let generation = self.rebalance_generation();
        let mut changed = false;
        let mut remove_queue_map = HashMap::new();
        let process_queue_table_cloned = self.process_queue_table.clone();
        {
            let process_queue_table = process_queue_table_cloned.read().await;
            // Drop process queues no longer belong to me
            for (mq, pq) in process_queue_table.iter() {
                if mq.get_topic() == topic {
                    if !mq_set.contains(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, because pull is pause, so \
                                     try to fixed it",
                                    self.consumer_group,
                                    mq.get_topic()
                                );
                            }
                        }
                    }
                }
            }
        }

        // The same transition rule as the repair path above (plan 49i I2 item 1): one retirement
        // per queue, others keep progressing, and an installation turned away is retried.
        let mut deferred_transition = false;
        for (mq, pq) in remove_queue_map {
            let Some(guard) = self.queue_transitions.try_retire(&mq, &pq) else {
                debug!(
                    "doRebalance, {:?}, leaving {} alone, another pass is already retiring it",
                    self.consumer_group, mq
                );
                continue;
            };
            // Plan 49h H1: same rule as the repair path above - only the object this pass decided
            // to drop may be removed and revoked here.
            if !table_holds_process_queue(&process_queue_table_cloned, &mq, &pq).await {
                debug!(
                    "doRebalance, {:?}, leaving {} alone, it answers to another process queue than the one being \
                     dropped",
                    self.consumer_group, mq
                );
                deferred_transition |= guard.release();
                continue;
            }
            let removed = if let Some(mut sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                sub_rebalance.remove_unnecessary_message_queue(&mq, &pq).await
            } else {
                false
            };
            if removed {
                let removed = remove_process_queue_if_same(&process_queue_table_cloned, &mq, &pq).await;
                if removed {
                    // Ownership ends with the table entry. Retiring the acquisition here - rather
                    // than in an assignment callback that has to wait for a heartbeat - means no
                    // later dispatch, and no replacement installation of the same queue, can
                    // inherit this loop's identity (plan 49c C1). The object is named so that only
                    // this pass's own acquisition ends (plan 49i I2 item 3).
                    if let Some(mut sub_rebalance) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                        sub_rebalance.note_queue_revoked(&mq, &pq).await;
                    }
                    changed = true;
                    info!(
                        "doRebalance, {:?}, remove unnecessary mq, {}",
                        self.consumer_group,
                        mq.get_topic()
                    );
                }
            }
            deferred_transition |= guard.release();
        }
        // Add new message queue
        let mut all_mq_locked = true;
        let mut pull_request_list = Vec::new();
        let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap().upgrade();
        if sub_rebalance_impl.is_none() {
            return false;
        }
        let mut sub_rebalance_impl = sub_rebalance_impl.unwrap();
        for mq in mq_set {
            let missing = {
                let process_queue_table = process_queue_table_cloned.read().await;
                !process_queue_table.contains_key(mq)
            };
            if missing {
                // An installation must not be published into a retirement of the same key that is
                // still waiting on persistence (plan 49i I2 item 1): the retiring pass would erase
                // the offset this one just prepared. Defer the queue and let its completion ask for
                // another pass.
                if self.queue_transitions.install_deferred(mq) {
                    debug!(
                        "doRebalance, {:?}, deferring {} until the retirement in flight ends",
                        self.consumer_group, mq
                    );
                    continue;
                }
                if is_order && !self.lock(mq).await {
                    warn!(
                        "doRebalance, {:?}, add a new mq failed, {}, because lock failed",
                        self.consumer_group,
                        mq.get_topic()
                    );
                    all_mq_locked = false;
                    continue;
                }

                sub_rebalance_impl.remove_dirty_offset(mq).await;
                let pq = Arc::new(sub_rebalance_impl.create_process_queue());
                pq.set_locked(true);
                let next_offset = sub_rebalance_impl.compute_pull_from_where(mq).await;
                if next_offset >= 0 && self.is_rebalance_generation_current(generation) {
                    let inserted = {
                        let mut process_queue_table = process_queue_table_cloned.write().await;
                        process_queue_table.insert(mq.clone(), pq.clone()).is_none()
                    };
                    if inserted {
                        info!(
                            "doRebalance, {:?}, add a new mq, {}",
                            self.consumer_group,
                            mq.get_topic()
                        );
                        // Publish this queue's acquisition before it is dispatched. The initial
                        // pull is only scheduled after this loop returns,
                        // but the admission check that guards it needs an
                        // owner by then, and waiting for the assignment callback
                        // would make a slow heartbeat drop the first pull of every new queue with
                        // no retry until the lost-pull detector fires (plan
                        // 49c C1).
                        sub_rebalance_impl.note_queue_installed(mq, &pq).await;
                        pull_request_list.push(PullRequest::new(
                            self.consumer_group.as_ref().unwrap().clone(),
                            mq.clone(),
                            pq,
                            next_offset,
                        ));
                        changed = true;
                    } else {
                        info!(
                            "doRebalance, {:?}, mq already exists, {}",
                            self.consumer_group,
                            mq.get_topic()
                        );
                    }
                } else {
                    warn!(
                        "doRebalance, {:?}, add new mq failed, {}",
                        self.consumer_group,
                        mq.get_topic()
                    );
                    // Queues were allocated but this one could not be placed: no pull loop was
                    // started for it, which is a preparation failure rather
                    // than an idle assignment. A pass that ended because the
                    // rebalance generation moved on is not that.
                    if next_offset < 0 {
                        sub_rebalance_impl
                            .note_allocation_pass(topic, AllocationOutcome::OffsetPreparationFailed)
                            .await;
                    }
                }
            }
        }

        if !all_mq_locked || deferred_transition {
            self.client_instance.as_mut().unwrap().rebalance_later(500);
        }
        sub_rebalance_impl.dispatch_pull_request(pull_request_list, 500).await;

        if !self.is_rebalance_generation_current(generation) {
            return false;
        }
        changed
    }

    async fn rebalance_by_topic(&mut self, topic: &CheetahString, is_order: bool) -> bool {
        match self.message_model.unwrap() {
            MessageModel::Broadcasting => {
                let topic_sub_cloned = self.topic_subscribe_info_table.clone();
                let topic_subscribe_info_table = topic_sub_cloned.read().await;
                let mq_set = topic_subscribe_info_table.get(topic);
                if let Some(mq_set) = mq_set {
                    let changed = self
                        .update_process_queue_table_in_rebalance(topic, mq_set, is_order)
                        .await;
                    if changed {
                        // Callbacks observe the committed actual set rather than the desired one: a
                        // queue whose preparation failed was not installed and must not be reported
                        // as owned (plan 49c C3).
                        let working = self.get_working_message_queue(topic).await;
                        let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap();
                        if let Some(mut sub_rebalance_impl) = sub_rebalance_impl.upgrade() {
                            sub_rebalance_impl.message_queue_changed(topic, mq_set, &working).await;
                        }
                    }
                    mq_set.eq(&self.get_working_message_queue(topic).await)
                } else {
                    if let Some(mut sub_rebalance_impl) = self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
                        // The pass completed and there is nothing to allocate from. That is a
                        // conclusion about this topic, recorded as one, and not an idle assignment.
                        sub_rebalance_impl
                            .note_allocation_pass(topic, AllocationOutcome::RouteAbsent)
                            .await;
                        sub_rebalance_impl
                            .message_queue_changed(topic, &HashSet::new(), &HashSet::new())
                            .await;
                        warn!(
                            "doRebalance, {}, but the topic[{}] not exist.",
                            self.consumer_group.as_ref().unwrap(),
                            topic
                        );
                    }
                    true
                }
            }
            MessageModel::Clustering => {
                //get consumer id list from broker
                let cid_all = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .find_consumer_id_list(topic, self.consumer_group.as_ref().unwrap())
                    .await;
                let topic_sub_cloned = self.topic_subscribe_info_table.clone();
                let topic_subscribe_info_table_inner = topic_sub_cloned.read().await;
                let mq_set = topic_subscribe_info_table_inner.get(topic);
                if mq_set.is_none() && !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    if let Some(mut sub_rebalance_impl) = self.sub_rebalance_impl.as_ref().unwrap().upgrade() {
                        sub_rebalance_impl
                            .note_allocation_pass(topic, AllocationOutcome::RouteAbsent)
                            .await;
                        sub_rebalance_impl
                            .message_queue_changed(topic, &HashSet::new(), &HashSet::new())
                            .await;
                        warn!(
                            "doRebalance, {}, but the topic[{}] not exist.",
                            self.consumer_group.as_ref().unwrap(),
                            topic
                        );
                    }
                }
                if cid_all.is_none() {
                    warn!(
                        "doRebalance, {}, {}, get consumer id list failed.",
                        self.consumer_group.as_ref().unwrap(),
                        topic
                    );
                    // The pass ended without an answer: health must not read the resulting zero
                    // assignment as an explicit idle assignment (plan 49a F4
                    // section 4.3 item 1).
                    self.note_allocation_pass(topic, AllocationOutcome::LookupFailed).await;
                    return true;
                }
                if let (Some(mq_set), Some(mut ci_all)) = (mq_set, cid_all) {
                    let mut mq_all = mq_set.iter().cloned().collect::<Vec<MessageQueue>>();
                    mq_all.sort();
                    ci_all.sort();

                    let strategy = self.allocate_message_queue_strategy.as_ref().unwrap();
                    let strategy_name = strategy.get_name();
                    let allocate_result = match strategy.allocate(
                        self.consumer_group.as_ref().unwrap(),
                        self.client_instance.as_ref().unwrap().client_id.as_ref(),
                        mq_all.as_slice(),
                        ci_all.as_slice(),
                    ) {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                "allocate message queue exception. strategy name: {}, ex: {}",
                                self.allocate_message_queue_strategy.as_ref().unwrap().get_name(),
                                e.to_string()
                            );
                            self.note_allocation_pass(topic, AllocationOutcome::LookupFailed).await;
                            return false;
                        }
                    };
                    let allocate_result_set = allocate_result.into_iter().collect::<HashSet<MessageQueue>>();
                    // The pass has an answer, and its kind is what tells a later zero assignment
                    // apart from a refresh that never finished.
                    self.note_allocation_pass(
                        topic,
                        if allocate_result_set.is_empty() {
                            AllocationOutcome::ExplicitZero
                        } else {
                            AllocationOutcome::QueuesOwned
                        },
                    )
                    .await;
                    let changed = self
                        .update_process_queue_table_in_rebalance(topic, &allocate_result_set, is_order)
                        .await;
                    if changed {
                        info!(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, \
                             topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, \
                             rebalanceResultSet={:?}",
                            strategy_name,
                            self.consumer_group.as_ref().unwrap(),
                            topic,
                            self.client_instance.as_ref().unwrap().client_id,
                            mq_set.len(),
                            ci_all.len(),
                            allocate_result_set.len(),
                            allocate_result_set
                        );

                        // The committed actual set again: whatever could not be prepared is not
                        // owned, and its failure stays recorded for this topic alone.
                        let working = self.get_working_message_queue(topic).await;
                        if let Some(mut sub_rebalance_impl) = self.sub_rebalance_impl.as_ref().unwrap().upgrade() {
                            sub_rebalance_impl.message_queue_changed(topic, mq_set, &working).await;
                        }
                    }
                    return allocate_result_set.eq(&self.get_working_message_queue(topic).await);
                }
                true
            }
        }
    }

    pub async fn get_working_message_queue(&self, topic: &str) -> HashSet<MessageQueue> {
        let mut queue_set = HashSet::new();
        let process_queue_table = self.process_queue_table.read().await;
        for (mq, pq) in process_queue_table.iter() {
            if mq.get_topic() == topic && !pq.is_dropped() {
                queue_set.insert(mq.clone());
            }
        }
        let pop_process_queue_table = self.pop_process_queue_table.read().await;
        for (mq, pq) in pop_process_queue_table.iter() {
            if mq.get_topic() == topic && !pq.is_dropped() {
                queue_set.insert(mq.clone());
            }
        }
        queue_set
    }

    pub async fn lock(&mut self, mq: &MessageQueue) -> bool {
        let locked = self.lock_with(mq, &HashMap::new()).await;
        if locked {
            let process_queue_table = self.process_queue_table.read().await;
            if let Some(pq) = process_queue_table.get(mq) {
                pq.set_locked(true);
                pq.set_last_pull_timestamp(get_current_millis());
            }
        }
        locked
    }

    pub async fn lock_with(
        &mut self,
        mq: &MessageQueue,
        process_queue_table: &HashMap<MessageQueue, Arc<ProcessQueue>>,
    ) -> bool {
        let client = self.client_instance.as_mut().unwrap();
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let find_broker_result = client
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
            .await;
        if let Some(find_broker_result) = find_broker_result {
            let mut request_body = LockBatchRequestBody {
                consumer_group: Some(self.consumer_group.clone().unwrap()),
                client_id: Some(client.client_id.clone()),
                ..Default::default()
            };
            request_body.mq_set.insert(mq.clone());
            let result = client
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                .await;
            match result {
                Ok(locked_mq) => {
                    for mq in &locked_mq {
                        if let Some(pq) = process_queue_table.get(mq) {
                            pq.set_locked(true);
                            pq.set_last_pull_timestamp(get_current_millis());
                        }
                    }
                    let lock_ok = locked_mq.contains(mq);
                    info!("message queue lock {}, {:?} {}", lock_ok, self.consumer_group, mq);
                    lock_ok
                }
                Err(e) => {
                    error!("lockBatchMQ exception {},{}", mq, e);
                    false
                }
            }
        } else {
            false
        }
    }

    pub async fn lock_all(&mut self) {
        let broker_mqs = self.build_process_queue_table_by_broker_name().await;

        let map = broker_mqs
            .into_iter()
            .map(|(broker_name, mqs)| {
                let mut client_instance = self.client_instance.clone();
                let process_queue_table = self.process_queue_table.clone();
                let consumer_group = self.consumer_group.clone().unwrap();
                async move {
                    if mqs.is_empty() {
                        return;
                    }
                    let client = client_instance.as_mut().unwrap();
                    let find_broker_result = client
                        .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
                        .await;
                    if let Some(find_broker_result) = find_broker_result {
                        let request_body = LockBatchRequestBody {
                            consumer_group: Some(consumer_group.to_owned()),
                            client_id: Some(client.client_id.clone()),
                            mq_set: mqs.clone(),
                            ..Default::default()
                        };
                        let result = client
                            .mq_client_api_impl
                            .as_mut()
                            .unwrap()
                            .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                            .await;
                        match result {
                            Ok(lock_okmqset) => {
                                let process_queue_table = process_queue_table.read().await;
                                for mq in &mqs {
                                    if let Some(pq) = process_queue_table.get(mq) {
                                        if lock_okmqset.contains(mq) {
                                            if pq.is_locked() {
                                                info!(
                                                    "the message queue locked OK, Group: {:?} {}",
                                                    consumer_group, mq
                                                );
                                            }
                                            pq.set_locked(true);
                                            pq.set_last_lock_timestamp(get_current_millis());
                                        } else {
                                            pq.set_locked(false);
                                            warn!(
                                                "the message queue locked Failed, Group: {:?} {}",
                                                consumer_group, mq
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("lockBatchMQ exception {}", e);
                            }
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
        futures::future::join_all(map).await;

        /*        for (broker_name, mqs) in broker_mqs {
            if mqs.is_empty() {
                continue;
            }
            let client = self.client_instance.as_mut().unwrap();
            let find_broker_result = client
                .find_broker_address_in_subscribe(broker_name.as_str(), mix_all::MASTER_ID, true)
                .await;
            if let Some(find_broker_result) = find_broker_result {
                let request_body = LockBatchRequestBody {
                    consumer_group: Some(self.consumer_group.clone().unwrap()),
                    client_id: Some(client.client_id.clone()),
                    mq_set: mqs.clone(),
                    ..Default::default()
                };
                let result = client
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                    .await;
                match result {
                    Ok(lock_okmqset) => {
                        let process_queue_table = self.process_queue_table.read().await;
                        for mq in &mqs {
                            if let Some(pq) = process_queue_table.get(mq) {
                                if lock_okmqset.contains(mq) {
                                    if pq.is_locked() {
                                        info!(
                                            "the message queue locked OK, Group: {:?} {}",
                                            self.consumer_group, mq
                                        );
                                    }
                                    pq.set_locked(true);
                                    pq.set_last_lock_timestamp(get_current_millis());
                                } else {
                                    pq.set_locked(false);
                                    warn!(
                                        "the message queue locked Failed, Group: {:?} {}",
                                        self.consumer_group, mq
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("lockBatchMQ exception {}", e);
                    }
                }
            }
        }*/
    }

    pub async fn unlock_all(&mut self, oneway: bool) {
        let broker_mqs = self.build_process_queue_table_by_broker_name().await;
        for (broker_name, mqs) in broker_mqs {
            if mqs.is_empty() {
                continue;
            }
            let client = self.client_instance.as_mut().unwrap();
            let find_broker_result = client
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
                .await;
            if let Some(find_broker_result) = find_broker_result {
                let request_body = UnlockBatchRequestBody {
                    consumer_group: Some(self.consumer_group.clone().unwrap()),
                    client_id: Some(client.client_id.clone()),
                    mq_set: mqs.clone(),
                    ..Default::default()
                };
                let result = client
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .unlock_batch_mq(&find_broker_result.broker_addr, request_body, 1_000, oneway)
                    .await;
                match result {
                    Ok(_) => {
                        let process_queue_table = self.process_queue_table.read().await;
                        for mq in &mqs {
                            if let Some(pq) = process_queue_table.get(mq) {
                                pq.set_locked(false);
                                info!("the message queue unlock OK, Group: {:?} {}", self.consumer_group, mq);
                            }
                        }
                    }
                    Err(e) => {
                        error!("unlockBatchMQ exception {}", e);
                    }
                }
            }
        }
    }

    async fn build_process_queue_table_by_broker_name(
        &self,
    ) -> HashMap<CheetahString /* brokerName */, HashSet<MessageQueue>> {
        let mut result = HashMap::new();
        let process_queue_table = self.process_queue_table.read().await;
        let client = self.client_instance.as_ref().unwrap();
        for (mq, pq) in process_queue_table.iter() {
            if pq.is_dropped() {
                continue;
            }
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            let entry = result.entry(broker_name).or_insert(HashSet::new());
            entry.insert(mq.to_owned());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::consumer_impl::re_balance::rebalance_push_impl::RebalancePushImpl;

    #[test]
    fn invalidating_rebalance_generation_discards_prepared_work() {
        let rebalance = RebalanceImpl::<RebalancePushImpl>::new(None, None, None, None);
        let generation = rebalance.rebalance_generation();
        rebalance.invalidate_rebalance_generation();

        assert!(!rebalance.is_rebalance_generation_current(generation));
    }
}

/// Plan 49h H1: dropping a queue is decided for one process queue object, and only that object's
/// removal ends its acquisition.
#[cfg(test)]
mod review49h {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn a_replacement_entry_is_not_the_process_queue_being_dropped() {
        let table: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>> = Arc::new(RwLock::new(HashMap::new()));
        let q = MessageQueue::from_parts("T", "broker", 0);
        let old = Arc::new(ProcessQueue::new());
        let replacement = Arc::new(ProcessQueue::new());
        table.write().await.insert(q.clone(), old.clone());

        // The object this pass decided to drop is still installed: it may be removed.
        assert!(table_holds_process_queue(&table, &q, &old).await);
        assert!(!table_holds_process_queue(&table, &q, &replacement).await);

        // A replacement installed under the same key belongs to somebody else's work: neither the
        // probe nor the removal may touch it.
        table.write().await.insert(q.clone(), replacement.clone());
        assert!(
            !table_holds_process_queue(&table, &q, &old).await,
            "a retired process queue was still treated as installed"
        );
        assert!(
            !remove_process_queue_if_same(&table, &q, &old).await,
            "the pass dropping a predecessor removed its replacement"
        );
        assert_eq!(table.read().await.len(), 1);

        // The replacement's own pass removes exactly it.
        assert!(remove_process_queue_if_same(&table, &q, &replacement).await);
        assert!(table.read().await.is_empty());
        assert!(!remove_process_queue_if_same(&table, &q, &replacement).await);
    }
}

#[cfg(test)]
mod review49i {
    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
    use crate::consumer::store::offset_store::OffsetStore;
    use crate::consumer::store::read_offset_type::ReadOffsetType;
    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn a_retirement_waiting_on_io_must_not_erase_a_replacements_offset() {
        let cc = ClientConfig::default();
        let group = CheetahString::from_static_str("review49i-retirement");
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let h = push.clone();
        push.set_default_mqpush_consumer_impl(h);
        push.mark_managed_lifecycle();
        let mut client = MQClientInstance::new_arc(cc, 0, "review49i-retirement-client", None);
        let path = std::env::temp_dir().join(format!("review49i-offsets-{}.json", std::process::id()));
        let mut local = LocalFileOffsetStore::new(client.clone(), group);
        let io = local.review49e_path_and_lock(path.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(local)));
        let store = push.offset_store.as_ref().unwrap().clone();
        let q = MessageQueue::from_parts("T", "broker", 0);
        let old = Arc::new(ProcessQueue::new());
        let replacement = Arc::new(ProcessQueue::new());
        let table = push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        table.write().await.insert(q.clone(), old.clone());
        push.note_queue_installed(&q, &old);
        store.update_offset(&q, 7, false).await;
        let guard = io.lock().await;
        let mut rebalance = push.rebalance_impl.clone();
        let empty = HashSet::new();
        let mut retiring = Box::pin(
            rebalance
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &empty, false),
        );
        tokio::select! { _=&mut retiring => panic!("old retirement should wait for the local writer"), _=tokio::time::sleep(Duration::from_millis(20))=>{} }
        assert!(old.is_dropped(), "fixture did not enter the actual retirement loop");
        table.write().await.insert(q.clone(), replacement.clone());
        push.note_queue_installed(&q, &replacement);
        let epoch = push.progress.current_epoch(&q).unwrap();
        store.update_offset(&q, 99, false).await;
        assert_eq!(store.read_offset(&q, ReadOffsetType::ReadFromMemory).await, 99);
        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), &mut retiring)
            .await
            .expect("the old writer did not finish");
        drop(retiring);
        let replacement_present = table
            .read()
            .await
            .get(&q)
            .is_some_and(|pq| Arc::ptr_eq(pq, &replacement));
        let current_epoch = push.progress.current_epoch(&q);
        let offset = store.read_offset(&q, ReadOffsetType::ReadFromMemory).await;
        eprintln!(
            "old retirement: replacement_present={replacement_present} epoch={epoch} current_epoch={current_epoch:?} \
             replacement_offset={offset}"
        );
        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        assert!(replacement_present);
        assert_eq!(current_epoch, Some(epoch));
        assert_eq!(
            offset, 99,
            "identity-safe table removal ran after a key-only offset deletion"
        );
    }

    /// The control for the test above: when nothing replaced the queue, retirement still erases its
    /// own offsets, drops its own entry and ends its own acquisition (plan 49i I2 item 5).
    #[tokio::test(flavor = "current_thread")]
    async fn a_retirement_of_the_same_process_queue_still_clears_its_own_acquisition() {
        let cc = ClientConfig::default();
        let group = CheetahString::from_static_str("review49i-own-retirement");
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = push.clone();
        push.set_default_mqpush_consumer_impl(handle);
        push.mark_managed_lifecycle();
        let mut client = MQClientInstance::new_arc(cc, 0, "review49i-own-retirement-client", None);
        let path = std::env::temp_dir().join(format!("review49i-own-offsets-{}.json", std::process::id()));
        let mut local = LocalFileOffsetStore::new(client.clone(), group);
        let io = local.review49e_path_and_lock(path.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(local)));
        let store = push.offset_store.as_ref().unwrap().clone();
        let q = MessageQueue::from_parts("T", "broker", 0);
        let owned = Arc::new(ProcessQueue::new());
        let table = push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        table.write().await.insert(q.clone(), owned.clone());
        push.note_queue_installed(&q, &owned);
        store.update_offset(&q, 7, false).await;

        let guard = io.lock().await;
        let mut rebalance = push.rebalance_impl.clone();
        let empty = HashSet::new();
        let mut retiring = Box::pin(
            rebalance
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &empty, false),
        );
        tokio::select! { _=&mut retiring => panic!("retirement should wait for the local writer"), _=tokio::time::sleep(Duration::from_millis(20))=>{} }
        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), &mut retiring)
            .await
            .expect("the retirement of its own queue never finished");
        drop(retiring);

        let offset = store.read_offset(&q, ReadOffsetType::ReadFromMemory).await;
        let epoch = push.progress.current_epoch(&q);
        let entries = table.read().await.len();
        eprintln!(
            "same-owner retirement: offset={offset} epoch={epoch:?} entries={entries} dropped={}\n",
            owned.is_dropped()
        );
        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        assert_eq!(
            offset, -1,
            "a retirement that owned its queue stopped erasing the offsets it always erased"
        );
        assert_eq!(
            epoch, None,
            "the acquisition of the retired process queue survived its own retirement"
        );
        assert_eq!(entries, 0, "the retired process queue stayed in the table");
    }

    /// A retirement that stopped owning its key before it ever reached the offset store asks once
    /// and stays its hands off - the same question re-asked after persistence is not the only
    /// guard (plan 49i I2 item 2).
    #[tokio::test(flavor = "current_thread")]
    async fn a_retirement_that_stopped_owing_the_queue_leaves_its_offsets_alone() {
        let cc = ClientConfig::default();
        let group = CheetahString::from_static_str("review49i-stale-retirement");
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = push.clone();
        push.set_default_mqpush_consumer_impl(handle);
        push.mark_managed_lifecycle();
        let mut client = MQClientInstance::new_arc(cc, 0, "review49i-stale-retirement-client", None);
        let path = std::env::temp_dir().join(format!("review49i-stale-offsets-{}.json", std::process::id()));
        let mut local = LocalFileOffsetStore::new(client.clone(), group);
        let _io = local.review49e_path_and_lock(path.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(local)));
        let store = push.offset_store.as_ref().unwrap().clone();
        let q = MessageQueue::from_parts("T", "broker", 0);
        let stale = Arc::new(ProcessQueue::new());
        let table = push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        table.write().await.insert(q.clone(), stale.clone());
        push.note_queue_installed(&q, &stale);
        store.update_offset(&q, 7, false).await;

        // A replacement owns the key before this retirement touches the offset store at all.
        let replacement = Arc::new(ProcessQueue::new());
        table.write().await.insert(q.clone(), replacement.clone());
        push.note_queue_installed(&q, &replacement);

        let mut rebalance = push.rebalance_impl.clone();
        let _ = std::fs::remove_file(&path);
        let erased = rebalance.remove_unnecessary_message_queue(&q, &stale).await;
        let offset = store.read_offset(&q, ReadOffsetType::ReadFromMemory).await;
        let persisted = std::path::Path::new(&path).exists();
        // The owner of the key still erases its own offsets.
        let owned_erased = rebalance.remove_unnecessary_message_queue(&q, &replacement).await;
        let owned_offset = store.read_offset(&q, ReadOffsetType::ReadFromMemory).await;
        let owned_persisted = std::path::Path::new(&path).exists();
        eprintln!(
            "stale retirement: erased={erased} offset_after={offset} persisted={persisted}; owner: \
             erased={owned_erased} offset_after={owned_offset} persisted={owned_persisted}"
        );
        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        assert!(
            !erased,
            "a retirement that no longer owned the key reported clearing it"
        );
        assert_eq!(offset, 7, "a stale retirement erased its replacement's offset");
        assert!(
            !persisted,
            "a retirement that no longer owned the key still persisted through the offset store"
        );
        assert!(owned_persisted, "the control never wrote the offsets it owns");
        assert!(owned_erased);
        assert_eq!(
            owned_offset, -1,
            "the owning retirement stopped clearing its own offsets"
        );
    }

    /// One queue's transition must not hold up the others: an installation of a key whose
    /// retirement is in flight waits for it, while an unrelated queue is assigned normally and
    /// the deferred one is assigned as soon as the retirement ends (plan 49i I2 item 1).
    #[tokio::test(flavor = "current_thread")]
    async fn an_installation_waits_only_behind_a_retirement_of_its_own_key() {
        let cc = ClientConfig::default();
        let group = CheetahString::from_static_str("review49i-deferred-install");
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            message_model: MessageModel::Broadcasting,
            // Offsets are prepared from the local store only; no broker is asked for a max offset.
            consume_from_where: ConsumeFromWhere::ConsumeFromFirstOffset,
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = push.clone();
        push.set_default_mqpush_consumer_impl(handle);
        push.mark_managed_lifecycle();
        let mut client = MQClientInstance::new_arc(cc, 0, "review49i-deferred-client", None);
        push.client_instance = Some(client.clone());
        push.rebalance_impl.rebalance_impl_inner.client_instance = Some(client.clone());
        // An assignment pass names its consumer group when it publishes an installation.
        push.rebalance_impl.rebalance_impl_inner.consumer_group =
            Some(CheetahString::from_static_str("review49i-deferred-install"));
        let path = std::env::temp_dir().join(format!("review49i-deferred-{}.json", std::process::id()));
        let mut local = LocalFileOffsetStore::new(client.clone(), group);
        let _io = local.review49e_path_and_lock(path.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(local)));

        let deferred_q = MessageQueue::from_parts("T", "broker", 0);
        let sibling_q = MessageQueue::from_parts("T", "broker", 1);
        let retiring_pq = Arc::new(ProcessQueue::new());
        let table = push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();

        // A retirement of the first key is in flight - its persistence has not returned.
        let claim = transitions
            .try_retire(&deferred_q, &retiring_pq)
            .expect("nothing retires this queue yet");

        let mut assigned = HashSet::new();
        assigned.insert(deferred_q.clone());
        assigned.insert(sibling_q.clone());
        let mut rebalance = push.rebalance_impl.clone();
        rebalance
            .rebalance_impl_inner
            .update_process_queue_table_in_rebalance("T", &assigned, false)
            .await;

        {
            let table = table.read().await;
            assert!(
                !table.contains_key(&deferred_q),
                "an installation was published into a retirement of the same key"
            );
            assert!(
                table.contains_key(&sibling_q),
                "a retirement of one queue stopped an unrelated queue from being assigned"
            );
        }
        assert_eq!(
            push.progress.current_epoch(&deferred_q),
            None,
            "the deferred queue's acquisition was published anyway"
        );

        // The retirement ends; the next pass assigns the deferred queue like any other.
        claim.release();
        rebalance
            .rebalance_impl_inner
            .update_process_queue_table_in_rebalance("T", &assigned, false)
            .await;
        let installed = table.read().await.contains_key(&deferred_q);
        let epoch = push.progress.current_epoch(&deferred_q);
        eprintln!(
            "deferred install: installed={installed} epoch={epoch:?} sibling_installed={}\n",
            table.read().await.contains_key(&sibling_q)
        );
        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        assert!(
            installed,
            "the queue deferred behind a retirement was never assigned once it ended"
        );
        assert!(epoch.is_some(), "the deferred installation published no identity");
    }

    /// A second caller reaching a key whose retirement is already in flight does nothing at all: it
    /// neither repeats the side effects nor acts on an installation it never owned (plan 49i I2
    /// item 4).
    #[tokio::test(flavor = "current_thread")]
    async fn a_second_retirement_of_one_queue_does_not_stack_behind_the_first() {
        use crate::consumer::consumer_impl::re_balance::Rebalance;

        let cc = ClientConfig::default();
        let group = CheetahString::from_static_str("review49i-double-retire");
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = push.clone();
        push.set_default_mqpush_consumer_impl(handle);
        push.mark_managed_lifecycle();
        let mut client = MQClientInstance::new_arc(cc, 0, "review49i-double-retire-client", None);
        push.rebalance_impl.rebalance_impl_inner.client_instance = Some(client.clone());
        let path = std::env::temp_dir().join(format!("review49i-double-{}.json", std::process::id()));
        let mut local = LocalFileOffsetStore::new(client.clone(), group);
        let _io = local.review49e_path_and_lock(path.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(local)));
        let store = push.offset_store.as_ref().unwrap().clone();

        let q = MessageQueue::from_parts("T", "broker", 0);
        let installed = Arc::new(ProcessQueue::new());
        let other = Arc::new(ProcessQueue::new());
        let table = push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        table.write().await.insert(q.clone(), installed.clone());
        push.note_queue_installed(&q, &installed);
        store.update_offset(&q, 7, false).await;

        // Somebody else is already retiring this key.
        let claim = push
            .rebalance_impl
            .rebalance_impl_inner
            .queue_transitions
            .try_retire(&q, &installed)
            .expect("nothing retires this queue yet");

        // A second retirement of the same key - even one naming a different object - leaves
        // everything alone instead of stacking its own round of persistence and offset
        // removal.
        let mut rebalance = push.rebalance_impl.clone();
        tokio::time::timeout(Duration::from_millis(200), rebalance.remove_process_queue(&q, &other))
            .await
            .expect("a second retirement waited behind the first instead of deferring this queue");
        {
            let table = table.read().await;
            assert!(
                table.get(&q).is_some_and(|pq| Arc::ptr_eq(pq, &installed)),
                "a second retirement changed what the first one owns"
            );
        }
        assert!(push.progress.current_epoch(&q).is_some());

        // The same is true of the object that already owns the transition: it is not retired twice.
        tokio::time::timeout(
            Duration::from_millis(200),
            rebalance.remove_process_queue(&q, &installed),
        )
        .await
        .expect("repeated retirement of the same process queue waited behind itself");
        let offset = store.read_offset(&q, ReadOffsetType::ReadFromMemory).await;
        eprintln!(
            "stacked retirement: offset={offset} dropped={}\n",
            installed.is_dropped()
        );
        assert_eq!(
            offset, 7,
            "a queued second retirement erased offsets the first one had not touched yet"
        );
        assert!(!installed.is_dropped(), "a deferred retirement still dropped the queue");

        claim.release();
        // With the transition free, the retirement runs for real.
        tokio::time::timeout(
            Duration::from_millis(500),
            rebalance.remove_process_queue(&q, &installed),
        )
        .await
        .expect("the retirement never ran once the transition was free");
        let offset = store.read_offset(&q, ReadOffsetType::ReadFromMemory).await;
        let epoch = push.progress.current_epoch(&q);
        let entries = table.read().await.len();
        eprintln!("unblocked retirement: offset={offset} epoch={epoch:?} entries={entries}\n",);
        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        assert_eq!(offset, -1, "the retirement that finally ran left its offsets behind");
        assert_eq!(
            epoch, None,
            "the retired acquisition still answers to its process queue"
        );
        assert_eq!(entries, 0, "the retired queue stayed in the table");
    }
}
