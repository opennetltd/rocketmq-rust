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

use std::collections::hash_map::Entry;
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

/// Publish an installation without ever replacing the entry that is already there (plan 49j J1 item
/// 3). The first pass to reach the table owns the key and publishes the acquisition naming that
/// very object; a second installer that prepared an offset meanwhile reports "nothing changed"
/// instead of swapping in an object no binding names, which would leave dispatch either failing
/// closed forever or pulling on a queue nobody owns.
pub(crate) async fn insert_process_queue_if_absent(
    table: &Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    mq: &MessageQueue,
    pq: &Arc<ProcessQueue>,
) -> bool {
    let mut table = table.write().await;
    match table.entry(mq.clone()) {
        Entry::Vacant(slot) => {
            slot.insert(Arc::clone(pq));
            true
        }
        Entry::Occupied(_) => false,
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
                    "doRebalance, {:?}, leaving {} alone, another transition owns it",
                    self.consumer_group, mq
                );
                // A busy key may be held by an installation as well as a retirement (plan 49j J1
                // item 2), and neither one retires this queue on this pass's behalf. Asking for
                // another pass is what keeps the wait bounded instead of leaving a dropped queue
                // until the next scheduled rebalance.
                deferred_transition = true;
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
                    // One installation per key, claimed before any of its work rather than merely
                    // checked for it (plan 49j J1 item 2): a retirement waiting on persistence and
                    // a second installer of the same absent queue both keep
                    // this pass out, and the claim is held through publication
                    // so neither can slip between preparation and publishing.
                    // Being turned away is recorded as a deferred transition, because a
                    // queue skipped in silence stays uninstalled until something else wakes
                    // rebalance.
                    let Some(installation) = self.queue_transitions.try_install(mq) else {
                        debug!(
                            "doRebalance, {:?}, deferring {} until the transition in flight ends",
                            self.consumer_group, mq
                        );
                        deferred_transition = true;
                        continue;
                    };
                    if is_order && !self.lock(mq).await {
                        warn!(
                            "doRebalance, {:?}, add a new mq failed, {}, because lock failed",
                            self.consumer_group,
                            mq.get_topic()
                        );
                        all_mq_locked = false;
                        deferred_transition |= installation.release();
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
                        deferred_transition |= installation.release();
                        continue;
                    };
                    if next_offset >= 0 && self.is_rebalance_generation_current(generation) {
                        // Publication adds, it never replaces (plan 49j J1 item 3): an entry that
                        // appeared while this pass prepared its offset belongs to the pass that
                        // published it, including its acquisition.
                        let inserted = insert_process_queue_if_absent(&process_queue_table_clone, mq, &pq).await;
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
                    // The claim ends only once publication is settled - table entry and the
                    // acquisition naming it are then one object, or this pass installed nothing.
                    deferred_transition |= installation.release();
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
                    "doRebalance, {:?}, leaving {} alone, another transition owns it",
                    self.consumer_group, mq
                );
                // Same as the repair path: a key held by an installation is retired when that
                // installation ends, so ask for the pass that will do it (plan 49j J1 item 2).
                deferred_transition = true;
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
                // The same claim as the repair path above (plan 49j J1 item 2): preparation and
                // publication happen under one installation, so no retirement begins in that window
                // and a competing installer of this key is turned away rather than replacing what
                // this pass publishes.
                let Some(installation) = self.queue_transitions.try_install(mq) else {
                    debug!(
                        "doRebalance, {:?}, deferring {} until the transition in flight ends",
                        self.consumer_group, mq
                    );
                    deferred_transition = true;
                    continue;
                };
                if is_order && !self.lock(mq).await {
                    warn!(
                        "doRebalance, {:?}, add a new mq failed, {}, because lock failed",
                        self.consumer_group,
                        mq.get_topic()
                    );
                    all_mq_locked = false;
                    deferred_transition |= installation.release();
                    continue;
                }

                sub_rebalance_impl.remove_dirty_offset(mq).await;
                let pq = Arc::new(sub_rebalance_impl.create_process_queue());
                pq.set_locked(true);
                let next_offset = sub_rebalance_impl.compute_pull_from_where(mq).await;
                if next_offset >= 0 && self.is_rebalance_generation_current(generation) {
                    // Adds, never replaces (plan 49j J1 item 3).
                    let inserted = insert_process_queue_if_absent(&process_queue_table_cloned, mq, &pq).await;
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
                // Claim held through publication (plan 49j J1 item 2).
                deferred_transition |= installation.release();
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

#[cfg(test)]
mod review49j {
    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
    use crate::consumer::store::offset_store::OffsetStore;
    use std::time::Duration;

    struct Fixture {
        push: ArcMut<DefaultMQPushConsumerImpl>,
        client: ArcMut<MQClientInstance>,
        io: Arc<tokio::sync::Mutex<()>>,
        path: std::path::PathBuf,
        q: MessageQueue,
    }
    fn fixture(label: &str) -> Fixture {
        let cc = ClientConfig::default();
        let group = CheetahString::from_string(format!("review49j-{label}"));
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            message_model: MessageModel::Broadcasting,
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let h = push.clone();
        push.set_default_mqpush_consumer_impl(h);
        push.mark_managed_lifecycle();
        let client = MQClientInstance::new_arc(cc, 0, format!("review49j-{label}").as_str(), None);
        push.client_instance = Some(client.clone());
        push.rebalance_impl.set_consumer_group(group.clone());
        push.rebalance_impl.set_message_model(MessageModel::Broadcasting);
        push.rebalance_impl.rebalance_impl_inner.client_instance = Some(client.clone());
        let path = std::env::temp_dir().join(format!("review49j-{label}-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut local = LocalFileOffsetStore::new(client.clone(), group);
        let io = local.review49e_path_and_lock(path.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(local)));
        Fixture {
            push,
            client,
            io,
            path,
            q: MessageQueue::from_parts("T", "broker", 0),
        }
    }
    async fn cleanup(mut f: Fixture) {
        f.push.work.close();
        f.client.shutdown().await;
        let _ = std::fs::remove_file(&f.path);
        let _ = std::fs::remove_file(f.path.with_extension("json.bak"));
    }
    async fn installation_race(two: bool) {
        let mut f = fixture(if two { "two-installers" } else { "one-installer" });
        let store = f.push.offset_store.as_mut().unwrap();
        store.update_offset(&f.q, 7, false).await;
        store.persist(&f.q).await;
        let io_guard = f.io.lock().await;
        let mut first = f.push.rebalance_impl.clone();
        let mut second = f.push.rebalance_impl.clone();
        let table = first.rebalance_impl_inner.process_queue_table.clone();
        let desired = HashSet::from([f.q.clone()]);
        let mut installers = Box::pin(async {
            if two {
                let (a, b) = tokio::join!(
                    first
                        .rebalance_impl_inner
                        .update_process_queue_table_in_rebalance("T", &desired, false),
                    second
                        .rebalance_impl_inner
                        .update_process_queue_table_in_rebalance("T", &desired, false)
                );
                (a, Some(b))
            } else {
                (
                    first
                        .rebalance_impl_inner
                        .update_process_queue_table_in_rebalance("T", &desired, false)
                        .await,
                    None,
                )
            }
        });
        tokio::select! { _=&mut installers=>panic!("offset preparation must wait behind the held file lock"), _=tokio::time::sleep(Duration::from_millis(20))=>{} }
        let table_guard = table.write().await;
        drop(io_guard);
        tokio::select! { _=&mut installers=>panic!("publication must wait behind the held table lock"), _=tokio::time::sleep(Duration::from_millis(50))=>{} }
        drop(table_guard);
        let outcomes = tokio::time::timeout(Duration::from_secs(2), &mut installers)
            .await
            .expect("installation did not finish");
        drop(installers);
        let installed = table
            .read()
            .await
            .get(&f.q)
            .cloned()
            .expect("no installation was published");
        let bound = f.push.progress.review49h_bound_to(&f.q).unwrap().upgrade().unwrap();
        let same = Arc::ptr_eq(&installed, &bound);
        let epoch = f.push.progress.current_epoch(&f.q);
        eprintln!(
            "installation publication: two={two} outcomes={outcomes:?} table_matches_binding={same} epoch={epoch:?}"
        );
        cleanup(f).await;
        assert!(
            same,
            "the losing installer replaced the winner's table entry without publishing a binding"
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_installers_must_not_replace_the_published_winner() {
        installation_race(true).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn one_installer_publishes_matching_table_and_binding() {
        installation_race(false).await;
    }

    #[cfg(unix)]
    async fn retirement_writer_window(cancel: bool) {
        use std::io::Write;
        let f = fixture(if cancel { "cancelled-writer" } else { "owned-writer" });
        let old = Arc::new(ProcessQueue::new());
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        table.write().await.insert(f.q.clone(), old.clone());
        f.push.note_queue_installed(&f.q, &old);
        f.push
            .offset_store
            .as_ref()
            .unwrap()
            .update_offset(&f.q, 7, false)
            .await;
        assert!(std::process::Command::new("mkfifo")
            .arg(&f.path)
            .status()
            .unwrap()
            .success());
        // The real blocking writer will wait for EOF here while owning the file-I/O mutex.
        let mut pipe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&f.path)
            .unwrap();
        let mut rebalance = f.push.rebalance_impl.clone();
        let mut retiring = Some(Box::pin(rebalance.remove_process_queue(&f.q, &old)));
        tokio::select! { _=retiring.as_mut().unwrap()=>panic!("retirement must wait for its blocking writer"), _=tokio::time::sleep(Duration::from_millis(30))=>{} }
        let owner_before = transitions.retiring(&f.q);
        let deferred_before = transitions.install_deferred(&f.q);
        if cancel {
            drop(retiring.take());
        }
        let writer_still_owned = f.io.try_lock().is_err();
        let owner_after = transitions.retiring(&f.q);
        let deferred_after = transitions.install_deferred(&f.q);
        let table_missing = !table.read().await.contains_key(&f.q);
        eprintln!(
            "retirement cancellation={cancel}: owner_before={owner_before} deferred_before={deferred_before} \
             writer_still_owned={writer_still_owned} owner_after={owner_after} deferred_after={deferred_after} \
             table_missing={table_missing}"
        );
        // Replace the path before waking the FIFO reader so backup/write use a regular file.
        std::fs::remove_file(&f.path).unwrap();
        let empty = br#"{"offsetTable":{}}"#;
        std::fs::write(&f.path, empty).unwrap();
        pipe.write_all(empty).unwrap();
        drop(pipe);
        let writer_done = tokio::time::timeout(Duration::from_secs(2), f.io.lock()).await.is_ok();
        if let Some(owner) = retiring.as_mut() {
            tokio::time::timeout(Duration::from_secs(2), owner)
                .await
                .expect("retirement bookkeeping did not finish");
        }
        drop(retiring);
        cleanup(f).await;
        assert!(writer_done, "fixture did not release the real blocking writer");
        assert!(
            owner_before && deferred_before && writer_still_owned && table_missing,
            "fixture missed the owned writer window"
        );
        assert!(
            owner_after && deferred_after,
            "cancelling the waiter authorized installation while its writer still owned the file"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn a_cancelled_retirement_must_keep_its_transition_while_the_writer_runs() {
        retirement_writer_window(true).await;
    }
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn an_owned_retirement_keeps_its_transition_until_the_writer_finishes() {
        retirement_writer_window(false).await;
    }

    /// Unblock the fixture's blocking offset writer: a regular file for it to read, then EOF for
    /// the pipe it is parked on. Every test that parks that writer calls this before asserting,
    /// because a panic with a blocking task still waiting on the pipe would hang when its
    /// runtime is dropped.
    #[cfg(unix)]
    fn release_offset_writer(path: &std::path::Path, pipe: std::fs::File) {
        use std::io::Write;
        let empty = br#"{"offsetTable":{}}"#;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::write(path, empty);
        let mut pipe = pipe;
        let _ = pipe.write_all(empty);
        // Closing the write end is what gives the parked reader EOF and lets its write finish.
        drop(pipe);
    }

    /// Cleanup joins a retirement it did not start (plan 49j J2 item 3): closing admission must not
    /// let shutdown pass an operation whose local writer is still running, because that write is
    /// part of the proof this consumer makes before it goes away.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn an_unfinished_retirement_keeps_the_drain_open() {
        let f = fixture("retirement-drain");
        let old = Arc::new(ProcessQueue::new());
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        table.write().await.insert(f.q.clone(), old.clone());
        f.push.note_queue_installed(&f.q, &old);
        f.push
            .offset_store
            .as_ref()
            .unwrap()
            .update_offset(&f.q, 7, false)
            .await;
        assert!(std::process::Command::new("mkfifo")
            .arg(&f.path)
            .status()
            .unwrap()
            .success());
        let pipe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&f.path)
            .unwrap();

        let mut rebalance = f.push.rebalance_impl.clone();
        let mut retiring = Some(Box::pin(rebalance.remove_process_queue(&f.q, &old)));
        let parked = tokio::select! {
            _ = retiring.as_mut().unwrap() => false,
            _ = tokio::time::sleep(Duration::from_millis(30)) => true,
        };
        drop(retiring);

        f.push.work.close();
        let still_running = tokio::time::timeout(Duration::from_millis(50), f.push.work.wait())
            .await
            .is_err();

        release_offset_writer(&f.path, pipe);
        let drained = tokio::time::timeout(Duration::from_secs(2), f.push.work.wait())
            .await
            .is_ok();

        assert!(parked, "retirement must wait for its blocking writer");
        assert!(
            still_running,
            "cleanup passed a retirement whose writer was still running"
        );
        assert!(drained, "the drain stayed open after its retirement finished");
        cleanup(f).await;
    }

    /// Publication adds, it never replaces (plan 49j J1 item 3): the first installer's object stays
    /// in the table, so the acquisition published beside it still names what dispatch will find.
    #[tokio::test(flavor = "current_thread")]
    async fn publication_adds_but_never_replaces() {
        let table: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>> = Arc::new(RwLock::new(HashMap::new()));
        let q = MessageQueue::from_parts("T", "broker", 9);
        let winner = Arc::new(ProcessQueue::new());
        let loser = Arc::new(ProcessQueue::new());

        assert!(
            insert_process_queue_if_absent(&table, &q, &winner).await,
            "an absent queue was not installed"
        );
        assert!(
            !insert_process_queue_if_absent(&table, &q, &loser).await,
            "a losing installer reported an installation it did not make"
        );
        let held = table.read().await.get(&q).cloned().unwrap();
        assert!(
            Arc::ptr_eq(&held, &winner),
            "the table no longer names the object whose acquisition was published"
        );
    }

    /// An installation holds its key while it publishes, not merely while it checks (plan 49j J1
    /// item 2): parked behind the table lock it is still the only pass that may transition that
    /// queue, so no retirement can begin between preparation and the acquisition naming the
    /// published object.
    #[tokio::test(flavor = "current_thread")]
    async fn an_installer_holds_its_key_through_publication() {
        let mut f = fixture("installer-holds-key");
        let store = f.push.offset_store.as_mut().unwrap();
        store.update_offset(&f.q, 7, false).await;
        store.persist(&f.q).await;

        let io_guard = f.io.lock().await;
        let mut installer = f.push.rebalance_impl.clone();
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        let desired = HashSet::from([f.q.clone()]);
        let mut pass = Box::pin(
            installer
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &desired, false),
        );
        // Parked in offset preparation.
        tokio::select! {
            _ = pass.as_mut() => panic!("offset preparation must wait behind the held file lock"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let table_guard = table.write().await;
        drop(io_guard);
        // Parked at publication, with nothing in the table yet.
        tokio::select! {
            _ = pass.as_mut() => panic!("publication must wait behind the held table lock"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        // Reading the table here would need the very lock this pass is waiting on; what the fixture
        // can prove from outside is that the key is still claimed while publication is pending.
        assert!(
            transitions.install_deferred(&f.q),
            "an installation stopped owning its queue before it published"
        );
        drop(table_guard);
        tokio::time::timeout(Duration::from_secs(2), pass)
            .await
            .expect("installation did not finish");

        let installed = table
            .read()
            .await
            .get(&f.q)
            .cloned()
            .expect("no installation was published");
        let bound = f.push.progress.review49h_bound_to(&f.q).unwrap().upgrade().unwrap();
        assert!(
            Arc::ptr_eq(&installed, &bound),
            "the published table entry and acquisition name different objects"
        );
        cleanup(f).await;
    }

    /// The repair path asks for another pass when a key it wanted to retire was busy (plan 49j J1
    /// item 2), exactly as the plain rebalance path does: whoever holds the key is not retiring
    /// this queue, and a removal request that ends in silence stays unretired until something
    /// wakes.
    #[tokio::test(flavor = "current_thread")]
    async fn the_repair_path_asks_for_another_pass_when_a_key_is_busy() {
        let f = fixture("busy-key-repair");
        let installed = Arc::new(ProcessQueue::new());
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        table.write().await.insert(f.q.clone(), installed.clone());
        f.push.note_queue_installed(&f.q, &installed);
        let claim = transitions
            .try_install(&f.q)
            .expect("nothing transitions this queue yet");

        let wakes = f.client.review49j_rebalance_wakes();
        // Nothing is assigned any more: the repair pass wants this queue retired and finds its key
        // held by an installation.
        let mut repair = f.push.rebalance_impl.clone();
        let topic = CheetahString::from_static_str("T");
        tokio::time::timeout(
            Duration::from_secs(2),
            repair
                .rebalance_impl_inner
                .update_message_queue_assignment(&topic, &HashSet::new(), false),
        )
        .await
        .expect("the repair pass did not finish");

        assert!(
            table.read().await.contains_key(&f.q),
            "a repair pass retired a queue whose installation was in flight"
        );
        for _ in 0..80 {
            if f.client.review49j_rebalance_wakes() > wakes {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            f.client.review49j_rebalance_wakes() > wakes,
            "a repair pass that could not retire a busy key reported itself as finished"
        );
        let _ = claim.release();
        cleanup(f).await;
    }

    /// The broker-assigned repair path makes the same two promises as the plain rebalance path
    /// (plan 49j J1 items 2-3): it holds its key from preparation through publication, and what it
    /// publishes adds to the table rather than replacing an entry that appeared meanwhile.
    #[tokio::test(flavor = "current_thread")]
    async fn the_repair_path_holds_its_key_and_adds_but_never_replaces() {
        let mut f = fixture("repair-loses-race");
        let store = f.push.offset_store.as_mut().unwrap();
        store.update_offset(&f.q, 7, false).await;
        store.persist(&f.q).await;

        let io_guard = f.io.lock().await;
        let assignment = MessageQueueAssignment {
            message_queue: Some(f.q.clone()),
            // Anything but Pop is a push assignment (plan 49g's repair path).
            mode: MessageRequestMode::Pull,
            attachments: None,
        };
        let assignments = HashSet::from([assignment]);
        let mut repair = f.push.rebalance_impl.clone();
        let topic = CheetahString::from_static_str("T");
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        let mut pass = Box::pin(repair.rebalance_impl_inner.update_message_queue_assignment(
            &topic,
            &assignments,
            false,
        ));
        // Parked in offset preparation.
        tokio::select! {
            _ = pass.as_mut() => panic!("offset preparation must wait behind the held file lock"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let mut table_guard = table.write().await;
        drop(io_guard);
        // Parked at publication, and still the only pass allowed to transition this key.
        tokio::select! {
            _ = pass.as_mut() => panic!("publication must wait behind the held table lock"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        assert!(
            transitions.install_deferred(&f.q),
            "a repair installation stopped owning its queue before it published"
        );
        // A competing installer finishes while this one waits at publication.
        let winner = Arc::new(ProcessQueue::new());
        f.push.note_queue_installed(&f.q, &winner);
        table_guard.insert(f.q.clone(), winner.clone());
        drop(table_guard);
        tokio::time::timeout(Duration::from_secs(2), pass)
            .await
            .expect("the repair pass did not finish");

        let held = table.read().await.get(&f.q).cloned().expect("the queue vanished");
        assert!(
            Arc::ptr_eq(&held, &winner),
            "a losing repair installation replaced the published winner"
        );
        let bound = f.push.progress.review49h_bound_to(&f.q).unwrap().upgrade().unwrap();
        assert!(
            Arc::ptr_eq(&held, &bound),
            "the table entry and acquisition name different objects"
        );
        cleanup(f).await;
    }

    /// A retirement that could not begin still has to happen (plan 49j J1 item 2). The key was held
    /// by an installation publishing this very queue, so the caller asks for another pass
    /// instead of leaving a dropped queue until some unrelated event wakes rebalance.
    #[tokio::test(flavor = "current_thread")]
    async fn a_refused_retirement_asks_for_another_pass() {
        let f = fixture("refused-retire");
        let installed = Arc::new(ProcessQueue::new());
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        table.write().await.insert(f.q.clone(), installed.clone());
        f.push.note_queue_installed(&f.q, &installed);
        // An installation of this key is in flight; a retirement must not publish over it.
        let claim = transitions
            .try_install(&f.q)
            .expect("nothing transitions this queue yet");

        let wakes = f.client.review49j_rebalance_wakes();
        let mut rebalance = f.push.rebalance_impl.clone();
        tokio::time::timeout(
            Duration::from_millis(200),
            rebalance.remove_process_queue(&f.q, &installed),
        )
        .await
        .expect("a retirement must not wait behind the transition it lost");

        assert!(
            table.read().await.contains_key(&f.q),
            "a refused retirement still removed the queue"
        );
        assert!(!transitions.retiring(&f.q), "the loser claimed a busy key");
        for _ in 0..80 {
            if f.client.review49j_rebalance_wakes() > wakes {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            f.client.review49j_rebalance_wakes() > wakes,
            "a retirement turned away by an in-flight installation never asked for another pass"
        );
        let _ = claim.release();
        cleanup(f).await;
    }

    /// The same promise on the rebalance side (plan 49j J1 item 2): a pass that reaches an
    /// unassigned queue whose key is busy defers that queue and asks to be back, rather than
    /// reporting a clean pass and leaving the queue assigned for another full rebalance
    /// interval.
    #[tokio::test(flavor = "current_thread")]
    async fn a_pass_that_cannot_retire_a_busy_key_asks_for_another_pass() {
        let f = fixture("busy-key-pass");
        let installed = Arc::new(ProcessQueue::new());
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        table.write().await.insert(f.q.clone(), installed.clone());
        f.push.note_queue_installed(&f.q, &installed);
        let claim = transitions
            .try_install(&f.q)
            .expect("nothing transitions this queue yet");

        let wakes = f.client.review49j_rebalance_wakes();
        // Nothing is assigned any more: the pass wants this queue retired and finds its key busy.
        let mut rebalance = f.push.rebalance_impl.clone();
        tokio::time::timeout(
            Duration::from_secs(2),
            rebalance
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &HashSet::new(), false),
        )
        .await
        .expect("the pass did not finish");

        assert!(
            table.read().await.contains_key(&f.q),
            "a pass retired a queue whose installation was in flight"
        );
        for _ in 0..80 {
            if f.client.review49j_rebalance_wakes() > wakes {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            f.client.review49j_rebalance_wakes() > wakes,
            "a pass that could not retire a busy key reported itself as finished"
        );
        let _ = claim.release();
        cleanup(f).await;
    }

    /// A pass that found a queue missing can find it installed by the time it publishes, because
    /// another installer claimed, published and released in between (plan 49j J1 item 3).
    /// Publication must then add nothing: replacing the winner would leave the acquisition
    /// naming an object that no table entry holds.
    #[tokio::test(flavor = "current_thread")]
    async fn an_installer_does_not_replace_a_competing_publication() {
        let mut f = fixture("installer-loses-race");
        let store = f.push.offset_store.as_mut().unwrap();
        store.update_offset(&f.q, 7, false).await;
        store.persist(&f.q).await;

        let io_guard = f.io.lock().await;
        let mut installer = f.push.rebalance_impl.clone();
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let desired = HashSet::from([f.q.clone()]);
        let mut pass = Box::pin(
            installer
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &desired, false),
        );
        tokio::select! {
            _ = pass.as_mut() => panic!("offset preparation must wait behind the held file lock"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let mut table_guard = table.write().await;
        drop(io_guard);
        // The competing installer finishes while this one waits at publication: its object owns the
        // key and its acquisition names that same object.
        let winner = Arc::new(ProcessQueue::new());
        f.push.note_queue_installed(&f.q, &winner);
        table_guard.insert(f.q.clone(), winner.clone());
        drop(table_guard);
        tokio::time::timeout(Duration::from_secs(2), pass)
            .await
            .expect("installation did not finish");

        let held = table.read().await.get(&f.q).cloned().expect("the queue vanished");
        assert!(
            Arc::ptr_eq(&held, &winner),
            "a losing installer replaced the published winner"
        );
        let bound = f.push.progress.review49h_bound_to(&f.q).unwrap().upgrade().unwrap();
        assert!(
            Arc::ptr_eq(&held, &bound),
            "the table entry and acquisition name different objects"
        );
        cleanup(f).await;
    }

    /// Exclusion is per key (plan 49j J1 items 1-2): a queue whose retirement is in flight waits,
    /// every other queue installs, and the turned-away installation is reported so another pass is
    /// scheduled rather than left to whatever wakes rebalance next.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unrelated_queue_installs_while_another_key_retires() {
        let mut f = fixture("unrelated-install");
        let store = f.push.offset_store.as_mut().unwrap();
        store.update_offset(&f.q, 7, false).await;
        store.persist(&f.q).await;

        let retiring_q = f.q.clone();
        let sibling = MessageQueue::from_parts("T", "broker", 1);
        store.update_offset(&sibling, 7, false).await;
        store.persist(&sibling).await;

        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        // A retirement of the first key is in flight: its entry has left the table and its side
        // effects have not finished, which is exactly the window an installation must not publish
        // into. Claiming it here keeps the test on the window rather than on a disk wait.
        let retiring = Arc::new(ProcessQueue::new());
        let guard = transitions
            .try_retire(&retiring_q, &retiring)
            .expect("nothing retires this queue yet");

        let desired = HashSet::from([retiring_q.clone(), sibling.clone()]);
        let mut rebalance = f.push.rebalance_impl.clone();
        tokio::time::timeout(
            Duration::from_secs(2),
            rebalance
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &desired, false),
        )
        .await
        .expect("the installation pass never finished");

        let installed = table
            .read()
            .await
            .get(&sibling)
            .cloned()
            .expect("an unrelated queue was held up by another key's retirement");
        let bound = f
            .push
            .progress
            .review49h_bound_to(&sibling)
            .unwrap()
            .upgrade()
            .expect("the sibling's installation published no acquisition");
        assert!(
            Arc::ptr_eq(&installed, &bound),
            "the sibling's table entry and acquisition name different objects"
        );
        assert!(
            !table.read().await.contains_key(&retiring_q),
            "an installation was published into a retirement of the same key"
        );
        assert!(
            guard.release(),
            "the turned-away installation was not reported for another pass"
        );
        cleanup(f).await;
    }

    /// Cancelling the waiter is not cancelling the retirement (plan 49j J2 items 1-3): while the
    /// operation's own writer still owns the offset file, an installation of that key stays turned
    /// away; once the SDK-owned task has finished its bookkeeping, the next pass installs normally.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn a_cancelled_waiter_keeps_its_key_closed_to_installation() {
        let f = fixture("cancelled-writer-install");
        let old = Arc::new(ProcessQueue::new());
        let table = f.push.rebalance_impl.rebalance_impl_inner.process_queue_table.clone();
        let transitions = f.push.rebalance_impl.rebalance_impl_inner.queue_transitions.clone();
        table.write().await.insert(f.q.clone(), old.clone());
        f.push.note_queue_installed(&f.q, &old);
        f.push
            .offset_store
            .as_ref()
            .unwrap()
            .update_offset(&f.q, 7, false)
            .await;

        // The real offset writer blocks here, holding the file-I/O mutex it must keep to finish.
        assert!(std::process::Command::new("mkfifo")
            .arg(&f.path)
            .status()
            .unwrap()
            .success());
        let pipe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&f.path)
            .unwrap();

        let mut rebalance = f.push.rebalance_impl.clone();
        let mut retiring = Some(Box::pin(rebalance.remove_process_queue(&f.q, &old)));
        let parked = tokio::select! {
            _ = retiring.as_mut().unwrap() => false,
            _ = tokio::time::sleep(Duration::from_millis(30)) => true,
        };
        drop(retiring);

        // The caller is gone; the retirement is not. An installation pass for this key now finds
        // the transition claimed and defers, rather than preparing an offset behind that
        // writer. Bounded, because a pass that prepared instead of deferring would wait on
        // exactly that writer.
        let mut rebalance = f.push.rebalance_impl.clone();
        let desired = HashSet::from([f.q.clone()]);
        let deferred_cleanly = tokio::time::timeout(
            Duration::from_secs(2),
            rebalance
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &desired, false),
        )
        .await
        .is_ok();
        let installed_early = table.read().await.contains_key(&f.q);
        let still_claimed = transitions.retiring(&f.q);

        release_offset_writer(&f.path, pipe);
        let writer = tokio::time::timeout(Duration::from_secs(2), f.io.lock()).await;
        let writer_done = writer.is_ok();
        drop(writer);
        // The SDK-owned task ends its claim only after its own bookkeeping, so waiting for that is
        // waiting for the whole operation.
        for _ in 0..200 {
            if !transitions.retiring(&f.q) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert!(parked, "retirement must wait for its blocking writer");
        assert!(
            deferred_cleanly,
            "an installation waited on the offset file of a retiring queue instead of deferring"
        );
        assert!(
            !installed_early,
            "an installation was authorized while the cancelled retirement's writer owned the file"
        );
        assert!(still_claimed, "the cancelled waiter ended its retirement");
        assert!(writer_done, "fixture did not release the real blocking writer");
        assert!(
            !transitions.retiring(&f.q),
            "the retirement never finished its bookkeeping"
        );

        // With the operation done, the same pass installs the queue and publishes a matching
        // binding.
        let mut rebalance = f.push.rebalance_impl.clone();
        tokio::time::timeout(
            Duration::from_secs(2),
            rebalance
                .rebalance_impl_inner
                .update_process_queue_table_in_rebalance("T", &desired, false),
        )
        .await
        .expect("installation never finished once its retirement ended");
        let installed = table
            .read()
            .await
            .get(&f.q)
            .cloned()
            .expect("the queue stayed uninstalled after its retirement ended");
        let bound = f.push.progress.review49h_bound_to(&f.q).unwrap().upgrade().unwrap();
        assert!(
            Arc::ptr_eq(&installed, &bound),
            "the reinstalled queue's table entry and acquisition name different objects"
        );
        cleanup(f).await;
    }
}
