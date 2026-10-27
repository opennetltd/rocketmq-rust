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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::store::controllable_offset::ControllableOffset;

use tokio::time::Instant as TokioInstant;

use crate::consumer::store::offset_store::offset_rpc_timeout;
use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::offset_store::PersistSummary;
use crate::consumer::store::offset_store::OFFSET_WRITE_TIMEOUT_MILLIS;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

pub struct RemoteBrokerOffsetStore {
    client_instance: ArcMut<MQClientInstance>,
    group_name: CheetahString,
    offset_table: Arc<Mutex<HashMap<MessageQueue, ControllableOffset>>>,
}

impl RemoteBrokerOffsetStore {
    pub fn new(client_instance: ArcMut<MQClientInstance>, group_name: CheetahString) -> Self {
        Self {
            client_instance,
            group_name,
            offset_table: Arc::new(Mutex::new(HashMap::with_capacity(64))),
        }
    }

    async fn fetch_consume_offset_from_broker(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
        let mut find_broker_result = self
            .client_instance
            .mut_from_ref()
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
            .await;

        if find_broker_result.is_none() {
            self.client_instance
                .mut_from_ref()
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
            find_broker_result = self
                .client_instance
                .mut_from_ref()
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
                .await;
        }
        if let Some(find_broker_result) = find_broker_result {
            let request_header = QueryConsumerOffsetRequestHeader {
                consumer_group: self.group_name.clone(),
                topic: CheetahString::from_string(mq.get_topic().to_string()),
                queue_id: mq.get_queue_id(),
                set_zero_if_not_found: None,
                topic_request_header: Some(TopicRequestHeader {
                    lo: None,
                    rpc: Some(RpcRequestHeader {
                        namespace: None,
                        namespaced: None,
                        broker_name: Some(CheetahString::from_string(mq.get_broker_name().to_string())),
                        oneway: None,
                    }),
                }),
            };
            self.client_instance
                .mut_from_ref()
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .query_consumer_offset(find_broker_result.broker_addr.as_str(), request_header, 5_000)
                .await
        } else {
            Err(mq_client_err!(format!("broker not found, {}", mq.get_broker_name())))
        }
    }

    /// Assigned offsets plus the unused entries pruned from the table. Kept out of the send loop so
    /// a stalled broker cannot hold the table lock.
    async fn snapshot_assigned(&mut self, mqs: &HashSet<MessageQueue>) -> Vec<(MessageQueue, i64)> {
        let mut unused_mq = HashSet::new();
        let mut used_mq = Vec::new();
        if mqs.is_empty() {
            return used_mq;
        }
        let mut offset_table = self.offset_table.lock().await;
        for (mq, offset) in offset_table.iter_mut() {
            if mqs.contains(mq) {
                let offset = offset.get_offset();
                used_mq.push((mq.clone(), offset));
            } else {
                unused_mq.insert(mq.clone());
            }
        }
        for mq in unused_mq {
            offset_table.remove(&mq);
            info!("remove unused mq, {}, {}", mq, self.group_name);
        }
        used_mq
    }

    /// One offset write including its broker lookup and any route refresh. `timeout_millis` bounds
    /// the send only; a cleanup pass bounds the whole step from the outside so lookup and refresh
    /// cannot silently outlive the caller's deadline.
    async fn write_offset(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
        let mut find_broker_result = self
            .client_instance
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
            .await;

        if find_broker_result.is_none() {
            self.client_instance
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
            find_broker_result = self
                .client_instance
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
                .await;
        }

        if let Some(find_broker_result) = find_broker_result {
            let request_header = UpdateConsumerOffsetRequestHeader {
                consumer_group: self.group_name.clone(),
                topic: mq.get_topic_cs().clone(),
                queue_id: mq.get_queue_id(),
                commit_offset: offset,
                topic_request_header: Some(TopicRequestHeader {
                    lo: None,
                    rpc: Some(RpcRequestHeader {
                        namespace: None,
                        namespaced: None,
                        broker_name: Some(CheetahString::from_string(mq.get_broker_name().to_string())),
                        oneway: None,
                    }),
                }),
            };
            if is_oneway {
                self.client_instance
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .update_consumer_offset_oneway(
                        find_broker_result.broker_addr.as_str(),
                        request_header,
                        timeout_millis,
                    )
                    .await?;
            } else {
                self.client_instance
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .update_consumer_offset(&find_broker_result.broker_addr, request_header, timeout_millis)
                    .await?;
            };
            Ok(())
        } else {
            Err(mq_client_err!(format!("broker not found, {}", mq.get_broker_name())))
        }
    }
}

impl OffsetStoreTrait for RemoteBrokerOffsetStore {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        let mut offset_table = self.offset_table.lock().await;
        let offset_old = offset_table
            .entry(mq.clone())
            .or_insert_with(|| ControllableOffset::new(offset));
        if increase_only {
            offset_old.update(offset, true);
        } else {
            offset_old.update_unconditionally(offset);
        }
    }

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table
            .entry(mq.clone())
            .or_insert_with(|| ControllableOffset::new(offset))
            .update_and_freeze(offset);
    }

    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        match type_ {
            ReadOffsetType::ReadFromMemory | ReadOffsetType::MemoryFirstThenStore => {
                let offset_table = self.offset_table.lock().await;
                if let Some(offset) = offset_table.get(mq) {
                    return offset.get_offset();
                } else if type_ == ReadOffsetType::ReadFromMemory {
                    return -1;
                }
            }
            ReadOffsetType::ReadFromStore => {
                return match self.fetch_consume_offset_from_broker(mq).await {
                    Ok(value) => {
                        self.update_offset(mq, value, false).await;
                        value
                    }
                    Err(e) => match e {
                        rocketmq_error::RocketMQError::BrokerOperationFailed { code, .. }
                            if code == rocketmq_remoting::code::response_code::ResponseCode::QueryNotFound as i32 =>
                        {
                            -1
                        }
                        _ => {
                            warn!("fetchConsumeOffsetFromBroker exception: {:?}", mq);
                            -2
                        }
                    },
                };
            }
        };
        -3
    }

    /// Writes assigned offsets one at a time. Sequential on purpose: the client handle is borrowed
    /// mutably per write, so there is never more than one in-flight write (and therefore never more
    /// than the eight the cleanup budget allows) nor two mutable aliases of the same client.
    async fn persist_all_until(&mut self, mqs: &HashSet<MessageQueue>, deadline: TokioInstant) -> PersistSummary {
        let mut summary = PersistSummary::default();
        // Snapshot only the assigned offsets once; the table lock is never held across I/O.
        let pending = self.snapshot_assigned(mqs).await;
        for (mq, offset) in pending {
            let remaining = deadline.saturating_duration_since(TokioInstant::now());
            let timeout = offset_rpc_timeout(remaining);
            if timeout == 0 {
                // No budget: report it instead of starting a write that would be cut off.
                summary.skipped += 1;
                continue;
            }
            summary.attempted += 1;
            // A one-way write is best effort and cannot be confirmed, so finishing its dispatch is
            // the most this pass can claim.
            let step = self.write_offset(&mq, offset, true, timeout);
            match tokio::time::timeout(remaining, step).await {
                Ok(Ok(())) => summary.succeeded += 1,
                Ok(Err(e)) => {
                    debug!("[persistAll] updateConsumeOffsetToBroker failed, {},{}", mq, e);
                    summary.failed += 1;
                }
                Err(_) => {
                    debug!("[persistAll] offset write cut off by the cleanup deadline, {}", mq);
                    summary.failed += 1;
                }
            }
        }
        summary
    }

    async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>) {
        if mqs.is_empty() {
            return;
        }
        let used_mq = self.snapshot_assigned(mqs).await;
        for (mq, offset) in used_mq {
            match self.update_consume_offset_to_broker(&mq, offset, true).await {
                Ok(_) => {
                    info!(
                        "[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                        self.group_name, self.client_instance.client_id, mq, offset
                    );
                }
                Err(e) => {
                    error!("updateConsumeOffsetToBroker exception, {},{}", mq, e);
                }
            }
        }
    }

    async fn persist(&mut self, mq: &MessageQueue) {
        let offset_table = self.offset_table.lock().await;
        let offset = offset_table.get(mq);
        if offset.is_none() {
            return;
        }
        let offset = offset.unwrap().get_offset();
        drop(offset_table);
        match self.update_consume_offset_to_broker(mq, offset, true).await {
            Ok(_) => {
                info!(
                    "[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                    self.group_name, self.client_instance.client_id, mq, offset
                );
            }
            Err(e) => {
                error!("updateConsumeOffsetToBroker exception, {},{}", mq, e);
            }
        }
    }

    async fn remove_offset(&self, mq: &MessageQueue) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table.remove(mq);
        info!(
            "remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}",
            mq,
            self.group_name,
            offset_table.len()
        );
    }

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        let offset_table = self.offset_table.lock().await;
        offset_table
            .iter()
            .filter(|(mq, _)| mq.get_topic() == topic)
            .map(|(mq, offset)| (mq.clone(), offset.get_offset()))
            .collect()
    }

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.write_offset(mq, offset, is_oneway, OFFSET_WRITE_TIMEOUT_MILLIS)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::base::client_config::ClientConfig;

    use super::*;

    /// A NameServer nothing listens on: every write has to discover that the broker is unknown and
    /// fail, which is the worst case the cleanup budget exists to bound.
    const DEAD_NAMESERVER: &str = "127.0.0.1:1";

    fn dead_instance(client_id: &str) -> ArcMut<MQClientInstance> {
        let mut client_config = ClientConfig::default();
        client_config.set_namesrv_addr(CheetahString::from_static_str(DEAD_NAMESERVER));
        MQClientInstance::new_arc(client_config, 0, client_id, None)
    }

    fn queues(topic: &str, count: i32) -> HashSet<MessageQueue> {
        (0..count)
            .map(|id| MessageQueue::from_parts(topic, "broker-a", id))
            .collect()
    }

    /// Plan 49 section 5 at scale: two hundred fifty-six assigned offset queues are written one at
    /// a time under one deadline. Without budget nothing is attempted and the whole shortfall
    /// is reported; with budget the phase ends on time and every queue is accounted for exactly
    /// once.
    #[tokio::test(flavor = "current_thread")]
    async fn two_hundred_fifty_six_offset_writes_stay_inside_the_cleanup_budget() {
        let mut store =
            RemoteBrokerOffsetStore::new(dead_instance("bulk-offset-test"), CheetahString::from("bulk-group"));
        let assigned = queues("BulkTopic", 256);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }

        // No budget: not one remote opportunity is taken, including the broker lookup.
        let summary = store.persist_all_until(&assigned, TokioInstant::now()).await;
        assert_eq!(0, summary.attempted, "a spent budget may not start a write");
        assert_eq!(256, summary.skipped);

        // A real budget bounds the whole phase however slow each attempt is.
        let budget = Duration::from_secs(3);
        let started = TokioInstant::now();
        let summary = store.persist_all_until(&assigned, TokioInstant::now() + budget).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed <= budget + Duration::from_secs(2),
            "bulk offset phase overran: {elapsed:?}"
        );
        assert_eq!(256, summary.attempted + summary.skipped, "{summary}");
        assert_eq!(summary.attempted, summary.succeeded + summary.failed, "{summary}");
        assert!(
            summary.attempted > 0,
            "a budget should reach the first queue: {summary}"
        );
    }
}
