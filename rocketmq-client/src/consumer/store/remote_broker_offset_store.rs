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
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

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

/// The budget for acquiring the offset snapshot: a pass' own absolute deadline when it has one, and
/// otherwise one finite allowance - never more than a single offset write was ever given - so even
/// the periodic task cannot wait on the table lock indefinitely (plan 49d R6).
fn preparation_deadline(deadline: Option<TokioInstant>) -> TokioInstant {
    match deadline {
        Some(deadline) => deadline,
        None => TokioInstant::now() + std::time::Duration::from_millis(OFFSET_WRITE_TIMEOUT_MILLIS),
    }
}

/// What acquiring the offset snapshot produced (plan 49d R6).
pub(crate) enum OffsetSnapshot {
    /// The assigned offsets this pass may write. Assignments with no recorded offset are absent
    /// from the list, which is a conclusion this pass actually established.
    Ready(Vec<(MessageQueue, i64)>),
    /// The snapshot was not acquired before its budget ended, so this pass cannot tell which of its
    /// assignments have stored offsets. It writes nothing and prunes nothing: the difference
    /// between an offset of zero and no offset at all is exactly what the snapshot could not
    /// establish, and either pruning or reporting an empty success would turn a stall into lost
    /// progress.
    NotAcquired,
}

/// How many offset writes one pass may have in flight at once (plan 49a F10 section 4.6 item 2).
/// Enough that a pass over hundreds of queues is not dominated by sequential connection setup, few
/// enough that one consumer's offset persistence cannot become a burst against the brokers.
pub(crate) const MAX_INFLIGHT_OFFSET_WRITES: usize = 8;

/// What one offset write produced, before it is folded into a [`PersistSummary`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteOutcome {
    /// A broker answered - only a request-reply write can produce this.
    Succeeded,
    Failed,
    /// Handed to the transport with no answer expected. The pass may report that it sent the
    /// offset, and nothing more (plan 49c C5).
    Dispatched,
}

/// Issues items of work with at most `max` of them in flight, and never abandons one that started.
///
/// The caller keeps whatever needs the client's mutable handle - broker resolution, the
/// cancellation probe, the deadline - while an item of work only borrows what it needs, which is
/// what lets several of them overlap without creating a second mutable alias of the same client.
/// [`Self::finish`] waits for everything already admitted: stopping means stopping new work, not
/// interrupting an offset write that is on its way.
struct BoundedInFlight<'a, R> {
    in_flight: FuturesUnordered<BoxFuture<'a, R>>,
    max: usize,
}

impl<'a, R> BoundedInFlight<'a, R> {
    fn new(max: usize) -> Self {
        Self {
            in_flight: FuturesUnordered::new(),
            max,
        }
    }

    #[cfg(test)]
    fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Admit one item of work, first waiting for a slot when the window is full. Whatever landed
    /// while waiting is handed back so the caller accounts for it.
    async fn admit<F>(&mut self, task: F) -> Vec<R>
    where
        F: Future<Output = R> + Send + 'a,
    {
        let mut landed = Vec::new();
        while self.in_flight.len() >= self.max {
            match self.next_result().await {
                Some(outcome) => landed.push(outcome),
                None => break,
            }
        }
        self.in_flight.push(Box::pin(task));
        landed
    }

    /// The next completed result, waiting for one. Draining with this until it returns `None` is
    /// how everything already admitted gets finished and reported; nothing started here is left
    /// undone.
    async fn next_result(&mut self) -> Option<R> {
        self.in_flight.next().await
    }

    /// Whether anything is admitted right now - the guard that lets a caller race result collection
    /// against other work without spinning on an empty window.
    fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }
}

/// One write's outcome folded into the pass summary, counted exactly once.
fn count(summary: &mut PersistSummary, outcome: WriteOutcome) {
    summary.attempted += 1;
    match outcome {
        WriteOutcome::Succeeded => summary.succeeded += 1,
        WriteOutcome::Failed => summary.failed += 1,
        // Dispatched with no answer to believe: counted as attempted and unproven, never as persisted
        // (plan 49c C5).
        WriteOutcome::Dispatched => summary.unproven += 1,
    }
}

impl RemoteBrokerOffsetStore {
    /// One pass over the assigned offsets, shared by the periodic task and cleanup so their
    /// snapshot and write primitive cannot drift apart (plan 49a F10). `deadline` is an absolute
    /// instant bounding the whole remote step of each queue - offset snapshot, broker lookup, route
    /// refresh, in-flight slot admission, send and result handling - and `cancelled` is consulted
    /// before a queue is offered and again once its lookup returns, never during a write, so a
    /// consumer that is stopping stops issuing new writes without waiting for its siblings or
    /// the client's scheduled task, and without abandoning a write already on the wire (plan
    /// 49c C5).
    ///
    /// No step computes its own relative budget from scratch: each one is bounded by the same
    /// instant, so a lookup that waits on the nameserver lock cannot silently restart the clock
    /// and leave the caller's deadline exceeded by however long the wait was.
    ///
    /// Broker resolution stays sequential because the client hands out its route tables through a
    /// mutable handle; the writes themselves only need a shared one, so up to
    /// [`MAX_INFLIGHT_OFFSET_WRITES`] of them overlap (plan 49a F10 section 4.6 item 2). A pass
    /// over two hundred and fifty-six queues used to spend that many sequential round trips on
    /// connection setup alone.
    async fn run_pass(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        deadline: Option<TokioInstant>,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> PersistSummary {
        let mut summary = PersistSummary::default();
        if mqs.is_empty() {
            return summary;
        }
        // Preparation belongs to this pass' budget (plan 49d R6): acquiring the snapshot is bounded
        // by the same absolute instant as the writes, and a periodic pass with no caller
        // deadline gets one finite preparation allowance rather than waiting on the table
        // lock for however long its holder feels like keeping it. Cancellation is consulted
        // before preparation starts, so a consumer that has begun stopping does not queue
        // behind a lock at all.
        if cancelled() {
            summary.skipped += mqs.len() as u64;
            return summary;
        }
        let preparation_deadline = preparation_deadline(deadline);
        // Snapshot only the assigned offsets once; the table lock is never held across I/O.
        let pending = match self.snapshot_assigned(mqs, preparation_deadline).await {
            OffsetSnapshot::Ready(pending) => pending,
            OffsetSnapshot::NotAcquired => {
                // One explanation per pass, and every assignment reported as unwritten work:
                // nothing was sent, and which of them have stored offsets is
                // unknown rather than absent.
                warn!(
                    "[persistAll] offset snapshot not acquired before its deadline, group={}, {} assigned queue(s) \
                     left unwritten",
                    self.group_name,
                    mqs.len()
                );
                summary.skipped += mqs.len() as u64;
                return summary;
            }
        };
        let total = pending.len() as u64;
        // A second shared handle to the same API object: writes borrow it, they never take it
        // mutably, so overlapping them cannot produce two mutable aliases of the client.
        let api = self.client_instance.mq_client_api_impl.as_ref().unwrap().clone();
        let group_name = self.group_name.clone();
        let mut in_flight = BoundedInFlight::<WriteOutcome>::new(MAX_INFLIGHT_OFFSET_WRITES);
        for (index, (mq, offset)) in pending.into_iter().enumerate() {
            let remaining_here = total - index as u64;
            if cancelled() {
                // The consumer is stopping: report the rest instead of writing on its behalf.
                summary.skipped += remaining_here;
                break;
            }
            // One absolute instant bounds this queue's whole remote step. Even with no caller
            // deadline the step cannot run forever: one write timeout is its historical
            // ceiling.
            let attempt_deadline = match deadline {
                Some(deadline) => deadline,
                None => TokioInstant::now() + std::time::Duration::from_millis(OFFSET_WRITE_TIMEOUT_MILLIS),
            };
            if TokioInstant::now() >= attempt_deadline {
                // No budget: report it instead of starting work that would be cut off.
                summary.skipped += remaining_here;
                break;
            }
            // Lookup needs the client's mutable handle, so it stays sequential - but it is bounded
            // by that same instant, and the writes already admitted keep being polled
            // while it waits, because a future nobody polls makes the window slower
            // than it actually is (plan 49c C5).
            let mut resolve = Box::pin(self.resolve_write_addr(&mq, attempt_deadline));
            let addr = loop {
                tokio::select! {
                    biased;
                    resolved = &mut resolve => break resolved,
                    Some(outcome) = in_flight.next_result(), if !in_flight.is_empty() => {
                        count(&mut summary, outcome);
                    }
                }
            };
            // The lookup either answered or hit the same deadline; both are visible here. Expiry is
            // reported as unwritten work rather than a broker failure, because nothing was sent.
            if TokioInstant::now() >= attempt_deadline {
                summary.skipped += remaining_here;
                break;
            }
            let addr = match addr {
                Some(addr) => addr,
                None => {
                    summary.attempted += 1;
                    summary.failed += 1;
                    debug!("[persistAll] broker not found for offset write, {}", mq);
                    continue;
                }
            };
            // Both gates again now that lookup has returned: the consumer may have started stopping
            // and the deadline may have expired while a nameserver lock was held.
            // Dispatching past either would be work nobody asked for, after its own
            // deadline (plan 49c C5).
            if cancelled() {
                summary.skipped += remaining_here;
                break;
            }
            let timeout_millis = offset_rpc_timeout(attempt_deadline.saturating_duration_since(TokioInstant::now()));
            if timeout_millis == 0 {
                summary.skipped += remaining_here;
                break;
            }
            let request_header = UpdateConsumerOffsetRequestHeader {
                consumer_group: group_name.clone(),
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
            // A one-way write is best effort and cannot be confirmed, so dispatching it is the most
            // this pass can claim - reported as unproven, never as persisted. Its bound
            // is the absolute end of this queue's step, not a budget measured before
            // lookup started.
            let write_deadline = TokioInstant::now() + std::time::Duration::from_millis(timeout_millis);
            let api = &api;
            let write = async move {
                let step = api.update_consumer_offset_oneway(addr.as_str(), request_header, timeout_millis);
                match tokio::time::timeout_at(write_deadline, step).await {
                    Ok(Ok(())) => WriteOutcome::Dispatched,
                    Ok(Err(e)) => {
                        debug!("[persistAll] updateConsumeOffsetToBroker failed, {}", e);
                        WriteOutcome::Failed
                    }
                    Err(_) => {
                        debug!("[persistAll] offset write cut off by its deadline, {}", addr);
                        WriteOutcome::Failed
                    }
                }
            };
            for outcome in in_flight.admit(write).await {
                count(&mut summary, outcome);
            }
        }
        // Everything already admitted is finished and accounted for, however the loop ended.
        while let Some(outcome) = in_flight.next_result().await {
            count(&mut summary, outcome);
        }
        self.report_pass(&summary);
        summary
    }

    /// Where a queue's offset goes: the master broker from the shared route tables, refreshed once
    /// on a miss exactly as before. `None` means this pass cannot address the write.
    ///
    /// The lookup waits on the client's route lock and may fetch a route from the nameserver, so it
    /// is bounded by the deadline of the step that needs it: an unreachable nameserver must not
    /// extend a pass whose budget has already run out (plan 49c C5).
    async fn resolve_write_addr(&mut self, mq: &MessageQueue, deadline: TokioInstant) -> Option<CheetahString> {
        tokio::time::timeout_at(deadline, self.lookup_write_addr(mq))
            .await
            .unwrap_or_default()
    }

    async fn lookup_write_addr(&mut self, mq: &MessageQueue) -> Option<CheetahString> {
        let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
        let mut found = self
            .client_instance
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
            .await;
        if found.is_none() {
            self.client_instance
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
            found = self
                .client_instance
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
                .await;
        }
        found.map(|found| CheetahString::from_string(found.broker_addr.to_string()))
    }

    fn report_pass(&self, summary: &PersistSummary) {
        if summary.attempted == 0 && summary.skipped == 0 {
            return;
        }
        debug!(
            "[persistAll] Group: {} ClientId: {} offset pass {}",
            self.group_name, self.client_instance.client_id, summary
        );
        if let Some(suppressed) = self
            .client_instance
            .log_gate()
            .try_acquire(&format!("offset-persist:{}", self.group_name))
        {
            info!(
                "[persistAll] Group: {} ClientId: {} persisted assigned offsets ({}), suppressed {} identical \
                 summaries",
                self.group_name, self.client_instance.client_id, summary, suppressed
            );
        }
    }

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
    /// Read the assigned offsets, and drop the entries no longer assigned. The table lock is taken
    /// inside `preparation_deadline` (plan 49d R6): a holder cannot make this pass outlive its own
    /// budget, and a pass that loses that race says so instead of writing on stale information or
    /// pruning what it never looked at.
    async fn snapshot_assigned(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        preparation_deadline: TokioInstant,
    ) -> OffsetSnapshot {
        let mut unused_mq = HashSet::new();
        let mut used_mq = Vec::new();
        if mqs.is_empty() {
            return OffsetSnapshot::Ready(used_mq);
        }
        // Polling the lock first means an acquisition that succeeds right at the deadline still
        // counts as a success; only a wait that outlives the budget is reported as not
        // acquired.
        let offset_table = tokio::select! {
            biased;
            guard = self.offset_table.lock() => guard,
            _ = tokio::time::sleep_until(preparation_deadline) => return OffsetSnapshot::NotAcquired,
        };
        let mut offset_table = offset_table;
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
        OffsetSnapshot::Ready(used_mq)
    }

    /// Test seam (plan 49d R6): the handle for the offset table lock, so a test can hold it and
    /// make a pass wait for preparation exactly as it would behind another writer. Never
    /// compiled into anything that ships.
    #[cfg(test)]
    pub(crate) fn offset_table_lock(&self) -> Arc<Mutex<HashMap<MessageQueue, ControllableOffset>>> {
        self.offset_table.clone()
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

    async fn persist_all_until(&mut self, mqs: &HashSet<MessageQueue>, deadline: TokioInstant) -> PersistSummary {
        self.run_pass(mqs, Some(deadline), &|| false).await
    }

    async fn persist_all(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> PersistSummary {
        self.run_pass(mqs, None, cancelled).await
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
        debug!(
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

    /// Plan 49a F10 section 4.6 item 2: a pass may overlap its writes, but only up to the window.
    /// Sequential dispatch made a pass over hundreds of queues pay one round trip of connection
    /// setup per queue; an unbounded fan-out would turn one consumer's persistence into a
    /// burst.
    #[tokio::test(flavor = "current_thread")]
    async fn at_most_eight_offset_writes_are_issued_at_once() {
        let current = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut in_flight = BoundedInFlight::<WriteOutcome>::new(MAX_INFLIGHT_OFFSET_WRITES);
        let mut summary = PersistSummary::default();
        for _ in 0..40 {
            let (current, peak) = (current.clone(), peak.clone());
            for outcome in in_flight
                .admit(async move {
                    let now = current.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    current.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    WriteOutcome::Succeeded
                })
                .await
            {
                count(&mut summary, outcome);
            }
        }
        while let Some(outcome) = in_flight.next_result().await {
            count(&mut summary, outcome);
        }
        let peak = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert!(peak <= MAX_INFLIGHT_OFFSET_WRITES, "window exceeded: {peak}");
        assert!(peak > 1, "writes never overlapped at all: {peak}");
        assert_eq!(40, summary.attempted, "{summary}");
        assert_eq!(40, summary.succeeded, "{summary}");
    }

    /// Stopping is a decision about work not yet started. Everything already admitted has to finish
    /// and be accounted for, or an offset write disappears between the consumer stopping and
    /// the pass ending.
    #[tokio::test(flavor = "current_thread")]
    async fn everything_admitted_is_finished_after_the_caller_stops_early() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let finished = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut in_flight = BoundedInFlight::<WriteOutcome>::new(MAX_INFLIGHT_OFFSET_WRITES);
        let mut summary = PersistSummary::default();
        // The caller stops offering queues part-way, exactly as a stopping consumer does.
        for index in 0..24 {
            if index >= 10 {
                break;
            }
            let (started, finished) = (started.clone(), finished.clone());
            for outcome in in_flight
                .admit(async move {
                    started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    finished.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    WriteOutcome::Succeeded
                })
                .await
            {
                count(&mut summary, outcome);
            }
        }
        while let Some(outcome) = in_flight.next_result().await {
            count(&mut summary, outcome);
        }
        assert_eq!(10, started.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            started.load(std::sync::atomic::Ordering::SeqCst),
            finished.load(std::sync::atomic::Ordering::SeqCst),
            "an admitted write was left unfinished"
        );
        assert_eq!(10, summary.attempted, "{summary}");
        assert_eq!(10, summary.succeeded, "{summary}");
    }

    /// Plan 49c C5: broker lookup used to sit outside the pass budget, so a consumer whose route
    /// lookup was waiting on the nameserver lock never returned - a ten millisecond deadline
    /// kept running past an independent eighty millisecond timeout. Lookup is now bounded by
    /// the same absolute instant as the write it feeds: the step ends when its deadline says so
    /// and reports what it did not send.
    #[tokio::test(flavor = "current_thread")]
    async fn a_waiting_lookup_cannot_outrun_the_persistence_deadline() {
        let instance = dead_instance("lookup-deadline-test");
        let mut store = RemoteBrokerOffsetStore::new(instance.clone(), CheetahString::from("deadline-group"));
        let assigned = queues("StuckLookupTopic", 4);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }

        // Hold the lock every route refresh takes, as a stalled nameserver would.
        let handle = instance.nameserver_lock();
        let _held = handle.lock().await;

        let deadline = TokioInstant::now() + Duration::from_millis(10);
        let summary = tokio::time::timeout(Duration::from_millis(80), store.persist_all_until(&assigned, deadline))
            .await
            .expect("the pass ends on its own deadline instead of waiting on the lookup");

        assert_eq!(0, summary.attempted, "nothing was dispatched: {summary}");
        assert_eq!(4, summary.skipped, "{summary}");
        assert_eq!(
            0, summary.unproven,
            "and nothing claims a write it never sent: {summary}"
        );
    }

    /// Plan 49a F10: periodic persistence belongs to its own consumer. Once that consumer is
    /// stopping, no further remote opportunity is taken on its behalf - the pass may not wait
    /// for a sibling or for the client's scheduled task to stop.
    #[tokio::test(flavor = "current_thread")]
    async fn a_stopping_consumer_takes_no_further_offset_writes() {
        let mut store =
            RemoteBrokerOffsetStore::new(dead_instance("cancel-offset-test"), CheetahString::from("cancel-group"));
        let assigned = queues("CancelTopic", 8);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }

        let started = TokioInstant::now();
        let summary = store.persist_all(&assigned, &|| true).await;
        assert_eq!(0, summary.attempted, "a stopping consumer may not start a write");
        assert_eq!(8, summary.skipped, "{summary}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the pass must stop at once, not after its queues: {:?}",
            started.elapsed()
        );
    }

    /// Cancellation is observed per queue rather than once per pass, so the offsets already written
    /// stay accounted for and the remainder is reported instead of silently dropped.
    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_stops_the_periodic_pass_between_queues() {
        let mut store = RemoteBrokerOffsetStore::new(
            dead_instance("cancel-between-test"),
            CheetahString::from("between-group"),
        );
        let assigned = queues("BetweenTopic", 6);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }

        // Stop after the first queue has been offered: exactly one write is taken. The probe is
        // asked once before preparation (plan 49d R6) and then again for each queue before
        // it is offered, so this lands ahead of the second queue rather than on the way in.
        let checks = std::sync::atomic::AtomicUsize::new(0);
        let summary = store
            .persist_all(&assigned, &|| {
                checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 2
            })
            .await;
        assert_eq!(1, summary.attempted, "{summary}");
        assert_eq!(5, summary.skipped, "{summary}");
        assert_eq!(summary.attempted, summary.succeeded + summary.failed, "{summary}");
        assert_eq!(6, summary.attempted + summary.skipped, "{summary}");

        // Without cancellation the same store walks every assigned queue.
        let full = store.persist_all(&assigned, &|| false).await;
        assert_eq!(6, full.attempted, "{full}");
        assert_eq!(0, full.skipped, "{full}");
    }

    /// Plan 49d R6: preparation belongs to the pass' budget. Taking the offset-table snapshot used
    /// to be an unbounded await outside it, so a holder of that lock decided how long a
    /// persistence pass ran - past its own deadline for a cleanup session, and indefinitely for
    /// the periodic task.
    #[tokio::test(flavor = "current_thread")]
    async fn a_held_snapshot_lock_ends_the_pass_on_its_own_deadline() {
        let mut store = RemoteBrokerOffsetStore::new(
            dead_instance("snapshot-contention-test"),
            CheetahString::from("snapshot-group"),
        );
        let assigned = queues("ContendedTopic", 3);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }

        // Hold the table lock, and give the pass more budget than it needs to prepare.
        let handle = store.offset_table_lock();
        let held = handle.lock().await;
        let deadline = TokioInstant::now() + Duration::from_millis(40);
        let started = TokioInstant::now();
        let summary = tokio::time::timeout(Duration::from_secs(5), store.persist_all_until(&assigned, deadline))
            .await
            .expect("the pass ended on its own deadline instead of waiting for the snapshot lock");
        let elapsed = started.elapsed();

        assert_eq!(0, summary.attempted, "preparation never reached a write: {summary}");
        assert_eq!(3, summary.skipped, "{summary}");
        assert_eq!(0, summary.succeeded, "{summary}");
        assert!(
            elapsed >= Duration::from_millis(30),
            "the pass did not use the budget it was given: {elapsed:?}"
        );
        assert!(
            elapsed <= Duration::from_millis(2_000),
            "a lock holder decided how long this pass ran: {elapsed:?}",
        );

        // And nothing was pruned or forgotten while the snapshot could not be read: with the lock
        // free again, every assignment is still there to write.
        drop(held);
        let after = store
            .persist_all_until(&assigned, TokioInstant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(3, after.attempted, "{after}");
        assert_eq!(0, after.skipped, "{after}");
    }

    /// A consumer that has begun stopping may not queue behind the snapshot lock either:
    /// cancellation is consulted before preparation, so it reports its assignments without
    /// touching the table.
    #[tokio::test(flavor = "current_thread")]
    async fn a_stopping_consumer_does_not_wait_to_prepare() {
        let mut store = RemoteBrokerOffsetStore::new(
            dead_instance("cancel-prepare-test"),
            CheetahString::from("cancel-prepare-group"),
        );
        let assigned = queues("CancelPrepareTopic", 4);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }
        let handle = store.offset_table_lock();
        let _held = handle.lock().await;

        // Cancellation ahead of preparation is the only way this pass ends while the lock is held
        // and its deadline is a minute away.
        let started = TokioInstant::now();
        let summary = tokio::time::timeout(Duration::from_secs(5), store.persist_all(&assigned, &|| true))
            .await
            .expect("a stopping consumer waited on the snapshot lock");
        assert_eq!(0, summary.attempted, "{summary}");
        assert_eq!(4, summary.skipped, "{summary}");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "a stopping consumer queued behind a lock: {:?}",
            started.elapsed()
        );
    }

    /// Nothing assigned means nothing to prepare, and no lock is needed at all.
    #[tokio::test(flavor = "current_thread")]
    async fn an_empty_assignment_prepares_nothing() {
        let mut store = RemoteBrokerOffsetStore::new(
            dead_instance("empty-assign-test"),
            CheetahString::from("empty-assign-group"),
        );
        let handle = store.offset_table_lock();
        let _held = handle.lock().await;
        let summary = tokio::time::timeout(
            Duration::from_millis(200),
            store.persist_all_until(&HashSet::new(), TokioInstant::now() + Duration::from_secs(60)),
        )
        .await
        .expect("an empty pass must not wait for the table lock");
        assert_eq!(0, summary.attempted + summary.skipped, "{summary}");
    }

    /// An uncontended snapshot taken close to the deadline still counts as preparation that
    /// succeeded: the writes are issued, and only what the budget could not reach is reported
    /// unwritten.
    #[tokio::test(flavor = "current_thread")]
    async fn a_snapshot_acquired_before_the_deadline_still_writes() {
        let mut store = RemoteBrokerOffsetStore::new(
            dead_instance("near-deadline-test"),
            CheetahString::from("near-deadline-group"),
        );
        let assigned = queues("NearDeadlineTopic", 2);
        for (id, mq) in assigned.iter().enumerate() {
            store.update_offset(mq, id as i64 + 1, false).await;
        }
        let summary = store
            .persist_all_until(&assigned, TokioInstant::now() + Duration::from_millis(500))
            .await;
        assert_eq!(
            2, summary.attempted,
            "preparation succeeded, so both were offered: {summary}"
        );
        assert_eq!(0, summary.skipped, "{summary}");
    }

    /// A periodic pass has no caller deadline, so preparation gets one finite allowance rather than
    /// an unbounded wait - and never more than a single write was ever given.
    #[test]
    fn a_pass_without_a_deadline_still_prepares_inside_a_budget() {
        let budget = std::time::Duration::from_millis(OFFSET_WRITE_TIMEOUT_MILLIS);
        let before = TokioInstant::now();
        let deadline = preparation_deadline(None);
        let after = TokioInstant::now();
        assert!(
            deadline >= before + budget && deadline <= after + budget,
            "preparation allowance drifted off the offset timeout: {deadline:?}"
        );

        // A supplied absolute deadline is used as it stands: preparation may not extend a pass that
        // was given one, and cannot shorten it either.
        let absolute = TokioInstant::now() + Duration::from_millis(25);
        assert_eq!(preparation_deadline(Some(absolute)), absolute);
    }
}
