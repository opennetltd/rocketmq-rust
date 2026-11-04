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

use tokio::time::Instant as TokioInstant;

use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::local_file_offset_store::LocalOffsetProofReceipt;
use crate::consumer::store::local_file_offset_store::LocalOffsetRetirement;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;

/// Outcome of one deadline-bounded offset persistence pass (plan 49 section 4.3). Reported as a
/// bounded summary; individual queue failures stay at DEBUG so cleanup cannot amplify logs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PersistSummary {
    pub attempted: u64,
    /// Writes the broker answered. A one-way dispatch is never counted here, because it has no
    /// answer to give (plan 49c C5).
    pub succeeded: u64,
    pub failed: u64,
    /// Assigned offsets left unwritten because the deadline, a stopping consumer, or a snapshot
    /// that could not be acquired in time came first. These were never sent. When the snapshot
    /// itself lost its budget, every assigned queue is counted here: such a pass cannot
    /// distinguish an assignment with no recorded offset from one it never got to read (plan
    /// 49d R6).
    pub skipped: u64,
    /// Writes handed to the transport without a broker acknowledgement. They left this process,
    /// which is not the same statement as "the broker stored this offset", so they are kept
    /// apart from `succeeded` rather than reported as durable persistence (plan 49c C5).
    pub unproven: u64,
}

impl std::fmt::Display for PersistSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "attempted={} succeeded={} failed={} skipped={} unproven={}",
            self.attempted, self.succeeded, self.failed, self.skipped, self.unproven
        )
    }
}

impl PersistSummary {
    /// Fold another partial result into this one. A pass is reported as a whole even though its
    /// resolution phase and its concurrent write phase are counted separately.
    pub fn merge(&mut self, other: &PersistSummary) {
        self.attempted += other.attempted;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.skipped += other.skipped;
        self.unproven += other.unproven;
    }
}

/// Per-write RPC timeout inside a bounded pass: never more than five seconds and never more than
/// what the caller's deadline still allows. Zero means there is no budget left to spend.
pub(crate) fn offset_rpc_timeout(remaining: std::time::Duration) -> u64 {
    let remaining = remaining.as_millis().min(u64::MAX as u128) as u64;
    remaining.min(OFFSET_WRITE_TIMEOUT_MILLIS)
}

/// One remote offset write is a single best-effort RPC; five seconds is the historical ceiling.
pub(crate) const OFFSET_WRITE_TIMEOUT_MILLIS: u64 = 5_000;

/// What one queue's offset write proved before a retirement was allowed to forget it (plan 49k K2
/// item 1). The void `persist` API stays for callers that only schedule a write; retiring a queue
/// is not one of them, because erasing an offset whose final write failed destroys the only copy
/// this SDK could still retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetWrite {
    /// A local file holds this queue's offset and its writer finished.
    Proven,
    /// The write failed or its I/O cannot be trusted. The memory copy is untouched and this queue
    /// is now carried by every later whole-table write, so a healed filesystem retries it even
    /// when no assignment names it any more (plan 49k K2 item 2).
    Deferred,
    /// No local file backs this store, or this queue has neither a memory offset nor required
    /// local debt. Broker-only stores keep their existing bounded best-effort policy.
    NoLocalState,
}

pub struct OffsetStore {
    remote_broker_offset_store: Option<RemoteBrokerOffsetStore>,
    local_file_offset_store: Option<LocalFileOffsetStore>,
}

impl OffsetStore {
    pub fn new(
        remote_broker_offset_store: Option<RemoteBrokerOffsetStore>,
        local_file_offset_store: Option<LocalFileOffsetStore>,
    ) -> Self {
        Self {
            remote_broker_offset_store,
            local_file_offset_store,
        }
    }

    pub fn new_with_remote(remote_broker_offset_store: RemoteBrokerOffsetStore) -> Self {
        Self {
            remote_broker_offset_store: Some(remote_broker_offset_store),
            local_file_offset_store: None,
        }
    }

    pub fn new_with_local(local_file_offset_store: LocalFileOffsetStore) -> Self {
        Self {
            remote_broker_offset_store: None,
            local_file_offset_store: Some(local_file_offset_store),
        }
    }

    /// Whether this store model keeps offsets in a local file. Cleanup persists that half
    /// **first**, because its writer owns its guard until the write finishes and must not be
    /// charged against the remote budget (plan 49e E5).
    pub(crate) fn has_local(&self) -> bool {
        self.local_file_offset_store.is_some()
    }

    /// Whether this store model writes offsets to brokers.
    pub(crate) fn has_remote(&self) -> bool {
        self.remote_broker_offset_store.is_some()
    }

    /// The local half of cleanup persistence, with no deadline: the whole-table file writer owns
    /// its guard for the entire write and is never cancelled mid-write (plan 49a F10), so this
    /// call simply waits for that writer. `None` when this store model has no local file.
    pub(crate) async fn persist_local(&mut self, mqs: &HashSet<MessageQueue>) -> Option<PersistSummary> {
        let store = self.local_file_offset_store.as_mut()?;
        Some(store.persist_all(mqs, &|| false).await)
    }

    /// The remote half of cleanup persistence, bounded by `deadline`. The returned summary is the
    /// cleanup session's evidence: a short budget leaves offsets recorded as skipped, never
    /// silently lost. `None` when this store model has no broker writes.
    pub(crate) async fn persist_remote_until(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        deadline: TokioInstant,
    ) -> Option<PersistSummary> {
        let store = self.remote_broker_offset_store.as_mut()?;
        Some(store.persist_all_until(mqs, deadline).await)
    }

    /// Whether this store's local half may be called durable: its own writer has finished **and**
    /// no offset it is required to persist is still unwritten (plan 49l L2 item 4).
    ///
    /// Both halves matter. `false` from the writer means an unfinished write is still owned here,
    /// which is what makes it this phase's work rather than someone else's; outstanding debt means
    /// a past attempt failed, and a successful write of some *other* queue says nothing about
    /// the key that failed. Cleanup may not finalize on either one alone.
    pub(crate) async fn local_writer_finished(&self) -> bool {
        match &self.local_file_offset_store {
            Some(store) => store.io_complete().await && !store.has_required_debt().await,
            None => true,
        }
    }

    pub(crate) async fn io_complete(&self) -> bool {
        match &self.local_file_offset_store {
            Some(store) => store.io_complete().await,
            None => true,
        }
    }

    pub async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        if let Some(store) = &self.remote_broker_offset_store {
            store.load().await?;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.load().await?;
        }
        Ok(())
    }

    pub async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        if let Some(store) = &self.remote_broker_offset_store {
            store.update_offset(mq, offset, increase_only).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.update_offset(mq, offset, increase_only).await;
        }
    }

    pub async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        if let Some(store) = &self.remote_broker_offset_store {
            store.update_and_freeze_offset(mq, offset).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.update_and_freeze_offset(mq, offset).await;
        }
    }
    pub async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        if let Some(ref store) = self.remote_broker_offset_store {
            return store.read_offset(mq, type_).await;
        }

        if let Some(ref store) = self.local_file_offset_store {
            return store.read_offset(mq, type_).await;
        }
        0
    }

    /// Periodic persistence for one consumer. `cancelled` is consulted between queues so stopping a
    /// consumer does not depend on stopping its siblings or the client's scheduled task, and never
    /// in the middle of a write (plan 49a F10).
    pub async fn persist_all(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> PersistSummary {
        let mut summary = PersistSummary::default();
        if let Some(ref mut store) = self.remote_broker_offset_store {
            let remote = store.persist_all(mqs, cancelled).await;
            summary.attempted += remote.attempted;
            summary.succeeded += remote.succeeded;
            summary.failed += remote.failed;
            summary.skipped += remote.skipped;
        }
        if let Some(ref mut store) = self.local_file_offset_store {
            let local = store.persist_all(mqs, cancelled).await;
            summary.attempted += local.attempted;
            summary.succeeded += local.succeeded;
            summary.failed += local.failed;
            summary.skipped += local.skipped;
        }
        summary
    }

    pub async fn persist(&mut self, mq: &MessageQueue) {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.persist(mq).await;
        }
        if let Some(ref mut store) = self.local_file_offset_store {
            store.persist(mq).await;
        }
    }

    /// Write one queue's offsets on the way to retiring it, and report what that write proved
    /// (plan 49k K2 item 1). Nothing is forgotten here: a replacement installation can appear while
    /// the write runs, and [`Self::remove_offset`] is only called once the retirement has rechecked
    /// that it still owns the queue.
    ///
    /// A store with both halves is judged by its file. The broker half keeps its existing
    /// best-effort semantics - making a broker acknowledgement a precondition of retiring a
    /// queue would let one unbounded RPC hold a queue's transition open forever.
    pub(crate) async fn persist_for_retirement(&mut self, mq: &MessageQueue) -> OffsetWrite {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.persist(mq).await;
        }
        match self.local_file_offset_store.as_mut() {
            Some(local) => local.persist_with_proof(mq).await,
            None => OffsetWrite::NoLocalState,
        }
    }

    /// Persist a concurrent-PULL retirement while retaining local file-I/O ownership through its
    /// conditional cache deletion. The caller revalidates the process-queue identity before it
    /// consumes a proven receipt (plan 49n N1 item 2).
    pub(crate) async fn persist_for_retirement_with_proof(&mut self, mq: &MessageQueue) -> LocalOffsetRetirement {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.persist(mq).await;
        }
        match self.local_file_offset_store.as_mut() {
            Some(local) => local.persist_with_proof_receipt(mq).await,
            None => LocalOffsetRetirement::NoLocalState,
        }
    }

    /// Consume the local proof receipt and remove the broker-side cache only after the local value
    /// was conditionally forgotten while the file-I/O guard remained held.
    pub(crate) async fn remove_offset_after_proof(&self, mq: &MessageQueue, receipt: LocalOffsetProofReceipt) -> bool {
        let Some(local) = &self.local_file_offset_store else {
            return false;
        };
        if !local.remove_offset_with_proof(mq, receipt).await {
            return false;
        }
        if let Some(remote) = &self.remote_broker_offset_store {
            remote.remove_offset(mq).await;
        }
        true
    }

    pub async fn remove_offset(&self, mq: &MessageQueue) {
        if let Some(store) = &self.remote_broker_offset_store {
            store.remove_offset(mq).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.remove_offset(mq).await;
        }
    }
    pub async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        if let Some(store) = &self.remote_broker_offset_store {
            return store.clone_offset_table(topic).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            return store.clone_offset_table(topic).await;
        }
        HashMap::new()
    }

    pub async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.update_consume_offset_to_broker(mq, offset, is_oneway).await?;
        }
        if let Some(ref mut store) = self.local_file_offset_store {
            store.update_consume_offset_to_broker(mq, offset, is_oneway).await?;
        }
        Ok(())
    }
}

pub trait OffsetStoreTrait {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()>;

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool);

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64);
    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64;

    /// Write every assigned offset, stopping between queues once `cancelled` says so. The periodic
    /// task and cleanup share this primitive (and the snapshot behind it) so their accounting
    /// cannot drift.
    async fn persist_all(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> PersistSummary;

    /// Bounded variant used by cleanup. Stores that issue remote writes override this so broker
    /// lookup, route refresh and the send all share one deadline.
    async fn persist_all_until(&mut self, mqs: &HashSet<MessageQueue>, _deadline: TokioInstant) -> PersistSummary {
        self.persist_all(mqs, &|| false).await
    }

    async fn persist(&mut self, mq: &MessageQueue);

    async fn remove_offset(&self, mq: &MessageQueue);

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64>;

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()>;
}

#[cfg(test)]
impl OffsetStore {
    /// Test seam (plan 49g G2): take the lock a remote offset query needs, so a test can make a
    /// repair step wait for preparation exactly as it would behind another writer. The guard's
    /// type stays private - callers keep it alive and drop it - and this is never compiled into
    /// anything that ships.
    pub(crate) async fn review49g_hold_offsets(&self) -> Option<Box<dyn Send>> {
        let remote = self.remote_broker_offset_store.as_ref()?;
        Some(Box::new(remote.offset_table_lock().lock_owned().await))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn an_offset_write_never_outruns_the_cleanup_budget() {
        assert_eq!(offset_rpc_timeout(Duration::from_secs(9)), OFFSET_WRITE_TIMEOUT_MILLIS);
        assert_eq!(offset_rpc_timeout(Duration::from_millis(120)), 120);
        assert_eq!(offset_rpc_timeout(Duration::ZERO), 0);
    }

    #[test]
    fn a_persist_summary_reports_what_the_budget_did_not_reach() {
        let summary = PersistSummary {
            attempted: 7,
            succeeded: 4,
            failed: 1,
            skipped: 2,
            unproven: 2,
        };
        assert_eq!(
            summary.to_string(),
            "attempted=7 succeeded=4 failed=1 skipped=2 unproven=2"
        );
        // Every write this pass touched is counted once, and only `succeeded` may be read as a
        // broker acknowledgement: one-way dispatch and unwritten offsets are reported apart
        // (plan 49c C5).
        assert_eq!(summary.attempted, summary.succeeded + summary.failed + summary.unproven);
    }
}
