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
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;

/// Outcome of one deadline-bounded offset persistence pass (plan 49 section 4.3). Reported as a
/// bounded summary; individual queue failures stay at DEBUG so cleanup cannot amplify logs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PersistSummary {
    pub attempted: u64,
    pub succeeded: u64,
    pub failed: u64,
    /// Assigned offsets left unwritten because the cleanup budget ran out first.
    pub skipped: u64,
}

impl std::fmt::Display for PersistSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "attempted={} succeeded={} failed={} skipped={}",
            self.attempted, self.succeeded, self.failed, self.skipped
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

    /// Persist assigned offsets without exceeding `deadline`. The returned summary is the cleanup
    /// session's evidence: a short budget leaves offsets recorded as skipped, never silently lost.
    pub(crate) async fn persist_all_until(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        deadline: TokioInstant,
    ) -> PersistSummary {
        let mut summary = PersistSummary::default();
        if let Some(store) = self.remote_broker_offset_store.as_mut() {
            let remote = store.persist_all_until(mqs, deadline).await;
            summary.attempted += remote.attempted;
            summary.succeeded += remote.succeeded;
            summary.failed += remote.failed;
            summary.skipped += remote.skipped;
        }
        if let Some(store) = self.local_file_offset_store.as_mut() {
            // The file writer owns its guard for the whole table: it is never cancelled mid-write
            // and reports its own accounting (plan 49a F10).
            let local = store.persist_all(mqs, &|| false).await;
            summary.attempted += local.attempted;
            summary.succeeded += local.succeeded;
            summary.failed += local.failed;
            summary.skipped += local.skipped;
        }
        summary
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
        };
        assert_eq!(summary.to_string(), "attempted=7 succeeded=4 failed=1 skipped=2");
    }
}
