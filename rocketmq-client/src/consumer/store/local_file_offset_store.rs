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
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use once_cell::sync::Lazy;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::utils::file_utils;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::consumer::store::controllable_offset::ControllableOffset;
use crate::consumer::store::offset_serialize::OffsetSerialize;
use crate::consumer::store::offset_serialize_wrapper::OffsetSerializeWrapper;
use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::offset_store::OffsetWrite;
use crate::consumer::store::offset_store::PersistSummary;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

static LOCAL_OFFSET_STORE_DIR: Lazy<PathBuf> = Lazy::new(|| {
    #[cfg(target_os = "windows")]
    let home = std::env::var("USERPROFILE").map_or(PathBuf::from("C:\\tmp\\.rocketmq_offsets"), |home| {
        PathBuf::from(home).join(".rocketmq_offsets")
    });

    #[cfg(not(target_os = "windows"))]
    let home = std::env::var("HOME").map_or(PathBuf::from("/tmp/.rocketmq_offsets"), |home| {
        PathBuf::from(home).join(".rocketmq_offsets")
    });

    std::env::var("rocketmq.client.localOffsetStoreDir").map_or(home, PathBuf::from)
});

/// One offset this store still owes its file (plan 49l L2 item 1).
///
/// A debt is identified by the publication that created it, not just by its queue: a successful
/// write may settle only the obligation its own snapshot carried, so an older success can never
/// clear a newer failed write of the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OffsetDebt {
    /// Identifies one publication of this obligation. Every new failure publishes a higher one.
    generation: u64,
    /// The memory offset this obligation was created for, reported for diagnosis.
    offset: i64,
}

pub struct LocalFileOffsetStore {
    client_instance: ArcMut<MQClientInstance>,
    group_name: CheetahString,
    store_path: CheetahString,
    offset_table: Arc<Mutex<HashMap<MessageQueue, ControllableOffset>>>,
    file_io: Arc<Mutex<()>>,
    io_failed: Arc<std::sync::atomic::AtomicBool>,
    /// Queues whose last local write was never proven (plan 49k K2 item 2). Their memory copy is
    /// what a retirement could not yet afford to lose, so every later whole-table write re-includes
    /// these keys - including a pass whose assigned set is empty, which is how a healed filesystem
    /// retries an offset for a queue no assignment names any more. Each entry carries the
    /// generation that created it (plan 49l L2 item 1).
    offsets_unproven: Arc<Mutex<HashMap<MessageQueue, OffsetDebt>>>,
    /// Monotone source of debt generations across every key in this store.
    debt_generation: Arc<std::sync::atomic::AtomicU64>,
}

impl LocalFileOffsetStore {
    pub fn new(client_instance: ArcMut<MQClientInstance>, group_name: CheetahString) -> Self {
        let store_path = LOCAL_OFFSET_STORE_DIR
            .clone()
            .join(client_instance.client_id.as_str())
            .join(group_name.as_str())
            .join("offsets.json")
            .to_string_lossy()
            .to_string();
        Self {
            client_instance,
            group_name,
            store_path: CheetahString::from(store_path),
            offset_table: Arc::new(Mutex::new(HashMap::new())),
            file_io: Arc::new(Mutex::new(())),
            io_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            offsets_unproven: Arc::new(Mutex::new(HashMap::new())),
            debt_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn read_local_offset_at(path: &str) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let content = file_utils::file_to_string(path).unwrap_or("".to_string());
        if content.is_empty() {
            Self::read_local_offset_bak(path)
        } else {
            match OffsetSerialize::decode(content.as_bytes()) {
                Ok(value) => Ok(Some(value.into())),
                Err(e) => Err(mq_client_err!(format!("Failed to deserialize local offset: {}", e))),
            }
        }
    }
    fn read_local_offset_bak(path: &str) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let content = file_utils::file_to_string(format!("{}{}", path, ".bak")).unwrap_or("".to_string());
        if content.is_empty() {
            Ok(None)
        } else {
            match OffsetSerialize::decode(content.as_bytes()) {
                Ok(value) => Ok(Some(value.into())),
                Err(_) => Err(mq_client_err!(format!(
                    "read local offset bak failed, content: {}",
                    content
                ))),
            }
        }
    }
}

#[cfg(test)]
mod review49n {
    use super::*;
    use crate::base::client_config::ClientConfig;

    #[tokio::test(flavor = "current_thread")]
    async fn stale_and_cancelled_keys_do_not_start_local_writes() {
        let label = format!("review49n-noop-{}", std::process::id());
        let mut client = MQClientInstance::new_arc(ClientConfig::default(), 0, &label, None);
        let mut store = LocalFileOffsetStore::new(client.clone(), CheetahString::from_string(label.clone()));
        let path = std::env::temp_dir().join(format!("{label}.json"));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        store.review49e_path_and_lock(path.clone());

        let stale = MessageQueue::from_parts(format!("{label}-stale"), "broker", 0);
        let live = MessageQueue::from_parts(format!("{label}-live"), "broker", 1);
        store.io_failed.store(true, Ordering::Release);
        assert_eq!(store.persist_with_proof(&stale).await, OffsetWrite::NoLocalState);
        assert!(
            !store.io_complete().await,
            "no-local-state retirement cleared unrelated failure evidence"
        );
        let no_op = store.persist_all(&HashSet::from([stale.clone()]), &|| false).await;
        assert_eq!((no_op.attempted, no_op.succeeded, no_op.failed), (0, 0, 0));
        assert!(!path.exists(), "a stale key opened or created the offset file");
        assert!(!store.io_complete().await, "a no-op cleared prior I/O failure evidence");

        store.io_failed.store(false, Ordering::Release);
        store.update_offset(&live, 10, false).await;
        let cancelled = store
            .persist_all(&HashSet::from([live.clone(), stale.clone()]), &|| true)
            .await;
        assert_eq!(
            (
                cancelled.attempted,
                cancelled.succeeded,
                cancelled.failed,
                cancelled.skipped
            ),
            (0, 0, 0, 1)
        );
        assert!(!path.exists(), "cancellation before I/O still opened the offset file");
        assert!(store.io_complete().await);

        let mixed = store
            .persist_all(&HashSet::from([live.clone(), stale.clone()]), &|| false)
            .await;
        assert_eq!((mixed.attempted, mixed.succeeded, mixed.failed), (1, 1, 0));
        assert_eq!(store.read_offset(&live, ReadOffsetType::ReadFromStore).await, 10);
        assert_eq!(store.read_offset(&stale, ReadOffsetType::ReadFromStore).await, -1);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        client.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_assignment_keeps_unmatched_debt_and_failure_evidence() {
        let label = format!("review49n-debt-{}", std::process::id());
        let mut client = MQClientInstance::new_arc(ClientConfig::default(), 0, &label, None);
        let mut store = LocalFileOffsetStore::new(client.clone(), CheetahString::from_string(label.clone()));
        let path = std::env::temp_dir().join(format!("{label}.json"));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        store.review49e_path_and_lock(path.clone());

        let q = MessageQueue::from_parts(label.as_str(), "broker", 0);
        store.update_offset(&q, 7, false).await;
        assert!(store.publish_debt(&q).await.is_some());
        store.offset_table.lock().await.remove(&q);
        store.io_failed.store(true, Ordering::Release);

        let summary = store.persist_all(&HashSet::new(), &|| false).await;
        assert_eq!(
            (summary.attempted, summary.succeeded, summary.failed, summary.skipped),
            (0, 0, 0, 1)
        );
        assert!(!path.exists(), "unmatched debt caused an empty file write");
        assert!(store.offsets_unproven.lock().await.contains_key(&q));
        assert!(
            !store.io_complete().await,
            "an empty assignment erased a genuine I/O failure"
        );
        assert!(!(store.io_complete().await && !store.has_required_debt().await));

        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
    }
}

#[cfg(test)]
impl LocalFileOffsetStore {
    /// Redirect this store at a test-owned path and hand back the same file-I/O mutex its writer
    /// takes, so a regression can hold that writer without adding a production lock-holding API
    /// (plan 49e E5).
    pub(crate) fn review49e_path_and_lock(&mut self, path: std::path::PathBuf) -> Arc<tokio::sync::Mutex<()>> {
        self.store_path = CheetahString::from(path.to_string_lossy().to_string());
        self.file_io.clone()
    }
}

impl LocalFileOffsetStore {
    async fn read_local_offset(&self) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let guard = self.file_io.clone().lock_owned().await;
        let path = self.store_path.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            Self::read_local_offset_at(path.as_str())
        })
        .await
        .map_err(|e| mq_client_err!(e.to_string()))?
    }

    /// Whether this store's own file writer has finished without an unfinished or unsafe write.
    /// This is the raw writer signal: it says nothing about offsets that still have to be written,
    /// which is what [`Self::has_required_debt`] is for (plan 49l L2 item 4).
    pub(crate) async fn io_complete(&self) -> bool {
        // The blocking closure owns the guard even if its awaiting future was cancelled.
        let _guard = self.file_io.lock().await;
        !self.io_failed.load(Ordering::Acquire)
    }

    /// Whether an offset this store is required to persist has not reached the file yet (plan 49l
    /// L2 item 4). Aggregate cleanup proof may not pass while this is true, however clean the last
    /// write of some *other* queue looked: a successful write of unrelated keys proves nothing
    /// about the key that failed.
    pub(crate) async fn has_required_debt(&self) -> bool {
        !self.offsets_unproven.lock().await.is_empty()
    }

    /// The debt outstanding for one queue, if any (plan 49l L2 item 1).
    async fn debt_of(&self, mq: &MessageQueue) -> Option<OffsetDebt> {
        self.offsets_unproven.lock().await.get(mq).copied()
    }

    /// Publish that this queue's current memory offset still has to reach the file, returning the
    /// generation that identifies this obligation (plan 49l L2 item 2). Publication happens before
    /// the write is attempted, so a caller whose future is cancelled - or a writer that fails -
    /// cannot leave the store looking proven. A queue with nothing in memory owes nothing: there is
    /// no value here that a failed write could lose.
    async fn publish_debt(&self, mq: &MessageQueue) -> Option<u64> {
        let offset = self.offset_table.lock().await.get(mq).map(|o| o.get_offset())?;
        let generation = self.debt_generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.offsets_unproven
            .lock()
            .await
            .insert(mq.clone(), OffsetDebt { generation, offset });
        Some(generation)
    }

    /// Settle the obligations this write actually carried (plan 49l L2 item 1). Called only after a
    /// proven write, and only with the debt entries that were in that write's snapshot: an entry a
    /// later failure re-published under a new generation stays exactly where it is.
    async fn settle_debts(&self, carried: &[(MessageQueue, OffsetDebt)]) {
        if carried.is_empty() {
            return;
        }
        let mut debt = self.offsets_unproven.lock().await;
        for (mq, expected) in carried {
            // The value this write stored is the memory value captured in the same snapshot as this
            // obligation, so file and memory agree for this key and nothing further is owed -
            // unless a newer failure published another obligation for it.
            if debt.get(mq) == Some(expected) {
                debt.remove(mq);
            }
        }
    }

    /// Write one queue's offset and report what that write proved, forgetting nothing (plan 49k K2
    /// item 1). A retirement needs this instead of the void `persist`: erasing an offset whose
    /// final write failed destroys the only copy this SDK can still retry.
    ///
    /// The obligation is published before the write and settled only by a proven write that carried
    /// it (plan 49l L2 item 2), so neither a cancelled awaiter nor a successful write of some other
    /// queue can make this key look settled. A failed attempt keeps the memory copy exactly as it
    /// was, and every later whole-table write re-includes the key - including one whose assigned
    /// set is empty - until a write lands.
    pub(crate) async fn persist_with_proof(&mut self, mq: &MessageQueue) -> OffsetWrite {
        let has_value = self.offset_table.lock().await.contains_key(mq);
        let has_debt = self.offsets_unproven.lock().await.contains_key(mq);
        match (has_value, has_debt) {
            (false, false) => return OffsetWrite::NoLocalState,
            (false, true) => return OffsetWrite::Deferred,
            (true, _) => {}
        }

        let obligation = self.publish_debt(mq).await;
        let summary = self.persist_all(&HashSet::from([mq.clone()]), &|| false).await;
        // Settled means an acknowledged write carried this exact value to the file. A newer failed
        // obligation for the same key keeps it open even if ours landed, because what memory holds
        // now is not what reached disk.
        let settled = match obligation {
            Some(_) => self.debt_of(mq).await.is_none(),
            None => true,
        };
        if summary.succeeded >= 1 && settled && self.io_complete().await {
            return OffsetWrite::Proven;
        }
        error!(
            "local offset write for {} is not proven ({}); retaining it for a later pass",
            mq, summary
        );
        // Re-publish rather than assume the earlier publication survived: what matters is that a
        // failed write always leaves an obligation behind (plan 49l L2 item 3).
        self.publish_debt(mq).await;
        OffsetWrite::Deferred
    }
}

impl OffsetStoreTrait for LocalFileOffsetStore {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        let offset_serialize_wrapper = self.read_local_offset().await?;
        if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
            let offset_table = offset_serialize_wrapper.offset_table;
            let mut offset_table_inner = self.offset_table.lock().await;
            for (mq, offset) in offset_table {
                let offset = offset.load(Ordering::Relaxed);
                info!("load consumer's offset, {} {} {}", self.group_name, mq, offset);
                offset_table_inner.insert(mq, ControllableOffset::new(offset));
            }
        }
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
                    offset.get_offset()
                } else {
                    -1
                }
            }
            ReadOffsetType::ReadFromStore => match self.read_local_offset().await {
                Ok(offset_serialize_wrapper) => {
                    if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
                        if let Some(offset) = offset_serialize_wrapper.offset_table.get(mq) {
                            offset.load(Ordering::Relaxed)
                        } else {
                            -1
                        }
                    } else {
                        -1
                    }
                }
                Err(_) => -1,
            },
        }
    }

    async fn persist_all(
        &mut self,
        mqs: &HashSet<MessageQueue>,
        cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> PersistSummary {
        let mut summary = PersistSummary::default();
        // One guard covers snapshot, write and acknowledgement for the whole table (plan 49a F10):
        // cancellation is honoured before a write starts and never during it, and an obligation can
        // be settled by the exact snapshot that carried it because no other writer or acknowledger
        // runs in between (plan 49l L2 item 3).
        let guard = self.file_io.clone().lock_owned().await;
        // An offset whose write was never proven is written again whatever the assignment says
        // (plan 49k K2 item 2); it is part of this table's durable state until a write proves it.
        let debt: HashMap<MessageQueue, OffsetDebt> = self.offsets_unproven.lock().await.clone();
        let keys: HashSet<MessageQueue> = mqs.union(&debt.keys().cloned().collect()).cloned().collect();
        // Assignment keys only identify candidates. A retired queue may still be present in an
        // earlier snapshot after its memory value and retry debt have both been removed; it must
        // not trigger a physical write or alter the real I/O proof (plan 49n N1 item 1).
        let snapshot_offsets = self
            .offset_table
            .lock()
            .await
            .iter()
            .filter(|(mq, _)| keys.contains(mq))
            .map(|(mq, offset)| (mq.clone(), offset.get_offset()))
            .collect::<HashMap<_, _>>();
        summary.skipped += debt.keys().filter(|mq| !snapshot_offsets.contains_key(*mq)).count() as u64;
        if snapshot_offsets.is_empty() {
            return summary;
        }
        if cancelled() {
            summary.skipped += snapshot_offsets.len() as u64;
            return summary;
        }
        let count = snapshot_offsets.len() as u64;
        summary.attempted = count;
        let offsets = snapshot_offsets
            .iter()
            .map(|(mq, offset)| (mq.clone(), AtomicI64::new(*offset)))
            .collect::<HashMap<_, _>>();
        // The obligations this write actually carries, captured with the values they are about
        // (plan 49l L2 item 1). A key that has no memory value cannot be settled by a write
        // of nothing.
        let carried: Vec<(MessageQueue, OffsetDebt)> =
            debt.into_iter().filter(|(mq, _)| offsets.contains_key(mq)).collect();
        // Test seam (plan 49l L2): once this writer's key set and the obligations it carries are
        // fixed - inside the file-I/O guard, before anything reaches the volume - a regression may
        // hold it there. That is how another queue's failure can be made to land inside an earlier
        // successful write, which is what debt settlement has to get right. Never compiled into
        // anything that ships.
        #[cfg(test)]
        review49l::after_key_snapshot(&snapshot_offsets.keys().cloned().collect()).await;
        let path = self.store_path.clone();
        let failed = self.io_failed.clone();
        // Mark unfinished work unsafe before spawning; panic/cancellation cannot report success.
        failed.store(true, Ordering::Release);
        let result = tokio::task::spawn_blocking(move || -> rocketmq_error::RocketMQResult<()> {
            let _guard = guard;
            let mut wrapper = Self::read_local_offset_at(path.as_str())?.unwrap_or_default();
            wrapper.offset_table.extend(offsets);
            let content = OffsetSerialize::from(wrapper)
                .serialize_json_pretty()
                .map_err(|e| mq_client_err!(e.to_string()))?;
            file_utils::string_to_file(&content, path.as_str()).map_err(|e| mq_client_err!(e.to_string()))?;
            failed.store(false, Ordering::Release);
            Ok(())
        })
        .await;
        if matches!(result, Ok(Ok(()))) {
            summary.succeeded += count;
            // Only the obligations this write carried are settled - including the ones no
            // assignment names any more (plan 49k K2 item 2) - and only while nothing newer has
            // republished them (plan 49l L2 item 1).
            self.settle_debts(&carried).await;
        } else {
            error!("persistAll consumer offset failed: {}", self.store_path);
            summary.failed += count;
        }
        summary
    }

    async fn persist(&mut self, mq: &MessageQueue) {
        self.persist_all(&HashSet::from([mq.clone()]), &|| false).await;
    }

    async fn remove_offset(&self, mq: &MessageQueue) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table.remove(mq);
        // Forgetting a queue's offset by request also ends its unproven debt: this call is the
        // caller saying it no longer needs the value at all, which a deferred retirement never
        // says.
        self.offsets_unproven.lock().await.remove(mq);
        // Per-queue bookkeeping is routine: one line per revoked queue buries the summaries that
        // matter (plan 49a F10).
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
            .filter(|(mq, _)| topic.is_empty() || mq.get_topic() == topic)
            .map(|(mq, offset)| (mq.clone(), offset.get_offset()))
            .collect()
    }

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }
}

/// Test seam (plan 49l L2): hold a whole-table write between its snapshot and its file work, so a
/// regression can make another queue's failure land inside it. Keyed by queue, because these tests
/// run concurrently and one test's writer must not park on another's hold. Production builds
/// compile none of this in.
#[cfg(test)]
pub(crate) mod review49l {
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;
    use crate::base::client_config::ClientConfig;

    /// One armed hold: the next write whose key set names `queue` parks until `release` resolves.
    struct Hold {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }

    static HOLDS: Lazy<Mutex<HashMap<MessageQueue, Vec<Hold>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

    /// Called with one write's key set once that writer holds the file-I/O guard and has fixed both
    /// the keys it will write and the obligations it carries (plan 49l L2 item 3).
    pub(super) async fn after_key_snapshot(keys: &HashSet<MessageQueue>) {
        let armed = {
            let mut holds = HOLDS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut armed = Vec::new();
            for queue in keys {
                if let Some(matching) = holds.remove(queue) {
                    armed.extend(matching);
                }
            }
            armed
        };
        for hold in armed {
            let Hold { entered, release } = hold;
            let _ = entered.send(());
            let _ = release.await;
        }
    }

    /// Arm a hold on the next write naming `queue`. The returned receiver resolves once that writer
    /// is parked inside its snapshot; dropping or resolving the returned sender lets it continue.
    pub(crate) fn hold_next_write(queue: &MessageQueue) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, entered_rx) = oneshot::channel();
        let (release_tx, release) = oneshot::channel();
        HOLDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(queue.clone())
            .or_default()
            .push(Hold { entered, release });
        (entered_rx, release_tx)
    }

    /// Forget every armed hold, so a test that gave up early cannot park a later one.
    pub(crate) fn clear_holds() {
        HOLDS.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clear();
    }

    /// Another handle on the same store state, which is how two writers are made to overlap on one
    /// offset table and one file.
    fn shared_handle(store: &LocalFileOffsetStore) -> LocalFileOffsetStore {
        LocalFileOffsetStore {
            client_instance: store.client_instance.clone(),
            group_name: store.group_name.clone(),
            store_path: store.store_path.clone(),
            offset_table: store.offset_table.clone(),
            file_io: store.file_io.clone(),
            io_failed: store.io_failed.clone(),
            offsets_unproven: store.offsets_unproven.clone(),
            debt_generation: store.debt_generation.clone(),
        }
    }

    macro_rules! poll_once {
        ($future:expr, $message:expr) => {
            tokio::select! { biased; _ = &mut $future => panic!($message), _ = std::future::ready(()) => {} }
        };
    }

    /// Whether this store still owes its file one queue's offset.
    async fn owes(store: &LocalFileOffsetStore, mq: &MessageQueue) -> bool {
        store.offsets_unproven.lock().await.contains_key(mq)
    }

    /// The reviewer's shared-store reproduction (plan 49l L2 item 5): a whole-table write whose key
    /// set was fixed before another queue failed may not settle that queue's obligation.
    ///
    /// `stale` fixes A's snapshot first, so B's failure lands inside an older successful write; the
    /// fresh control lets B fail first, and then A's own snapshot carries it. Because this store
    /// now takes its file-I/O guard before it snapshots (plan 49l L2 item 3), B's failing
    /// attempt is staged with a second hold instead of run concurrently: publication happens
    /// ahead of any write, so B's obligation exists while A holds the guard, and its failure
    /// follows as soon as the volume breaks. Both variants must show that B's value still
    /// reaches the file.
    async fn debt_snapshot(stale: bool) {
        let label = if stale { "stale" } else { "fresh" };
        let mut client = MQClientInstance::new_arc(
            ClientConfig::default(),
            0,
            format!("review49l-debt-{label}").as_str(),
            None,
        );
        let mut store = LocalFileOffsetStore::new(
            client.clone(),
            CheetahString::from_string(format!("review49l-debt-{label}")),
        );
        let path = std::env::temp_dir().join(format!("review49l-debt-{label}-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&path);
        store.review49e_path_and_lock(path.clone());
        let topic = format!("review49l-debt-{label}");
        let a = MessageQueue::from_parts(topic.as_str(), "broker", 0);
        let b = MessageQueue::from_parts(topic.as_str(), "broker", 1);
        store.update_offset(&a, 99, false).await;
        store.update_offset(&b, 7, false).await;

        let verdict;
        let debt_before;
        let mut old = shared_handle(&store);
        let selected: HashSet<MessageQueue> = HashSet::from([a.clone()]);
        let mut writing = Box::pin(old.persist_all(&selected, &|| false));
        if stale {
            // A is parked with its key set fixed and no obligation in it; B's value is owed from
            // here on, and A never sees it.
            let (mut entered, release) = hold_next_write(&a);
            tokio::select! {
                _ = &mut entered => {}
                _ = &mut writing => panic!("the earlier writer did not pause"),
                _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("the key snapshot hook was never reached"),
            }
            let mut failing = shared_handle(&store);
            let failing_queue = b.clone();
            let mut b_attempt = Box::pin(failing.persist_with_proof(&failing_queue));
            // One poll publishes this obligation before any write is attempted, then waits for the
            // guard A still holds.
            poll_once!(b_attempt, "B's write finished while an older writer owned the file");
            debt_before = owes(&store, &b).await;
            let (mut entered_b, release_b) = hold_next_write(&b);
            drop(release);
            let earlier = tokio::time::timeout(Duration::from_secs(2), &mut writing)
                .await
                .expect("the parked writer never came back");
            eprintln!("stale debt: earlier={earlier} owed_before_b_wrote={debt_before}");
            assert_eq!(earlier.succeeded, 1, "the older pass wrote more than its own key");
            // B continues now that A is gone, and finds a broken volume.
            tokio::select! {
                _ = &mut entered_b => {}
                _ = &mut b_attempt => panic!("B wrote while its hold was armed"),
                _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("B never reached its key snapshot"),
            }
            let saved = path.with_extension("saved");
            let _ = std::fs::remove_file(&path);
            std::fs::create_dir(&path).unwrap();
            drop(release_b);
            verdict = tokio::time::timeout(Duration::from_secs(2), &mut b_attempt)
                .await
                .expect("B's write never came back");
            let _ = std::fs::remove_dir(&path);
            let _ = std::fs::rename(&saved, &path);
        } else {
            debt_before = true;
            // The control: B fails first, so A's own snapshot carries it and lands it.
            let saved = path.with_extension("saved");
            let _ = std::fs::remove_file(&path);
            std::fs::create_dir(&path).unwrap();
            verdict = store.persist_with_proof(&b).await;
            let _ = std::fs::remove_dir(&path);
            let _ = std::fs::rename(&saved, &path);
            let earlier = tokio::time::timeout(Duration::from_secs(2), &mut writing)
                .await
                .expect("the writer never came back");
            eprintln!("fresh debt: earlier={earlier} owed_before_b_wrote={debt_before}");
            assert_eq!(
                earlier.succeeded, 2,
                "a pass that snapshot an obligation did not carry it to the file"
            );
        }
        let debt_after = owes(&store, &b).await;
        // A pass naming no assigned queue at all: only an obligation that survived can land B.
        let later = store.persist_all(&HashSet::new(), &|| false).await;
        let memory = store.read_offset(&b, ReadOffsetType::ReadFromMemory).await;
        let disk = store.read_offset(&b, ReadOffsetType::ReadFromStore).await;
        let proof = store.io_complete().await && !store.has_required_debt().await;
        eprintln!(
            "offset debt: stale={stale} verdict={verdict:?} owed_after_earlier={debt_after} later={later} \
             memory={memory} disk={disk} proof={proof}"
        );
        client.shutdown().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&path);
        let _ = std::fs::remove_file(path.with_extension("json.bak"));
        let _ = std::fs::remove_file(path.with_extension("saved"));

        assert_eq!(verdict, OffsetWrite::Deferred, "B's own write was not its own");
        assert!(debt_before, "a failed write published no obligation");
        assert_eq!(memory, 7, "a failed write lost the memory copy");
        if stale {
            assert!(debt_after, "an older snapshot settled an obligation it never carried");
            assert!(later.attempted >= 1, "a surviving obligation wrote nothing");
        } else {
            assert!(!debt_after, "the pass that carried the obligation left it open");
            assert_eq!(later.attempted, 0, "an obligation already written is owed again");
        }
        assert!(proof, "aggregate proof stayed false once every obligation was met");
        assert_eq!(
            disk, 7,
            "a successful older snapshot cleared debt for a queue it never wrote"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_old_success_must_not_clear_debt_created_after_its_snapshot() {
        debt_snapshot(true).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_fresh_success_carries_and_clears_the_recorded_debt() {
        debt_snapshot(false).await;
    }
}
