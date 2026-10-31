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

use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DEFAULT_CONSUMER_GROUP;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::ClientErr;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tokio::time::Instant as TokioInstant;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::consumer::ack_callback::AckCallback;
use crate::consumer::ack_result::AckResult;
use crate::consumer::cleanup_session::spawn_cleanup_session;
use crate::consumer::cleanup_session::CleanupDriver;
use crate::consumer::cleanup_session::CleanupProgress as CleanupSessionProgress;
use crate::consumer::cleanup_session::CleanupSession;
use crate::consumer::consumer_impl::consume_message_concurrently_service::ConsumeMessageConcurrentlyService;
use crate::consumer::consumer_impl::consume_message_orderly_service::ConsumeMessageOrderlyService;
use crate::consumer::consumer_impl::consume_message_pop_concurrently_service::ConsumeMessagePopConcurrentlyService;
use crate::consumer::consumer_impl::consume_message_pop_orderly_service::ConsumeMessagePopOrderlyService;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessagePopServiceGeneral;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceGeneral;
use crate::consumer::consumer_impl::consumer_lifecycle::ConsumerProgress;
use crate::consumer::consumer_impl::consumer_work::DeliveryGate;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::rebalance_push_impl::RebalancePushImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::message_listener::MessageListener;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::consumer::pop_callback::DefaultPopCallback;
use crate::consumer::pop_result::PopResult;
use crate::consumer::pop_status::PopStatus;
use crate::consumer::pull_callback::DefaultPullCallback;
use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::offset_store::PersistSummary;
use crate::consumer::store::offset_store::OFFSET_WRITE_TIMEOUT_MILLIS;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::filter_message_context::FilterMessageContext;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::producer::mq_producer::MQProducer;

const PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL: u64 = 50;
pub(crate) const PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL: u64 = 20;
const PULL_TIME_DELAY_MILLS_WHEN_SUSPEND: u64 = 1000;
const BROKER_SUSPEND_MAX_TIME_MILLIS: u64 = 1000 * 15;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: u64 = 1000 * 30;
/// Room beyond a pull's own response timeout before an **admitted attempt** counts as unanswered
/// (plan 49d R1). Covers dispatch and decoding, not a hung RPC; the managed controller's pull
/// deadline is this same total, so health and repair judge one attempt exactly like the loop that
/// issued it.
const PULL_ANSWER_ALLOWANCE_MILLIS: u64 = 5_000;
/// How long one queue's next pull may wait for room in the **process-wide** pull channel before
/// this consumer stops treating the wait as progress and lets health re-dispatch it (plan 49f
/// F2/F4).
///
/// The channel is shared by every consumer on the client instance, so an unbounded hand-off lets
/// one queue's lease wait behind thousands of requests for other queues. Waiting briefly is
/// legitimate backpressure; waiting indefinitely turns capacity pressure into a fault that looks
/// like a stalled queue.
pub(crate) const PULL_ADMISSION_ALLOWANCE: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_POP_INVISIBLE_TIME: u64 = 300000;
const MIN_POP_INVISIBLE_TIME: u64 = 5000;
const ASYNC_TIMEOUT: u64 = 3000;

/// Bounded summary of one cleanup pass: fixed phase timings, the first blocker and whether local
/// quiescence was proved. Emitted once when the pass ends so a slow teardown is diagnosable without
/// turning every teardown step into its own log line (plan 49 section 4.3).
struct CleanupReport {
    group: CheetahString,
    phases: Vec<(&'static str, std::time::Duration)>,
    blocker: Option<&'static str>,
    detail: Option<String>,
    complete: bool,
}

impl CleanupReport {
    /// Fixed number of recorded phases; cleanup has a fixed shape, so this cannot grow.
    const MAX_PHASES: usize = 8;

    fn new(group: CheetahString) -> Self {
        Self {
            group,
            phases: Vec::with_capacity(Self::MAX_PHASES),
            blocker: None,
            detail: None,
            complete: false,
        }
    }

    fn record(&mut self, phase: &'static str, started: TokioInstant) {
        if self.phases.len() < Self::MAX_PHASES {
            self.phases.push((phase, started.elapsed()));
        }
    }

    fn block(&mut self, blocker: &'static str) {
        self.blocker.get_or_insert(blocker);
    }

    /// One extra fact about the pass; only the first is kept so the line stays bounded.
    fn detail(&mut self, detail: String) {
        if self.detail.is_none() {
            self.detail = Some(detail);
        }
    }
}

impl Drop for CleanupReport {
    fn drop(&mut self) {
        let phases = self
            .phases
            .iter()
            .map(|(name, took)| format!("{name}={}ms", took.as_millis()))
            .collect::<Vec<_>>()
            .join(" ");
        let detail = self.detail.as_deref().unwrap_or("");
        info!(
            "consumer cleanup group={}, complete={}, blocker={}, {}{}",
            self.group,
            self.complete,
            self.blocker.unwrap_or("none"),
            phases,
            if detail.is_empty() {
                String::new()
            } else {
                format!(" {detail}")
            }
        );
    }
}

/// Finalization ceiling for a consumer that was never given a lifecycle policy: the same local
/// budget a managed stop targets, times the remote allowance beside it.
const DEFAULT_CLEANUP_CEILING: std::time::Duration = std::time::Duration::from_secs(90);

/// One queue's whole remote offset-preparation allowance during a lost-pull repair (plan 49e E4):
/// the same budget as an offset query, covering its locks, nameserver lookup, broker query and
/// admin fallback rather than only the final RPC.
pub(crate) const LOST_PULL_PREP_ALLOWANCE: std::time::Duration =
    std::time::Duration::from_millis(crate::consumer::store::offset_store::OFFSET_WRITE_TIMEOUT_MILLIS);

/// The least wall time a lost-pull preparation step after the first may take before its round hands
/// off (plan 49e E4). A slice that has already run out must not hand the next queue a zero-length
/// step; because a step that cannot answer ends the round, at most one step overruns by more.
const LOST_PULL_STEP_MINIMUM: std::time::Duration = std::time::Duration::from_millis(50);

/// The order `repair_lost_pulls` walks lost queues in, as one comparable key (plan 49e E3). Sorting
/// and cursoring read the same encoding, so a resumed round cannot land between two keys it never
/// actually passed.
fn lost_pull_cursor_key(mq: &MessageQueue) -> CheetahString {
    CheetahString::from_string(format!(
        "{}\u{1}{}\u{1}{:010}",
        mq.get_topic(),
        mq.get_broker_name(),
        mq.get_queue_id()
    ))
}

/// What one bounded lost-pull round reports to the controller (plan 49e E3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LostPullRound {
    /// Every queue this round was told about was reached. Zero dispatches with a completed sweep is
    /// how a fruitless attempt reaches the replacement budget - an empty slice is never reported as
    /// completion when a real lookup was still owed.
    Complete { dispatched: usize },
    /// The round exhausted its budget or one step did not answer inside it. The next round
    /// continues strictly after this queue in the same deterministic order.
    Incomplete { after: CheetahString },
}

impl LostPullRound {
    /// Whether this kind is finished and the controller should re-evaluate.
    pub(crate) fn finished(&self) -> bool {
        matches!(self, LostPullRound::Complete { .. })
    }
}

/// Remote opportunity bulk offset persistence gets when no lifecycle policy said otherwise - the
/// same allowance one best-effort offset write may take, times a small window of them (plan 49c
/// C5).
const DEFAULT_REMOTE_PERSIST_OPPORTUNITY: std::time::Duration = std::time::Duration::from_secs(10);
/// What unregistration and client teardown keep back before persistence starts, for the same
/// reason.
const DEFAULT_FINALIZATION_RESERVE: std::time::Duration = std::time::Duration::from_secs(7);

/// First wait before this pass re-runs its own unfinished offset write. Storage faults are usually
/// transient - a volume remounting, a reader holding the file a moment too long - and one owner
/// waiting briefly costs less than reporting cleanup unproved for the rest of the process (plan 49c
/// C6).
const LOCAL_PERSIST_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);
/// Ceiling on that wait, so repeated attempts cannot dominate the session they belong to.
const MAX_LOCAL_PERSIST_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
/// Attempts this pass makes before it accepts that the write is not going to land and reports the
/// blocker instead. Bounded so an unrecoverable fault stays Blocked without spinning.
const MAX_LOCAL_PERSIST_RETRIES: u32 = 8;

/// Backoff before attempt number `attempt` (0-based) of an unfinished local offset write: doubling
/// from the first wait up to its ceiling, which it reaches and then holds at.
pub(crate) fn local_persist_backoff(attempt: u32) -> std::time::Duration {
    LOCAL_PERSIST_BACKOFF
        .saturating_mul(1u32 << attempt.min(5))
        .min(MAX_LOCAL_PERSIST_BACKOFF)
}

/// How the offset persistence phase of a cleanup pass ended.
pub(crate) enum PersistPhase {
    /// The writer finished. Whatever the summary says about individual queues, no local write is
    /// left unfinished and finalization may proceed.
    Finished { summary: PersistSummary, attempts: u32 },
    /// Retry could not help within this session's budget; the blocker has to be reported instead of
    /// a second writer being imagined into finishing the job.
    Blocked { summary: PersistSummary, attempts: u32 },
}

/// What the offset persistence phase drives. Separated from `OffsetStore` so this phase's retry
/// policy is testable without a broker or a filesystem (plan 49c C6).
pub(crate) trait PersistTarget {
    /// One attempt at writing everything assigned, bounded by `deadline`.
    fn persist(&mut self, deadline: TokioInstant) -> impl Future<Output = PersistSummary>;
    /// Whether the store's own writer has finished. `false` means an unfinished write is still
    /// owned here, which is exactly what makes it this phase's work rather than someone else's.
    fn writer_finished(&mut self) -> impl Future<Output = bool>;
}

/// The consumer's offset store plus the queues one cleanup pass was assigned. Cleanup runs this in
/// two ordered phases rather than one call, because an `OffsetStore` can hold both halves and only
/// the remote half may be bounded (plan 49e E5).
struct LocalFilePersist<'a> {
    store: &'a mut OffsetStore,
    assigned: &'a HashSet<rocketmq_common::common::message::message_queue::MessageQueue>,
}

impl PersistTarget for LocalFilePersist<'_> {
    /// No deadline: the whole-table file writer owns its guard until the write finishes and is
    /// never cancelled mid-write (plan 49a F10). Waiting here is the point - what it must not
    /// do is spend the remote budget, which this phase has not opened yet.
    async fn persist(&mut self, _deadline: TokioInstant) -> PersistSummary {
        self.store.persist_local(self.assigned).await.unwrap_or_default()
    }

    async fn writer_finished(&mut self) -> bool {
        self.store.local_writer_finished().await
    }
}

/// The broker half of cleanup persistence. Every write shares the deadline this phase opened, and
/// an unfinished step is reported rather than retried here: only a local writer can be owned while
/// it finishes.
struct RemoteBrokerPersist<'a> {
    store: &'a mut OffsetStore,
    assigned: &'a HashSet<rocketmq_common::common::message::message_queue::MessageQueue>,
}

impl PersistTarget for RemoteBrokerPersist<'_> {
    async fn persist(&mut self, deadline: TokioInstant) -> PersistSummary {
        self.store
            .persist_remote_until(self.assigned, deadline)
            .await
            .unwrap_or_default()
    }

    async fn writer_finished(&mut self) -> bool {
        true
    }
}

/// Persist everything assigned on this pass' own store, retrying an unfinished write **in place**
/// with bounded backoff (plan 49c C6). One owner and one writer repeat work that is safe to repeat,
/// so storage that becomes available again lets cleanup prove itself rather than leave a consumer
/// blocked on a fault that outlived its own deadline. Nothing else in this process could make that
/// write land: re-observing an ended pass from outside certainly cannot.
///
/// Each attempt is bounded by the same session deadline minus what finalization reserved, so
/// retrying cannot reach past the budget the pass was given, and `on_retry` hears about every
/// attempt so the session can report actual progress rather than a phase name that never moves.
pub(crate) async fn persist_offsets_with_phase_retry<S: PersistTarget + ?Sized>(
    store: &mut S,
    session_deadline: TokioInstant,
    opportunity: std::time::Duration,
    reserve: std::time::Duration,
    mut on_retry: impl FnMut(),
) -> PersistPhase {
    let mut summary = PersistSummary::default();
    let mut attempt = 0u32;
    loop {
        let now = TokioInstant::now();
        let window = remote_step(now, session_deadline, opportunity, reserve);
        summary.merge(&store.persist(now + window).await);
        if store.writer_finished().await {
            return PersistPhase::Finished {
                summary,
                attempts: attempt,
            };
        }
        let backoff = local_persist_backoff(attempt);
        let remaining = session_deadline
            .saturating_duration_since(TokioInstant::now())
            .saturating_sub(reserve);
        if attempt >= MAX_LOCAL_PERSIST_RETRIES || backoff >= remaining {
            return PersistPhase::Blocked {
                summary,
                attempts: attempt,
            };
        }
        attempt += 1;
        on_retry();
        tokio::time::sleep(backoff).await;
    }
}

fn millis_or(millis: u64, default: std::time::Duration) -> std::time::Duration {
    if millis == 0 {
        default
    } else {
        std::time::Duration::from_millis(millis)
    }
}

/// How much of the session ceiling bulk offset persistence may use, measured from `now` - the
/// moment local work finished draining - with `reserve` already promised to unregistration and
/// teardown (plan 49c C5).
///
/// The opportunity starts here rather than at session creation: slow local release used to be spent
/// out of the same budget, so a late listener could leave the remote step with nothing while still
/// taking the finalization reserve with it. Zero means no remote step may start, which is reported
/// as unwritten offsets rather than attempted and abandoned writes.
fn remote_step(
    now: TokioInstant,
    session_deadline: TokioInstant,
    opportunity: std::time::Duration,
    reserve: std::time::Duration,
) -> std::time::Duration {
    let available = session_deadline.saturating_duration_since(now).saturating_sub(reserve);
    opportunity.min(available)
}

/// How long the local offset-file phase may retry an unfinished write, sized once at its entry from
/// this phase's own policy (plan 49e E5): one write allowance plus the whole backoff schedule. It
/// is deliberately not derived from a cleanup observation deadline - an observer that stopped
/// waiting says nothing about whether this process still owns a file writer that has to finish.
fn local_persist_window() -> std::time::Duration {
    let mut window = std::time::Duration::from_millis(OFFSET_WRITE_TIMEOUT_MILLIS);
    for attempt in 0..MAX_LOCAL_PERSIST_RETRIES {
        window += local_persist_backoff(attempt);
    }
    window
}

fn remaining_millis(deadline: TokioInstant) -> u64 {
    deadline
        .saturating_duration_since(TokioInstant::now())
        .as_millis()
        .min(u64::MAX as u128) as u64
}
//const DO_NOT_UPDATE_TOPIC_SUBSCRIBE_INFO_WHEN_SUBSCRIPTION_CHANGED: bool = false;
const _1MB: u64 = 1024 * 1024;

pub struct DefaultMQPushConsumerImpl {
    pub(crate) global_lock: Arc<Mutex<()>>,
    pub(crate) work: Arc<super::consumer_work::ConsumerWork>,
    pub(crate) pull_progress_listener: Option<Arc<dyn Fn(bool) + Send + Sync>>,
    registered: bool,
    pop_used: bool,
    pub(crate) pull_time_delay_mills_when_exception: u64,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) rebalance_impl: ArcMut<RebalancePushImpl>,
    filter_message_hook_list: Vec<Arc<Box<dyn FilterMessageHook + Send + Sync>>>,
    consume_message_hook_list: Vec<Arc<Box<dyn ConsumeMessageHook + Send + Sync>>>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    service_state: ArcMut<ServiceState>,
    pub(crate) client_instance: Option<ArcMut<MQClientInstance>>,
    pub(crate) pull_api_wrapper: Option<ArcMut<PullAPIWrapper>>,
    pause: Arc<AtomicBool>,
    consume_orderly: bool,
    message_listener: Option<ArcMut<MessageListener>>,
    pub(crate) offset_store: Option<ArcMut<OffsetStore>>,
    pub(crate) consume_message_service:
        Option<ArcMut<ConsumeMessageServiceGeneral<ConsumeMessageConcurrentlyService, ConsumeMessageOrderlyService>>>,
    pub(crate) consume_message_pop_service: Option<
        ArcMut<ConsumeMessagePopServiceGeneral<ConsumeMessagePopConcurrentlyService, ConsumeMessagePopOrderlyService>>,
    >,
    queue_flow_control_times: u64,
    queue_max_span_flow_control_times: u64,
    pub(crate) pop_delay_level: Arc<[i32; 16]>,
    default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    /// SDK-owned progress bus for this incarnation, updated at real work transitions.
    pub(crate) progress: Arc<ConsumerProgress>,
    /// Listener admission shared with the consume service; closed first on cleanup so a batch that
    /// never started is rejected instead of racing teardown.
    pub(crate) delivery: Arc<DeliveryGate>,
    /// The one cleanup session of this consumer, created by the first stop request. Observers join
    /// it; nobody may cancel it (plan 50 section 4.1).
    cleanup_session: Arc<parking_lot::Mutex<Option<CleanupSession>>>,
    /// Millis of finalization ceiling the SDK gives its own cleanup pass; `0` means unset so the
    /// default applies. Never derived from an observer's wait (plan 49b R2).
    cleanup_ceiling_millis: std::sync::atomic::AtomicU64,
    /// Millis of remote opportunity bulk offset persistence gets once local work has drained, and
    /// millis of the session ceiling that unregistration and client teardown keep back for
    /// themselves. `0` means unset, so the defaults apply; both come from the lifecycle policy
    /// and never from an observer's wait (plan 49c C5).
    remote_persist_millis: std::sync::atomic::AtomicU64,
    finalization_reserve_millis: std::sync::atomic::AtomicU64,
    /// Set when the SDK-owned controller built this incarnation. Startup classification differs
    /// between the two owners on purpose: legacy callers keep the error variants they already
    /// match on, while a managed incarnation reports configuration failures as a typed
    /// permanent rejection (plan 49b R5).
    managed_lifecycle: std::sync::atomic::AtomicBool,
}

/// Identity rules that hold before any resource exists, shared by legacy start and managed spec
/// validation so an application cannot fail later than the SDK could have told it (plan 50 F3).
pub(crate) fn check_consumer_identity(group: &str) -> rocketmq_error::RocketMQResult<()> {
    Validators::check_group(group)?;
    if group.is_empty() {
        return Err(mq_client_err!(format!(
            "consumer_group is empty, {}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }
    if group == DEFAULT_CONSUMER_GROUP {
        return Err(mq_client_err!(format!(
            "consumer_group can not equal {} please specify another one.{}",
            DEFAULT_CONSUMER_GROUP,
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }
    Ok(())
}

/// One topic/expression pair, validated without a client instance or runtime: the same rules
/// `subscribe` applies later, run early enough to reject a spec before anything is created.
pub(crate) fn check_subscription(topic: &str, expression: &str) -> rocketmq_error::RocketMQResult<()> {
    Validators::check_topic(topic)?;
    let topic = CheetahString::from_slice(topic);
    let expression = CheetahString::from_slice(expression);
    FilterAPI::build_subscription_data(&topic, &expression)
        .map_err(|err| mq_client_err!(format!("buildSubscriptionData exception, {err}")))?;
    Ok(())
}

/// Whether a listener will be attached to the consumer being validated. Legacy start reads this
/// from its own configuration; a managed spec knows it independently, because its listener is
/// attached to the object after construction rather than stored in `ConsumerConfig` (plan 49b R5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListenerPresence {
    Provided,
    Missing,
    Malformed,
}

/// One normalized, side-effect-free configuration check shared by legacy start and managed
/// preflight (plan 49b R5). Identity, allocation strategy, listener presence and every numeric
/// limit are decided from the configuration alone: no I/O, no runtime handle, no client instance,
/// nothing mutated. Errors keep their historical variant here - translating them into a typed
/// permanent rejection is the managed boundary's job, so legacy callers that match on the old
/// variants keep working unchanged.
pub(crate) fn validate_consumer_config(
    config: &ConsumerConfig,
    listener: ListenerPresence,
) -> rocketmq_error::RocketMQResult<()> {
    check_consumer_identity(config.consumer_group.as_str())?;

    if config.allocate_message_queue_strategy.is_none() {
        return Err(mq_client_err!(format!(
            "allocate_message_queue_strategy is null{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    match listener {
        ListenerPresence::Missing => {
            return Err(mq_client_err!(format!(
                "messageListener is null{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }
        ListenerPresence::Malformed => {
            return Err(mq_client_err!(format!(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }
        ListenerPresence::Provided => {}
    }

    let consume_thread_min = config.consume_thread_min;
    let consume_thread_max = config.consume_thread_max;
    if !(1..=1000).contains(&consume_thread_min) {
        return Err(mq_client_err!(format!(
            "consumeThreadMin Out of range [1, 1000]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }
    if !(1..=1000).contains(&consume_thread_max) {
        return Err(mq_client_err!(format!(
            "consumeThreadMax Out of range [1, 1000]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }
    if consume_thread_min > consume_thread_max {
        return Err(mq_client_err!(format!(
            "consumeThreadMin ({}) is larger than consumeThreadMax ({})",
            consume_thread_min, consume_thread_max
        )));
    }

    if config.consume_concurrently_max_span < 1 || config.consume_concurrently_max_span > 65535 {
        return Err(mq_client_err!(format!(
            "consumeConcurrentlyMaxSpan Out of range [1, 65535]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pull_threshold_for_queue < 1 || config.pull_threshold_for_queue > 65535 {
        return Err(mq_client_err!(format!(
            "pullThresholdForQueue Out of range [1, 65535]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pull_threshold_for_topic != -1
        && (config.pull_threshold_for_topic < 1 || config.pull_threshold_for_topic > 6553500)
    {
        return Err(mq_client_err!(format!(
            "pullThresholdForTopic Out of range [1, 65535]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pull_threshold_size_for_queue < 1 || config.pull_threshold_size_for_queue > 1024 {
        return Err(mq_client_err!(format!(
            "pullThresholdSizeForQueue Out of range [1, 1024]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pull_threshold_size_for_topic != -1
        && (config.pull_threshold_size_for_topic < 1 || config.pull_threshold_size_for_topic > 102400)
    {
        return Err(mq_client_err!(format!(
            "pullThresholdSizeForTopic Out of range [1, 102400]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pull_interval > 65535 {
        return Err(mq_client_err!(format!(
            "pullInterval Out of range [0, 65535]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.consume_message_batch_max_size < 1 || config.consume_message_batch_max_size > 1024 {
        return Err(mq_client_err!(format!(
            "consumeMessageBatchMaxSize Out of range [1, 1024]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pull_batch_size < 1 || config.pull_batch_size > 1024 {
        return Err(mq_client_err!(format!(
            "pullBatchSize Out of range [1, 1024]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pop_invisible_time < MIN_POP_INVISIBLE_TIME || config.pop_invisible_time > MAX_POP_INVISIBLE_TIME {
        return Err(mq_client_err!(format!(
            "popInvisibleTime Out of range [{}, {}]{}",
            MIN_POP_INVISIBLE_TIME,
            MAX_POP_INVISIBLE_TIME,
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    if config.pop_batch_nums < 1 || config.pop_batch_nums > 32 {
        return Err(mq_client_err!(format!(
            "popBatchNums Out of range [1, 32]{}",
            FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
        )));
    }

    Ok(())
}

impl DefaultMQPushConsumerImpl {
    /// Legacy start's own check: the same normalized rules, with listener presence read from this
    /// consumer's configuration so its historical first-error behaviour is unchanged (plan 49b R5).
    fn check_config(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let listener = match self.consumer_config.message_listener.as_ref() {
            None => ListenerPresence::Missing,
            Some(listener) => {
                if listener.message_listener_orderly.is_none() && listener.message_listener_concurrently.is_none() {
                    ListenerPresence::Malformed
                } else {
                    ListenerPresence::Provided
                }
            }
        };
        validate_consumer_config(&self.consumer_config, listener)?;
        Ok(())
    }

    pub(crate) fn is_running(&self) -> bool {
        *self.service_state == ServiceState::Running
    }

    /// Records whether the latest rebalance pass could see a route for its own subscriptions. Plan
    /// 49 section 3.2 calls a zero assignment healthy only when it was explicit; without route
    /// evidence the managed lifecycle reports a failed assignment refresh and repairs. Retry
    /// and DLQ topics follow their main subscription and never decide the outcome.
    pub(crate) async fn note_route_observation(&self) {
        let subscriptions = self.rebalance_impl.rebalance_impl_inner.subscription_inner.read().await;
        let topics: Vec<CheetahString> = subscriptions
            .keys()
            .filter(|topic| {
                !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
                    && !topic.starts_with(mix_all::DLQ_GROUP_TOPIC_PREFIX)
            })
            .cloned()
            .collect();
        drop(subscriptions);
        if topics.is_empty() {
            return;
        }
        let routes = self
            .rebalance_impl
            .rebalance_impl_inner
            .topic_subscribe_info_table
            .read()
            .await;
        // A topic with no usable queues is not evidence of a route either.
        let seen = topics
            .iter()
            .filter(|topic| routes.get(*topic).is_some_and(|set| !set.is_empty()))
            .count();
        drop(routes);
        self.progress.note_route_observation(seen == topics.len());
    }

    pub fn new(
        client_config: ClientConfig,
        consumer_config: ArcMut<ConsumerConfig>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let mut this = Self {
            global_lock: Arc::new(Default::default()),
            work: Arc::new(super::consumer_work::ConsumerWork::default()),
            pull_progress_listener: None,
            registered: false,
            pop_used: false,
            pull_time_delay_mills_when_exception: 3_000,
            client_config: ArcMut::new(client_config.clone()),
            consumer_config: consumer_config.clone(),
            rebalance_impl: ArcMut::new(RebalancePushImpl::new(client_config, consumer_config)),
            filter_message_hook_list: vec![],
            consume_message_hook_list: vec![],
            rpc_hook,
            service_state: ArcMut::new(ServiceState::CreateJust),
            client_instance: None,
            pull_api_wrapper: None,
            pause: Arc::new(AtomicBool::new(false)),
            consume_orderly: false,
            message_listener: None,
            offset_store: None,
            consume_message_service: None,
            consume_message_pop_service: None,
            progress: ConsumerProgress::new(),
            delivery: Arc::new(DeliveryGate::default()),
            queue_flow_control_times: 0,
            queue_max_span_flow_control_times: 0,
            pop_delay_level: Arc::new([
                10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ]),
            default_mqpush_consumer_impl: None,
            cleanup_session: Arc::new(parking_lot::Mutex::new(None)),
            cleanup_ceiling_millis: std::sync::atomic::AtomicU64::new(0),
            remote_persist_millis: std::sync::atomic::AtomicU64::new(0),
            finalization_reserve_millis: std::sync::atomic::AtomicU64::new(0),
            managed_lifecycle: std::sync::atomic::AtomicBool::new(false),
        };
        let wrapper = ArcMut::downgrade(&this.rebalance_impl);
        this.rebalance_impl.set_rebalance_impl(wrapper);
        this
    }

    pub fn set_default_mqpush_consumer_impl(
        &mut self,
        default_mqpush_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    ) {
        self.rebalance_impl
            .set_default_mqpush_consumer_impl(default_mqpush_consumer_impl.clone());
        self.default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl.clone());
        if let Some(ref mut consume_message_concurrently_service) = self.consume_message_service {
            consume_message_concurrently_service
                .get_consume_message_concurrently_service()
                .default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl);
        }
    }

    #[inline]
    pub fn is_consume_orderly(&self) -> bool {
        self.consume_orderly
    }
}

impl DefaultMQPushConsumerImpl {
    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let global_lock = self.global_lock.clone();
        let _lock = global_lock.lock().await;
        match *self.service_state {
            ServiceState::CreateJust => {
                info!(
                    "the consumer [{}] start beginning. message_model={}, isUnitMode={}",
                    self.consumer_config.consumer_group,
                    self.consumer_config.message_model,
                    self.consumer_config.unit_mode
                );
                *self.service_state = ServiceState::Starting;
                // check all config. A configuration problem is tagged as such so the recovery
                // controller can separate "never retry" from "not yet" by type instead of by
                // message text (plan 50 F3) - but only for a consumer the
                // controller owns. Tagging every start changed the variant legacy
                // callers had always matched on, so that translation belongs to the
                // managed boundary alone (plan 49b R5).
                if let Err(err) = self.check_config() {
                    return Err(if self.managed_lifecycle.load(Ordering::Acquire) {
                        rocketmq_error::RocketMQError::configuration(format!(
                            "consumer configuration is invalid: {err}"
                        ))
                    } else {
                        err
                    });
                }
                //copy_subscription is can be removed
                self.copy_subscription().await?;
                if self.consumer_config.message_model() == MessageModel::Clustering {
                    self.client_config.change_instance_name_to_pid();
                }
                let client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(self.client_config.as_ref().clone(), self.rpc_hook.clone());
                self.client_instance = Some(client_instance.clone());
                let registered = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .register_consumer(
                        self.consumer_config.consumer_group.as_ref(),
                        MQConsumerInnerImpl {
                            default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone().unwrap(),
                        },
                    )
                    .await;
                if !registered {
                    *self.service_state = ServiceState::StartFailed;
                    return Err(mq_client_err!(format!(
                        "the consumer group[{}] exist already.",
                        self.consumer_config.consumer_group
                    )));
                }
                self.registered = true;
                self.rebalance_impl
                    .set_consumer_group(self.consumer_config.consumer_group.clone());
                self.rebalance_impl
                    .set_message_model(self.consumer_config.message_model);
                self.rebalance_impl.set_allocate_message_queue_strategy(
                    self.consumer_config
                        .allocate_message_queue_strategy
                        .clone()
                        .expect("allocate_message_queue_strategy is null, please set it before start"),
                );
                self.rebalance_impl.set_mq_client_factory(client_instance.clone());
                if self.pull_api_wrapper.is_none() {
                    self.pull_api_wrapper = Some(ArcMut::new(PullAPIWrapper::new(
                        client_instance.clone(),
                        self.consumer_config.consumer_group.clone(),
                        self.consumer_config.unit_mode,
                    )));
                }
                if let Some(pull_api_wrapper) = self.pull_api_wrapper.as_mut() {
                    pull_api_wrapper.register_filter_message_hook(self.filter_message_hook_list.clone());
                }
                match self.consumer_config.message_model {
                    MessageModel::Broadcasting => {
                        self.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(LocalFileOffsetStore::new(
                            client_instance.clone(),
                            self.consumer_config.consumer_group.clone(),
                        ))));
                    }
                    MessageModel::Clustering => {
                        self.offset_store =
                            Some(ArcMut::new(OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
                                client_instance.clone(),
                                self.consumer_config.consumer_group.clone(),
                            ))));
                    }
                }
                self.offset_store.as_mut().unwrap().load().await?;

                if let Some(message_listener) = self.message_listener.as_ref() {
                    if message_listener.message_listener_concurrently.is_some() {
                        let (listener, _) = message_listener.message_listener_concurrently.clone().unwrap();
                        self.consume_orderly = false;
                        let consume_message_concurrently_service = ArcMut::new(ConsumeMessageConcurrentlyService::new(
                            self.client_config.clone(),
                            self.consumer_config.clone(),
                            self.consumer_config.consumer_group.clone(),
                            listener.clone().expect("listener is None"),
                            self.default_mqpush_consumer_impl.clone(),
                            self.delivery.clone(),
                        ));
                        self.consume_message_service = Some(ArcMut::new(ConsumeMessageServiceGeneral::new(
                            Some(consume_message_concurrently_service),
                            None,
                        )));
                        let consume_message_pop_concurrently_service =
                            ArcMut::new(ConsumeMessagePopConcurrentlyService::new(
                                self.client_config.clone(),
                                self.consumer_config.clone(),
                                self.consumer_config.consumer_group.clone(),
                                listener.expect("listener is None"),
                                self.default_mqpush_consumer_impl.clone(),
                            ));

                        self.consume_message_pop_service = Some(ArcMut::new(ConsumeMessagePopServiceGeneral::new(
                            Some(consume_message_pop_concurrently_service),
                            None,
                        )));
                    } else if message_listener.message_listener_orderly.is_some() {
                        let (listener, _) = message_listener.message_listener_orderly.clone().unwrap();
                        self.consume_orderly = true;
                        let consume_message_orderly_service = ArcMut::new(ConsumeMessageOrderlyService::new(
                            self.client_config.clone(),
                            self.consumer_config.clone(),
                            self.consumer_config.consumer_group.clone(),
                            listener.clone().expect("listener is None"),
                            self.default_mqpush_consumer_impl.clone(),
                        ));
                        self.consume_message_service = Some(ArcMut::new(ConsumeMessageServiceGeneral::new(
                            None,
                            Some(consume_message_orderly_service),
                        )));

                        let consume_message_pop_orderly_service = ArcMut::new(ConsumeMessagePopOrderlyService::new(
                            self.client_config.clone(),
                            self.consumer_config.clone(),
                            self.consumer_config.consumer_group.clone(),
                            listener.expect("listener is None"),
                            self.default_mqpush_consumer_impl.clone(),
                        ));
                        self.consume_message_pop_service = Some(ArcMut::new(ConsumeMessagePopServiceGeneral::new(
                            None,
                            Some(consume_message_pop_orderly_service),
                        )));
                    }
                }

                if let Some(consume_message_concurrently_service) = self.consume_message_service.as_mut() {
                    consume_message_concurrently_service.start();
                }

                if let Some(consume_message_orderly_service) = self.consume_message_pop_service.as_mut() {
                    consume_message_orderly_service.start();
                }
                let cloned = self.client_instance.as_mut().cloned().unwrap();
                if let Err(error) = self.client_instance.as_mut().unwrap().start(cloned).await {
                    *self.service_state = ServiceState::StartFailed;
                    return Err(error);
                }
                self.update_topic_subscribe_info_when_subscription_changed().await;
                if let Err(error) = self.client_instance.as_mut().unwrap().check_client_in_broker().await {
                    *self.service_state = ServiceState::StartFailed;
                    return Err(error);
                }
                info!(
                    "the consumer [{}] start OK, message_model={}, isUnitMode={}",
                    self.consumer_config.consumer_group,
                    self.consumer_config.message_model,
                    self.consumer_config.unit_mode
                );
                *self.service_state = ServiceState::Running;
                // Plan 49 section 3.2: the first assignment attempt must not be gated on a
                // confirmed heartbeat. With the NameServer unreachable the
                // heartbeat reports false, no rebalance is requested, and an
                // unroutable consumer would look like healthy idle for one whole
                // rebalance interval before its first route query. Wakes coalesce into one retained
                // pass, so asking unconditionally cannot multiply work.
                let _heartbeat_confirmed = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .send_heartbeat_to_all_broker_with_lock()
                    .await;
                self.client_instance.as_mut().unwrap().re_balance_immediately();
            }
            ServiceState::Running => {
                return Err(mq_client_err!("The PushConsumer service state is Running"));
            }
            ServiceState::ShutdownAlready => {
                return Err(mq_client_err!("The PushConsumer service state is ShutdownAlready"));
            }
            ServiceState::Starting => {
                return Err(mq_client_err!("The PushConsumer service is Starting"));
            }
            ServiceState::StartFailed => {
                return Err(mq_client_err!(format!(
                    "The PushConsumer service state not OK, maybe started once,{:?},{}",
                    *self.service_state,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                )));
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self, await_terminate_millis: u64) {
        let deadline = TokioInstant::now() + std::time::Duration::from_millis(await_terminate_millis);
        if !self.shutdown_until(deadline).await {
            warn!("consumer cleanup incomplete: {}", self.consumer_config.consumer_group);
        }
    }

    /// False means local cleanup is unproved; callers must not start a replacement. The caller's
    /// deadline bounds only this wait - the session it joins keeps running afterwards.
    pub(crate) async fn shutdown_until(&mut self, deadline: TokioInstant) -> bool {
        let started = self.begin_cleanup();
        if started {
            let session = self.cleanup_session.lock().clone();
            return match session {
                Some(session) => session.join(deadline).await,
                // Lost the slot to a concurrent caller that has already dropped it; nothing left to
                // join, and inventing success would be worse than reporting the missing proof.
                None => false,
            };
        }
        // Degenerate case: this consumer was built without an owned handle, so the pass cannot be
        // detached from the caller. It stays bounded as it always was.
        tokio::time::timeout_at(deadline, self.cleanup_pass(deadline, CleanupDriver::detached()))
            .await
            .unwrap_or(false)
    }

    /// Close admission and make sure one cleanup pass exists, without waiting for it. Admission is
    /// immediate and unconditional: no new pull and no new listener batch. The pass itself is not
    /// cancellable, so a caller that stops watching leaves it running rather than stranded.
    /// Returns whether an owned session backs the pass (`false` only for the degenerate handle-less
    /// consumer, which can only run the pass inline).
    pub(crate) fn begin_cleanup(&mut self) -> bool {
        self.pause.store(true, Ordering::Release);
        self.work.close();
        self.delivery.close();
        self.rebalance_impl.invalidate_rebalance_generation();
        if self.cleanup_session.lock().is_some() {
            return true;
        }
        self.begin_cleanup_session()
    }

    /// The phase stream of the one cleanup pass, or `None` while no stop was requested.
    pub(crate) fn cleanup_progress_changes(&self) -> Option<tokio::sync::watch::Receiver<CleanupSessionProgress>> {
        self.cleanup_session.lock().as_ref().map(|session| session.subscribe())
    }

    /// The ceiling this SDK gives its own cleanup pass. Set from the lifecycle policy by a managed
    /// consumer; otherwise the default budget applies. It never depends on an observer's wait.
    pub(crate) fn cleanup_session_ceiling(&self) -> std::time::Duration {
        let millis = self.cleanup_ceiling_millis.load(Ordering::Acquire);
        if millis == 0 {
            DEFAULT_CLEANUP_CEILING
        } else {
            std::time::Duration::from_millis(millis)
        }
    }

    /// Record that the SDK lifecycle controller owns this incarnation. Only then does a
    /// configuration failure become a typed permanent rejection; a legacy consumer keeps its
    /// historical error variants.
    /// Whether this consumer's assignment and progress are tracked by the SDK rather than left to
    /// whatever the application wired up.
    pub(crate) fn tracks_assignment(&self) -> bool {
        self.managed_lifecycle.load(Ordering::Acquire)
    }

    pub(crate) fn mark_managed_lifecycle(&self) {
        self.managed_lifecycle.store(true, Ordering::Release);
    }

    /// Records what a completed allocation pass concluded for one topic, when the SDK owns this
    /// consumer's assignment. For other consumers such a record would describe a table that is
    /// never populated.
    pub(crate) fn note_allocation_pass(
        &self,
        topic: &str,
        outcome: crate::consumer::consumer_impl::consumer_lifecycle::AllocationOutcome,
    ) {
        if self.tracks_assignment() {
            self.progress.note_allocation_pass(topic, outcome);
        }
    }

    /// Publishes the acquisition identity of an installed process queue, so that ownership exists
    /// before any pull is dispatched for it (plan 49c C1). A legacy consumer that leaves assignment
    /// observation to the application publishes nothing.
    pub(crate) fn note_queue_installed(&self, mq: &MessageQueue) {
        if self.tracks_assignment() {
            self.progress.note_queue_installed(mq);
        }
    }

    /// Ends an acquisition whose process queue left the table, retiring its epoch so a replacement
    /// installation receives a newer identity.
    pub(crate) fn note_queue_revoked(&self, mq: &MessageQueue) {
        if self.tracks_assignment() {
            self.progress.note_queue_revoked(mq);
        }
    }

    /// Keeps allocation evidence bounded by the subscription table (plan 49c C3).
    pub(crate) fn note_subscription_set(&self, topics: &[cheetah_string::CheetahString]) {
        if self.tracks_assignment() {
            self.progress.retain_allocation_passes(topics);
        }
    }

    pub(crate) fn set_cleanup_session_ceiling(&self, ceiling: std::time::Duration) {
        self.cleanup_ceiling_millis
            .store(ceiling.as_millis().min(u128::from(u64::MAX)) as u64, Ordering::Release);
    }

    /// The two remote budgets of a cleanup pass, taken from the lifecycle policy alongside its
    /// ceiling: how long bulk offset persistence may take once local work has drained, and what
    /// the final phases keep back before it starts (plan 49c C5).
    pub(crate) fn set_remote_budgets(&self, persist_opportunity: std::time::Duration, reserve: std::time::Duration) {
        self.remote_persist_millis.store(
            persist_opportunity.as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Release,
        );
        self.finalization_reserve_millis
            .store(reserve.as_millis().min(u128::from(u64::MAX)) as u64, Ordering::Release);
    }

    fn remote_persist_opportunity(&self) -> std::time::Duration {
        millis_or(
            self.remote_persist_millis.load(Ordering::Acquire),
            DEFAULT_REMOTE_PERSIST_OPPORTUNITY,
        )
    }

    fn finalization_reserve(&self) -> std::time::Duration {
        millis_or(
            self.finalization_reserve_millis.load(Ordering::Acquire),
            DEFAULT_FINALIZATION_RESERVE,
        )
    }

    /// Where the cleanup session stands, or `None` while no stop has been requested. Observation
    /// only: reading it cannot start, restart or cancel anything.
    pub(crate) fn cleanup_progress(&self) -> Option<CleanupSessionProgress> {
        self.cleanup_session.lock().as_ref().map(|session| session.progress())
    }

    /// Start the one cleanup pass on a thread this SDK owns. Its ceiling comes from the lifecycle
    /// policy, never from how long the first observer waits (plan 49b R2).
    fn begin_cleanup_session(&mut self) -> bool {
        let Some(mut handle) = self.default_mqpush_consumer_impl.clone() else {
            return false;
        };
        let session_deadline = TokioInstant::now() + self.cleanup_session_ceiling();
        let group = self.consumer_config.consumer_group.to_string();
        let session = spawn_cleanup_session(&group, move |driver| async move {
            handle.cleanup_pass(session_deadline, driver).await
        });
        *self.cleanup_session.lock() = Some(session);
        true
    }

    async fn cleanup_pass(&mut self, deadline: TokioInstant, driver: CleanupDriver) -> bool {
        // Declared first so it drops last: the summary is emitted once this pass ends on any path,
        // including an early return, and after the cleanup lock is released.
        let mut report = CleanupReport::new(self.consumer_config.consumer_group.clone());
        let lock = self.global_lock.clone();
        let _lock = lock.lock().await;
        if *self.service_state == ServiceState::ShutdownAlready {
            // Repeated stop: the first pass already proved cleanup, so this one adds nothing.
            report.complete = true;
            return true;
        }
        // ponytail: verified drain covers concurrent PULL consumers; orderly/POP
        // must implement task ownership before they can advertise safe recreation.
        if self.consume_orderly || self.pop_used {
            report.block("orderly_or_pop_consumer");
            return false;
        }
        // Begin consume-service shutdown before awaiting either tracker so a scheduled retry cannot
        // be re-admitted while the delivery tracker drains.
        if let Some(service) = self.consume_message_service.as_mut() {
            service.begin_shutdown();
        }
        driver.enter("scheduled_work");
        let started = TokioInstant::now();
        self.work.wait().await;
        report.record("scheduled_work", started);
        driver.enter("delivery");
        let started = TokioInstant::now();
        self.delivery.wait().await;
        report.record("delivery", started);
        driver.enter("consume_service");
        let started = TokioInstant::now();
        if let Some(service) = self.consume_message_service.as_mut() {
            service.shutdown(remaining_millis(deadline)).await;
        }
        if let Some(service) = self.consume_message_pop_service.as_mut() {
            service.shutdown(remaining_millis(deadline)).await;
        }
        report.record("consume_service", started);
        let queues = self
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await;
        let assigned: std::collections::HashSet<_> = queues.keys().cloned().collect();
        drop(queues);

        // **Local offsets first, and they belong to this pass rather than to the remote budget**
        // (plan 49e E5). The whole-table file writer owns its guard until the write finishes and is
        // never cancelled mid-write (plan 49a F10), so waiting for it is unavoidable; charging that
        // elapsed time against unregistration - as one combined phase did - let a slow local disk
        // spend the reserve before finalization ever saw it. The retry window is therefore sized
        // once, at this phase's entry, from this phase's own policy rather than from an
        // observation deadline that may already have expired, and an unfinished write stays
        // owned here even if it outlives that window (plan 49c C6).
        if let Some(store) = self.offset_store.as_mut() {
            if store.has_local() {
                driver.enter("local_offset_persist");
                let started = TokioInstant::now();
                let window = local_persist_window();
                let phase = persist_offsets_with_phase_retry(
                    &mut LocalFilePersist {
                        store,
                        assigned: &assigned,
                    },
                    started + window,
                    window,
                    std::time::Duration::ZERO,
                    || {
                        // Observers see each attempt happen: same session, same owner, real
                        // progress.
                        driver.retry("local_offset_persist_retry");
                    },
                )
                .await;
                let (summary, attempts) = match phase {
                    PersistPhase::Finished { summary, attempts } => (summary, attempts),
                    PersistPhase::Blocked { summary, attempts } => {
                        report.detail(format!("local_offsets[{summary}] retries={attempts}"));
                        report.block("offset_file_writer_busy");
                        return false;
                    }
                };
                report.record("local_offset_persist", started);
                if attempts > 0 {
                    report.detail(format!("local_offsets[{summary}] retries={attempts}"));
                } else {
                    report.detail(format!("local_offsets[{summary}]"));
                }
            }
        }

        // **The remote envelope opens here**, measured from this instant: local release *and* local
        // persistence have finished, so what a listener took to hand its batches back and what the
        // offset file took to land can no longer be subtracted from the broker's chance to take
        // this group's offsets and unregistration (plan 49d R5, plan 49e E5). The envelope
        // is the persist opportunity plus what finalization keeps back - a fast drain
        // therefore gets no more than the configured remote budget - and every step inside
        // it keeps an absolute deadline that neither a retry nor a re-observing caller can
        // renew. An observer whose own wait expired stopped watching; it did not shorten
        // this pass.
        let finalization_reserve = self.finalization_reserve();
        let persist_opportunity = self.remote_persist_opportunity();
        let remote_deadline = TokioInstant::now() + persist_opportunity + finalization_reserve;
        if let Some(store) = self.offset_store.as_mut() {
            if store.has_remote() {
                // One bounded pass: broker lookup, route refresh and the send all share one
                // deadline derived from this envelope, and anything the budget
                // could not reach is reported, not hidden (plan 49c C5). Nothing
                // here retries in place: a remote step has no local writer to wait
                // for, so an unfinished one is evidence, not this phase's work.
                driver.enter("remote_offset_persist");
                let started = TokioInstant::now();
                let phase = persist_offsets_with_phase_retry(
                    &mut RemoteBrokerPersist {
                        store,
                        assigned: &assigned,
                    },
                    remote_deadline,
                    persist_opportunity,
                    finalization_reserve,
                    || driver.retry("remote_offset_persist_retry"),
                )
                .await;
                let PersistPhase::Finished { summary, attempts } = phase else {
                    report.block("offset_store_writer_busy");
                    return false;
                };
                report.record("remote_offset_persist", started);
                if attempts > 0 {
                    report.detail(format!("offsets[{summary}] retries={attempts}"));
                } else {
                    report.detail(format!("offsets[{summary}]"));
                }
            }
        }

        // No offset store means nothing was ever persisted, which is proved, not blocked.
        let queues = self
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await;
        for queue in queues.values() {
            queue.set_dropped(true);
        }
        drop(queues);
        driver.enter("unregister");
        if let Some(client) = self.client_instance.as_mut() {
            let started = TokioInstant::now();
            // A duplicate-registration failure does not own the sibling's entry.
            if self.registered {
                client
                    .unregister_consumer_local(&self.consumer_config.consumer_group)
                    .await;
                self.registered = false;
                // Only the owner may unregister this group remotely; notify before closing
                // transport.
                // The attempt gets at most five seconds of what is left, and never less than a
                // finalization reserve, so teardown cannot be cut off mid-release. What is left
                // here is what the reservation made before persistence promised it,
                // inside the envelope opened when local release finished (plan 49c
                // C5, plan 49d R5).
                let remaining = remote_deadline.saturating_duration_since(TokioInstant::now());
                let reserve = std::time::Duration::from_secs(2).min(remaining / 4);
                let remote_budget = remaining.saturating_sub(reserve).min(std::time::Duration::from_secs(5));
                let remote_deadline = TokioInstant::now() + remote_budget;
                client
                    .unregister_client_until(None, Some(self.consumer_config.consumer_group.clone()), remote_deadline)
                    .await;
            }
            report.record("unregister", started);
            driver.enter("client_shutdown");
            let started = TokioInstant::now();
            client.shutdown().await;
            report.record("client_shutdown", started);
        }
        self.consume_message_service = None;
        self.consume_message_pop_service = None;
        self.pull_api_wrapper = None;
        self.offset_store = None;
        self.client_instance = None;
        self.rebalance_impl.default_mqpush_consumer_impl = None;
        self.rebalance_impl.rebalance_impl_inner.client_instance = None;
        self.default_mqpush_consumer_impl = None;
        *self.service_state = ServiceState::ShutdownAlready;
        report.complete = true;
        true
    }

    async fn update_topic_subscribe_info_when_subscription_changed(&mut self) {
        // if DO_NOT_UPDATE_TOPIC_SUBSCRIBE_INFO_WHEN_SUBSCRIPTION_CHANGED {
        //     return;
        // }
        let keys = {
            let sub_table = self.rebalance_impl.get_subscription_inner();
            let sub_table_inner = sub_table.read().await;
            sub_table_inner.keys().cloned().collect::<Vec<_>>()
        };
        let client = self.client_instance.as_mut().unwrap();
        for topic in keys {
            client.update_topic_route_info_from_name_server_topic(&topic).await;
        }
    }

    async fn copy_subscription(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let sub = self.consumer_config.subscription();
        if !sub.is_empty() {
            for (topic, sub_expression) in sub.as_ref() {
                let subscription_data = FilterAPI::build_subscription_data(topic, sub_expression).map_err(|e| {
                    rocketmq_error::RocketmqError::MQClientErr(ClientErr::new(format!(
                        "buildSubscriptionData exception, {e}",
                    )))
                })?;
                self.rebalance_impl
                    .put_subscription_data(topic.clone(), subscription_data)
                    .await;
            }
        }
        if self.message_listener.is_none() {
            self.message_listener = self.consumer_config.message_listener.clone();
        }

        match self.consumer_config.message_model {
            MessageModel::Broadcasting => {}
            MessageModel::Clustering => {
                let retry_topic =
                    CheetahString::from_string(mix_all::get_retry_topic(self.consumer_config.consumer_group.as_str()));
                let subscription_data = FilterAPI::build_subscription_data(
                    retry_topic.as_ref(),
                    &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                )
                .map_err(|e| {
                    rocketmq_error::RocketmqError::MQClientErr(ClientErr::new(format!(
                        "buildSubscriptionData exception, {e}",
                    )))
                })?;
                self.rebalance_impl
                    .put_subscription_data(retry_topic, subscription_data)
                    .await;
            }
        }
        Ok(())
    }

    pub fn register_consume_message_hook(&mut self, hook: impl ConsumeMessageHook) {
        unimplemented!("registerConsumeMessageHook");
    }

    pub fn register_message_listener(&mut self, message_listener: Option<ArcMut<MessageListener>>) {
        self.message_listener = message_listener;
    }

    pub async fn subscribe(
        &mut self,
        topic: CheetahString,
        sub_expression: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let subscription_data = FilterAPI::build_subscription_data(&topic, &sub_expression);
        if let Err(e) = subscription_data {
            return Err(mq_client_err!(format!("buildSubscriptionData exception, {}", e)));
        }
        let subscription_data = subscription_data.unwrap();
        self.rebalance_impl
            .put_subscription_data(topic, subscription_data)
            .await;
        if let Some(ref mut client_instance) = self.client_instance {
            client_instance.send_heartbeat_to_all_broker_with_lock().await;
        }
        Ok(())
    }

    /// Hand a continuation straight to the shared pull service. An immediate continuation records
    /// no wait expectation (plan 49d R1): its next attempt should be admitted on the service's
    /// own tick, so if that never happens the queue stays diagnosable as an idle lease instead
    /// of being declared healthy by a timer nobody owes.
    pub async fn execute_pull_request_immediately(&mut self, pull_request: PullRequest) {
        match self.dispatch_pull_request(pull_request, PULL_ADMISSION_ALLOWANCE).await {
            crate::consumer::consumer_impl::pull_message_service::Admission::Accepted => {}
            crate::consumer::consumer_impl::pull_message_service::Admission::TimedOut => warn!(
                "the shared pull service had no room for this queue's next request inside its allowance, leaving the \
                 queue to be re-dispatched"
            ),
            other => debug!(?other, "this pull request was not admitted to the shared pull service"),
        }
    }

    /// Hand one request to the shared pull service under this consumer's own bound (plan 49f
    /// F2/F4).
    ///
    /// Queue ownership and channel capacity are separate conditions: the lease says who may run the
    /// next pull, and finding room in a channel shared by every consumer on this client instance
    /// says nothing about it. Admission is therefore bounded and cancellable, and an accepted
    /// request records its queued expectation before anyone can observe it.
    ///
    /// Consumers that do not use SDK-owned assignment keep the unbounded hand-off: nothing tracks
    /// their queues, so there is no expectation to record and no controller to re-dispatch a
    /// dropped request.
    async fn dispatch_pull_request(
        &self,
        pull_request: PullRequest,
        budget: std::time::Duration,
    ) -> crate::consumer::consumer_impl::pull_message_service::Admission {
        use crate::consumer::consumer_impl::pull_message_service::Admission;
        if self.work.is_closed() {
            return Admission::Closed;
        }
        let Some(service) = self
            .client_instance
            .as_ref()
            .map(|instance| instance.pull_message_service.clone())
        else {
            return Admission::Closed;
        };
        if !self.managed_lifecycle.load(Ordering::Acquire) {
            service.execute_pull_request_immediately(pull_request).await;
            return Admission::Accepted;
        }
        let cancellation = self.work.cancellation();
        service
            .admit_pull_request(pull_request, tokio::time::Instant::now() + budget, Some(&cancellation))
            .await
    }
    pub fn execute_pull_request_later(&mut self, pull_request: PullRequest, time_delay: u64) {
        if self.work.is_closed() {
            return;
        }
        // Plan 49e E1: a continuation is owed by the acquisition that issued it. A request whose
        // lease outlived its acquisition must not schedule a wait for - or clear the outstanding
        // attempt of - the queue's current owner, so it is dropped here instead. Dropping releases
        // its lease, and release is epoch-checked too, so the current owner keeps its own.
        if let Some(epoch) = pull_request.lease_epoch() {
            if !self.progress.is_current(pull_request.get_message_queue(), epoch) {
                debug!(
                    queue = %pull_request.get_message_queue(),
                    "this pull continuation belongs to an expired acquisition, dropping it instead of rescheduling work the current owner already holds"
                );
                return;
            }
        }
        self.progress.note_retry_scheduled();
        // The delay this schedules **is** the queue's own expectation (plan 49d R1 item 3):
        // suspension, flow control, broker-busy and error continuations all arrive here, so
        // the controller sees a deliberate wait that is healthy until its own due time
        // passes instead of an RPC that never finished.
        self.progress.note_queue_wait_for(
            pull_request.get_message_queue(),
            pull_request.lease_epoch(),
            std::time::Duration::from_millis(time_delay),
        );
        // A delayed retry is dispatched by the shared pull service, so its timer belongs to the
        // client instance's runtime rather than to whichever task happened to schedule it (F2).
        let Some(client_instance) = self.client_instance.as_ref() else {
            return;
        };
        let handle = client_instance.runtime_handle();
        let service = client_instance.pull_message_service.clone();
        let work = self.work.clone();
        // An expired retry timer is dispatched like any other request: once its delay has passed it
        // needs room in the shared channel, and that wait is bounded and cancellable too
        // (plan 49f F2 item 3).
        let managed = self.managed_lifecycle.load(Ordering::Acquire);
        self.work.spawn(&handle, async move {
            tokio::time::sleep(std::time::Duration::from_millis(time_delay)).await;
            if !managed {
                service.execute_pull_request_immediately(pull_request).await;
                return;
            }
            let cancellation = work.cancellation();
            match service
                .admit_pull_request(
                    pull_request,
                    tokio::time::Instant::now() + PULL_ADMISSION_ALLOWANCE,
                    Some(&cancellation),
                )
                .await
            {
                crate::consumer::consumer_impl::pull_message_service::Admission::Accepted => {}
                other => debug!(
                    ?other,
                    "a scheduled pull continuation was not admitted to the shared pull service",
                ),
            }
        });
    }

    pub async fn execute_pop_request_immediately(&mut self, pop_request: PopRequest) {
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pop_pull_request_immediately(pop_request)
            .await;
    }
    pub fn execute_pop_request_later(&mut self, pop_request: PopRequest, time_delay: u64) {
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pop_pull_request_later(pop_request, time_delay);
    }

    pub(crate) async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        if let Some(consume_message_service) = self.consume_message_service.as_ref() {
            Some(consume_message_service.consume_message_directly(msg, broker_name).await)
        } else if let Some(consume_message_pop_service) = self.consume_message_pop_service.as_ref() {
            Some(
                consume_message_pop_service
                    .consume_message_directly(msg, broker_name)
                    .await,
            )
        } else {
            None
        }
    }

    pub(crate) async fn pop_message(&mut self, pop_request: PopRequest) {
        let Some(_work) = self.work.enter() else { return };
        self.pop_used = true;
        let process_queue = pop_request.get_pop_process_queue();
        if process_queue.is_dropped() {
            info!("the pop request[{}] is dropped.", pop_request);
            return;
        }
        process_queue.set_last_pop_timestamp(get_current_millis());

        if let Err(e) = self.make_sure_state_ok() {
            warn!("pop_message exception, consumer state not ok {}", e);
            if !self.is_terminal_state() {
                self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            }
            return;
        }

        if self.pause.load(Ordering::Acquire) {
            warn!(
                "consumer was paused, execute pull request later. instanceName={}, group={}",
                self.client_config.instance_name, self.consumer_config.consumer_group
            );
            self.execute_pop_request_later(pop_request, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        if process_queue.get_wai_ack_msg_count() > self.consumer_config.pop_threshold_for_queue as usize {
            self.execute_pop_request_later(pop_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            return;
        }

        let subscription_data = self
            .rebalance_impl
            .get_subscription_inner()
            .read()
            .await
            .get(pop_request.get_message_queue().get_topic_cs())
            .cloned();
        if subscription_data.is_none() {
            self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            warn!("find the consumer's subscription failed");
            return;
        }
        let subscription_data = subscription_data.unwrap();
        let expression_type = subscription_data.expression_type.clone();
        let sub_string = subscription_data.sub_string.clone();
        let mut invisible_time = self.consumer_config.pop_invisible_time;
        if !(MIN_POP_INVISIBLE_TIME..=MAX_POP_INVISIBLE_TIME).contains(&invisible_time) {
            invisible_time = 60_000;
        }
        let mq = pop_request.get_message_queue().clone();
        let consumer_group = pop_request.get_consumer_group().clone();
        let init_mode = pop_request.get_init_mode();
        let this = self.default_mqpush_consumer_impl.clone().unwrap();

        match self
            .pull_api_wrapper
            .as_mut()
            .unwrap()
            .pop_async(
                pop_request.get_message_queue(),
                invisible_time,
                self.consumer_config.pop_batch_nums,
                consumer_group,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                DefaultPopCallback {
                    push_consumer_impl: this,
                    message_queue_inner: Some(mq),
                    subscription_data: Some(subscription_data),
                    pop_request: Some(pop_request.clone()),
                },
                true,
                init_mode,
                false,
                expression_type,
                sub_string,
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                error!("popAsync error: {}", err);
                self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            }
        }
    }

    /// Takes (or keeps) the single in-flight pull lease for this request's queue.
    ///
    /// A request that already carries the current acquisition's lease continues - a delayed retry
    /// is the same loop, not new work. Any other request for that queue is refused while the
    /// loop runs, which is what stops initial dispatch, retries and repair from stacking
    /// duplicate pulls behind one live long poll. Refusal simply drops the request; consumers
    /// that do not use SDK-owned assignment tracking keep their existing behaviour unchanged
    /// (plan 49a F4 section 4.3 item 3).
    fn admit_pull_request(&self, pull_request: &mut PullRequest) -> bool {
        if !self.managed_lifecycle.load(Ordering::Acquire) {
            return true;
        }
        let queue = pull_request.get_message_queue();
        match (pull_request.lease_epoch(), self.progress.current_epoch(queue)) {
            (Some(leased), Some(current)) => leased == current,
            (None, Some(_)) => match self.progress.acquire_pull_lease(queue) {
                Some(lease) => {
                    pull_request.hold_lease(lease);
                    true
                }
                None => false,
            },
            // Not assigned to this incarnation any more: end the loop instead of pulling a stranger's queue.
            _ => false,
        }
    }

    /// Ends the RPC attempt one request admitted, if it recorded one (plan 49d R1). Called by every
    /// path that owns an issued attempt without a callback answering it - issuance failure above
    /// all - and by the pull callback on answer, failure and cancellation. Ownership identity
    /// is checked in [`ConsumerProgress::finish_pull_attempt`], so this can never retire a
    /// newer attempt.
    pub(crate) fn finish_pull_attempt(&self, pull_request: &PullRequest) {
        let attempt_id = pull_request.attempt_id();
        if attempt_id == 0 {
            return;
        }
        if let Some(epoch) = pull_request.lease_epoch() {
            self.progress
                .finish_pull_attempt(pull_request.get_message_queue(), epoch, attempt_id);
        }
    }

    /// Whether work issued under `epoch` may still touch `queue` (plan 49a F4 section 4.3 item 2).
    /// An answer or retry that outlived its acquisition must not publish into the queue a later
    /// owner holds.
    pub(crate) fn holds_acquisition(&self, queue: &MessageQueue, epoch: Option<u64>) -> bool {
        if !self.managed_lifecycle.load(Ordering::Acquire) {
            return true;
        }
        match epoch {
            Some(epoch) => self.progress.is_current(queue, epoch),
            // Nothing tracked this request's acquisition (legacy path): leave it alone.
            None => true,
        }
    }

    /// Re-dispatch one pull per queue whose loop was lost (plan 49a F4 section 4.3 item 5), through
    /// the same pull service as any other request and therefore under that acquisition's lease,
    /// resuming at the offset this consumer has actually reached rather than at a guessed
    /// position. Queues no longer owned here are skipped: assignment belongs to rebalance,
    /// which will place them again.
    ///
    /// Re-dispatch the pulls this consumer lost, spending at most `slice` of wall time (plan 49c
    /// C2, plan 49e E4). A round that ran out of budget reports where it stopped and the next one
    /// continues strictly after that queue, so neither a slow first queue nor an exhausted slice
    /// repeats. Every examined queue advances the position - preparation failure and already-owned
    /// skips included - and each prepared request is dispatched as soon as it is revalidated, so a
    /// later queue's I/O cannot strand earlier work.
    ///
    /// Each queue's step carries one absolute deadline covering its locks, nameserver lookup,
    /// broker query and admin fallback: a step that cannot answer inside it leaves the queue
    /// exactly as lost as it was and ends the round, instead of waiting indefinitely or
    /// spending every later slice on one stalled broker. Preparation is read-only - no local
    /// write owns anything here - so ending a step early neither loses nor proves local
    /// persistence. `prep_allowance` is that whole-step budget, the same allowance as an offset
    /// query: the first examined queue may use all of it even when the slice has run out, so a
    /// slow but live broker can still answer; later queues get what is left.
    ///
    /// The lease is claimed *before* the offset is prepared and revalidated afterwards, so that one
    /// lost queue cannot accumulate queued repairs while its preparation waits on an offset store
    /// or a broker, and so that a repair whose acquisition disappeared during preparation -
    /// revoked, or replaced by another process queue under the same desired allocation - never
    /// dispatches against work it does not own (plan 49c C1). A queue whose starting offset
    /// cannot be determined is left stalled rather than pulled from an invalid position:
    /// leaving it untouched keeps the fault visible, and the next bounded repair pass tries
    /// again.
    ///
    /// A queue whose lease is held by a loop whose own attempt deadline expired is **not** stolen:
    /// this path skips it because that loop still owns its next pull (plan 49d R1). Such an RPC
    /// is drained or cancelled by the ownership rules that already apply to it, and if it never
    /// answers the controller escalates through replacement, which stops the incarnation
    /// instead of stacking a second pull on the same queue.
    pub(crate) async fn repair_lost_pulls(
        &mut self,
        after: Option<CheetahString>,
        stall_after: std::time::Duration,
        pull_deadline: std::time::Duration,
        prep_allowance: std::time::Duration,
        slice: std::time::Duration,
    ) -> LostPullRound {
        use crate::consumer::consumer_impl::pull_message_service::Admission;
        let started = std::time::Instant::now();
        // Deterministic order, and the same key names where an unfinished round stopped (plan 49e
        // E3): continuing strictly after that queue is what keeps one slow or broken broker from
        // spending every slice while the queues behind it never get a step.
        let mut lost: Vec<(CheetahString, MessageQueue)> = self
            .progress
            .lost_pulls(stall_after, pull_deadline)
            .into_iter()
            .map(|mq| (lost_pull_cursor_key(&mq), mq))
            .collect();
        if lost.is_empty() {
            return LostPullRound::Complete { dispatched: 0 };
        }
        lost.sort_by(|left, right| left.0.cmp(&right.0));
        // A resumed round works from a fresh fault list: queues repaired or revoked since are
        // simply gone, and anything that appeared at or before the cursor waits for the
        // next request.
        let pending: Vec<(CheetahString, MessageQueue)> = match after.as_ref() {
            Some(done) => lost
                .into_iter()
                .filter(|(key, _)| key.as_str() > done.as_str())
                .collect(),
            None => lost,
        };
        if pending.is_empty() {
            // Nothing remains behind the cursor. Reporting completion is what lets the controller
            // re-evaluate fresh evidence - and charge a fruitless attempt - instead of spinning
            // inside this kind; a new repair request starts again from the beginning.
            return LostPullRound::Complete { dispatched: 0 };
        }
        // Snapshot the owned queues first and release the table before any I/O: holding that read
        // guard across a broker round trip would block rebalance from updating the same table.
        let owned: Vec<_> = {
            let table = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            pending
                .into_iter()
                .filter_map(|(key, mq)| {
                    let pq = table.get(&mq)?.clone();
                    (!pq.is_dropped()).then_some((key, mq, pq))
                })
                .collect()
        };
        if owned.is_empty() {
            return LostPullRound::Complete { dispatched: 0 };
        }
        // The last queue this round examined. The next one continues strictly after it, which is
        // how a queue whose own step timed out gets skipped instead of repeating every
        // slice. Every examined queue advances it - preparation failure and already-owned
        // skips included.
        let mut cursor: Option<CheetahString> = None;
        let mut unfinished = false;
        let mut dispatched = 0usize;
        for (index, (key, mq, pq)) in owned.into_iter().enumerate() {
            if index > 0 && started.elapsed() >= slice {
                // Stop between queues rather than start another lookup on an exhausted round.
                unfinished = true;
                break;
            }
            cursor = Some(key);
            // One repair per acquisition at a time: whoever holds the queue's lease already owns
            // its next pull, and this repair has nothing to add.
            let Some(lease) = self.progress.acquire_pull_lease(&mq) else {
                debug!(
                    queue = %mq,
                    "a pull loop or repair already owns this queue, leaving the lost-pull repair to it"
                );
                continue;
            };
            let epoch = lease.epoch();
            // Every part of this queue's step - the offset read and the consumption-point lookup
            // both wait on the nameserver - carries an absolute deadline (plan 49e E3). A queue
            // that cannot answer inside its step is left lost and handed to a later
            // round *after* it, so one stalled broker neither spends the rest of this
            // slice nor starves what is behind it. The first queue may use its whole preparation
            // allowance; later ones get what is left of the slice.
            let remaining = slice.saturating_sub(started.elapsed());
            let budget = if index == 0 {
                remaining.max(prep_allowance)
            } else {
                remaining.max(LOST_PULL_STEP_MINIMUM)
            };
            let prepared = tokio::time::timeout(budget, self.prepare_repair_offset(&mq)).await;
            let Ok(next_offset) = prepared else {
                warn!(
                    queue = %mq,
                    "this queue's offset preparation did not answer inside its step, leaving it lost and continuing with the queues behind it"
                );
                unfinished = true;
                // Dropping the lease leaves this queue exactly as lost as it was.
                break;
            };
            // Preparation awaited on other tasks: the acquisition may have ended, and the table may
            // hold a different process queue under this key.
            let still_ours = self.progress.is_current(&mq, epoch) && {
                let table = self
                    .rebalance_impl
                    .rebalance_impl_inner
                    .process_queue_table
                    .read()
                    .await;
                matches!(table.get(&mq), Some(current) if Arc::ptr_eq(current, &pq))
            };
            if !still_ours {
                debug!(
                    queue = %mq,
                    epoch,
                    "this queue's acquisition ended while its offset was prepared, dropping the repair"
                );
                continue;
            }
            if next_offset < 0 {
                warn!(
                    queue = %mq,
                    "cannot determine where to resume this queue, leaving it stalled for the next bounded repair"
                );
                continue;
            }
            // Dispatched as soon as it is prepared and revalidated, so a later queue's I/O can
            // never strand work this round already earned. The lease travels with the
            // request and whichever path finishes that pull releases it.
            if let Some(service) = self
                .client_instance
                .as_ref()
                .map(|instance| instance.pull_message_service.clone())
            {
                // Handing the request to the shared service is **not** part of owning this queue's
                // lease (plan 49f F4): the channel belongs to every consumer on the client
                // instance, so waiting for room may not spend the rest of this
                // round. One step per queue, and a request that finds no room is
                // dropped - which releases only its own lease and leaves
                // the queue exactly as lost as it was.
                let remaining = slice.saturating_sub(started.elapsed());
                let admission_budget = if index == 0 {
                    remaining.max(prep_allowance)
                } else {
                    remaining.max(LOST_PULL_STEP_MINIMUM)
                };
                // Kept for the logs below: the request takes ownership of the queue it resumes.
                let repair_queue = mq.clone();
                let mut request = PullRequest::new(self.consumer_config.consumer_group.clone(), mq, pq, next_offset);
                request.hold_lease(lease);
                debug!("re-dispatching a queue whose pull loop was lost");
                let cancellation = self.work.cancellation();
                match service
                    .admit_pull_request(
                        request,
                        tokio::time::Instant::now() + admission_budget,
                        Some(&cancellation),
                    )
                    .await
                {
                    Admission::Accepted => dispatched += 1,
                    Admission::Superseded => debug!(
                        queue = %repair_queue,
                        "this queue's acquisition ended while its repair waited for pull-service room, dropping it"
                    ),
                    other => {
                        // Room did not open in this queue's step, or the consumer/service ended.
                        // Either way the round stops here as deferred:
                        // continuing would spend later queues' steps on the
                        // same exhausted capacity, and a fresh sweep reaches this one again.
                        warn!(
                            queue = %repair_queue,
                            ?other,
                            "this queue's repair was not admitted to the shared pull service inside its step, leaving it lost"
                        );
                        unfinished = true;
                        break;
                    }
                }
            }
        }
        if unfinished {
            debug!(
                ?cursor,
                "lost-pull repair reached its step budget, continuing after this queue on the next status round"
            );
            if let Some(after) = cursor {
                return LostPullRound::Incomplete { after };
            }
        }
        LostPullRound::Complete { dispatched }
    }

    /// Where a lost pull resumes from: the offset this consumer has committed, or - when nothing is
    /// committed - the configured consumption point. Both lookups can wait on the nameserver, so
    /// this is one step its caller bounds rather than an open-ended await (plan 49e E3).
    async fn prepare_repair_offset(&mut self, mq: &MessageQueue) -> i64 {
        let commit_offset = match &mut self.offset_store {
            Some(store) => store.read_offset(mq, ReadOffsetType::MemoryFirstThenStore).await,
            None => -1,
        };
        // Nothing committed and nothing consumed yet: start where this consumer would have started
        // on first assignment rather than from an offset nobody has agreed on.
        if commit_offset >= 0 {
            return commit_offset;
        }
        Rebalance::compute_pull_from_where(self.rebalance_impl.as_mut(), mq).await
    }

    pub(crate) async fn pull_message(&mut self, mut pull_request: PullRequest) {
        let Some(_work) = self.work.enter() else { return };
        //let process_queue = pull_request.get_process_queue_mut();
        if pull_request.process_queue.is_dropped() {
            info!("the pull request[{}] is dropped.", pull_request);
            return;
        }
        // One in-flight pull per queue (plan 49a F4 section 4.3 item 3). Checked before any
        // observation is recorded, so a refused duplicate never looks like pull progress on
        // its own.
        if !self.admit_pull_request(&mut pull_request) {
            debug!(
                "another pull loop owns {} or the queue is no longer assigned, dropping this request",
                pull_request.get_message_queue()
            );
            return;
        }
        self.progress.note_pull_started(pull_request.get_message_queue());
        if let Err(e) = self.make_sure_state_ok() {
            if !self.is_terminal_state() {
                self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            }
            return;
        }
        if self.pause.load(Ordering::Acquire) {
            warn!(
                "consumer was paused, execute pull request later. instanceName={}, group={}",
                self.client_config.instance_name, self.consumer_config.consumer_group
            );
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        let cached_message_count = pull_request.process_queue.msg_count();
        let cached_message_size_in_mib = pull_request.process_queue.msg_size() / _1MB;
        if cached_message_count > self.consumer_config.pull_threshold_for_queue as u64 {
            if self.queue_flow_control_times % 1000 == 0 {
                let msg_tree_map = pull_request.process_queue.msg_tree_map.read().await;
                let first_key_value = msg_tree_map.first_key_value().unwrap();
                let last_key_value = msg_tree_map.last_key_value().unwrap();
                warn!(
                    "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, \
                     maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    self.consumer_config.pull_threshold_for_queue,
                    first_key_value.0,
                    last_key_value.0,
                    cached_message_count,
                    cached_message_size_in_mib,
                    pull_request.to_string(),
                    self.queue_flow_control_times
                );
            }
            self.progress.note_flow_control();
            pull_request.process_queue.mark_flow_control();
            let stall_ms = pull_request.process_queue.flow_control_stall_ms();
            if stall_ms > *crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME / 2 {
                warn!(
                    "[STALL] queue flow-controlled by cached-count for {}ms, topic={}, queueId={}, count={}",
                    stall_ms,
                    pull_request.get_message_queue().get_topic(),
                    pull_request.get_message_queue().get_queue_id(),
                    cached_message_count,
                );
            }
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);

            self.queue_flow_control_times += 1;
            return;
        }

        if cached_message_size_in_mib > self.consumer_config.pull_threshold_size_for_queue as u64 {
            if self.queue_flow_control_times % 1000 == 0 {
                let msg_tree_map = pull_request.process_queue.msg_tree_map.read().await;
                let first_key_value = msg_tree_map.first_key_value().unwrap();
                let last_key_value = msg_tree_map.last_key_value().unwrap();
                warn!(
                    "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, \
                     maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    self.consumer_config.pull_threshold_size_for_queue,
                    first_key_value.0,
                    last_key_value.0,
                    cached_message_count,
                    cached_message_size_in_mib,
                    pull_request.to_string(),
                    self.queue_flow_control_times
                );
            }
            pull_request.process_queue.mark_flow_control();
            let stall_ms = pull_request.process_queue.flow_control_stall_ms();
            if stall_ms > *crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME / 2 {
                warn!(
                    "[STALL] queue flow-controlled by cached-size for {}ms, topic={}, queueId={}, size_mib={}",
                    stall_ms,
                    pull_request.get_message_queue().get_topic(),
                    pull_request.get_message_queue().get_queue_id(),
                    cached_message_size_in_mib,
                );
            }
            self.progress.note_flow_control();
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            self.queue_flow_control_times += 1;
            return;
        }

        if !self.consume_orderly {
            let max_span = pull_request.process_queue.get_max_span().await;
            if max_span > self.consumer_config.consume_concurrently_max_span as u64 {
                if self.queue_max_span_flow_control_times % 1000 == 0 {
                    let msg_tree_map = pull_request.process_queue.msg_tree_map.read().await;
                    let first_key_value = msg_tree_map.first_key_value().unwrap();
                    let last_key_value = msg_tree_map.last_key_value().unwrap();
                    warn!(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, \
                         maxSpan={}, pullRequest={}, flowControlTimes={}",
                        first_key_value.0,
                        last_key_value.0,
                        max_span,
                        pull_request.to_string(),
                        self.queue_max_span_flow_control_times
                    );
                }
                self.queue_max_span_flow_control_times += 1;
                self.progress.note_flow_control();
                pull_request.process_queue.mark_flow_control();
                let stall_ms = pull_request.process_queue.flow_control_stall_ms();
                if stall_ms > *crate::consumer::consumer_impl::PULL_MAX_IDLE_TIME / 2 {
                    warn!(
                        "[STALL] queue flow-controlled by max-span for {}ms, topic={}, queueId={}, span={}",
                        stall_ms,
                        pull_request.get_message_queue().get_topic(),
                        pull_request.get_message_queue().get_queue_id(),
                        max_span,
                    );
                }
                self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
                return;
            }
        } else if pull_request.get_process_queue().is_locked() {
            if !pull_request.is_previously_locked() {
                let offset = match self
                    .rebalance_impl
                    .compute_pull_from_where_with_exception(pull_request.get_message_queue())
                    .await
                {
                    Ok(value) => {
                        if value < 0 {
                            error!("Failed to compute pull offset, pullResult: {}", pull_request);
                            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);

                            return;
                        }
                        value
                    }
                    Err(e) => {
                        error!("Failed to compute pull offset, pullResult: {}, {}", pull_request, e);
                        self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);

                        return;
                    }
                };
                let broker_busy = offset < pull_request.get_next_offset();
                info!(
                    "the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} \
                     brokerBusy: {}",
                    pull_request.to_string(),
                    offset,
                    broker_busy
                );
                if broker_busy {
                    warn!(
                        "[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume \
                         offset. pullRequest: {} NewOffset: {}",
                        pull_request.to_string(),
                        offset
                    )
                }
                pull_request.set_previously_locked(true);
                pull_request.set_next_offset(offset);
            }
        } else {
            info!("pull message later because not locked in broker, {}", pull_request);
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        }
        let message_queue = pull_request.get_message_queue().clone();
        let inner = self.rebalance_impl.get_subscription_inner();
        let guard = inner.read().await;
        let subscription_data = guard.get(message_queue.get_topic()).cloned();

        if subscription_data.is_none() {
            error!(
                "find the consumer's subscription failed, {}, {}",
                message_queue, self.consumer_config.consumer_group
            );
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        }
        // Update liveness timestamp only after passing all flow-control checks and just before
        // issuing the actual broker pull.  Updating it on entry (as it was before) caused every
        // 50 ms flow-control retry to refresh the timestamp, preventing is_pull_expired() from
        // ever triggering rebalance recovery (F1 in plan/29).
        pull_request.process_queue.set_last_pull_timestamp(get_current_millis());
        let begin_timestamp = Instant::now();
        let topic = message_queue.get_topic().to_string();

        let message_queue_inner = message_queue.clone();
        let next_offset = pull_request.next_offset;
        let mut commit_offset_enable = false;
        let mut commit_offset_value = 0;
        if MessageModel::Clustering == self.consumer_config.message_model {
            commit_offset_value = self
                .offset_store
                .as_ref()
                .unwrap()
                .read_offset(&message_queue, ReadOffsetType::ReadFromMemory)
                .await;
            if commit_offset_value > 0 {
                commit_offset_enable = true;
            }
        }
        let mut sub_expression = None;
        let mut class_filter = false;
        if let Some(subscription_data) = subscription_data.as_ref() {
            if self.consumer_config.post_subscription_when_pull && !subscription_data.class_filter_mode {
                sub_expression = Some(subscription_data.sub_string.clone());
            }
            class_filter = subscription_data.class_filter_mode
        }
        let sys_flag = PullSysFlag::build_sys_flag(commit_offset_enable, true, sub_expression.is_some(), class_filter);
        let subscription_data = subscription_data.unwrap();
        let this = self.default_mqpush_consumer_impl.clone().unwrap();
        let Some(work) = self.work.enter() else { return };
        // Plan 49d R1: **this** is where an attempt reaches the transport, after state, pause and
        // every flow-control check, so its answer window starts here and nowhere earlier.
        // Loop ownership (the lease) is untouched; a completed pull retires only its own
        // attempt.
        if let Some(epoch) = pull_request.lease_epoch() {
            match self.progress.begin_pull_attempt(
                &message_queue,
                epoch,
                std::time::Duration::from_millis(CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND + PULL_ANSWER_ALLOWANCE_MILLIS),
            ) {
                Some(attempt_id) => pull_request.set_attempt_id(attempt_id),
                None => {
                    debug!(
                        queue = %message_queue,
                        "this queue's acquisition ended before its pull was admitted, dropping the request"
                    );
                    return;
                }
            }
        }
        let result = self
            .pull_api_wrapper
            .as_mut()
            .unwrap()
            .pull_kernel_impl(
                &message_queue,
                sub_expression.unwrap_or_default(),
                subscription_data.expression_type.clone(),
                subscription_data.sub_version,
                next_offset,
                self.consumer_config.pull_batch_size as i32,
                self.consumer_config.pull_batch_size_in_bytes as i32,
                sys_flag as i32,
                commit_offset_value,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                CommunicationMode::Async,
                DefaultPullCallback {
                    work,
                    push_consumer_impl: this,
                    message_queue_inner: Some(message_queue_inner),
                    subscription_data: Some(subscription_data),
                    pull_request: Some(pull_request.clone()),
                },
            )
            .await;

        if let Err(e) = result {
            error!(
                "pullKernelImpl exception, {}, cost: {}ms, {}",
                pull_request,
                begin_timestamp.elapsed().as_millis(),
                e
            );
            // The attempt was never handed to anyone who could answer it, so this issuance path
            // ends it; leaving it open would show an outstanding RPC that no callback
            // can retire (plan 49d R1).
            self.finish_pull_attempt(&pull_request);
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
        }
    }

    pub(crate) async fn process_pop_result(
        &mut self,
        mut pop_result: PopResult,
        subscription_data: &SubscriptionData,
    ) -> PopResult {
        if pop_result.pop_status == PopStatus::Found {
            let msg_vec = pop_result.msg_found_list.take().unwrap_or_default();
            let msg_list_filter_again =
                if !subscription_data.tags_set.is_empty() && !subscription_data.class_filter_mode {
                    let mut msg_vec_again = Vec::with_capacity(msg_vec.len());
                    for msg in msg_vec {
                        if let Some(ref tag) = msg.get_tags() {
                            if subscription_data.tags_set.contains(tag.as_str()) {
                                msg_vec_again.push(msg);
                            }
                        }
                    }
                    msg_vec_again
                } else {
                    msg_vec
                };
            if !self.filter_message_hook_list.is_empty() {
                let context = FilterMessageContext {
                    unit_mode: self.consumer_config.unit_mode,
                    msg_list: &msg_list_filter_again,
                    ..Default::default()
                };
                for hook in self.filter_message_hook_list.iter() {
                    hook.filter_message(&context);
                }
            }
            let mut final_msg_list = Vec::with_capacity(msg_list_filter_again.len());
            for msg in msg_list_filter_again {
                if msg.reconsume_times > self.get_max_reconsume_times() {
                    let consumer_group = self.consumer_config.consumer_group().clone();
                    self.ack_async(&msg, &consumer_group).await;
                } else {
                    final_msg_list.push(msg);
                }
            }
            pop_result.msg_found_list = Some(final_msg_list);
        }
        pop_result
    }

    #[inline]
    fn make_sure_state_ok(&self) -> rocketmq_error::RocketMQResult<()> {
        if *self.service_state != ServiceState::Running {
            return Err(mq_client_err!(format!(
                "The consumer service state not OK, {},{}",
                *self.service_state,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
            )));
        }
        Ok(())
    }

    #[inline]
    fn is_terminal_state(&self) -> bool {
        matches!(
            *self.service_state,
            ServiceState::StartFailed | ServiceState::ShutdownAlready
        )
    }

    pub(crate) async fn correct_tags_offset(&mut self, pull_request: &PullRequest) {
        if pull_request.process_queue.msg_count() == 0 {
            self.offset_store
                .as_mut()
                .unwrap()
                .update_offset(pull_request.get_message_queue(), pull_request.next_offset, true)
                .await;
        }
    }

    pub fn try_reset_pop_retry_topic(msgs: &mut [ArcMut<MessageExt>], consumer_group: &str) {
        let pop_retry_prefix = format!("{}{}_{}", mix_all::RETRY_GROUP_TOPIC_PREFIX, consumer_group, "_");
        for msg in msgs.iter_mut() {
            if msg.get_topic().starts_with(&pop_retry_prefix) {
                let normal_topic = KeyBuilder::parse_normal_topic(msg.get_topic(), consumer_group);

                if !normal_topic.is_empty() {
                    msg.set_topic(CheetahString::from_string(normal_topic));
                }
            }
        }
    }

    pub fn reset_retry_and_namespace(&mut self, msgs: &mut [ArcMut<MessageExt>], consumer_group: &str) {
        let group_topic = mix_all::get_retry_topic(consumer_group);
        let namespace = self.client_config.get_namespace().unwrap_or_default();
        for msg in msgs.iter_mut() {
            if let Some(retry_topic) =
                msg.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC))
            {
                if group_topic == msg.get_topic().as_str() {
                    msg.set_topic(retry_topic);
                }
            }

            if !namespace.is_empty() {
                let topic = msg.get_topic().to_string();
                msg.set_topic(CheetahString::from_string(
                    NamespaceUtil::without_namespace_with_namespace(topic.as_str(), namespace.as_str()),
                ));
            }
        }
    }

    #[inline]
    pub fn has_hook(&self) -> bool {
        !self.consume_message_hook_list.is_empty()
    }

    pub fn execute_hook_before(&self, context: &mut Option<ConsumeMessageContext>) {
        for hook in self.consume_message_hook_list.iter() {
            hook.consume_message_before(context.as_mut());
        }
    }

    pub fn execute_hook_after(&self, context: &mut Option<ConsumeMessageContext>) {
        for hook in self.consume_message_hook_list.iter() {
            hook.consume_message_after(context.as_mut());
        }
    }

    pub async fn send_message_back(
        &mut self,
        msg: &mut MessageExt,
        delay_level: i32,
        mq: &MessageQueue,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.send_message_back_with_broker_name(msg, delay_level, None, Some(mq))
            .await
    }
    pub async fn send_message_back_with_broker_name(
        &mut self,
        msg: &mut MessageExt,
        delay_level: i32,
        broker_name: Option<CheetahString>,
        mq: Option<&MessageQueue>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let need_retry = true;
        if broker_name.is_some()
            && broker_name
                .as_ref()
                .unwrap()
                .starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
            && mq.is_some()
            && mq
                .unwrap()
                .get_broker_name()
                .starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
        {
            let _ = self.send_message_back_as_normal_message(msg).await;
        } else {
            let broker_addr = if let Some(ref broker_name_) = broker_name {
                self.client_instance
                    .as_mut()
                    .unwrap()
                    .find_broker_address_in_publish(broker_name_.as_ref())
                    .await
                    .unwrap()
            } else {
                CheetahString::from_string(msg.store_host.to_string())
            };
            let max_consume_retry_times = self.get_max_reconsume_times();
            let result = self
                .client_instance
                .as_mut()
                .unwrap()
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .consumer_send_message_back(
                    broker_addr.as_str(),
                    broker_name.as_ref().unwrap().as_str(),
                    msg,
                    self.consumer_config.consumer_group.as_str(),
                    delay_level,
                    5000,
                    max_consume_retry_times,
                )
                .await;
            if let Err(e) = result {
                error!("send message back error: {}", e);

                self.send_message_back_as_normal_message(msg).await?;
            }
        }
        msg.set_topic(CheetahString::from_string(
            NamespaceUtil::without_namespace_with_namespace(
                msg.get_topic().as_str(),
                self.client_config.get_namespace().unwrap_or_default().as_str(),
            ),
        ));
        Ok(())
    }

    async fn send_message_back_as_normal_message(&mut self, msg: &MessageExt) -> rocketmq_error::RocketMQResult<()> {
        let topic = mix_all::get_retry_topic(self.consumer_config.consumer_group());
        let body = msg.get_body().cloned();
        let mut new_msg = Message::new_body(topic.as_str(), body);
        let origin_msg_id = MessageAccessor::get_origin_message_id(&new_msg).unwrap_or(msg.msg_id.clone());
        MessageAccessor::set_origin_message_id(&mut new_msg, origin_msg_id);
        new_msg.set_flag(msg.get_flag());
        MessageAccessor::set_properties(&mut new_msg, msg.get_properties().clone());
        MessageAccessor::put_property(
            &mut new_msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            msg.get_topic().to_owned(),
        );
        MessageAccessor::set_reconsume_time(
            &mut new_msg,
            CheetahString::from_string((msg.reconsume_times + 1).to_string()),
        );
        MessageAccessor::set_max_reconsume_times(
            &mut new_msg,
            CheetahString::from_string(self.get_max_reconsume_times().to_string()),
        );
        MessageAccessor::clear_property(&mut new_msg, MessageConst::PROPERTY_TRANSACTION_PREPARED);
        new_msg.set_delay_time_level(3 + msg.reconsume_times);
        self.client_instance
            .as_mut()
            .unwrap()
            .default_producer
            .send(new_msg)
            .await?;
        Ok(())
    }

    pub fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            16
        } else {
            self.consumer_config.max_reconsume_times
        }
    }

    pub(crate) async fn ack_async(&mut self, message: &MessageExt, consumer_group: &CheetahString) {
        let extra_info = message
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
            .unwrap_or_default();
        let extra_info_strs = ExtraInfoUtil::split(extra_info.as_str());
        /*        if extra_info_strs.is_err() {
            error!("ackAsync error: {}", extra_info_strs.unwrap_err());
            return;
        }
        let extra_info_strs = extra_info_strs.unwrap();*/
        let queue_id = ExtraInfoUtil::get_queue_id(extra_info_strs.as_slice());
        if let Err(e) = queue_id {
            error!("ackAsync error: {}", e);
            return;
        }
        let queue_id = queue_id.unwrap();
        let queue_offset = ExtraInfoUtil::get_queue_offset(extra_info_strs.as_slice());
        if let Err(e) = queue_offset {
            error!("ackAsync error: {}", e);
            return;
        }
        let queue_offset = queue_offset.unwrap();
        let broker_name =
            CheetahString::from(ExtraInfoUtil::get_broker_name(extra_info_strs.as_slice()).unwrap_or_default());
        let topic = message.get_topic();

        let client_instance = self.client_instance.as_mut().unwrap();
        let des_broker_name = if !broker_name.is_empty()
            && broker_name.starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
        {
            let mq =
                self.client_config
                    .queue_with_namespace(MessageQueue::from_parts(topic, broker_name.clone(), queue_id));
            client_instance.get_broker_name_from_message_queue(&mq).await
        } else {
            broker_name.clone()
        };

        let mut find_broker_result = client_instance
            .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
            .await;
        if find_broker_result.is_none() {
            client_instance
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
            find_broker_result = client_instance
                .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
                .await;
        }
        if find_broker_result.is_none() {
            error!("The broker[{}] not exist", des_broker_name);
            return;
        }
        let find_broker_result = find_broker_result.unwrap();
        let request_header = AckMessageRequestHeader {
            consumer_group: consumer_group.clone(),
            topic: CheetahString::from_string(
                ExtraInfoUtil::get_real_topic(extra_info_strs.as_slice(), topic, consumer_group).unwrap_or_default(),
            ),
            queue_id,
            extra_info,
            offset: queue_offset,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
                lo: None,
            }),
        };
        struct DefaultAckCallback;
        impl AckCallback for DefaultAckCallback {
            fn on_success(&self, _ack_result: AckResult) {}

            fn on_exception(&self, _e: Box<dyn Error>) {}
        }
        match client_instance
            .mq_client_api_impl
            .as_mut()
            .unwrap()
            .ack_message_async(
                &find_broker_result.broker_addr,
                request_header,
                ASYNC_TIMEOUT,
                DefaultAckCallback,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("ackAsync error: {}", e);
            }
        }
    }

    pub(crate) async fn change_pop_invisible_time_async(
        &mut self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        extra_info: &CheetahString,
        invisible_time: u64,
        callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        let extra_info_strs = ExtraInfoUtil::split(extra_info);
        let broker_name = CheetahString::from_string(ExtraInfoUtil::get_broker_name(extra_info_strs.as_slice())?);
        let queue_id = ExtraInfoUtil::get_queue_id(extra_info_strs.as_slice())?;
        let des_broker_name = if !broker_name.is_empty()
            && broker_name.starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
        {
            let queue =
                self.client_config
                    .queue_with_namespace(MessageQueue::from_parts(topic, broker_name.clone(), queue_id));
            self.client_instance
                .as_mut()
                .unwrap()
                .get_broker_name_from_message_queue(&queue)
                .await
        } else {
            broker_name.clone()
        };
        let client_instance = self.client_instance.as_mut().unwrap();
        let mut find_broker_result = client_instance
            .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
            .await;
        if find_broker_result.is_none() {
            client_instance
                .update_topic_route_info_from_name_server_default(topic, false, None)
                .await;
            find_broker_result = client_instance
                .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
                .await;
        }

        if find_broker_result.is_none() {
            return Err(mq_client_err!(format!(
                "The broker[{}] not exist",
                des_broker_name.as_str()
            )));
        }
        let request_header = ChangeInvisibleTimeRequestHeader {
            consumer_group: consumer_group.clone(),
            topic: CheetahString::from_string(ExtraInfoUtil::get_real_topic(
                extra_info_strs.as_slice(),
                topic,
                consumer_group,
            )?),
            queue_id,
            extra_info: extra_info.clone(),
            offset: ExtraInfoUtil::get_queue_offset(extra_info_strs.as_slice())?,
            invisible_time: invisible_time as i64,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
                lo: None,
            }),
        };
        let find_broker_result = find_broker_result.unwrap();
        client_instance
            .get_mq_client_api_impl()
            .change_invisible_time_async(
                &broker_name,
                &find_broker_result.broker_addr,
                request_header,
                ASYNC_TIMEOUT,
                callback,
            )
            .await
    }
}

impl MQConsumerInner for DefaultMQPushConsumerImpl {
    fn group_name(&self) -> CheetahString {
        self.consumer_config.consumer_group().clone()
    }

    fn message_model(&self) -> MessageModel {
        self.consumer_config.message_model
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consumer_config.consume_from_where
    }

    async fn subscriptions(&self) -> HashSet<SubscriptionData> {
        let inner = self.rebalance_impl.rebalance_impl_inner.subscription_inner.clone();
        let inner = inner.read().await;
        inner.values().cloned().collect()
    }

    fn do_rebalance(&self) {
        todo!()
    }

    async fn try_rebalance(&self) -> rocketmq_error::RocketMQResult<bool> {
        let Some(_work) = self.work.enter() else {
            return Ok(false);
        };
        if !self.pause.load(Ordering::Acquire) {
            let changed = self
                .rebalance_impl
                .mut_from_ref()
                .do_rebalance(self.consume_orderly)
                .await;
            self.note_route_observation().await;
            return Ok(changed);
        }
        Ok(false)
    }

    async fn persist_consumer_offset(&self) {
        let Some(_work) = self.work.enter() else { return };
        if let Err(err) = self.make_sure_state_ok() {
            error!(
                "group: {} persistConsumerOffset exception:{}",
                self.consumer_config.consumer_group, err
            );
        } else {
            let allocate_mq = {
                let guard = self
                    .rebalance_impl
                    .rebalance_impl_inner
                    .process_queue_table
                    .read()
                    .await;
                guard.keys().cloned().collect::<HashSet<_>>()
            };
            // The pass is cancellable by this consumer alone: stopping must not depend on stopping
            // its siblings or the client's scheduled task, and a write already in
            // flight is never cut off (plan 49a F10).
            let work = self.work.clone();
            let summary = self
                .offset_store
                .as_ref()
                .unwrap()
                .mut_from_ref()
                .persist_all(&allocate_mq, &|| work.is_closed())
                .await;
            if summary.succeeded > 0 {
                self.progress.note_offset_persisted();
            }
        }
    }

    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>) {
        let Some(_work) = self.work.enter() else { return };
        let sub_table = self.rebalance_impl.get_subscription_inner();
        let sub_table_inner = sub_table.read().await;
        if sub_table_inner.contains_key(&topic) {
            let mut guard = self
                .rebalance_impl
                .rebalance_impl_inner
                .topic_subscribe_info_table
                .write()
                .await;
            guard.insert(topic, info.clone());
        }
    }

    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        let sub_table = self.rebalance_impl.get_subscription_inner();
        let sub_table_inner = sub_table.read().await;
        if sub_table_inner.contains_key(topic) {
            drop(sub_table_inner);
            let guard = self
                .rebalance_impl
                .rebalance_impl_inner
                .topic_subscribe_info_table
                .read()
                .await;
            return !guard.contains_key(topic);
        }
        false
    }

    fn is_unit_mode(&self) -> bool {
        self.consumer_config.unit_mode
    }

    async fn consumer_running_info(&self) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo> {
        let _work = self
            .work
            .enter()
            .ok_or_else(|| mq_client_err!("consumer is shutting down"))?;
        // Build the snapshot without blocking the runtime thread. Copy queue entries before
        // awaiting offset reads so rebalance can continue while diagnostics run.
        let mut info = ConsumerRunningInfo::default();
        let ns_addr = self.client_config.get_namesrv_addr().unwrap_or_default().to_string();
        info.properties.insert(
            CheetahString::from_static_str(ConsumerRunningInfo::PROP_NAMESERVER_ADDR),
            CheetahString::from_string(ns_addr),
        );
        info.properties.insert(
            CheetahString::from_static_str(ConsumerRunningInfo::PROP_CONSUME_TYPE),
            CheetahString::from_static_str("CONSUME_PASSIVELY"),
        );
        info.properties.insert(
            CheetahString::from_static_str(ConsumerRunningInfo::PROP_CONSUME_ORDERLY),
            CheetahString::from_string(self.consume_orderly.to_string()),
        );
        info.properties.insert(
            CheetahString::from_static_str(ConsumerRunningInfo::PROP_CLIENT_VERSION),
            CheetahString::from_static_str(env!("CARGO_PKG_VERSION")),
        );
        info.properties.insert(
            CheetahString::from_static_str(ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE),
            CheetahString::from_string(self.consumer_config.consume_thread_max.to_string()),
        );

        let subscription_inner = self.rebalance_impl.rebalance_impl_inner.subscription_inner.clone();
        info.subscription_set = subscription_inner.read().await.values().cloned().collect();
        let queue_entries = {
            let table = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            table
                .iter()
                .map(|(mq, pq)| (mq.clone(), pq.clone()))
                .collect::<Vec<_>>()
        };
        for (mq, pq) in queue_entries {
            let commit_offset = if let Some(store) = &self.offset_store {
                store
                    .read_offset(
                        &mq,
                        crate::consumer::store::read_offset_type::ReadOffsetType::MemoryFirstThenStore,
                    )
                    .await
            } else {
                -1
            };
            info.mq_table
                .insert(mq, pq.fill_process_queue_info(commit_offset.max(0) as u64));
        }
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use crate::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
    use crate::consumer::mq_push_consumer::MQPushConsumer;
    use rocketmq_error::RocketMQError;

    use std::io::Write;
    use std::process::Command;
    use std::time::Duration;

    use super::*;

    /// Plan 49c C5: bulk persistence gets its own remote window, measured from the moment local
    /// work drained, capped by policy, and never allowed to eat what unregistration and
    /// teardown keep back.
    #[test]
    fn offset_persistence_gets_its_own_remote_window_after_the_drain() {
        let now = TokioInstant::now();
        let ceiling = now + DEFAULT_CLEANUP_CEILING;
        let reserve = Duration::from_secs(7);

        // Plenty of session left: the policy opportunity caps the step, not everything that
        // remains.
        assert_eq!(
            remote_step(now, ceiling, Duration::from_secs(10), reserve),
            Duration::from_secs(10)
        );

        // Local release was slow and took most of the session: persistence gets what is left once
        // the reserve is set aside - no more than that, and never more than its
        // opportunity.
        let drained = ceiling - Duration::from_secs(14);
        assert_eq!(
            remote_step(drained, ceiling, Duration::from_secs(10), reserve),
            Duration::from_secs(7)
        );

        // Only the reserve is left: starting no write at all beats dispatching writes past their
        // own deadline, and those offsets are then reported unwritten rather than silently
        // lost.
        assert_eq!(
            remote_step(ceiling - reserve, ceiling, Duration::from_secs(10), reserve),
            Duration::ZERO
        );
        // The session already over: same answer, and no panic from the elapsed comparison.
        assert_eq!(
            remote_step(now + Duration::from_secs(5), now, Duration::from_secs(10), reserve),
            Duration::ZERO
        );
    }

    /// A store whose writer fails a fixed number of times and then succeeds (plan 49c C6 fixture):
    /// the fault is in the write, not in the caller's patience.
    struct FlakyStore {
        failures: u32,
        writes: std::cell::Cell<u32>,
    }

    impl PersistTarget for FlakyStore {
        async fn persist(&mut self, _deadline: TokioInstant) -> PersistSummary {
            let n = self.writes.get() + 1;
            self.writes.set(n);
            let (failed, succeeded) = if n <= self.failures { (1, 0) } else { (0, 1) };
            PersistSummary {
                attempted: 1,
                failed,
                succeeded,
                ..Default::default()
            }
        }

        async fn writer_finished(&mut self) -> bool {
            self.writes.get() > self.failures
        }
    }

    /// Plan 49c C6: an unfinished offset write belongs to the pass that started it. It retries that
    /// one phase in place, with one writer and bounded backoff, so storage that becomes
    /// available again lets cleanup prove itself instead of leaving a consumer blocked on a
    /// fault that outlived its deadline.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unfinished_write_is_retried_by_the_same_owner_until_it_lands() {
        let mut store = FlakyStore {
            failures: 2,
            writes: std::cell::Cell::new(0),
        };
        let mut reported = 0u32;
        let phase = persist_offsets_with_phase_retry(
            &mut store,
            TokioInstant::now() + Duration::from_secs(60),
            Duration::from_secs(10),
            Duration::from_secs(7),
            || reported += 1,
        )
        .await;
        match phase {
            PersistPhase::Finished { summary, attempts } => {
                assert_eq!(2, attempts, "two failed attempts before the write landed");
                assert_eq!(3, summary.attempted);
                assert_eq!(1, summary.succeeded, "only the write that landed claims success");
            }
            PersistPhase::Blocked { attempts, .. } => {
                panic!("storage recovered, so cleanup should have proved: {attempts} attempts")
            }
        }
        assert_eq!(3, store.writes.get(), "one write per attempt, none extra");
        assert_eq!(2, reported, "each retry is reported to the session as progress");
    }

    /// Plan 49c C6: a write that does not land stays Blocked with bounded work - no spin, no second
    /// writer, and no fabricated proof of completion. What bounds it is the session's own budget:
    /// once the next wait would eat what finalization reserved, the pass stops trying and says
    /// so.
    #[tokio::test(flavor = "current_thread")]
    async fn a_write_that_never_lands_stays_blocked_without_spinning() {
        let mut store = FlakyStore {
            failures: u32::MAX,
            writes: std::cell::Cell::new(0),
        };
        let began = TokioInstant::now();
        // One retry fits in this budget; the next wait would not.
        let phase = persist_offsets_with_phase_retry(
            &mut store,
            TokioInstant::now() + Duration::from_millis(150),
            Duration::from_secs(10),
            Duration::ZERO,
            || {},
        )
        .await;
        match phase {
            PersistPhase::Blocked { summary, attempts } => {
                assert!(attempts >= 1, "one bounded retry was still affordable: {attempts}");
                assert!(
                    attempts < MAX_LOCAL_PERSIST_RETRIES,
                    "the budget ended first: {attempts}"
                );
                assert_eq!(attempts + 1, store.writes.get(), "one write per attempt, no more");
                assert_eq!(0, summary.succeeded, "a failing write is never reported as persisted");
            }
            PersistPhase::Finished { .. } => panic!("the write never lands, so nothing may be proved"),
        }
        // Bounded by cadence rather than by how fast the loop can turn over.
        assert!(began.elapsed() < Duration::from_secs(5), "{:?}", began.elapsed());
        // And the waits themselves stay bounded and monotonic up to their ceiling.
        let mut previous = Duration::ZERO;
        for attempt in 0..16 {
            let backoff = local_persist_backoff(attempt);
            assert!(backoff > previous || backoff == MAX_LOCAL_PERSIST_BACKOFF, "{attempt}");
            assert!(backoff <= MAX_LOCAL_PERSIST_BACKOFF);
            previous = backoff;
        }
    }

    /// Plan 49 section 5: a delivery that never yields must cost the caller its deadline and
    /// nothing else - no wedged process, no lost status, and completion as soon as the batch is
    /// released. The scenario runs in a child process so that a cleanup session which ignored
    /// the external deadline fails this test with an overrun instead of hanging the whole
    /// suite, which is exactly what an application must never suffer.
    const CHILD_ENV: &str = "ROCKETMQ_TEST_BLOCKED_CLEANUP_CHILD";
    const MARKERS_ENV: &str = "ROCKETMQ_TEST_BLOCKED_CLEANUP_MARKERS";
    const SELF_TEST: &str = "consumer::consumer_impl::default_mq_push_consumer_impl::tests::\
                             a_delivery_that_never_yields_honours_an_external_deadline";

    fn mark(path: &std::path::Path, marker: &str) {
        if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(file, "{marker}");
        }
    }

    /// One unstarted consumer plus one claimed listener batch that is never released on its own.
    fn consumer_with_a_blocked_delivery() -> (
        DefaultMQPushConsumerImpl,
        crate::consumer::consumer_impl::consumer_work::DeliveryPermit,
    ) {
        let mut client_config = ClientConfig::default();
        client_config.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:1"));
        let consumer_config = ConsumerConfig {
            consumer_group: CheetahString::from_static_str("blocked-listener-group"),
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumerImpl::new(client_config, ArcMut::new(consumer_config), None);
        let permit = consumer
            .delivery
            .admit()
            .expect("an open gate admits the batch that is already running");
        (consumer, permit)
    }

    async fn blocked_child() {
        let markers = std::path::PathBuf::from(std::env::var_os(MARKERS_ENV).unwrap());
        let (mut consumer, permit) = consumer_with_a_blocked_delivery();

        // The caller's budget is short and the listener never returns: Pending is the only honest
        // answer, and answering it may not block, panic or exit.
        let pending = consumer
            .shutdown_until(TokioInstant::now() + Duration::from_millis(300))
            .await;
        assert!(!pending, "an unyielding delivery must report Pending");
        mark(&markers, "pending");

        // The retiring incarnation is still owned and its state is still readable while it drains.
        assert_ne!(ServiceState::ShutdownAlready, *consumer.service_state);
        assert!(consumer.delivery.is_closed(), "admission closed at once");

        // Releasing the batch lets the same session finish; no second session was started.
        drop(permit);
        let complete = consumer
            .shutdown_until(TokioInstant::now() + Duration::from_secs(10))
            .await;
        assert!(complete, "releasing the delivery permits completion");
        assert_eq!(ServiceState::ShutdownAlready, *consumer.service_state);
        mark(&markers, "complete");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_delivery_that_never_yields_honours_an_external_deadline() {
        if std::env::var_os(CHILD_ENV).is_some() {
            blocked_child().await;
            return;
        }
        let markers = std::env::temp_dir().join(format!("rocketmq-blocked-cleanup-{}.markers", std::process::id()));
        let _ = std::fs::remove_file(&markers);
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg(SELF_TEST)
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(MARKERS_ENV, &markers)
            .spawn()
            .expect("spawn the blocked-cleanup child");
        // An external deadline the child cannot ignore: it is a wall-clock bound owned by the
        // parent.
        let limit = Instant::now() + Duration::from_secs(90);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    assert!(
                        Instant::now() < limit,
                        "cleanup ignored an external deadline and wedged the process"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("cannot wait for the child process: {err}"),
            }
        };
        let progress = std::fs::read_to_string(&markers).unwrap_or_default();
        let _ = std::fs::remove_file(&markers);
        assert!(status.success(), "child failed ({status}) after: {progress}");
        assert_eq!("pending\ncomplete\n", progress, "both stages must be reached in order");
    }

    /// One consumer wired the way the public wrapper wires it, plus one claimed listener batch. The
    /// self-handle matters: it is what lets the cleanup pass move onto the SDK's own thread instead
    /// of living and dying inside the caller's await.
    fn wired_consumer_with_a_blocked_delivery() -> (
        ArcMut<DefaultMQPushConsumerImpl>,
        crate::consumer::consumer_impl::consumer_work::DeliveryPermit,
    ) {
        let mut client_config = ClientConfig::default();
        client_config.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:1"));
        let consumer_config = ArcMut::new(ConsumerConfig {
            consumer_group: CheetahString::from_static_str("cleanup-outlives-callers-group"),
            ..Default::default()
        });
        let mut consumer = ArcMut::new(DefaultMQPushConsumerImpl::new(client_config, consumer_config, None));
        let handle = consumer.clone();
        consumer.set_default_mqpush_consumer_impl(handle);
        let permit = consumer
            .delivery
            .admit()
            .expect("an open gate admits the batch that is already running");
        (consumer, permit)
    }

    /// Plan 50 F1: cleanup belongs to the SDK, not to whoever asked for it. A caller whose deadline
    /// has passed walks away with "not proved yet"; the pass it left behind still runs to the end
    /// and releases what it holds, and a later joiner sees that completion instead of starting a
    /// second writer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_finishes_after_every_caller_has_walked_away() {
        let (mut consumer, permit) = wired_consumer_with_a_blocked_delivery();

        // A caller that cannot wait at all: it gets Pending and leaves.
        assert!(
            !consumer.shutdown_until(TokioInstant::now()).await,
            "an elapsed deadline reports not proved yet"
        );
        let progress = consumer
            .cleanup_progress()
            .expect("the stop request started one session");
        assert!(
            progress.running,
            "the pass must keep running without a caller: {progress:?}"
        );

        // Nobody is waiting any more. Releasing the batch lets the abandoned pass finish on its
        // own.
        drop(permit);
        for _ in 0..200 {
            if !consumer.cleanup_progress().expect("session still there").running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let progress = consumer.cleanup_progress().expect("session still there");
        assert!(
            !progress.running && progress.proved,
            "cleanup must finish unwatched: {progress:?}"
        );

        // A later joiner gets the remembered answer; joining again starts neither a pass nor a
        // second offset writer.
        assert!(
            consumer
                .shutdown_until(TokioInstant::now() + Duration::from_secs(1))
                .await,
            "a proved session answers every later caller"
        );
        let again = consumer.cleanup_progress().expect("session still there");
        assert_eq!(again.phase, progress.phase, "no second pass rewrote the outcome");
        assert!(!again.running && again.proved);
    }
    /// Plan 49b R5: translating a configuration failure into the typed variant belongs to the
    /// managed boundary alone. A legacy consumer has always returned `IllegalArgument` here,
    /// and callers match on that variant; wrapping it unconditionally was a silent
    /// public-contract change.
    #[tokio::test(flavor = "current_thread")]
    async fn legacy_start_keeps_its_historical_configuration_error() {
        let mut legacy = DefaultMQPushConsumer::builder()
            .consumer_group("group-legacy-config")
            .subscribe("topic", "*")
            .consume_thread_min(0)
            .build();
        let err = legacy
            .start()
            .await
            .expect_err("an invalid thread bound must be rejected");
        assert!(matches!(err, RocketMQError::IllegalArgument(_)), "{err:?}");

        // The identical failure under SDK ownership is the typed permanent rejection the controller
        // needs in order to stop retrying without reading message text.
        let mut managed = DefaultMQPushConsumer::builder()
            .consumer_group("group-managed-config")
            .subscribe("topic", "*")
            .consume_thread_min(0)
            .build();
        managed.mark_managed_lifecycle();
        let err = managed
            .start()
            .await
            .expect_err("an invalid thread bound must be rejected");
        assert!(matches!(err, RocketMQError::Configuration(_)), "{err:?}");
    }

    /// Plan 49d R5: what draining took may not be spent out of the broker's chance to take the
    /// offsets and this group's unregistration. The remote envelope opens when local release
    /// finishes, so a stalled lookup still receives its post-drain window even though the
    /// session ceiling expired long before that release - while callers whose waits expired
    /// neither shorten it nor start a second pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_local_release_gets_a_fresh_remote_envelope() {
        use crate::consumer::consumer_impl::process_queue::ProcessQueue;
        use crate::consumer::store::offset_store::OffsetStore;
        use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;

        let group = CheetahString::from_static_str("late-drain-group");
        let mut cc = ClientConfig::default();
        cc.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:1"));
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            ..Default::default()
        });
        let mut consumer = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = consumer.clone();
        consumer.set_default_mqpush_consumer_impl(handle);

        // One claimed batch, released only after the session ceiling has expired. Remote
        // persistence may take 60ms with 20ms kept back for finalization; the session
        // itself gets 10ms.
        let permit = consumer
            .delivery
            .admit()
            .expect("an open gate admits the batch that is already running");
        consumer.set_cleanup_session_ceiling(Duration::from_millis(10));
        consumer.set_remote_budgets(Duration::from_millis(60), Duration::from_millis(20));

        // One offset to hand to a broker whose lookup cannot answer while this lock is held.
        let client = MQClientInstance::new_arc(cc.clone(), 0, "late-drain-client", None);
        let nameserver = client.nameserver_lock();
        let stalled_lookup = nameserver.lock().await;
        let mq = MessageQueue::from_parts("T", "missing-broker", 0);
        let store = OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(client.clone(), group.clone()));
        store.update_offset(&mq, 1, false).await;
        consumer.offset_store = Some(ArcMut::new(store));
        consumer.client_instance = Some(client);
        consumer
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), Arc::new(ProcessQueue::new()));

        // A caller whose wait expires while the batch is still held.
        assert!(
            !consumer
                .shutdown_until(TokioInstant::now() + Duration::from_millis(5))
                .await,
            "an elapsed wait reports not proved yet"
        );

        // Local release happens long after that ceiling expired.
        tokio::time::sleep(Duration::from_millis(40)).await;
        drop(permit);
        let released = TokioInstant::now();

        // A second caller that also cannot wait: it joins the one pass and changes nothing about
        // it.
        assert!(
            !consumer.shutdown_until(TokioInstant::now()).await,
            "a second elapsed caller also gets not proved yet"
        );
        let during = consumer
            .cleanup_progress()
            .expect("the stop request started one session");
        assert!(during.running, "one pass keeps running for every caller: {during:?}");

        // The remote envelope opened at release, so the stalled lookup is still being bounded by
        // it. It ends on that deadline rather than reaching past it, and cleanup proves
        // itself afterwards.
        let mut settled = None;
        for _ in 0..300 {
            let progress = consumer.cleanup_progress().expect("session still there");
            if !progress.running {
                settled = Some(progress);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(stalled_lookup);
        let after = settled.expect("the abandoned pass finishes on its own");
        assert!(after.proved, "cleanup must prove itself unwatched: {after:?}");

        let post_drain = released.elapsed();
        assert!(
            post_drain >= Duration::from_millis(20),
            "the stalled lookup received no post-drain remote window: {post_drain:?}"
        );
        // The envelope is the configured opportunity plus its reserve, not an unbounded wait: an
        // unreachable broker may not extend it.
        assert!(
            post_drain <= Duration::from_millis(3_000),
            "the remote envelope outlived its policy budget: {post_drain:?}"
        );

        // A later caller gets the remembered answer; no second pass and no second writer ran.
        assert!(
            consumer
                .shutdown_until(TokioInstant::now() + Duration::from_secs(1))
                .await,
            "a proved session answers every later caller"
        );
        let again = consumer.cleanup_progress().expect("session still there");
        assert_eq!(again.phase, after.phase, "no second pass rewrote the outcome");
    }
}

#[cfg(test)]
mod review49e {
    use super::*;
    use crate::consumer::consumer_impl::consumer_lifecycle::PullFailureKind;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use std::time::Duration;

    fn request_for(queue: &MessageQueue) -> PullRequest {
        PullRequest::new(
            CheetahString::from_static_str("review49e"),
            queue.clone(),
            Arc::new(ProcessQueue::new()),
            0,
        )
    }

    /// Plan 49e E1: a continuation of an **expired** acquisition must not touch the evidence of the
    /// acquisition that replaced it. The old request's lease is retired by revocation, so
    /// scheduling it may neither record a wait nor clear the newer attempt's answer window -
    /// and the later admission check cannot undo state written before it.
    #[test]
    fn a_stale_retry_must_not_erase_the_current_attempt() {
        let mut push =
            DefaultMQPushConsumerImpl::new(ClientConfig::default(), ArcMut::new(ConsumerConfig::default()), None);
        let q = MessageQueue::from_parts("T", "broker", 0);
        push.progress.note_queue_installed(&q);
        let old = push.progress.acquire_pull_lease(&q).unwrap();
        let mut request = request_for(&q);
        request.hold_lease(old);
        // The queue is taken away and handed out again while the old request is still queued.
        push.progress.note_queue_revoked(&q);
        push.progress.note_queue_installed(&q);
        let current = push.progress.acquire_pull_lease(&q).unwrap();
        push.progress
            .begin_pull_attempt(&q, current.epoch(), Duration::ZERO)
            .unwrap();
        assert_eq!(
            push.progress
                .snapshot(Duration::from_secs(30), Duration::from_secs(35))
                .lost_pull_queues,
            1,
            "the newer attempt's expired answer window is a lost pull"
        );

        push.execute_pull_request_later(request, 3000);

        let s = push.progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!("stale retry: lost={} waiting={}", s.lost_pull_queues, s.waiting_queues);
        assert_eq!(
            s.lost_pull_queues, 1,
            "the old retry cleared a newer attempt's deadline"
        );
        assert_eq!(
            s.waiting_queues, 0,
            "an expired request scheduled a wait for the new owner"
        );
        assert_eq!(s.retries_scheduled, 0);
    }

    /// Positive control on the same production path: the continuation of the **current**
    /// acquisition still records its wait, keeps exactly one loop lease, and its failure is
    /// still attributed.
    #[test]
    fn a_current_continuation_still_records_its_wait_and_failure() {
        let mut push =
            DefaultMQPushConsumerImpl::new(ClientConfig::default(), ArcMut::new(ConsumerConfig::default()), None);
        let q = MessageQueue::from_parts("T", "broker", 0);
        push.progress.note_queue_installed(&q);
        let lease = push.progress.acquire_pull_lease(&q).unwrap();
        let epoch = lease.epoch();
        // The loop holds the queue's single lease while its continuation is scheduled.
        assert!(
            push.progress.acquire_pull_lease(&q).is_none(),
            "a second pull loop may not stack onto a leased queue"
        );
        let mut request = request_for(&q);
        request.hold_lease(lease);
        push.progress.begin_pull_attempt(&q, epoch, Duration::ZERO).unwrap();

        push.execute_pull_request_later(request, 3000);

        let s = push.progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        eprintln!(
            "current retry: lost={} waiting={} retries={}",
            s.lost_pull_queues, s.waiting_queues, s.retries_scheduled
        );
        assert_eq!(s.waiting_queues, 1, "the current owner's wait was not recorded");
        assert_eq!(s.lost_pull_queues, 0);
        assert_eq!(s.retries_scheduled, 1);

        // A failure of the same (current) acquisition is still attributed to it.
        push.progress
            .note_pull_failed_for(&q, Some(epoch), PullFailureKind::RouteOrBrokerMiss);
        let s = push.progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(s.route_miss_queues, 1);

        // And a revoked queue takes neither a failure nor a wait from work still in flight on it.
        push.progress.note_queue_revoked(&q);
        let before = push.progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert!(!push
            .progress
            .note_pull_failed_for(&q, Some(epoch), PullFailureKind::RpcTimeout));
        assert!(!push
            .progress
            .note_queue_wait_for(&q, Some(epoch), Duration::from_millis(10)));
        let after = push.progress.snapshot(Duration::from_secs(30), Duration::from_secs(35));
        assert_eq!(after.route_miss_queues, before.route_miss_queues);
        assert_eq!(after.rpc_timeout_queues, 0);
        assert_eq!(after.waiting_queues, 0);
    }
}

/// Plan 49e E4: each queue's offset preparation is one bounded step, and an unfinished round names
/// the queue it reached instead of repeating a slow first broker forever.
#[cfg(test)]
mod review49e_steps {
    use std::time::Duration;

    use super::*;
    use crate::consumer::consumer_impl::message_request::MessageRequest;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::store::offset_store::OffsetStore;
    use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;

    struct Fixture {
        push: ArcMut<DefaultMQPushConsumerImpl>,
        client: ArcMut<MQClientInstance>,
        capture: tokio::sync::mpsc::Receiver<Box<dyn MessageRequest + Send + 'static>>,
    }

    /// Installed lost queues in the given order. A queue with an offset already in memory can
    /// resume without asking anybody; one without it has to resolve a route and so waits on the
    /// nameserver.
    async fn queues(brokers: &[(&str, Option<i64>)]) -> Fixture {
        let mut cc = ClientConfig::default();
        cc.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:1"));
        let group = CheetahString::from_string(format!(
            "review49e-steps-{}",
            brokers
                .iter()
                .map(|(broker, _)| broker.chars().next().unwrap_or('x'))
                .collect::<String>()
        ));
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = push.clone();
        push.set_default_mqpush_consumer_impl(handle);
        let mut client = MQClientInstance::new_arc(cc, 0, "review49e-steps-client", None);
        let capture = client.pull_message_service.review49e_capture();
        push.client_instance = Some(client.clone());
        push.rebalance_impl.rebalance_impl_inner.client_instance = Some(client.clone());
        push.offset_store = Some(ArcMut::new(OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
            client.clone(),
            group,
        ))));
        for (index, (broker, offset)) in brokers.iter().enumerate() {
            let mq = MessageQueue::from_parts("T", *broker, index as i32);
            push.progress.note_queue_installed(&mq);
            push.rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .write()
                .await
                .insert(mq.clone(), Arc::new(ProcessQueue::new()));
            if let Some(offset) = offset {
                push.offset_store
                    .as_ref()
                    .unwrap()
                    .update_offset(&mq, *offset, false)
                    .await;
            }
        }
        Fixture { push, client, capture }
    }

    /// Queues whose pull loop is gone right now, judged the way the repair itself judges them.
    fn lost(progress: &ConsumerProgress) -> usize {
        progress.lost_pulls(Duration::ZERO, Duration::from_secs(35)).len()
    }

    /// What the repair dispatched, in order, as (queue, resume offset).
    fn dispatched(
        capture: &mut tokio::sync::mpsc::Receiver<Box<dyn MessageRequest + Send + 'static>>,
    ) -> Vec<(MessageQueue, i64)> {
        let mut out = Vec::new();
        while let Ok(request) = capture.try_recv() {
            if let Some(pull) = request.as_any().downcast_ref::<PullRequest>() {
                out.push((pull.get_message_queue().clone(), pull.get_next_offset()));
            }
        }
        out
    }

    /// An offset lookup that never answers may not keep the round open: it ends on its own step and
    /// says which queue it stopped at.
    #[tokio::test(flavor = "current_thread")]
    async fn a_stalled_offset_lookup_ends_the_round_on_its_own_step() {
        let mut fixture = queues(&[("a-broken", None), ("b-ready", Some(7))]).await;
        let first = MessageQueue::from_parts("T", "a-broken", 0);
        assert_eq!(lost(&fixture.push.progress), 2);
        let lock = fixture.client.nameserver_lock();
        let guard = lock.lock().await;
        let started = std::time::Instant::now();
        let round = tokio::time::timeout(
            Duration::from_secs(3),
            fixture.push.repair_lost_pulls(
                None,
                Duration::ZERO,
                Duration::from_secs(35),
                // The same allowance production derives from the offset-query timeout; a test uses a
                // short one so that what is under test is the bound and not real network latency.
                Duration::from_millis(250),
                Duration::from_millis(10),
            ),
        )
        .await;
        let elapsed = started.elapsed();
        drop(guard);
        let round = match round {
            Ok(round) => round,
            Err(_) => panic!(
                "the offset lookup was still waiting after 3s: an unbounded wait inside one queue spent the whole \
                 repair"
            ),
        };
        eprintln!("stalled lookup: {round:?} after {elapsed:?}");
        assert!(
            elapsed < Duration::from_secs(2),
            "the round did not end on its own step: {elapsed:?}"
        );
        match round {
            LostPullRound::Incomplete { after } => assert_eq!(after, lost_pull_cursor_key(&first)),
            LostPullRound::Complete { dispatched } => {
                panic!("a queue still owed a lookup was reported finished (dispatched={dispatched})")
            }
        }
        // The queue whose step timed out is still lost: nothing was faked about it.
        assert_eq!(lost(&fixture.push.progress), 2);
        fixture.client.shutdown().await;
    }

    /// Three rounds, each with a first queue that cannot answer inside its step: the cursor has to
    /// move past it so the ready queue is reached rather than every slice repeating the same
    /// failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_first_queue_cannot_starve_the_repair_cursor() {
        let mut fixture = queues(&[("a-broken", None), ("b-ready", Some(7))]).await;
        let first = MessageQueue::from_parts("T", "a-broken", 0);
        let later = MessageQueue::from_parts("T", "b-ready", 1);
        let lock = fixture.client.nameserver_lock();
        let guard = lock.lock().await;
        let mut after = None;
        let mut total = 0usize;
        for round in 0..3 {
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                fixture.push.repair_lost_pulls(
                    after.clone(),
                    Duration::ZERO,
                    Duration::from_secs(35),
                    Duration::from_millis(20),
                    // The reviewer's fixture: a slice so short that only the cursor makes progress.
                    Duration::from_millis(1),
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("repair round {round} never ended"));
            eprintln!("repair round {round}: {result:?}");
            match result {
                LostPullRound::Incomplete { after: next } => {
                    assert_eq!(next, lost_pull_cursor_key(&first));
                    after = Some(next);
                }
                LostPullRound::Complete { dispatched: count } => total += count,
            }
        }
        drop(guard);
        let reached = dispatched(&mut fixture.capture);
        eprintln!("total={total} reached={reached:?}");
        assert!(
            total >= 1 && reached.iter().any(|(mq, _)| mq.get_broker_name() == "b-ready"),
            "every round repeated its slow first queue and never reached the ready one"
        );

        // Positive control: the same production logic from scratch with a usable slice reaches and
        // dispatches the ready queue, resuming exactly at its committed offset.
        let mut control = queues(&[("a-broken", None), ("b-ready", Some(7))]).await;
        let round = control
            .push
            .repair_lost_pulls(
                None,
                Duration::ZERO,
                Duration::from_secs(35),
                Duration::from_millis(20),
                Duration::from_millis(1_000),
            )
            .await;
        assert!(round.finished(), "control round: {round:?}");
        let ready = dispatched(&mut control.capture);
        // The stalled first queue is skipped for this round, so only the ready one may be pulled -
        // never twice, never from a negative position.
        let ready_only: Vec<_> = ready
            .into_iter()
            .filter(|(mq, _)| mq.get_broker_name() == "b-ready")
            .collect();
        assert_eq!(ready_only, vec![(later.clone(), 7)], "one pull at the known offset");
        control.client.shutdown().await;

        fixture.client.shutdown().await;
    }

    /// A round with nothing left behind its cursor is a completed sweep, not an incomplete one:
    /// that is what lets the controller charge a fruitless attempt instead of spinning inside
    /// the kind.
    #[tokio::test(flavor = "current_thread")]
    async fn an_empty_sweep_completes_instead_of_spinning() {
        let mut fixture = queues(&[("a-broken", None)]).await;
        let first = MessageQueue::from_parts("T", "a-broken", 0);
        // Nothing is lost behind the cursor: no lookup, complete.
        let round = tokio::time::timeout(
            Duration::from_secs(2),
            fixture.push.repair_lost_pulls(
                Some(lost_pull_cursor_key(&first)),
                Duration::from_secs(3600),
                Duration::from_secs(35),
                Duration::from_millis(250),
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("a repair with no lost queue waited on something");
        assert_eq!(round, LostPullRound::Complete { dispatched: 0 });
        fixture.client.shutdown().await;
    }

    /// A sweep in which every examined queue fails still advances past each of them and settles, so
    /// the controller judges one unsuccessful sweep instead of repeating the first failure forever.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unsuccessful_sweep_advances_and_settles() {
        let mut fixture = queues(&[("a-broken", None), ("b-broken", None)]).await;
        let lock = fixture.client.nameserver_lock();
        let guard = lock.lock().await;
        let mut after = None;
        for round_no in 0..2 {
            let round = tokio::time::timeout(
                Duration::from_secs(5),
                fixture.push.repair_lost_pulls(
                    after.clone(),
                    Duration::ZERO,
                    Duration::from_secs(35),
                    Duration::from_millis(20),
                    Duration::from_millis(1_000),
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("round {round_no} never ended"));
            match round {
                LostPullRound::Incomplete { after: stop } => after = Some(stop),
                other => panic!("each broken queue ends its own round: {other:?}"),
            }
        }
        let settled = fixture
            .push
            .repair_lost_pulls(
                after,
                Duration::ZERO,
                Duration::from_secs(35),
                Duration::from_millis(20),
                Duration::from_millis(1_000),
            )
            .await;
        drop(guard);
        assert!(
            matches!(settled, LostPullRound::Complete { dispatched: 0 }),
            "the sweep never settled: {settled:?}"
        );
        fixture.client.shutdown().await;
    }

    /// Work prepared before a later queue stalls is dispatched anyway - at this consumer's
    /// committed offset - and the stalled queue keeps its fault instead of being reported
    /// repaired.
    #[tokio::test(flavor = "current_thread")]
    async fn prepared_work_is_dispatched_before_a_later_queue_stalls() {
        let mut fixture = queues(&[("a-ready", Some(7)), ("b-broken", None)]).await;
        let ready = MessageQueue::from_parts("T", "a-ready", 0);
        let broken = MessageQueue::from_parts("T", "b-broken", 1);
        let lock = fixture.client.nameserver_lock();
        let guard = lock.lock().await;
        let started = std::time::Instant::now();
        let round = tokio::time::timeout(
            Duration::from_secs(5),
            fixture.push.repair_lost_pulls(
                None,
                Duration::ZERO,
                Duration::from_secs(35),
                Duration::from_millis(200),
                Duration::from_millis(1_000),
            ),
        )
        .await
        .expect("the round ended on its own step");
        drop(guard);
        assert!(
            matches!(round, LostPullRound::Incomplete { .. }),
            "the stalled queue leaves the round unfinished: {round:?}"
        );
        // Checked before the capture is drained: reading what was dispatched drops those requests,
        // and a finished pull releases its queue's lease.
        assert!(
            fixture.push.progress.acquire_pull_lease(&ready).is_none(),
            "the dispatched pull no longer owns its queue"
        );
        let sent = dispatched(&mut fixture.capture);
        assert_eq!(
            sent,
            vec![(ready.clone(), 7)],
            "prepared work was stranded by the stall"
        );
        assert!(
            fixture.push.progress.acquire_pull_lease(&broken).is_some(),
            "the stalled queue's lease was not released, so it cannot be repaired again"
        );
        assert!(!fixture.push.progress.is_current(&broken, 0));
        assert!(started.elapsed() < Duration::from_secs(3));
        fixture.client.shutdown().await;
    }

    /// An acquisition that ended while its offset was prepared must not dispatch: the process queue
    /// was removed, so this repair no longer owns that work even though its preparation finished.
    #[tokio::test(flavor = "current_thread")]
    async fn revocation_during_preparation_dispatches_nothing() {
        let mut fixture = queues(&[("a-broken", None)]).await;
        let queue = MessageQueue::from_parts("T", "a-broken", 0);
        let lock = fixture.client.nameserver_lock();
        let blocker = lock.lock().await;

        // Preparation waits on route resolution; assignment takes the queue away while it waits,
        // and then the lookup is allowed to answer (and fail) against nothing.
        let late = fixture.push.clone();
        let target = queue.clone();
        let revoke = async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            late.rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .write()
                .await
                .remove(&target);
            late.progress.note_queue_revoked(&target);
        };
        let release = async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            drop(blocker);
        };
        let repair = async {
            fixture
                .push
                .repair_lost_pulls(
                    None,
                    Duration::ZERO,
                    Duration::from_secs(35),
                    Duration::from_millis(2_000),
                    Duration::from_millis(1_000),
                )
                .await
        };
        let (round, (), ()) = tokio::join!(repair, revoke, release);

        assert_eq!(round, LostPullRound::Complete { dispatched: 0 }, "{round:?}");
        assert!(
            dispatched(&mut fixture.capture).is_empty(),
            "a repair whose acquisition disappeared dispatched a pull anyway"
        );
        assert!(
            fixture.push.progress.acquire_pull_lease(&queue).is_none(),
            "the dropped repair must not keep the queue's lease once revoked"
        );
        fixture.client.shutdown().await;
    }

    /// The cursor and the walk share one ordering, so a resumed round really continues after the
    /// queue it stopped at - including several queues on the same broker.
    #[test]
    fn the_cursor_orders_the_same_way_the_repair_walks() {
        let mut queues = Vec::from([
            MessageQueue::from_parts("T", "b-ready", 0),
            MessageQueue::from_parts("T", "a-broken", 11),
            MessageQueue::from_parts("A", "z", 3),
            MessageQueue::from_parts("T", "a-broken", 2),
        ]);
        let mut keys: Vec<_> = queues.iter().map(lost_pull_cursor_key).collect();
        keys.sort();
        queues.sort_by_key(lost_pull_cursor_key);
        assert_eq!(
            keys,
            queues.iter().map(lost_pull_cursor_key).collect::<Vec<_>>(),
            "sorting and cursoring must agree"
        );
        // Strictly-after filtering picks up exactly the queues behind the cursor.
        let cursor = lost_pull_cursor_key(&MessageQueue::from_parts("T", "a-broken", 2));
        let behind: Vec<_> = keys
            .iter()
            .filter(|key| key.as_str() > cursor.as_str())
            .map(|key| key.to_string())
            .collect();
        // Everything strictly behind the cursor: the higher queue on the same broker and the next
        // broker, but never the queue the round stopped at or anything before it.
        assert_eq!(
            behind,
            vec![
                lost_pull_cursor_key(&MessageQueue::from_parts("T", "a-broken", 11)).to_string(),
                lost_pull_cursor_key(&MessageQueue::from_parts("T", "b-ready", 0)).to_string(),
            ]
        );
    }
}

/// Plan 49e E5: a slow local offset file must not spend the reserve that unregistration and
/// finalization were given. Local persistence runs in its own phase, before any remote budget is
/// opened, and the envelope for brokers is measured from the moment that phase finished.
#[cfg(test)]
mod review49e_local {
    use std::io::ErrorKind;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use tokio::net::TcpListener;
    use tokio::sync::Mutex as TokioMutex;

    use super::*;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
    use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;

    #[derive(Clone, Copy)]
    enum Shape {
        Local,
        Remote,
        Both,
    }

    struct Fixture {
        push: ArcMut<DefaultMQPushConsumerImpl>,
        path: std::path::PathBuf,
        /// The same mutex the local offset writer takes. Holding it stalls that writer without
        /// adding a production lock-holding API.
        io: Arc<TokioMutex<()>>,
        contacted: Arc<AtomicUsize>,
        accepted: Arc<TokioMutex<Vec<Instant>>>,
    }

    /// A consumer with an offset store of the requested shape, one installed queue holding one
    /// recorded offset and - when it has a remote half - one loopback broker target that answers
    /// nothing but counts every connection.
    async fn fixture(name: &str, shape: Shape) -> Fixture {
        let mut cc = ClientConfig::default();
        cc.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:1"));
        let group = CheetahString::from_string(format!("review49e-e5-{name}"));
        let cfg = ArcMut::new(ConsumerConfig {
            consumer_group: group.clone(),
            ..Default::default()
        });
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg, None));
        let handle = push.clone();
        push.set_default_mqpush_consumer_impl(handle);
        let instance_id = format!("review49e-e5-{name}");
        let client = MQClientInstance::new_arc(cc, 0, instance_id.as_str(), None);
        push.client_instance = Some(client.clone());

        let dir = std::path::PathBuf::from(std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into()))
            .join(format!("review49e-e5-{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("offsets.json");
        let _ = std::fs::remove_file(&path);
        let mut local = LocalFileOffsetStore::new(client.clone(), group.clone());
        let io = local.review49e_path_and_lock(path.clone());

        // A loopback target that accepts and then says nothing: every attempt is contacted, no
        // attempt is answered.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let contacted = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(TokioMutex::new(Vec::<Instant>::new()));
        {
            let contacted = contacted.clone();
            let accepted = accepted.clone();
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((mut socket, _)) => {
                            contacted.fetch_add(1, Ordering::SeqCst);
                            accepted.lock().await.push(Instant::now());
                            tokio::spawn(async move {
                                let mut buf = [0u8; 512];
                                loop {
                                    match tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await {
                                        Ok(0) | Err(_) => break,
                                        Ok(_) => {}
                                    }
                                }
                            });
                        }
                        Err(err) if err.kind() == ErrorKind::ConnectionReset => continue,
                        Err(_) => break,
                    }
                }
            });
        }

        let remote = match shape {
            Shape::Local => None,
            Shape::Remote | Shape::Both => Some(RemoteBrokerOffsetStore::new(client.clone(), group.clone())),
        };
        let local_store = match shape {
            Shape::Remote => None,
            Shape::Local | Shape::Both => Some(local),
        };
        push.offset_store = Some(ArcMut::new(OffsetStore::new(remote, local_store)));
        if matches!(shape, Shape::Remote | Shape::Both) {
            client.review49e_broker_target(&addr).await;
        }

        let mq = MessageQueue::from_parts("T", "broker-a", 0);
        push.progress.note_queue_installed(&mq);
        push.rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), Arc::new(ProcessQueue::new()));
        push.offset_store.as_ref().unwrap().update_offset(&mq, 7, false).await;
        Fixture {
            push,
            path,
            io,
            contacted,
            accepted,
        }
    }

    /// A local writer that takes 60 ms may not spend a 20 ms unregister reserve: the file lands
    /// first and unregistration is still attempted inside its own envelope.
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_local_writer_must_not_spend_the_unregister_reserve() {
        let mut fixture = fixture("slow", Shape::Both).await;
        fixture
            .push
            .set_remote_budgets(Duration::from_millis(10), Duration::from_millis(20));
        fixture.push.registered = true;
        // Own the file mutex now, release it 60 ms into the pass: what is under test is a local
        // write that takes real time, not one that happens to lose a race.
        let io = fixture.io.clone();
        let guard = io.clone().lock_owned().await;
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            drop(guard);
        });
        let started = Instant::now();
        let proven = tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .push
                .shutdown_until(TokioInstant::now() + Duration::from_secs(2)),
        )
        .await
        .expect("local completion happens before an unregister attempt");
        tokio::time::timeout(Duration::from_millis(500), releaser)
            .await
            .expect("the file mutex is released")
            .expect("the releasing task ran");
        let progress = fixture
            .push
            .cleanup_progress()
            .expect("the stop request started one session");
        assert!(
            proven,
            "cleanup must prove local persistence and unregistration: {progress:?}"
        );
        assert!(fixture.path.exists(), "the local offset file was written");
        assert!(fixture.contacted.load(Ordering::SeqCst) > 0, "unregister was attempted");
        // And in that order: the first connection arrives only after the slow write finished.
        let first = fixture
            .accepted
            .lock()
            .await
            .first()
            .copied()
            .expect("a connection was accepted")
            .duration_since(started);
        assert!(
            first >= Duration::from_millis(50),
            "unregistration was attempted {first:?} after the pass started, before the local writer finished"
        );
    }

    /// Positive control: with an immediate file release the same fixture contacts that target.
    #[tokio::test(flavor = "current_thread")]
    async fn an_immediate_local_writer_still_contacts_the_broker() {
        let mut fixture = fixture("fast", Shape::Both).await;
        fixture
            .push
            .set_remote_budgets(Duration::from_millis(10), Duration::from_millis(20));
        fixture.push.registered = true;
        let proven = tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .push
                .shutdown_until(TokioInstant::now() + Duration::from_secs(2)),
        )
        .await
        .expect("cleanup ends on its own budget");
        assert!(proven, "cleanup must prove local persistence and unregistration");
        assert!(fixture.path.exists(), "the local offset file was written");
        assert!(fixture.contacted.load(Ordering::SeqCst) > 0);
    }

    /// A store model with only a local half proves its offsets without ever needing a broker, so
    /// the ordering change cannot make local-only consumers wait for a remote envelope that
    /// will not come.
    #[tokio::test(flavor = "current_thread")]
    async fn a_local_only_store_proves_offsets_without_a_broker() {
        let mut fixture = fixture("local-only", Shape::Local).await;
        fixture
            .push
            .set_remote_budgets(Duration::from_millis(10), Duration::from_millis(20));
        let proven = tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .push
                .shutdown_until(TokioInstant::now() + Duration::from_secs(2)),
        )
        .await
        .expect("a local-only pass ends on its own budget");
        assert!(proven, "local persistence alone is proof for this store model");
        assert!(fixture.path.exists(), "the local offset file was written");
        assert_eq!(
            fixture.contacted.load(Ordering::SeqCst),
            0,
            "a consumer with no broker target must not be made to wait for one"
        );
    }

    /// A store model with only a remote half keeps its existing bound: an unreachable broker ends
    /// the pass inside its envelope rather than extending it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_remote_only_store_keeps_its_own_bound() {
        let mut fixture = fixture("remote-only", Shape::Remote).await;
        fixture
            .push
            .set_remote_budgets(Duration::from_millis(30), Duration::from_millis(20));
        fixture.push.registered = true;
        let started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .push
                .shutdown_until(TokioInstant::now() + Duration::from_secs(2)),
        )
        .await
        .expect("a remote-only pass ends on its own budget");
        assert!(!fixture.path.exists(), "this store model has no offset file");
        assert!(
            started.elapsed() < Duration::from_millis(1_900),
            "elapsed {:?}",
            started.elapsed()
        );
    }

    /// An unresponsive broker may delay finalization, never extend it: the pass ends within its own
    /// envelope even though connections keep being accepted.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unresponsive_broker_stays_inside_the_envelope() {
        let mut fixture = fixture("unresponsive", Shape::Both).await;
        fixture
            .push
            .set_remote_budgets(Duration::from_millis(30), Duration::from_millis(20));
        fixture.push.registered = true;
        let started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .push
                .shutdown_until(TokioInstant::now() + Duration::from_secs(2)),
        )
        .await
        .expect("an unanswered broker does not extend the pass");
        assert!(fixture.contacted.load(Ordering::SeqCst) > 0);
        assert!(
            started.elapsed() < Duration::from_millis(1_900),
            "elapsed {:?}",
            started.elapsed()
        );
    }
}
/// Plan 49f F4: handing a prepared repair to the shared pull service is its own bounded step. With
/// the channel full, the round must end inside its requested budgets instead of waiting on capacity
/// that belongs to every consumer on the client instance.
#[cfg(test)]
mod review49f {
    use super::*;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::store::offset_store::OffsetStore;
    use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
    #[tokio::test(flavor = "current_thread")]
    async fn a_full_pull_channel_must_not_hold_the_repair_past_its_step() {
        let cc = ClientConfig::default();
        let group = CheetahString::from_static_str("review49f-full-channel");
        let mut push = DefaultMQPushConsumerImpl::new(
            cc.clone(),
            ArcMut::new(ConsumerConfig {
                consumer_group: group.clone(),
                ..Default::default()
            }),
            None,
        );
        let mut client = MQClientInstance::new_arc(cc, 0, "review49f-full-client", None);
        let capture = client.pull_message_service.review49e_capture();
        let dummy = MessageQueue::from_parts("T", "dummy", 0);
        for _ in 0..32 {
            client
                .pull_message_service
                .execute_pull_request_immediately(PullRequest::new(
                    group.clone(),
                    dummy.clone(),
                    Arc::new(ProcessQueue::new()),
                    0,
                ))
                .await;
        }
        assert_eq!(capture.len(), 32);
        let q = MessageQueue::from_parts("T", "ready", 0);
        push.progress.note_queue_installed(&q);
        push.progress
            .review49f_age_activity(&q, std::time::Duration::from_secs(31));
        push.rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await
            .insert(q.clone(), Arc::new(ProcessQueue::new()));
        let store = OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(client.clone(), group));
        store.update_offset(&q, 7, false).await;
        push.offset_store = Some(ArcMut::new(store));
        push.client_instance = Some(client.clone());
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(6),
            push.repair_lost_pulls(
                None,
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(35),
                std::time::Duration::from_millis(10),
                std::time::Duration::from_millis(10),
            ),
        )
        .await;
        eprintln!("full pull channel: result={result:?} buffered={}", capture.len());
        client.shutdown().await;
        assert!(
            result.is_ok(),
            "preparation ended but enqueue waited outside every repair-step deadline"
        );
    }
}
