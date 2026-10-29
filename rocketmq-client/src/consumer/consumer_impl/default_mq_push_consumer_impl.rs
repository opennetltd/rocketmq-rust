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
    pub(crate) fn mark_managed_lifecycle(&self) {
        self.managed_lifecycle.store(true, Ordering::Release);
    }

    pub(crate) fn set_cleanup_session_ceiling(&self, ceiling: std::time::Duration) {
        self.cleanup_ceiling_millis
            .store(ceiling.as_millis().min(u128::from(u64::MAX)) as u64, Ordering::Release);
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
        if let Some(store) = self.offset_store.as_mut() {
            let queues = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            let assigned = queues.keys().cloned().collect();
            drop(queues);
            // One bounded pass: broker lookup, route refresh and the send all share the cleanup
            // deadline, and anything the budget could not reach is reported, not hidden.
            driver.enter("offset_persist");
            let started = TokioInstant::now();
            let summary = store.persist_all_until(&assigned, deadline).await;
            report.record("offset_persist", started);
            report.detail(format!("offsets[{summary}]"));
            if !store.io_complete().await {
                // The local file writer still owns the pending write; report it instead of claiming
                // a second writer finished the job.
                report.block("offset_file_writer_busy");
                return false;
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
                // finalization reserve, so teardown cannot be cut off mid-release.
                let remaining = deadline.saturating_duration_since(TokioInstant::now());
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

    pub async fn execute_pull_request_immediately(&mut self, pull_request: PullRequest) {
        if self.work.is_closed() {
            return;
        }
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pull_request_immediately(pull_request)
            .await;
    }
    pub fn execute_pull_request_later(&mut self, pull_request: PullRequest, time_delay: u64) {
        if self.work.is_closed() {
            return;
        }
        self.progress.note_retry_scheduled();
        // A delayed retry is dispatched by the shared pull service, so its timer belongs to the
        // client instance's runtime rather than to whichever task happened to schedule it (F2).
        let Some(client_instance) = self.client_instance.as_ref() else {
            return;
        };
        let handle = client_instance.runtime_handle();
        let service = client_instance.pull_message_service.clone();
        self.work.spawn(&handle, async move {
            tokio::time::sleep(std::time::Duration::from_millis(time_delay)).await;
            service.execute_pull_request_immediately(pull_request).await;
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
    pub(crate) async fn repair_lost_pulls(&mut self, stall_after: std::time::Duration) {
        let lost = self.progress.lost_pulls(stall_after);
        if lost.is_empty() {
            return;
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
            lost.into_iter()
                .filter_map(|mq| {
                    let pq = table.get(&mq)?.clone();
                    (!pq.is_dropped()).then_some((mq, pq))
                })
                .collect()
        };
        let mut requests = Vec::new();
        for (mq, pq) in owned {
            let commit_offset = match &mut self.offset_store {
                Some(store) => store.read_offset(&mq, ReadOffsetType::MemoryFirstThenStore).await,
                None => -1,
            };
            // Nothing committed and nothing consumed yet: start where this consumer would have
            // started on first assignment rather than from an offset nobody has agreed on.
            let next_offset = if commit_offset >= 0 {
                commit_offset
            } else {
                Rebalance::compute_pull_from_where(self.rebalance_impl.as_mut(), &mq).await
            };
            requests.push(PullRequest::new(
                self.consumer_config.consumer_group.clone(),
                mq,
                pq,
                next_offset,
            ));
        }
        if requests.is_empty() || self.client_instance.is_none() {
            return;
        }
        debug!("re-dispatching {} queue(s) whose pull loop was lost", requests.len());
        for request in requests {
            self.execute_pull_request_immediately(request).await;
        }
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
}
