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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as SyncOrdering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rand::seq::IndexedRandom;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::base::connection_net_event::ConnectionNetEvent;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::consumer_data::ConsumerData;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::producer_data::ProducerData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioMutex;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::admin::mq_admin_ext_async_inner::MQAdminExtInnerImpl;
use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::pull_message_service::PullMessageService;
use crate::consumer::consumer_impl::re_balance::rebalance_service::RebalanceService;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::factory::route_refresh_guard::BeginRefresh;
use crate::factory::route_refresh_guard::RouteRefreshGuard;
use crate::factory::transport_health::emit_consumer_transport_event;
use crate::factory::transport_health::ConsumerTransportEvent;
use crate::factory::transport_health::ConsumerTransportOperation;
use crate::factory::transport_health::ConsumerTransportOutcome;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::find_broker_result::FindBrokerResult;
use crate::implementation::mq_admin_impl::MQAdminImpl;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;

const LOCK_TIMEOUT_MILLIS: u64 = 3000;

/// Cooldown applied to a successful miss-triggered route refresh (plan 49 section
/// 4.1.4). Scheduled route refreshes are unaffected.
pub(crate) const ROUTE_REFRESH_COOLDOWN: Duration = Duration::from_millis(1_000);

/// Interval between bounded INFO summaries of repetitive route/broker bookkeeping.
const LOG_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);

/// Selects the address to use for one `(broker_name, broker_id)` lookup (plan 49 section
/// 4.1.3): the requested ID first, then the master when a slave was requested, and only
/// when `only_this_broker` is false any other advertised entry, preferring the master so the
/// choice stays deterministic. `None` means no address was selected; callers must not treat a
/// bare map hit as success.
fn select_broker_addr(
    addrs: &HashMap<u64, CheetahString>,
    broker_id: u64,
    only_this_broker: bool,
) -> Option<(CheetahString, bool)> {
    if let Some(addr) = addrs.get(&broker_id).filter(|addr| !addr.is_empty()) {
        return Some((addr.clone(), broker_id != mix_all::MASTER_ID));
    }
    if broker_id != mix_all::MASTER_ID {
        // A slave that vanished is served by its master, never by an unrelated peer.
        if let Some(addr) = addrs.get(&mix_all::MASTER_ID).filter(|addr| !addr.is_empty()) {
            return Some((addr.clone(), false));
        }
    }
    if only_this_broker {
        return None;
    }
    addrs
        .iter()
        .filter(|(_, addr)| !addr.is_empty())
        .min_by_key(|(id, _)| **id != mix_all::MASTER_ID)
        .map(|(id, addr)| (addr.clone(), *id != mix_all::MASTER_ID))
}

pub struct MQClientInstance {
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) client_id: CheetahString,
    boot_timestamp: u64,
    /**
     * The container of the producer in the current client. The key is the name of
     * producerGroup.
     */
    producer_table: Arc<RwLock<HashMap<CheetahString, MQProducerInnerImpl>>>,
    /**
     * The container of the consumer in the current client. The key is the name of
     * consumer_group.
     */
    consumer_table: Arc<RwLock<HashMap<CheetahString, MQConsumerInnerImpl>>>,
    /**
     * The container of the adminExt in the current client. The key is the name of
     * adminExtGroup.
     */
    admin_ext_table: Arc<RwLock<HashMap<CheetahString, MQAdminExtInnerImpl>>>,
    pub(crate) mq_client_api_impl: Option<ArcMut<MQClientAPIImpl>>,
    pub(crate) mq_admin_impl: ArcMut<MQAdminImpl>,
    pub(crate) topic_route_table: Arc<RwLock<HashMap<CheetahString /* Topic */, TopicRouteData>>>,
    topic_end_points_table:
        Arc<RwLock<HashMap<CheetahString /* Topic */, HashMap<MessageQueue, CheetahString /* brokerName */>>>>,
    lock_namesrv: Arc<RocketMQTokioMutex<()>>,
    lock_heartbeat: Arc<RocketMQTokioMutex<()>>,
    /// Serializes shared factory startup before any background work is exposed.
    lock_startup: Arc<RocketMQTokioMutex<()>>,
    pub(crate) work: Arc<crate::consumer::consumer_impl::consumer_work::ConsumerWork>,

    service_state: ServiceState,
    pub(crate) pull_message_service: ArcMut<PullMessageService>,
    rebalance_service: RebalanceService,
    pub(crate) default_producer: ArcMut<DefaultMQProducer>,
    instance_runtime: Option<RocketMQRuntime>,
    broker_addr_table: Arc<RwLock<HashMap<CheetahString, HashMap<u64, CheetahString>>>>,
    broker_version_table:
        Arc<RwLock<HashMap<CheetahString /* Broker Name */, HashMap<CheetahString /* address */, i32>>>>,
    send_heartbeat_times_total: Arc<AtomicI64>,
    /// Sequence of the most recent successful route fetch per topic. A higher sequence
    /// always wins when two topics disagree on one broker name/ID address.
    topic_route_sequence: Arc<RwLock<HashMap<CheetahString, u64>>>,
    route_update_sequence: Arc<AtomicU64>,
    /// Coalesces broker-miss route refreshes; scheduled refreshes bypass it.
    pub(crate) route_refresh_guard: Arc<RouteRefreshGuard>,
    /// Bounded INFO summaries for repetitive client bookkeeping events.
    log_gate: crate::utils::log_gate::LogGate,
}

impl MQClientInstance {
    pub fn new(
        client_config: ClientConfig,
        instance_index: i32,
        client_id: String,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        /* let broker_addr_table = Arc::new(Default::default());
        let (tx, _) = tokio::sync::broadcast::channel::<ConnectionNetEvent>(16);
        let rx = tx.subscribe();
        let mq_client_api_impl = ArcMut::new(MQClientAPIImpl::new(
            Arc::new(TokioClientConfig::default()),
            ClientRemotingProcessor::new(),
            rpc_hook,
            client_config.clone(),
            Some(tx),
        ));
        if let Some(namesrv_addr) = client_config.namesrv_addr.as_deref() {
            let handle = Handle::current();
            let mq_client_api_impl_cloned = mq_client_api_impl.clone();
            let namesrv_addr = namesrv_addr.to_string();
            thread::spawn(move || {
                handle.block_on(async move {
                    mq_client_api_impl_cloned
                        .update_name_server_address_list(namesrv_addr.as_str())
                        .await;
                })
            });
        }
        let instance = MQClientInstance {
            client_config: Arc::new(client_config.clone()),
            client_id,
            boot_timestamp: get_current_millis(),
            producer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_table: Arc::new(Default::default()),
            admin_ext_table: Arc::new(Default::default()),
            mq_client_api_impl:None,
            mq_admin_impl: ArcMut::new(MQAdminImpl::new()),
            topic_route_table: Arc::new(Default::default()),
            topic_end_points_table: Arc::new(Default::default()),
            lock_namesrv: Default::default(),
            lock_heartbeat: Default::default(),
            lock_startup: Default::default(),
            service_state: ServiceState::CreateJust,
            pull_message_service: ArcMut::new(PullMessageService::new()),
            rebalance_service: RebalanceService::new(),
            default_producer: ArcMut::new(
                DefaultMQProducer::builder()
                    .producer_group(mix_all::CLIENT_INNER_PRODUCER_GROUP)
                    .client_config(client_config.clone())
                    .build(),
            ),
            instance_runtime: Some(RocketMQRuntime::new_multi(
                num_cpus::get(),
                "mq-client-instance",
            )),
            broker_addr_table,
            broker_version_table: Arc::new(Default::default()),
            send_heartbeat_times_total: Arc::new(AtomicI64::new(0)),
            tx: Some(rx),
        };
        // let instance_ = instance.clone();

        instance*/
        unimplemented!()
    }

    pub fn new_arc(
        client_config: ClientConfig,
        instance_index: i32,
        client_id: impl Into<CheetahString>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> ArcMut<MQClientInstance> {
        let broker_addr_table = Arc::new(Default::default());
        let mut instance = ArcMut::new(MQClientInstance {
            client_config: ArcMut::new(client_config.clone()),
            client_id: client_id.into(),
            boot_timestamp: get_current_millis(),
            producer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_table: Arc::new(Default::default()),
            admin_ext_table: Arc::new(Default::default()),
            mq_client_api_impl: None,
            mq_admin_impl: ArcMut::new(MQAdminImpl::new()),
            topic_route_table: Arc::new(Default::default()),
            topic_end_points_table: Arc::new(Default::default()),
            lock_namesrv: Default::default(),
            lock_heartbeat: Default::default(),
            lock_startup: Default::default(),
            work: Arc::new(Default::default()),
            service_state: ServiceState::CreateJust,
            pull_message_service: ArcMut::new(PullMessageService::new()),
            rebalance_service: RebalanceService::new(),
            default_producer: ArcMut::new(
                DefaultMQProducer::builder()
                    .producer_group(mix_all::CLIENT_INNER_PRODUCER_GROUP)
                    .client_config(client_config.clone())
                    .build(),
            ),
            instance_runtime: Some(RocketMQRuntime::new_multi(num_cpus::get(), "mq-client-instance")),
            broker_addr_table,
            broker_version_table: Arc::new(Default::default()),
            send_heartbeat_times_total: Arc::new(AtomicI64::new(0)),
            topic_route_sequence: Arc::new(Default::default()),
            route_update_sequence: Arc::new(AtomicU64::new(0)),
            route_refresh_guard: Arc::new(RouteRefreshGuard::new(ROUTE_REFRESH_COOLDOWN)),
            log_gate: crate::utils::log_gate::LogGate::new(LOG_SUMMARY_INTERVAL),
        });
        let instance_clone = instance.clone();
        instance.mq_admin_impl.set_client(instance_clone);
        let weak_instance = ArcMut::downgrade(&instance);
        let (tx, mut rx) = tokio::sync::broadcast::channel::<ConnectionNetEvent>(16);

        let mq_client_api_impl = ArcMut::new(MQClientAPIImpl::new(
            Arc::new(TokioClientConfig::default()),
            ClientRemotingProcessor::new(instance.clone()),
            rpc_hook,
            client_config.clone(),
            Some(tx),
        ));
        instance.mq_client_api_impl = Some(mq_client_api_impl.clone());
        if let Some(namesrv_addr) = client_config.namesrv_addr.as_deref() {
            let handle = Handle::current();

            let namesrv_addr = namesrv_addr.to_string();
            instance.work.spawn(&handle, async move {
                mq_client_api_impl
                    .update_name_server_address_list(namesrv_addr.as_str())
                    .await;
            });
        }
        instance.work.spawn(&Handle::current(), async move {
            while let Ok(value) = rx.recv().await {
                if let Some(instance_) = weak_instance.upgrade() {
                    match value {
                        ConnectionNetEvent::CONNECTED(remote_address) => {
                            info!("ConnectionNetEvent CONNECTED");
                            let matching_brokers = {
                                let broker_addr_table = instance_.broker_addr_table.read().await;
                                broker_addr_table
                                    .iter()
                                    .flat_map(|(broker_name, broker_addrs)| {
                                        broker_addrs.iter().filter_map(|(id, addr)| {
                                            (addr == remote_address.to_string().as_str())
                                                .then(|| (broker_name.clone(), *id, addr.clone()))
                                        })
                                    })
                                    .collect::<Vec<_>>()
                            };
                            for (broker_name, id, addr) in matching_brokers {
                                if instance_.send_heartbeat_to_broker(id, &broker_name, &addr).await {
                                    instance_.re_balance_immediately();
                                }
                            }
                        }
                        ConnectionNetEvent::DISCONNECTED => {}
                        ConnectionNetEvent::EXCEPTION => {}
                        ConnectionNetEvent::IDLE => {}
                    }
                }
            }
            warn!("ConnectionNetEvent recv error");
        });
        instance
    }

    pub fn re_balance_immediately(&self) {
        self.rebalance_service.wakeup();
    }

    pub fn re_balance_later(&self, delay_millis: Duration) {
        if delay_millis <= Duration::from_millis(0) {
            self.rebalance_service.wakeup();
        } else {
            let service = self.rebalance_service.clone();
            self.work.spawn(&Handle::current(), async move {
                tokio::time::sleep(delay_millis).await;
                service.wakeup();
            });
        }
    }

    pub async fn start(&mut self, this: ArcMut<Self>) -> rocketmq_error::RocketMQResult<()> {
        let startup_lock = self.lock_startup.clone();
        let _startup_guard = startup_lock.lock().await;
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::Starting;
                // If not specified,looking address from name remoting_server
                if self.client_config.namesrv_addr.is_none() {
                    self.mq_client_api_impl
                        .as_mut()
                        .expect("mq_client_api_impl is None")
                        .fetch_name_server_addr()
                        .await;
                }
                // Start request-response channel
                self.mq_client_api_impl
                    .as_mut()
                    .expect("mq_client_api_impl is None")
                    .start()
                    .await;
                // Start various schedule tasks
                self.start_scheduled_task(this.clone());
                // Start pull service
                let instance = this.clone();
                self.pull_message_service.start(instance).await;
                // Start rebalance service
                self.rebalance_service.start(this).await;
                // Start push service

                if let Err(error) = self
                    .default_producer
                    .default_mqproducer_impl
                    .as_mut()
                    .unwrap()
                    .start_with_factory(false)
                    .await
                {
                    self.pull_message_service.shutdown();
                    self.rebalance_service.shutdown();
                    // A failed shared factory cannot safely be reused: its remoting and
                    // background services may have been partially initialized. Remove it
                    // from the manager so the next consumer generation creates a clean one.
                    self.service_state = ServiceState::ShutdownAlready;
                    crate::implementation::mq_client_manager::MQClientManager::get_instance()
                        .remove_client_factory(&self.client_id);
                    return Err(error);
                }
                info!("the client factory[{}] start OK", self.client_id);
                self.service_state = ServiceState::Running;
            }
            ServiceState::Running => {}
            ServiceState::ShutdownAlready => {}
            ServiceState::Starting => {
                return Err(mq_client_err!(format!(
                    "The Factory object[{}] is already starting.",
                    self.client_id
                )));
            }
            ServiceState::StartFailed => {
                return Err(mq_client_err!(format!(
                    "The Factory object[{}] has been created before, and failed.",
                    self.client_id
                )));
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        let startup_lock = self.lock_startup.clone();
        let _startup_guard = startup_lock.lock().await;
        let consumers = self.consumer_table.read().await;
        let producers = self.producer_table.read().await;
        let admins = self.admin_ext_table.read().await;
        if !consumers.is_empty()
            || !admins.is_empty()
            || producers
                .keys()
                .any(|group| group.as_str() != mix_all::CLIENT_INNER_PRODUCER_GROUP)
        {
            return;
        }
        if self.instance_runtime.is_none() {
            return;
        }
        // Cancel route-refresh waiters before draining tasks so no background work can start
        // a nameserver fetch during shutdown (plan 49 section 4.1.4).
        self.route_refresh_guard.close();
        self.work.close();
        drop((consumers, producers, admins));
        self.work.wait().await;
        self.mq_client_api_impl.as_mut().unwrap().shutdown().await;
        if let Some(runtime) = self.instance_runtime.take() {
            runtime.shutdown();
        }
        self.default_producer
            .default_mqproducer_impl
            .as_mut()
            .unwrap()
            .shutdown_internal()
            .await;
        self.mq_client_api_impl = None;
        self.mq_admin_impl = ArcMut::new(MQAdminImpl::new());
        self.producer_table
            .write()
            .await
            .remove(mix_all::CLIENT_INNER_PRODUCER_GROUP);
        self.pull_message_service.shutdown();
        self.rebalance_service.shutdown();
        self.service_state = ServiceState::ShutdownAlready;
        crate::implementation::mq_client_manager::MQClientManager::get_instance()
            .remove_client_factory(&self.client_id);
    }

    pub async fn register_producer(&mut self, group: &str, producer: MQProducerInnerImpl) -> bool {
        if group.is_empty() {
            return false;
        }
        let mut producer_table = self.producer_table.write().await;
        if self.work.is_closed() || producer_table.contains_key(group) {
            warn!("the producer group[{}] exist already.", group);
            return false;
        }
        producer_table.insert(group.into(), producer);
        true
    }

    pub async fn register_admin_ext(&mut self, group: &str, admin: MQAdminExtInnerImpl) -> bool {
        if group.is_empty() {
            return false;
        }
        let mut admin_ext_table = self.admin_ext_table.write().await;
        if self.work.is_closed() || admin_ext_table.contains_key(group) {
            warn!("the admin group[{}] exist already.", group);
            return false;
        }
        admin_ext_table.insert(group.into(), admin);
        true
    }

    fn start_scheduled_task(&mut self, this: ArcMut<Self>) {
        if self.client_config.namesrv_addr.is_none() {
            // Fetch name server address
            let mut mq_client_api_impl = self.mq_client_api_impl.as_ref().unwrap().clone();
            self.work
                .spawn(self.instance_runtime.as_ref().unwrap().get_handle(), async move {
                    info!("ScheduledTask fetchNameServerAddr started");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    loop {
                        let current_execution_time = tokio::time::Instant::now();
                        mq_client_api_impl.fetch_name_server_addr().await;
                        let next_execution_time = current_execution_time + Duration::from_secs(120);
                        let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(delay).await;
                    }
                });
        }

        // Update topic route info from name server
        let mut client_instance = this.clone();
        let poll_name_server_interval = self.client_config.poll_name_server_interval;
        self.work
            .spawn(self.instance_runtime.as_ref().unwrap().get_handle(), async move {
                info!("ScheduledTask update_topic_route_info_from_name_server started");
                tokio::time::sleep(Duration::from_millis(10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    client_instance.update_topic_route_info_from_name_server().await;
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(poll_name_server_interval as u64);
                    let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        // Clean offline broker and send heartbeat to all broker
        let mut client_instance = this.clone();
        let heartbeat_broker_interval = self.client_config.heartbeat_broker_interval;
        self.work
            .spawn(self.instance_runtime.as_ref().unwrap().get_handle(), async move {
                info!("ScheduledTask clean_offline_broker started");
                tokio::time::sleep(Duration::from_secs(1)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    client_instance.clean_offline_broker().await;
                    client_instance.send_heartbeat_to_all_broker_with_lock().await;
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(heartbeat_broker_interval as u64);
                    let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });

        // Persist all consumer offset
        let mut client_instance = this;
        let persist_consumer_offset_interval = self.client_config.persist_consumer_offset_interval as u64;
        self.work
            .spawn(self.instance_runtime.as_ref().unwrap().get_handle(), async move {
                info!("ScheduledTask persistAllConsumerOffset started");
                tokio::time::sleep(Duration::from_secs(10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    client_instance.persist_all_consumer_offset().await;
                    let next_execution_time =
                        current_execution_time + Duration::from_millis(persist_consumer_offset_interval);
                    let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });
    }

    pub async fn update_topic_route_info_from_name_server(&mut self) {
        let mut topic_list = HashSet::new();

        {
            let producer_table = self.producer_table.read().await;
            for value in producer_table.values() {
                topic_list.extend(value.get_publish_topic_list());
            }
        }

        {
            let consumer_table = self.consumer_table.read().await;
            for value in consumer_table.values() {
                value.subscriptions().await.iter().for_each(|sub| {
                    topic_list.insert(sub.topic.clone());
                });
            }
        }

        for topic in topic_list.iter() {
            self.update_topic_route_info_from_name_server_topic(topic).await;
        }
    }

    #[inline]
    pub async fn update_topic_route_info_from_name_server_topic(&mut self, topic: &CheetahString) -> bool {
        self.update_topic_route_info_from_name_server_default(topic, false, None)
            .await
    }

    pub async fn find_consumer_id_list(
        &mut self,
        topic: &CheetahString,
        group: &CheetahString,
    ) -> Option<Vec<CheetahString>> {
        let mut broker_addr = self.find_broker_addr_by_topic(topic).await;
        if broker_addr.is_none() {
            self.update_topic_route_info_from_name_server_topic(topic).await;
            broker_addr = self.find_broker_addr_by_topic(topic).await;
        }
        if let Some(broker_addr) = broker_addr {
            match self
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .get_consumer_id_list_by_group(broker_addr.as_str(), group, self.client_config.mq_client_api_timeout)
                .await
            {
                Ok(value) => {
                    self.emit_consumer_transport_event(
                        group,
                        ConsumerTransportOperation::ConsumerIdList,
                        ConsumerTransportOutcome::Success,
                    );
                    return Some(value);
                }
                Err(e) => {
                    self.emit_consumer_transport_event(
                        group,
                        ConsumerTransportOperation::ConsumerIdList,
                        ConsumerTransportOutcome::Failure,
                    );
                    warn!(
                        "getConsumerIdListByGroup exception,{}  {}, err:{}",
                        broker_addr,
                        group,
                        e.to_string()
                    );
                    return None;
                }
            }
        }
        self.emit_consumer_transport_event(
            group,
            ConsumerTransportOperation::ConsumerIdList,
            ConsumerTransportOutcome::Failure,
        );
        None
    }

    pub async fn find_broker_addr_by_topic(&self, topic: &str) -> Option<CheetahString> {
        let topic_route_table = self.topic_route_table.read().await;
        if let Some(topic_route_data) = topic_route_table.get(topic) {
            let brokers = &topic_route_data.broker_datas;
            if !brokers.is_empty() {
                let bd = brokers.choose(&mut rand::rng());
                if let Some(bd) = bd {
                    return bd.select_broker_addr();
                }
            }
        }
        None
    }

    pub async fn update_topic_route_info_from_name_server_default(
        &mut self,
        topic: &CheetahString,
        is_default: bool,
        producer_config: Option<&Arc<ProducerConfig>>,
    ) -> bool {
        // Plan 49 section 4.1.1: the nameserver lock stays held across fetch and publish so
        // route state changes remain serialized; table locks are taken route-then-broker and
        // every one of them is released before a listener callback or any network I/O.
        // Cloning the lock handle keeps the guard from borrowing `self`, so the fetch and the
        // table publishes below can still take &mut self.
        let namesrv_lock = self.lock_namesrv.clone();
        let _namesrv_guard = namesrv_lock.lock().await;

        let Some(mut topic_route_data) = self.fetch_topic_route(topic, is_default, producer_config).await else {
            // A failed fetch must never erase the last known route (plan 49 section 4.1.2).
            warn!(
                "updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]",
                topic, self.client_id
            );
            return false;
        };

        let mut changed = {
            let topic_route_table = self.topic_route_table.read().await;
            topic_route_data.topic_route_data_changed(topic_route_table.get(topic))
        };
        if !changed {
            changed = self.is_need_update_topic_route_info(topic).await;
        }
        if !changed {
            debug!("the topic[{}] route info unchanged, client: {}", topic, self.client_id);
            return false;
        }

        // Default topics carry no priority queues; keep the upstream queue-number clamp.
        if let Some(producer_config) = producer_config.filter(|_| is_default) {
            for data in topic_route_data.queue_datas.iter_mut() {
                let queue_nums = producer_config.default_topic_queue_nums().max(data.read_queue_nums);
                data.read_queue_nums = queue_nums;
                data.write_queue_nums = queue_nums;
            }
        }

        // Publish the route snapshot and its sequence first: broker reconciliation derives
        // from cached routes, so it must observe this response.
        let old_route = {
            let topic_route_table = self.topic_route_table.read().await;
            topic_route_table.get(topic).cloned()
        };
        let sequence = self.route_update_sequence.fetch_add(1, SyncOrdering::SeqCst) + 1;
        {
            let mut topic_route_table = self.topic_route_table.write().await;
            topic_route_table.insert(topic.clone(), TopicRouteData::from_existing(&topic_route_data));
        }
        self.topic_route_sequence.write().await.insert(topic.clone(), sequence);

        // Reconcile the derived broker address table against every cached route instead of
        // replacing one broker name wholesale (plan 49 section 4.1.1).
        let retired = self.reconcile_broker_addr_table().await;
        if let Some(suppressed) = self.log_gate.try_acquire(&format!("route-changed:{topic}")) {
            info!(
                topic = %topic,
                sequence,
                retired_brokers = retired.len(),
                suppressed,
                "topic[{}] route info changed, client: {}, retired {} obsolete broker address(es), suppressed {} identical summaries",
                topic,
                self.client_id,
                retired.len(),
                suppressed
            );
        }
        debug!(
            "new topic route info, topic: {}; old: {:?}, new: {:?}",
            topic, old_route, topic_route_data
        );
        for (broker_name, broker_id, address) in retired {
            debug!(
                "topic route no longer contains broker[{}:{}] at [{}], client: {}",
                broker_name, broker_id, address, self.client_id
            );
        }

        // Update endpoint map
        {
            let mq_end_points = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, &topic_route_data);
            if let Some(mq_end_points) = mq_end_points {
                if !mq_end_points.is_empty() {
                    let mut topic_end_points_table = self.topic_end_points_table.write().await;
                    topic_end_points_table.insert(topic.into(), mq_end_points);
                }
            }
        }

        // Update Pub info. Snapshot the table, release it, then notify: no table lock is
        // held across a consumer/producer callback.
        {
            let mut publish_info = topic_route_data2topic_publish_info(topic, &mut topic_route_data);
            publish_info.have_topic_router_info = true;
            let producers: Vec<_> = self.producer_table.read().await.values().cloned().collect();
            for mut value in producers {
                value.update_topic_publish_info(topic.to_string(), Some(publish_info.clone()));
            }
        }

        // Update sub info
        {
            let consumers = {
                let consumer_table = self.consumer_table.read().await;
                consumer_table.values().cloned().collect::<Vec<_>>()
            };
            if !consumers.is_empty() {
                let subscribe_info = topic_route_data2topic_subscribe_info(topic, &topic_route_data);
                for value in consumers {
                    value.update_topic_subscribe_info(topic.clone(), &subscribe_info).await;
                }
            }
        }
        true
    }

    /// Fetches one route from the nameserver. `None` means "no information", which callers
    /// must treat as "keep the last known route".
    async fn fetch_topic_route(
        &mut self,
        topic: &CheetahString,
        is_default: bool,
        producer_config: Option<&Arc<ProducerConfig>>,
    ) -> Option<TopicRouteData> {
        if let (true, Some(_)) = (is_default, producer_config) {
            match self
                .mq_client_api_impl
                .as_mut()?
                .get_default_topic_route_info_from_name_server(self.client_config.mq_client_api_timeout)
                .await
            {
                Ok(value) => value,
                Err(e) => {
                    error!(
                        "get_default_topic_route_info_from_name_server failed, topic: {}, error: {}",
                        topic, e
                    );
                    None
                }
            }
        } else {
            self.mq_client_api_impl
                .as_mut()?
                .get_topic_route_info_from_name_server(topic, self.client_config.mq_client_api_timeout)
                .await
                .unwrap_or_else(|err| {
                    warn!(
                        "getTopicRouteInfoFromNameServer failed, topic: {}, err: {:?}",
                        topic, err
                    );
                    None
                })
        }
    }

    /// Rebuilds the broker address table from every cached topic route. A key present in a
    /// newer response takes that address; a key still advertised by any other cached route
    /// survives, and only keys no cached route advertises are retired (plan 49 section
    /// 4.1.1). Returns the retired `(broker_name, broker_id, address)` triples.
    async fn reconcile_broker_addr_table(&self) -> Vec<(CheetahString, u64, CheetahString)> {
        // Snapshot cached routes under one read lock; no callback and no I/O run here.
        let desired: HashMap<(CheetahString, u64), (CheetahString, u64)> = {
            let topic_route_table = self.topic_route_table.read().await;
            let sequences = self.topic_route_sequence.read().await;
            let mut desired: HashMap<(CheetahString, u64), (CheetahString, u64)> = HashMap::new();
            for (topic, route) in topic_route_table.iter() {
                // Topics fetched before this counter existed compare as oldest.
                let sequence = sequences.get(topic).copied().unwrap_or(0);
                for broker in route.broker_datas.iter() {
                    for (broker_id, address) in broker.broker_addrs().iter() {
                        let key = (broker.broker_name().clone(), *broker_id);
                        match desired.get(&key) {
                            Some((_, seen)) if *seen >= sequence => {}
                            _ => {
                                desired.insert(key, (address.clone(), sequence));
                            }
                        }
                    }
                }
            }
            desired
        };
        if desired.is_empty() {
            // No cached route knowledge: keep the last known addresses rather than wiping
            // every broker because a fetch has not answered yet.
            return Vec::new();
        }
        let mut broker_addr_table = self.broker_addr_table.write().await;
        let mut retired = Vec::new();
        for (broker_name, addrs) in broker_addr_table.iter() {
            for (broker_id, address) in addrs.iter() {
                if !desired.contains_key(&(broker_name.clone(), *broker_id)) {
                    retired.push((broker_name.clone(), *broker_id, address.clone()));
                }
            }
        }
        // Rebuild from the reconciled snapshot so a key advertised by any still-cached route
        // survives and only unreferenced keys disappear.
        let mut rebuilt: HashMap<CheetahString, HashMap<u64, CheetahString>> =
            HashMap::with_capacity(broker_addr_table.len());
        for ((broker_name, broker_id), (address, _)) in desired {
            rebuilt
                .entry(broker_name)
                .or_insert_with(|| HashMap::with_capacity(2))
                .insert(broker_id, address);
        }
        *broker_addr_table = rebuilt;
        retired
    }

    async fn is_need_update_topic_route_info(&self, topic: &CheetahString) -> bool {
        let mut result = false;
        let producer_table = self.producer_table.read().await;
        for (key, value) in producer_table.iter() {
            if !result {
                result = value.is_publish_topic_need_update(topic);
                break;
            }
        }
        if result {
            return true;
        }

        let consumers = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table.values().cloned().collect::<Vec<_>>()
        };
        for value in consumers {
            if !result {
                result = value.is_subscribe_topic_need_update(topic).await;
                break;
            }
        }
        result
    }

    pub async fn persist_all_consumer_offset(&mut self) {
        let consumers = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table.values().cloned().collect::<Vec<_>>()
        };
        for value in consumers {
            value.persist_consumer_offset().await;
        }
    }

    pub async fn clean_offline_broker(&mut self) {
        let lock = self
            .lock_namesrv
            .try_lock_timeout(Duration::from_millis(LOCK_TIMEOUT_MILLIS))
            .await;
        if lock.is_none() {
            return;
        }
        // Reconcile from cached routes rather than matching addresses alone: a broker name/ID
        // no cached route advertises is retired, while one still advertised by another topic
        // survives even if this pass saw it only indirectly (plan 49 section 4.1.1).
        let retired = self.reconcile_broker_addr_table().await;
        for (broker_name, broker_id, address) in &retired {
            debug!(
                "the broker[{}:{}] host [{}] is offline, removed from the address table, client: {}",
                broker_name, broker_id, address, self.client_id
            );
        }
        if !retired.is_empty() {
            if let Some(suppressed) = self.log_gate.try_acquire("offline-broker") {
                info!(
                    "reconciled the broker address table for client[{}]: retired {} obsolete address(es), suppressed \
                     {} identical summaries",
                    self.client_id,
                    retired.len(),
                    suppressed
                );
            }
        }
        // Bound per-topic refresh state to topics this instance still knows about.
        let known = {
            let topic_route_table = self.topic_route_table.read().await;
            topic_route_table.keys().cloned().collect::<HashSet<CheetahString>>()
        };
        self.route_refresh_guard.retain(|topic| known.contains(topic));
        self.topic_route_sequence
            .write()
            .await
            .retain(|topic, _| known.contains(topic));
    }
    pub async fn send_heartbeat_to_all_broker_with_lock(&mut self) -> bool {
        if let Some(lock) = self.lock_heartbeat.try_lock().await {
            if self.client_config.use_heartbeat_v2 {
                self.send_heartbeat_to_all_broker_v2(false).await
            } else {
                self.send_heartbeat_to_all_broker().await
            }
        } else {
            warn!("lock heartBeat, but failed. [{}]", self.client_id);
            false
        }
    }

    pub async fn send_heartbeat_to_all_broker_with_lock_v2(&mut self, is_rebalance: bool) -> bool {
        if let Some(lock) = self.lock_heartbeat.try_lock_timeout(Duration::from_secs(2)).await {
            if self.client_config.use_heartbeat_v2 {
                self.send_heartbeat_to_all_broker_v2(is_rebalance).await
            } else {
                self.send_heartbeat_to_all_broker().await
            }
        } else {
            warn!("lock heartBeat, but failed. [{}]", self.client_id);
            false
        }
    }

    pub fn get_mq_client_api_impl(&self) -> ArcMut<MQClientAPIImpl> {
        self.mq_client_api_impl.as_ref().unwrap().clone()
    }

    pub async fn get_broker_name_from_message_queue(&self, message_queue: &MessageQueue) -> CheetahString {
        let guard = self.topic_end_points_table.read().await;
        if let Some(broker_name) = guard.get(message_queue.get_topic()) {
            if let Some(addr) = broker_name.get(message_queue) {
                return addr.clone();
            }
        }
        message_queue.get_broker_name().clone()
    }

    pub async fn find_broker_address_in_publish(&self, broker_name: &CheetahString) -> Option<CheetahString> {
        if broker_name.is_empty() {
            return None;
        }
        let guard = self.broker_addr_table.read().await;
        let map = guard.get(broker_name);
        if let Some(map) = map {
            return map.get(&(mix_all::MASTER_ID)).cloned();
        }
        None
    }

    async fn send_heartbeat_to_all_broker_v2(&self, is_rebalance: bool) -> bool {
        unimplemented!()
    }

    async fn send_heartbeat_to_all_broker(&self) -> bool {
        let heartbeat_data = self.prepare_heartbeat_data(false).await;
        let producer_empty = heartbeat_data.producer_data_set.is_empty();
        let consumer_empty = heartbeat_data.consumer_data_set.is_empty();
        if producer_empty && consumer_empty {
            warn!(
                "sending heartbeat, but no consumer and no producer. [{}]",
                self.client_id
            );
            return false;
        }
        let broker_addr_table = self.broker_addr_table.read().await;
        if broker_addr_table.is_empty() {
            return false;
        }
        for (broker_name, broker_addrs) in broker_addr_table.iter() {
            if broker_addrs.is_empty() {
                continue;
            }
            for (id, addr) in broker_addrs.iter() {
                if addr.is_empty() {
                    continue;
                }
                if consumer_empty && *id != mix_all::MASTER_ID {
                    continue;
                }
                self.send_heartbeat_to_broker_inner(*id, broker_name, addr, &heartbeat_data)
                    .await;
            }
        }

        true
    }

    pub async fn send_heartbeat_to_broker(&self, id: u64, broker_name: &CheetahString, addr: &CheetahString) -> bool {
        if let Some(lock) = self.lock_heartbeat.try_lock().await {
            let heartbeat_data = self.prepare_heartbeat_data(false).await;
            let producer_empty = heartbeat_data.producer_data_set.is_empty();
            let consumer_empty = heartbeat_data.consumer_data_set.is_empty();
            if producer_empty && consumer_empty {
                warn!(
                    "sending heartbeat, but no consumer and no producer. [{}]",
                    self.client_id
                );
                return false;
            }

            if self.client_config.use_heartbeat_v2 {
                unimplemented!("sendHeartbeatToBrokerV2")
            } else {
                self.send_heartbeat_to_broker_inner(id, broker_name, addr, &heartbeat_data)
                    .await
            }
        } else {
            false
        }
    }

    async fn send_heartbeat_to_broker_inner(
        &self,
        id: u64,
        broker_name: &CheetahString,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
    ) -> bool {
        if let Ok(version) = self
            .mq_client_api_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .send_heartbeat(addr, heartbeat_data, self.client_config.mq_client_api_timeout)
            .await
        {
            self.emit_heartbeat_transport_event(ConsumerTransportOutcome::Success)
                .await;
            let mut broker_version_table = self.broker_version_table.write().await;
            let map = broker_version_table.get_mut(broker_name);
            if let Some(map) = map {
                map.insert(addr.clone(), version);
            } else {
                let mut map = HashMap::new();
                map.insert(addr.clone(), version);
                broker_version_table.insert(broker_name.clone(), map);
            }

            let times = self
                .send_heartbeat_times_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Per-target success is routine bookkeeping and stays at DEBUG; failures keep
            // WARN below (plan 49 section 4.1.6).
            debug!(
                "send heart beat to broker[{} {} {}] success, times: {}",
                broker_name, id, addr, times,
            );
            return true;
        }
        if self.is_broker_in_name_server(addr).await {
            warn!("send heart beat to broker[{} {} {}] failed", broker_name, id, addr);
        } else {
            warn!(
                "send heart beat to broker[{} {} {}] exception, because the broker not up, forget it",
                broker_name, id, addr
            )
        }
        self.emit_heartbeat_transport_event(ConsumerTransportOutcome::Failure)
            .await;
        false
    }

    fn emit_consumer_transport_event(
        &self,
        consumer_group: &CheetahString,
        operation: ConsumerTransportOperation,
        outcome: ConsumerTransportOutcome,
    ) {
        emit_consumer_transport_event(ConsumerTransportEvent {
            client_id: self.client_id.to_string(),
            consumer_group: consumer_group.to_string(),
            operation,
            outcome,
        });
    }

    async fn emit_heartbeat_transport_event(&self, outcome: ConsumerTransportOutcome) {
        let consumer_groups = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table.keys().cloned().collect::<Vec<_>>()
        };
        for consumer_group in consumer_groups {
            self.emit_consumer_transport_event(&consumer_group, ConsumerTransportOperation::Heartbeat, outcome);
        }
    }

    async fn is_broker_in_name_server(&self, broker_name: &str) -> bool {
        let broker_addr_table = self.topic_route_table.read().await;
        for value in broker_addr_table.values() {
            for bd in value.broker_datas.iter() {
                for value in bd.broker_addrs().values() {
                    if value.as_str() == broker_name {
                        return true;
                    }
                }
            }
        }
        false
    }

    async fn prepare_heartbeat_data(&self, is_without_sub: bool) -> HeartbeatData {
        let mut heartbeat_data = HeartbeatData {
            client_id: self.client_id.clone(),
            ..Default::default()
        };

        let consumers = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table.values().cloned().collect::<Vec<_>>()
        };
        for value in consumers {
            let mut consumer_data = ConsumerData {
                group_name: value.group_name(),
                consume_type: value.consume_type(),
                message_model: value.message_model(),
                consume_from_where: value.consume_from_where(),
                subscription_data_set: value.subscriptions().await,
                unit_mode: value.is_unit_mode(),
            };
            if !is_without_sub {
                value.subscriptions().await.iter().for_each(|sub| {
                    consumer_data.subscription_data_set.insert(sub.clone());
                });
            }
            heartbeat_data.consumer_data_set.insert(consumer_data);
        }
        let producer_groups = {
            let producer_table = self.producer_table.read().await;
            producer_table.keys().cloned().collect::<Vec<_>>()
        };
        for group_name in producer_groups {
            heartbeat_data.producer_data_set.insert(ProducerData { group_name });
        }
        heartbeat_data.is_without_sub = is_without_sub;
        heartbeat_data
    }

    pub async fn register_consumer(&mut self, group: &CheetahString, consumer: MQConsumerInnerImpl) -> bool {
        let mut consumer_table = self.consumer_table.write().await;
        if self.work.is_closed() || consumer_table.contains_key(group) {
            warn!("the consumer group[{}] exist already.", group);
            return false;
        }
        consumer_table.insert(group.clone(), consumer);
        true
    }

    pub async fn check_client_in_broker(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let consumer_table = self.consumer_table.read().await;
        for (key, value) in consumer_table.iter() {
            let subscription_inner = value.subscriptions().await;
            if subscription_inner.is_empty() {
                return Ok(());
            }
            for subscription_data in subscription_inner.iter() {
                if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
                    continue;
                }
                let addr = self.find_broker_addr_by_topic(subscription_data.topic.as_str()).await;
                if let Some(addr) = addr {
                    match self
                        .mq_client_api_impl
                        .as_mut()
                        .unwrap()
                        .check_client_in_broker(
                            addr.as_str(),
                            key,
                            self.client_id.as_str(),
                            subscription_data,
                            self.client_config.mq_client_api_timeout,
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => match e {
                            rocketmq_error::RocketMQError::IllegalArgument(_) => {
                                return Err(e);
                            }
                            _ => {
                                let desc = format!(
                                    "Check client in broker error, maybe because you use {} to filter message, but \
                                     server has not been upgraded to support!This error would not affect the launch \
                                     of consumer, but may has impact on message receiving if you have use the new \
                                     features which are not supported by server, please check the log!",
                                    subscription_data.expression_type
                                );
                            }
                        },
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn do_rebalance(&mut self) -> bool {
        let consumers = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        };
        let mut balanced = true;
        for (key, value) in consumers {
            match value.try_rebalance().await {
                Ok(result) => {
                    if !result {
                        balanced = false;
                    }
                }
                Err(e) => {
                    error!("doRebalance for consumer group [{}] exception:{}", key, e.to_string());
                }
            }
        }
        balanced
    }

    pub fn rebalance_later(&mut self, delay_millis: u64) {
        if delay_millis == 0 {
            self.rebalance_service.wakeup();
        } else {
            let service = self.rebalance_service.clone();
            self.work
                .spawn(self.instance_runtime.as_ref().unwrap().get_handle(), async move {
                    tokio::time::sleep(Duration::from_millis(delay_millis)).await;
                    service.wakeup();
                });
        }
    }

    pub async fn find_broker_address_in_subscribe(
        &mut self,
        broker_name: &CheetahString,
        broker_id: u64,
        only_this_broker: bool,
    ) -> Option<FindBrokerResult> {
        if broker_name.is_empty() {
            return None;
        }
        // Resolve from one snapshot of the table (plan 49 section 4.1.3): the requested ID
        // first, then the master when a slave was requested, then any other entry only if the
        // caller tolerates another broker. No result is produced unless an address was really
        // selected, so callers can distinguish "resolved" from "route refresh needed".
        let selected = {
            let broker_addr_table = self.broker_addr_table.read().await;
            broker_addr_table
                .get(broker_name)
                .and_then(|addrs| select_broker_addr(addrs, broker_id, only_this_broker))
        };
        if let Some((broker_addr, slave)) = selected {
            let broker_version = self.find_broker_version(broker_name, broker_addr.as_str()).await;
            Some(FindBrokerResult {
                broker_addr,
                slave,
                broker_version,
            })
        } else {
            None
        }
    }

    /// Repairs a broker-address miss through the shared refresh guard so thousands of queues
    /// on one topic coalesce into a single nameserver fetch plus cooldown instead of one
    /// sequential round trip each (plan 49 section 4.1.4). Returns whether the caller should
    /// re-check its tables; `false` only when the client is stopping, in which case no wait and
    /// no further fetch are allowed. A joiner waits at most `join_budget` and never issues a
    /// second round trip.
    pub async fn repair_route_after_broker_miss(&mut self, topic: &CheetahString, join_budget: Duration) -> bool {
        match self.route_refresh_guard.begin(topic) {
            BeginRefresh::Fetch => {
                let refreshed = self.update_topic_route_info_from_name_server_topic(topic).await;
                self.route_refresh_guard.finish(topic, refreshed);
                true
            }
            BeginRefresh::Joining => {
                // Wait for the owner's fetch, then re-check our own tables.
                self.route_refresh_guard.await_in_flight(topic, join_budget).await;
                true
            }
            // A recent success means the tables are as fresh as the cooldown allows.
            BeginRefresh::Cooldown => true,
            BeginRefresh::Closed => false,
        }
    }
    async fn find_broker_version(&self, broker_name: &str, broker_addr: &str) -> i32 {
        let broker_version_table = self.broker_version_table.read().await;
        if let Some(map) = broker_version_table.get(broker_name) {
            if let Some(version) = map.get(broker_addr) {
                return *version;
            }
        }
        0
    }

    pub async fn select_consumer(&self, group: &str) -> Option<MQConsumerInnerImpl> {
        let consumer_table = self.consumer_table.read().await;
        consumer_table
            .get(group)
            .filter(|consumer| consumer.is_running())
            .cloned()
    }

    pub async fn select_producer(&self, group: &str) -> Option<MQProducerInnerImpl> {
        let producer_table = self.producer_table.read().await;
        producer_table.get(group).cloned()
    }

    pub(crate) async fn unregister_consumer_local(&self, group: &CheetahString) {
        self.consumer_table.write().await.remove(group);
    }

    pub async fn unregister_consumer(&mut self, group: impl Into<CheetahString>) {
        self.unregister_consumer_until(
            group,
            Instant::now() + Duration::from_millis(self.client_config.mq_client_api_timeout),
        )
        .await;
    }

    pub async fn unregister_consumer_until(&mut self, group: impl Into<CheetahString>, deadline: Instant) {
        let group = group.into();
        // Always remove the local entry first. Broker unregistration is best effort;
        // retaining a stale local entry makes a later retry permanently duplicate.
        let Some(mut consumer_table) = tokio::time::timeout_at(deadline, self.consumer_table.write())
            .await
            .ok()
        else {
            return;
        };
        consumer_table.remove(&group);
        drop(consumer_table);
        self.unregister_client_until(None, Some(group), deadline).await;
    }
    pub async fn unregister_producer(&mut self, group: impl Into<CheetahString>) {
        self.unregister_client(Some(group.into()), None).await;
    }

    pub async fn unregister_admin_ext(&mut self, group: impl Into<CheetahString>) {
        let mut write_guard = self.admin_ext_table.write().await;
        let _ = write_guard.remove(&group.into());
    }
    async fn unregister_client(
        &mut self,
        producer_group: Option<CheetahString>,
        consumer_group: Option<CheetahString>,
    ) {
        self.unregister_client_until(
            producer_group,
            consumer_group,
            Instant::now() + Duration::from_millis(self.client_config.mq_client_api_timeout),
        )
        .await;
    }

    pub(crate) async fn unregister_client_until(
        &mut self,
        producer_group: Option<CheetahString>,
        consumer_group: Option<CheetahString>,
        deadline: Instant,
    ) {
        let broker_addr_table = match tokio::time::timeout_at(deadline, self.broker_addr_table.read()).await {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let mut seen_addresses = HashSet::new();
        let mut broker_targets = Vec::new();
        for (broker_name, broker_addrs) in broker_addr_table.iter() {
            for (id, addr) in broker_addrs {
                if seen_addresses.insert(addr.clone()) {
                    broker_targets.push((broker_name.clone(), *id, addr.clone()));
                }
            }
        }
        drop(broker_addr_table);
        let Some(api) = self.mq_client_api_impl.as_ref().cloned() else {
            return;
        };
        let timeout_millis = self.client_config.mq_client_api_timeout;
        let mut pending = futures::stream::FuturesUnordered::new();
        let mut targets = broker_targets.into_iter();
        loop {
            while pending.len() < 8 {
                let Some((broker_name, id, addr)) = targets.next() else {
                    break;
                };
                let api = api.clone();
                let client_id = self.client_id.clone();
                let producer_group = producer_group.clone();
                let consumer_group = consumer_group.clone();
                let request_timeout = timeout_millis.min(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                );
                if request_timeout == 0 {
                    return;
                }
                pending.push(async move {
                    let result = api
                        .as_ref()
                        .unregister_client(&addr, client_id, producer_group, consumer_group, request_timeout)
                        .await;
                    (broker_name, id, addr, result)
                });
            }
            if pending.is_empty() {
                break;
            }
            let Some(result) = tokio::time::timeout_at(deadline, futures::StreamExt::next(&mut pending))
                .await
                .ok()
                .flatten()
            else {
                break;
            };
            if result.3.is_ok() {
                debug!(
                    "unregister client[Producer: {:?} Consumer: {:?}] from broker[{} {} {}] success",
                    producer_group, consumer_group, result.0, result.1, result.2,
                );
            }
        }
    }

    /// Queries the assignment for a given topic.
    ///
    /// This function attempts to find the broker address for the specified topic. If the broker
    /// address is not found, it updates the topic route information from the name server and
    /// retries. If the broker address is found, it queries the assignment from the broker.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to query.
    /// * `consumer_group` - A reference to a `CheetahString` representing the consumer group.
    /// * `strategy_name` - A reference to a `CheetahString` representing the allocation strategy
    ///   name.
    /// * `message_model` - The message model to use for the query.
    /// * `timeout` - The timeout duration for the query.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with a `HashSet` of `MessageQueueAssignment` if the query
    /// is successful, or an error if it fails.
    pub async fn query_assignment(
        &mut self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        strategy_name: &CheetahString,
        message_model: MessageModel,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<HashSet<MessageQueueAssignment>>> {
        // Try to find broker address
        let mut broker_addr = self.find_broker_addr_by_topic(topic).await;

        // If not found, update and retry
        if broker_addr.is_none() {
            self.update_topic_route_info_from_name_server_topic(topic).await;
            broker_addr = self.find_broker_addr_by_topic(topic).await;
        }
        if let Some(broker_addr) = broker_addr {
            let client_id = self.client_id.clone();
            match self.mq_client_api_impl.as_mut() {
                Some(api_impl) => {
                    api_impl
                        .query_assignment(
                            &broker_addr,
                            topic.clone(),
                            consumer_group.clone(),
                            client_id,
                            strategy_name.clone(),
                            message_model,
                            timeout,
                        )
                        .await
                }
                None => Err(mq_client_err!("mq_client_api_impl is None")),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn consume_message_directly(
        &self,
        message: MessageExt,
        consumer_group: &CheetahString,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        let consumer = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table.get(consumer_group).cloned()
        };
        if let Some(consumer) = consumer {
            return consumer.consume_message_directly(message, broker_name).await;
        }

        None
    }
}

#[allow(clippy::unnecessary_unwrap)]
pub fn topic_route_data2topic_publish_info(topic: &str, route: &mut TopicRouteData) -> TopicPublishInfo {
    let mut info = TopicPublishInfo {
        topic_route_data: Some(route.clone()),
        ..Default::default()
    };
    if route.order_topic_conf.is_some() && !route.order_topic_conf.as_ref().unwrap().is_empty() {
        let brokers = route
            .order_topic_conf
            .as_ref()
            .unwrap()
            .split(";")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        for broker in brokers {
            let item = broker.split(":").collect::<Vec<&str>>();
            if item.len() == 2 {
                let queue_num = item[1].parse::<i32>().unwrap();
                for i in 0..queue_num {
                    let mq = MessageQueue::from_parts(topic, item[0], i);
                    info.message_queue_list.push(mq);
                }
            }
        }
        info.order_topic = true;
    } else if route.order_topic_conf.is_none()
        && route.topic_queue_mapping_by_broker.is_some()
        && !route.topic_queue_mapping_by_broker.as_ref().unwrap().is_empty()
    {
        info.order_topic = false;
        let mq_end_points = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route);
        if let Some(mq_end_points) = mq_end_points {
            for (mq, broker_name) in mq_end_points {
                info.message_queue_list.push(mq);
            }
        }
        info.message_queue_list
            .sort_by(|a, b| match a.get_queue_id().cmp(&b.get_queue_id()) {
                Ordering::Less => std::cmp::Ordering::Less,
                Ordering::Equal => std::cmp::Ordering::Equal,
                Ordering::Greater => std::cmp::Ordering::Greater,
            });
    } else {
        route.queue_datas.sort();
        for queue_data in route.queue_datas.iter() {
            if PermName::is_writeable(queue_data.perm) {
                let mut broker_data = None;
                for bd in route.broker_datas.iter() {
                    if bd.broker_name() == queue_data.broker_name.as_str() {
                        broker_data = Some(bd.clone());
                        break;
                    }
                }
                if broker_data.is_none() {
                    continue;
                }
                if !broker_data
                    .as_ref()
                    .unwrap()
                    .broker_addrs()
                    .contains_key(&(mix_all::MASTER_ID))
                {
                    continue;
                }
                for i in 0..queue_data.write_queue_nums {
                    let mq = MessageQueue::from_parts(topic, queue_data.broker_name.as_str(), i as i32);
                    info.message_queue_list.push(mq);
                }
            }
        }
    }
    info
}

pub fn topic_route_data2topic_subscribe_info(topic: &str, route: &TopicRouteData) -> HashSet<MessageQueue> {
    if let Some(ref topic_queue_mapping_by_broker) = route.topic_queue_mapping_by_broker {
        if !topic_queue_mapping_by_broker.is_empty() {
            let mq_endpoints = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route);
            return mq_endpoints
                .unwrap_or_default()
                .keys()
                .cloned()
                .collect::<HashSet<MessageQueue>>();
        }
    }
    let mut mq_list = HashSet::new();
    for qd in &route.queue_datas {
        if PermName::is_readable(qd.perm) {
            for i in 0..qd.read_queue_nums {
                let mq = MessageQueue::from_parts(topic, qd.broker_name.as_str(), i as i32);
                mq_list.insert(mq);
            }
        }
    }
    mq_list
}

#[cfg(test)]
mod tests {
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    fn test_consumer(group: &str) -> MQConsumerInnerImpl {
        let mut consumer_config = ArcMut::new(ConsumerConfig::default());
        consumer_config.consumer_group = group.into();
        let mut consumer_impl = ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            consumer_config,
            None,
        ));
        let consumer_impl_clone = consumer_impl.clone();
        consumer_impl.set_default_mqpush_consumer_impl(consumer_impl_clone.clone());
        MQConsumerInnerImpl {
            default_mqpush_consumer_impl: consumer_impl_clone,
        }
    }

    #[tokio::test]
    async fn unregister_consumer_always_removes_the_local_entry() {
        let mut instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase3-unregister-client", None);
        let group = CheetahString::from("phase3-unregister-group");

        assert!(instance.register_consumer(&group, test_consumer(group.as_str())).await);
        assert!(instance.consumer_table.read().await.contains_key(&group));

        instance.unregister_consumer(group.clone()).await;

        assert!(!instance.consumer_table.read().await.contains_key(&group));
    }

    #[tokio::test]
    async fn select_consumer_hides_non_running_consumers() {
        let mut instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase3-select-client", None);
        let group = CheetahString::from("phase3-select-group");
        assert!(instance.register_consumer(&group, test_consumer(group.as_str())).await);

        assert!(instance.select_consumer(group.as_str()).await.is_none());
    }

    #[tokio::test]
    async fn shutdown_keeps_the_shared_client_alive_for_a_sibling_consumer() {
        let mut instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase3-sibling-client", None);
        let first = CheetahString::from("phase3-sibling-one");
        let second = CheetahString::from("phase3-sibling-two");
        instance.service_state = ServiceState::Running;
        assert!(instance.register_consumer(&first, test_consumer(first.as_str())).await);
        assert!(
            instance
                .register_consumer(&second, test_consumer(second.as_str()))
                .await
        );

        instance.unregister_consumer(first).await;
        instance.shutdown().await;
        assert_eq!(instance.service_state, ServiceState::Running);

        instance.unregister_consumer(second).await;
        instance.shutdown().await;
        assert_eq!(instance.service_state, ServiceState::ShutdownAlready);
    }

    fn addrs(entries: &[(u64, &str)]) -> HashMap<u64, CheetahString> {
        entries
            .iter()
            .map(|(id, addr)| (*id, CheetahString::from_string(addr.to_string())))
            .collect()
    }

    #[test]
    fn broker_lookup_prefers_the_requested_id() {
        let table = addrs(&[(0, "master:10911"), (1, "slave:10912")]);
        assert_eq!(
            select_broker_addr(&table, 1, true),
            Some((CheetahString::from_static_str("slave:10912"), true))
        );
        assert_eq!(
            select_broker_addr(&table, mix_all::MASTER_ID, true),
            Some((CheetahString::from_static_str("master:10911"), false))
        );
    }

    #[test]
    fn missing_slave_falls_back_to_the_master() {
        let table = addrs(&[(0, "master:10911")]);
        assert_eq!(
            select_broker_addr(&table, 1, true),
            Some((CheetahString::from_static_str("master:10911"), false))
        );
    }

    #[test]
    fn another_entry_is_used_only_when_the_caller_allows_it() {
        let table = addrs(&[(2, "other:10913")]);
        assert_eq!(select_broker_addr(&table, 1, true), None);
        assert_eq!(
            select_broker_addr(&table, 1, false),
            Some((CheetahString::from_static_str("other:10913"), true))
        );
    }

    #[test]
    fn an_empty_address_is_never_returned() {
        let table = addrs(&[(0, "")]);
        assert_eq!(select_broker_addr(&table, mix_all::MASTER_ID, false), None);
        assert_eq!(select_broker_addr(&table, 1, false), None);
    }

    #[test]
    fn any_entry_lookup_prefers_the_master_deterministically() {
        let table = addrs(&[(3, "c:10914"), (2, "b:10913"), (0, "a:10911")]);
        assert_eq!(
            select_broker_addr(&table, 7, false),
            Some((CheetahString::from_static_str("a:10911"), false))
        );
    }

    fn route(broker_name: &str, entries: &[(u64, &str)]) -> TopicRouteData {
        let mut topic_route_data = TopicRouteData::new();
        topic_route_data.broker_datas.push(BrokerData::new(
            CheetahString::from_static_str("cluster"),
            CheetahString::from_string(broker_name.to_string()),
            addrs(entries),
            None,
        ));
        topic_route_data
    }

    async fn seed(instance: &MQClientInstance, topic: &str, route: TopicRouteData, sequence: u64) {
        instance
            .topic_route_table
            .write()
            .await
            .insert(CheetahString::from_string(topic.to_string()), route);
        instance
            .topic_route_sequence
            .write()
            .await
            .insert(CheetahString::from_string(topic.to_string()), sequence);
    }

    async fn addresses(instance: &MQClientInstance) -> Vec<(String, u64, String)> {
        let table = instance.broker_addr_table.read().await;
        let mut out = table
            .iter()
            .flat_map(|(name, addrs)| addrs.iter().map(|(id, addr)| (name.to_string(), *id, addr.to_string())))
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    #[tokio::test]
    async fn reconciliation_keeps_ids_advertised_by_another_topic() {
        let instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase1-reconcile", None);
        // Two topics share one broker name but disagree on which IDs exist.
        seed(
            &instance,
            "TopicA",
            route("broker-a", &[(0, "10.0.0.1:10911"), (1, "10.0.0.2:10911")]),
            1,
        )
        .await;
        seed(&instance, "TopicB", route("broker-a", &[(2, "10.0.0.3:10911")]), 2).await;

        let retired = instance.reconcile_broker_addr_table().await;
        assert!(retired.is_empty(), "nothing is unreferenced: {retired:?}");
        assert_eq!(
            addresses(&instance).await,
            vec![
                ("broker-a".to_string(), 0, "10.0.0.1:10911".to_string()),
                ("broker-a".to_string(), 1, "10.0.0.2:10911".to_string()),
                ("broker-a".to_string(), 2, "10.0.0.3:10911".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn reconciliation_lets_the_newest_response_win_and_retires_nothing_else() {
        let instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase1-newest", None);
        seed(&instance, "TopicA", route("broker-a", &[(0, "old-master:10911")]), 1).await;
        assert!(instance.reconcile_broker_addr_table().await.is_empty());

        // A newer response for the same broker name/ID replaces the address.
        seed(&instance, "TopicB", route("broker-a", &[(0, "new-master:10911")]), 2).await;
        let retired = instance.reconcile_broker_addr_table().await;
        assert!(retired.is_empty(), "a replacement is not a retirement: {retired:?}");
        assert_eq!(
            addresses(&instance).await,
            vec![("broker-a".to_string(), 0, "new-master:10911".to_string())]
        );

        // An older response must not roll the newer address back.
        seed(&instance, "TopicA", route("broker-a", &[(0, "old-master:10911")]), 1).await;
        instance.reconcile_broker_addr_table().await;
        assert_eq!(
            addresses(&instance).await,
            vec![("broker-a".to_string(), 0, "new-master:10911".to_string())]
        );
    }

    #[tokio::test]
    async fn reconciliation_retires_only_keys_no_cached_route_advertises() {
        let instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase1-retire", None);
        seed(
            &instance,
            "TopicA",
            route("broker-a", &[(0, "master:10911"), (1, "slave:10912")]),
            1,
        )
        .await;
        instance.reconcile_broker_addr_table().await;

        // The slave disappears from the only route that advertised it.
        seed(&instance, "TopicA", route("broker-a", &[(0, "master:10911")]), 2).await;
        let retired = instance.reconcile_broker_addr_table().await;
        assert_eq!(retired.len(), 1);
        assert_eq!(
            (retired[0].0.as_str(), retired[0].1, retired[0].2.as_str()),
            ("broker-a", 1, "slave:10912")
        );
        assert_eq!(
            addresses(&instance).await,
            vec![("broker-a".to_string(), 0, "master:10911".to_string())]
        );
    }

    #[tokio::test]
    async fn reconciliation_without_route_knowledge_keeps_addresses() {
        let instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase1-keep", None);
        seed(&instance, "TopicA", route("broker-a", &[(0, "master:10911")]), 1).await;
        instance.reconcile_broker_addr_table().await;

        // Losing every cached route must not wipe known brokers (a fetch may be in flight).
        instance.topic_route_table.write().await.clear();
        instance.topic_route_sequence.write().await.clear();
        let retired = instance.reconcile_broker_addr_table().await;
        assert!(retired.is_empty());
        assert_eq!(addresses(&instance).await.len(), 1);
    }

    #[tokio::test]
    async fn shared_factory_start_lock_is_single_flight() {
        let instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "phase3-start-lock-client", None);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..4 {
            let lock = instance.lock_startup.clone();
            let active = active.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = lock.lock().await;
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for task in tasks {
            task.await.expect("startup lock task");
        }

        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }
}
