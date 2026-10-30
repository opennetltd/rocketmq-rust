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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::consumer_lifecycle::call_listener_catching;
use crate::consumer::consumer_impl::consumer_lifecycle::DeliveryOwner;
use crate::consumer::consumer_impl::consumer_lifecycle::ListenerCall;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::consume_return_type::ConsumeReturnType;
use crate::consumer::listener::message_listener_concurrently::ArcBoxMessageListenerConcurrently;
use crate::hook::consume_message_context::ConsumeMessageContext;

pub struct ConsumeMessageConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerConcurrently,
    pub(crate) consume_runtime: Option<RocketMQRuntime>,
    work: Arc<super::consumer_work::ConsumerWork>,
    /// Shared with the push consumer: one close stops admission on both sides.
    delivery: Arc<super::consumer_work::DeliveryGate>,
    /// Test-only answers for `send_message_back`, consumed in order (plan 49d R4): lets a test
    /// drive the all-successful and mixed clustering send-back outcomes without a broker. Not
    /// present in a production build.
    #[cfg(test)]
    pub(crate) send_back_outcomes: std::sync::Mutex<std::collections::VecDeque<bool>>,
}

impl ConsumeMessageConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
        delivery: Arc<super::consumer_work::DeliveryGate>,
    ) -> Self {
        let consume_thread = consumer_config.consume_thread_max;
        let consumer_group_tag = format!("{}_{}", "ConsumeMessageThread_", consumer_group);
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            consume_runtime: Some(RocketMQRuntime::new_multi(
                consume_thread as usize,
                consumer_group_tag.as_str(),
            )),
            work: Arc::new(super::consumer_work::ConsumerWork::default()),
            delivery,
            #[cfg(test)]
            send_back_outcomes: Default::default(),
        }
    }
}

impl ConsumeMessageConcurrentlyService {
    async fn clean_expire_msg(&mut self) {
        let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.clone().unwrap();

        let process_queue_table = default_mqpush_consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await;
        for process_queue in process_queue_table.values() {
            process_queue
                .clean_expired_msg(self.default_mqpush_consumer_impl.clone())
                .await;
        }
    }

    async fn process_consume_result(
        &mut self,
        this: ArcMut<Self>,
        status: ConsumeConcurrentlyStatus,
        context: &ConsumeConcurrentlyContext,
        consume_request: &mut ConsumeRequest,
    ) {
        if consume_request.msgs.is_empty() {
            return;
        }
        let msgs_len = consume_request.msgs.len() as i32;
        let ack_index = match status {
            ConsumeConcurrentlyStatus::ConsumeSuccess => match context.ack_index {
                None => msgs_len - 1,
                Some(idx) => idx.clamp(-1, msgs_len - 1),
            },
            ConsumeConcurrentlyStatus::ReconsumeLater => match context.ack_index {
                None => -1,
                Some(idx) => idx.clamp(-1, msgs_len - 1),
            },
        };

        // Messages this attempt **finished** without the listener acknowledging them: dropped after
        // exhausting broadcast retries, or handed back to the broker successfully. They are
        // collected here and removed from the process queue with the acknowledged ones
        // (plan 49d R4) - a terminal message that stays cached pins the commit offset
        // behind it and can hold enough of the queue's count/size budget to stop further
        // pulls, while its delivery ownership has already been released, so nothing else
        // will ever clean it up.
        let mut finalized: Vec<ArcMut<MessageExt>> = Vec::new();

        match self.consumer_config.message_model {
            MessageModel::Broadcasting => {
                let pending_start = (ack_index + 1) as usize;
                if pending_start < consume_request.msgs.len() {
                    let pending = consume_request.msgs.split_off(pending_start);
                    let max_retries = self.consumer_config.broadcast_max_retries;
                    let retry_delay = self.consumer_config.broadcast_retry_delay;
                    let mut to_retry = Vec::with_capacity(pending.len());
                    for mut msg in pending {
                        let attempts = msg.reconsume_times() as u32;
                        if attempts >= max_retries {
                            warn!(
                                "BROADCASTING terminal failure: topic={} queueOffset={} msgId={} retryCnt={}, \
                                 dropping message",
                                msg.get_topic(),
                                msg.queue_offset,
                                msg.msg_id,
                                attempts,
                            );
                            finalized.push(msg);
                        } else {
                            msg.set_reconsume_times(attempts as i32 + 1);
                            to_retry.push(msg);
                        }
                    }
                    if !to_retry.is_empty() {
                        // The retried messages leave this attempt but are not finished: their
                        // ownership moves to the delayed request, which
                        // releases them when it commits. Without the
                        // transfer this batch's owner would subtract them at the end of the attempt
                        // and the sleeping retry would be invisible until
                        // it ran again (plan 49c C4).
                        let accounting = consume_request
                            .accounting
                            .as_mut()
                            .and_then(|owner| owner.split_off(to_retry.len()));
                        self.submit_consume_request_later_with_delay(
                            to_retry,
                            this,
                            consume_request.process_queue.clone(),
                            consume_request.message_queue.clone(),
                            retry_delay,
                            accounting,
                        );
                    }
                }
            }
            MessageModel::Clustering => {
                let len = consume_request.msgs.len();
                let failed_msgs = consume_request.msgs.split_off((ack_index + 1) as usize);
                // What the queue still holds, in order; a message someone else already took has
                // nothing left to send back and nothing left to remove.
                let mut held = Vec::with_capacity(len);
                for msg in failed_msgs {
                    if consume_request.process_queue.contains_message(&msg).await {
                        held.push(msg);
                    }
                }
                let mut sent_back = Vec::with_capacity(held.len());
                for msg in held.iter_mut() {
                    sent_back.push(self.send_message_back(msg.as_mut(), context).await);
                }
                let (msg_back_failed, sent_back) = partition_send_backs(held, &sent_back);
                // The broker took these back for redelivery: this queue is done with them, whether
                // or not any sibling send-back failed (plan 49d R4). Keeping the
                // successful ones only when something also failed left an
                // all-successful batch cached forever.
                finalized.extend(sent_back);
                if !msg_back_failed.is_empty() {
                    // Same transfer as the broadcasting retry above: these messages are queued
                    // again and stay outstanding work until that attempt
                    // commits them (plan 49c C4).
                    let accounting = consume_request
                        .accounting
                        .as_mut()
                        .and_then(|owner| owner.split_off(msg_back_failed.len()));
                    self.submit_consume_request_later(
                        msg_back_failed,
                        this,
                        consume_request.process_queue.clone(),
                        consume_request.message_queue.clone(),
                        accounting,
                    );
                }
            }
        }

        // Hold the per-queue commit lock while removing messages and updating the offset
        // so that concurrent chunk tasks for the same queue cannot interleave their
        // offset advancement. Independent queues are not blocked because each
        // ProcessQueue carries its own lock.
        let _commit_guard = consume_request.process_queue.commit_lock.lock().await;
        // One removal for the whole finished set, under the same commit lock: cached count, cached
        // bytes and the resulting memory offset then all describe the messages this attempt is done
        // with, and only locally retried messages stay in `ProcessQueue` (plan 49d R4).
        let mut removals = consume_request.msgs.clone();
        removals.extend(finalized);
        let offset = consume_request.process_queue.remove_message(&removals).await;
        if offset >= 0 && !consume_request.process_queue.is_dropped() {
            let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_mut().unwrap();
            default_mqpush_consumer_impl
                .offset_store
                .as_mut()
                .unwrap()
                .update_offset(&consume_request.message_queue, offset, true)
                .await;
        }
    }

    /// Queue a retry of messages whose accounting ownership was transferred from the attempt that
    /// failed them. `accounting` is `None` only when the batch carried no SDK ownership; a
    /// cancelled sleep releases it here rather than leaving the count held by a task that will
    /// never run.
    fn submit_consume_request_later(
        &self,
        msgs: Vec<ArcMut<MessageExt>>,
        this: ArcMut<Self>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        accounting: Option<DeliveryOwner>,
    ) {
        self.work
            .spawn(self.consume_runtime.as_ref().unwrap().get_handle(), async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let this_ = this.clone();
                this.submit_consume_request_accounted(this_, msgs, process_queue, message_queue, true, accounting)
                    .await;
            });
    }

    fn submit_consume_request_later_with_delay(
        &self,
        msgs: Vec<ArcMut<MessageExt>>,
        this: ArcMut<Self>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        delay: Duration,
        accounting: Option<DeliveryOwner>,
    ) {
        self.work
            .spawn(self.consume_runtime.as_ref().unwrap().get_handle(), async move {
                tokio::time::sleep(delay).await;
                let this_ = this.clone();
                this.submit_consume_request_accounted(this_, msgs, process_queue, message_queue, true, accounting)
                    .await;
            });
    }

    pub async fn send_message_back(&mut self, msg: &mut MessageExt, context: &ConsumeConcurrentlyContext) -> bool {
        // Test-only broker answer (plan 49d R4): a queued outcome is returned instead of calling
        // the broker, so what leaves the process queue can be tested per send-back result.
        #[cfg(test)]
        if let Some(outcome) = self.send_back_outcomes.lock().unwrap().pop_front() {
            return outcome;
        }
        let delay_level = context.delay_level_when_next_consume;
        msg.set_topic(self.client_config.with_namespace(msg.get_topic().as_str()));

        self.default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .send_message_back(
                msg,
                delay_level,
                &self
                    .client_config
                    .queue_with_namespace(context.get_message_queue().clone()),
            )
            .await
            .is_ok()
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessageConcurrentlyService {
    fn start(&mut self, mut this: ArcMut<Self>) {
        self.work
            .spawn(self.consume_runtime.as_ref().unwrap().get_handle(), async move {
                let timeout = this.consumer_config.consume_timeout;
                let mut interval = tokio::time::interval(Duration::from_secs(timeout * 60));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    this.clean_expire_msg().await;
                }
            });
    }

    /// Stops accepting new delivery without waiting for anything; idempotent so every cleanup
    /// entry point may call it before it awaits a tracker.
    fn begin_shutdown(&mut self) {
        self.delivery.close();
        self.work.close();
    }

    async fn shutdown(&mut self, _await_terminate_millis: u64) {
        // The caller owns the absolute timeout. A blocked synchronous listener
        // keeps its task token, so a timeout cannot be mistaken for safe cleanup.
        self.begin_shutdown();
        self.work.wait().await;
        if let Some(runtime) = self.consume_runtime.take() {
            runtime.shutdown();
        }
        self.default_mqpush_consumer_impl = None;
    }

    async fn consume_message_directly(
        &self,
        mut msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        let Some(_callback) = self.work.enter() else {
            let mut result = ConsumeMessageDirectlyResult::default();
            result.set_consume_result(CMResult::CRLater);
            return result;
        };
        info!("consumeMessageDirectly receive new message: {}", msg);
        msg.broker_name = broker_name.unwrap_or_default();
        let mq = MessageQueue::from_parts(msg.topic().clone(), msg.broker_name.clone(), msg.queue_id());
        let mut msgs = vec![ArcMut::new(msg)];
        let mut context = ConsumeConcurrentlyContext::new(mq);
        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());

        let begin_timestamp = Instant::now();

        // A panicking listener must still produce a result for the admin/broadcast caller instead
        // of unwinding out of this task (plan 49a F5).
        let status = call_listener_catching(|| {
            self.message_listener.consume_message(
                &msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>(),
                &mut context,
            )
        });
        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(false);
        result.set_auto_commit(true);
        match status {
            ListenerCall::ThrewPanic => {
                error!("message listener panicked while consuming directly, reporting a thrown exception");
                result.set_consume_result(CMResult::CRThrowException);
                result.set_remark(CheetahString::from_static_str("listener panicked"));
            }
            ListenerCall::Returned(Ok(status)) => match status {
                ConsumeConcurrentlyStatus::ConsumeSuccess => {
                    result.set_consume_result(CMResult::CRSuccess);
                }
                ConsumeConcurrentlyStatus::ReconsumeLater => {
                    result.set_consume_result(CMResult::CRLater);
                }
            },
            ListenerCall::Returned(Err(e)) => {
                result.set_consume_result(CMResult::CRThrowException);
                result.set_remark(CheetahString::from_string(e.to_string()))
            }
        }
        result.set_spent_time_mills(begin_timestamp.elapsed().as_millis() as u64);
        info!("consumeMessageDirectly Result: {}", result);
        result
    }

    async fn submit_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<ArcMut<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        // No SDK ownership accompanies a batch submitted without one; such a request neither adds
        // nor releases counts of its own.
        self.submit_consume_request_accounted(this, msgs, process_queue, message_queue, dispatch_to_consume, None)
            .await;
    }

    async fn submit_consume_request_accounted(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<ArcMut<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
        mut accounting: Option<DeliveryOwner>,
    ) {
        let queued = accounting.as_ref().map_or(0, |owner| owner.messages());
        if self.work.is_closed() {
            // Rejected admission: this batch will never run, so its ownership ends here rather than
            // being counted as work forever (plan 49c C4).
            debug!(
                messages = queued,
                "consume service is closed, releasing the queued batch instead of admitting it"
            );
            return;
        }
        if !dispatch_to_consume {
            // Nothing was scheduled for these messages; they stay in the process queue until a
            // later submit dispatches them. Their pull-side ownership ends with this
            // call - holding it would report a listener that has not been called as
            // outstanding work.
            debug!(
                messages = queued,
                "batch not dispatched, its messages wait in the process queue for the next submit"
            );
            return;
        }
        let consume_batch_size = self.consumer_config.consume_message_batch_max_size;
        if msgs.len() <= consume_batch_size as usize {
            let owned = accounting.as_mut().and_then(|owner| owner.split_off(msgs.len()));
            let mut consume_request = ConsumeRequest {
                msgs,
                message_listener: self.message_listener.clone(),
                process_queue,
                message_queue,
                dispatch_to_consume,
                consumer_group: self.consumer_group.clone(),
                default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
                delivery: self.delivery.clone(),
                accounting: owned,
            };

            self.work
                .spawn_draining(self.consume_runtime.as_ref().unwrap().get_handle(), async move {
                    consume_request.run(this).await
                });
        } else {
            // One owner per chunk, taken from the single batch admitted at handover, so the chunks
            // can commit in any order and each releases exactly its own messages (plan
            // 49c C4).
            for chunk in msgs.chunks(consume_batch_size as usize) {
                let chunk = chunk.to_vec();
                let owned = accounting.as_mut().and_then(|owner| owner.split_off(chunk.len()));
                let mut consume_request = ConsumeRequest {
                    msgs: chunk,
                    message_listener: self.message_listener.clone(),
                    process_queue: process_queue.clone(),
                    message_queue: message_queue.clone(),
                    dispatch_to_consume,
                    consumer_group: self.consumer_group.clone(),
                    default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
                    delivery: self.delivery.clone(),
                    accounting: owned,
                };
                let consume_message_concurrently_service = this.clone();
                self.work
                    .spawn_draining(self.consume_runtime.as_ref().unwrap().get_handle(), async move {
                        consume_request.run(consume_message_concurrently_service).await
                    });
            }
        }
    }

    async fn submit_pop_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        unimplemented!("ConsumeMessageConcurrentlyService not support submit_pop_consume_request");
    }
}

/// Decide what a refused batch does next from the broker's own answers, in the order the send-backs
/// were attempted (plan 49d R4). A message the broker accepted is finished with this queue; only
/// one it refused is tried again, and that one carries its incremented attempt count.
pub(crate) fn partition_send_backs(
    messages: Vec<ArcMut<MessageExt>>,
    sent_back: &[bool],
) -> (Vec<ArcMut<MessageExt>>, Vec<ArcMut<MessageExt>>) {
    let mut failed = Vec::with_capacity(messages.len());
    let mut succeeded = Vec::with_capacity(messages.len());
    for (index, mut msg) in messages.into_iter().enumerate() {
        if sent_back.get(index).copied().unwrap_or(false) {
            succeeded.push(msg);
        } else {
            let reconsume_times = msg.reconsume_times() + 1;
            msg.set_reconsume_times(reconsume_times);
            failed.push(msg);
        }
    }
    (failed, succeeded)
}

struct ConsumeRequest {
    /// Accounting ownership of `msgs`, taken when the batch was admitted. `None` for a request
    /// built without SDK ownership, which then neither adds nor releases counts.
    accounting: Option<DeliveryOwner>,
    msgs: Vec<ArcMut<MessageExt>>,
    message_listener: ArcBoxMessageListenerConcurrently,
    process_queue: Arc<ProcessQueue>,
    message_queue: MessageQueue,
    dispatch_to_consume: bool,
    consumer_group: CheetahString,
    default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    delivery: Arc<super::consumer_work::DeliveryGate>,
}

impl ConsumeRequest {
    async fn run(&mut self, mut consume_message_concurrently_service: ArcMut<ConsumeMessageConcurrentlyService>) {
        // SDK-owned delivery accounting (plan 49 section 4.2, plan 49c C4): this request holds the
        // ownership taken when its batch was admitted, so a revoked queue still releases the batch
        // it will never deliver and a listener that blocks or unwinds cannot leave pending
        // work counted forever - and nothing here has to add a count that someone else
        // already added.
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        if self.process_queue.is_dropped() {
            info!(
                "the message queue not be able to consume, because it's dropped. group={} {}",
                self.consumer_group, self.message_queue,
            );
            return;
        }
        // Listener admission (plan 49 section 4.3): claimed atomically before anything is executed.
        // A batch that never started is rejected here without advancing its offsets, while a batch
        // that did start holds the permit until its commit, so cleanup drains it instead of losing
        // it. The lock is not held across the listener call.
        let Some(_admission) = self.delivery.admit() else {
            return;
        };
        let mut context = ConsumeConcurrentlyContext::new(self.message_queue.clone());
        let consumer_group = self.consumer_group.clone();
        DefaultMQPushConsumerImpl::try_reset_pop_retry_topic(&mut self.msgs, consumer_group.as_str());
        default_mqpush_consumer_impl.reset_retry_and_namespace(&mut self.msgs, consumer_group.as_str());

        let mut consume_message_context = None;

        let begin_timestamp = Instant::now();
        let mut has_exception = false;

        let mut status = None;

        if !self.msgs.is_empty() {
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(
                    msg.as_mut(),
                    CheetahString::from_string(get_current_millis().to_string()),
                );
            }
            if default_mqpush_consumer_impl.has_hook() {
                let queue = self.message_queue.clone();
                consume_message_context = Some(ConsumeMessageContext {
                    consumer_group,
                    msg_list: &self.msgs,
                    mq: Some(queue),
                    success: false,
                    status: CheetahString::new(),
                    mq_trace_context: None,
                    props: Default::default(),
                    namespace: default_mqpush_consumer_impl
                        .client_config
                        .get_namespace()
                        .unwrap_or_default(),
                    access_channel: Default::default(),
                });
                default_mqpush_consumer_impl.execute_hook_before(&mut consume_message_context);
            }
            let vec = self.msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>();
            if let Some(owner) = self.accounting.as_mut() {
                owner.enter_listener();
            }
            match call_listener_catching(|| self.message_listener.consume_message(&vec, &mut context)) {
                ListenerCall::Returned(Ok(value)) => {
                    status = Some(value);
                }
                ListenerCall::Returned(Err(_)) => {
                    has_exception = true;
                }
                // Plan 49a F5: an unwinding listener is failed consumption, not a dead task. The delivery
                // guard still releases this batch as the panic unwinds, and leaving `status` unset routes
                // the messages through the existing retry/send-back path below.
                ListenerCall::ThrewPanic => {
                    error!(
                        "message listener panicked, treating {} message(s) as failed consumption, mq={}",
                        self.msgs.len(),
                        self.message_queue
                    );
                    // A listener that unwound may have acknowledged part of the batch on its way
                    // out. That acknowledgement is void: the panic means the
                    // work it covered did not finish, and honouring it would
                    // advance consumption past business work while reporting the batch
                    // as retried (plan 49c C4). A listener that *returned* with an ack index keeps
                    // it.
                    context.ack_index = None;
                    has_exception = true;
                }
            }
            // The callback is over: user code no longer occupies a slot, and the batch moves to the
            // commit phase whose offsets this call still has to write (plan 49c C4).
            if let Some(owner) = self.accounting.as_mut() {
                owner.leave_listener();
            }
            // Update last_consume_timestamp after the listener returns so that
            // admin/running-info reflects real callback progress, not flow-control entries.
            self.process_queue
                .last_consume_timestamp
                .store(get_current_millis(), std::sync::atomic::Ordering::Release);
        }

        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;

        let return_type = if let Some(s) = status {
            if consume_rt > default_mqpush_consumer_impl.consumer_config.consume_timeout * 60 * 1000 {
                ConsumeReturnType::TimeOut
            } else if s == ConsumeConcurrentlyStatus::ReconsumeLater {
                ConsumeReturnType::Failed
            } else {
                // Must be ConsumeSuccess
                ConsumeReturnType::Success
            }
        } else if has_exception {
            ConsumeReturnType::Exception
        } else {
            ConsumeReturnType::ReturnNull
        };

        if default_mqpush_consumer_impl.has_hook() {
            consume_message_context.as_mut().unwrap().props.insert(
                CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE),
                return_type.to_string().into(),
            );
        }

        if status.is_none() {
            status = Some(ConsumeConcurrentlyStatus::ReconsumeLater);
        }

        if default_mqpush_consumer_impl.has_hook() {
            let cmc = consume_message_context.as_mut().unwrap();
            cmc.status = status.unwrap().to_string().into();
            cmc.success = status.unwrap() == ConsumeConcurrentlyStatus::ConsumeSuccess;
            cmc.access_channel = Some(default_mqpush_consumer_impl.client_config.access_channel);
            default_mqpush_consumer_impl.execute_hook_after(&mut consume_message_context);
        }

        if self.process_queue.is_dropped() {
            warn!(
                "the message queue not be able to consume, because it's dropped. group={} {}",
                self.consumer_group, self.message_queue,
            );
        } else {
            let this = consume_message_concurrently_service.clone();

            consume_message_concurrently_service
                .process_consume_result(this, status.unwrap(), &context, self)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use super::*;
    use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
    use crate::consumer::store::offset_store::OffsetStore;
    use crate::consumer::store::read_offset_type::ReadOffsetType;
    use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
    use crate::factory::mq_client_instance::MQClientInstance;

    struct Listener {
        status: ConsumeConcurrentlyStatus,
        ack_index: Option<i32>,
    }

    impl MessageListenerConcurrently for Listener {
        fn consume_message(
            &self,
            _: &[&MessageExt],
            context: &mut ConsumeConcurrentlyContext,
        ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
            context.ack_index = self.ack_index;
            Ok(self.status)
        }
    }

    fn message(queue_offset: i64, reconsume_times: i32) -> MessageExt {
        let mut msg = MessageExt {
            queue_offset,
            reconsume_times,
            ..Default::default()
        };
        msg.message.body = Some(bytes::Bytes::from_static(b"x"));
        msg
    }

    /// A consumer whose every remote call fails fast, a process queue holding `messages`, and the
    /// request that runs one delivery attempt over them.
    struct Fixture {
        group: CheetahString,
        mq: MessageQueue,
        push: ArcMut<DefaultMQPushConsumerImpl>,
        service: ArcMut<ConsumeMessageConcurrentlyService>,
        pq: StdArc<ProcessQueue>,
        client: ArcMut<MQClientInstance>,
    }

    impl Fixture {
        async fn new(
            name: &str,
            message_model: MessageModel,
            broadcast_max_retries: u32,
            messages: Vec<MessageExt>,
        ) -> Self {
            let group = CheetahString::from_string(format!("rereview49d-{name}"));
            let cc = ClientConfig {
                namesrv_addr: Some(CheetahString::from_static_str("127.0.0.1:1")),
                ..Default::default()
            };
            let cfg = ArcMut::new(ConsumerConfig {
                consumer_group: group.clone(),
                message_model,
                consume_thread_min: 1,
                consume_thread_max: 1,
                broadcast_max_retries,
                // Long enough that a locally retried message is still parked when the test looks.
                broadcast_retry_delay: Duration::from_secs(600),
                consume_concurrently_max_span: 0,
                ..Default::default()
            });
            let listener: ArcBoxMessageListenerConcurrently = StdArc::new(Box::new(Listener {
                status: ConsumeConcurrentlyStatus::ReconsumeLater,
                ack_index: None,
            }));
            let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(cc.clone(), cfg.clone(), None));
            let handle = push.clone();
            push.set_default_mqpush_consumer_impl(handle);
            let client = MQClientInstance::new_arc(cc.clone(), 0, format!("rereview49d-{name}"), None);
            push.offset_store = Some(ArcMut::new(OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
                client.clone(),
                group.clone(),
            ))));
            let service = ArcMut::new(ConsumeMessageConcurrentlyService::new(
                ArcMut::new(cc),
                cfg,
                group.clone(),
                listener.clone(),
                Some(push.clone()),
                push.delivery.clone(),
            ));
            let pq = StdArc::new(ProcessQueue::new());
            let mq = MessageQueue::from_parts("T", "broker-a", 0);
            let msgs = messages.into_iter().map(ArcMut::new).collect::<Vec<_>>();
            // Keep shared handles so the test can inspect exactly what is still cached.
            let cached_handles = msgs.clone();
            pq.put_message(cached_handles).await;
            Fixture {
                group,
                mq,
                push,
                service,
                pq,
                client,
            }
        }

        fn request(
            &self,
            msgs: Vec<ArcMut<MessageExt>>,
            listener: ArcBoxMessageListenerConcurrently,
        ) -> ConsumeRequest {
            let accounting = Some(self.push.progress.admit_batch(&self.mq, msgs.len()));
            ConsumeRequest {
                msgs,
                message_listener: listener,
                process_queue: self.pq.clone(),
                message_queue: self.mq.clone(),
                dispatch_to_consume: true,
                consumer_group: self.group.clone(),
                default_mqpush_consumer_impl: Some(self.push.clone()),
                delivery: self.push.delivery.clone(),
                accounting,
            }
        }

        fn listener(
            &self,
            status: ConsumeConcurrentlyStatus,
            ack_index: Option<i32>,
        ) -> ArcBoxMessageListenerConcurrently {
            StdArc::new(Box::new(Listener { status, ack_index }))
        }

        async fn memory_offset(&self) -> i64 {
            self.push
                .offset_store
                .as_ref()
                .unwrap()
                .read_offset(&self.mq, ReadOffsetType::ReadFromMemory)
                .await
        }

        async fn shutdown(mut self) {
            self.service.shutdown(100).await;
            self.client.shutdown().await;
        }
    }

    fn handles(messages: &[MessageExt]) -> Vec<ArcMut<MessageExt>> {
        messages.iter().cloned().map(ArcMut::new).collect()
    }

    /// Control the broker's answers to this batch's send-backs (test-only field, plan 49d R4).
    fn queue_send_back(fixture: &Fixture, outcomes: &[bool]) {
        let mut guard = fixture.service.send_back_outcomes.lock().unwrap();
        guard.clear();
        guard.extend(outcomes.iter().copied());
    }

    /// A broadcast message that has used its last retry is finished with the queue: it must leave
    /// `ProcessQueue` and advance the memory offset, not sit there pinning both forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_terminal_broadcast_failure_leaves_the_process_queue() {
        let messages = vec![message(0, 0)];
        let fixture = Fixture::new("terminal", MessageModel::Broadcasting, 0, messages.clone()).await;
        let listener = fixture.listener(ConsumeConcurrentlyStatus::ReconsumeLater, None);
        let mut request = fixture.request(handles(&messages), listener);
        request.run(fixture.service.clone()).await;
        drop(request);

        let snapshot = fixture
            .push
            .progress
            .snapshot(Duration::from_secs(30), Duration::from_secs(45));
        let cached = fixture.pq.msg_count();
        let offset = fixture.memory_offset().await;
        assert_eq!(cached, 0, "terminal drop left a message pinning the queue");
        assert_eq!(offset, 1, "the memory offset must move past a terminal drop");
        assert_eq!(snapshot.pending_delivery_msgs, 0);
        assert_eq!(snapshot.committing_delivery_msgs, 0);
        fixture.shutdown().await;
    }

    /// Mixed batch: acknowledged and terminal messages leave, the one still being retried stays
    /// cached, and the offset may not pass what is still in the queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mixed_broadcast_batch_removes_only_what_it_finished() {
        let messages = vec![
            message(0, 0), // acknowledged by ack_index
            message(1, 0), // one retry left -> stays cached
            message(2, 1), // out of retries -> terminal
        ];
        let fixture = Fixture::new("mixed", MessageModel::Broadcasting, 1, messages.clone()).await;
        let listener = fixture.listener(ConsumeConcurrentlyStatus::ReconsumeLater, Some(0));
        let mut request = fixture.request(handles(&messages), listener);
        request.run(fixture.service.clone()).await;
        drop(request);

        assert_eq!(fixture.pq.msg_count(), 1, "only the retried message stays cached");
        assert!(
            fixture.pq.contains_message(&messages[1]).await,
            "the retried message must still be in the queue"
        );
        assert_eq!(
            fixture.memory_offset().await,
            1,
            "the offset may not pass a message that is still cached"
        );
        let snapshot = fixture
            .push
            .progress
            .snapshot(Duration::from_secs(30), Duration::from_secs(45));
        assert_eq!(
            snapshot.pending_delivery_msgs + snapshot.committing_delivery_msgs,
            1,
            "the retried message stays outstanding work while the finished ones are released"
        );
        fixture.shutdown().await;
    }

    /// A partially acknowledged successful batch removes exactly what it acknowledged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_partially_acknowledged_batch_removes_the_acknowledged_prefix() {
        let messages = vec![message(0, 0), message(1, 0), message(2, 0)];
        let fixture = Fixture::new("partial-ack", MessageModel::Broadcasting, 5, messages.clone()).await;
        let listener = fixture.listener(ConsumeConcurrentlyStatus::ConsumeSuccess, Some(0));
        let mut request = fixture.request(handles(&messages), listener);
        request.run(fixture.service.clone()).await;
        drop(request);

        assert_eq!(fixture.pq.msg_count(), 2, "the acknowledged message leaves");
        assert!(!fixture.pq.contains_message(&messages[0]).await);
        assert_eq!(fixture.memory_offset().await, 1);
        fixture.shutdown().await;
    }

    /// A clustering send-back the broker refused stays cached and outstanding: only what the broker
    /// accepted is finished with the queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_send_back_stays_in_the_process_queue() {
        let messages = vec![message(0, 0)];
        let fixture = Fixture::new("send-back", MessageModel::Clustering, 3, messages.clone()).await;
        queue_send_back(&fixture, &[false]);
        let listener = fixture.listener(ConsumeConcurrentlyStatus::ReconsumeLater, None);
        let mut request = fixture.request(handles(&messages), listener);
        request.run(fixture.service.clone()).await;
        drop(request);

        assert_eq!(fixture.pq.msg_count(), 1, "a refused send-back stays cached");
        assert!(fixture.pq.contains_message(&messages[0]).await);
        assert_eq!(
            fixture.memory_offset().await,
            0,
            "the offset may only point back at the message that is still cached"
        );
        let snapshot = fixture
            .push
            .progress
            .snapshot(Duration::from_secs(30), Duration::from_secs(45));
        assert_eq!(
            snapshot.pending_delivery_msgs + snapshot.committing_delivery_msgs,
            1,
            "the refused message stays outstanding work"
        );
        fixture.shutdown().await;
    }

    /// Every send-back the broker accepted finishes the whole batch: nothing may stay cached and
    /// the offset moves past all of it, even though no message was acknowledged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_all_successful_send_back_empties_the_process_queue() {
        let messages = vec![message(0, 0), message(1, 0)];
        let fixture = Fixture::new("send-back-all", MessageModel::Clustering, 3, messages.clone()).await;
        queue_send_back(&fixture, &[true, true]);
        let listener = fixture.listener(ConsumeConcurrentlyStatus::ReconsumeLater, None);
        let mut request = fixture.request(handles(&messages), listener);
        request.run(fixture.service.clone()).await;
        drop(request);

        assert_eq!(
            fixture.pq.msg_count(),
            0,
            "a handed-back batch is finished with the queue"
        );
        assert_eq!(fixture.memory_offset().await, 2);
        let snapshot = fixture
            .push
            .progress
            .snapshot(Duration::from_secs(30), Duration::from_secs(45));
        assert_eq!(
            snapshot.pending_delivery_msgs + snapshot.committing_delivery_msgs,
            0,
            "a message the broker took back is no longer outstanding work here"
        );
        fixture.shutdown().await;
    }

    /// A mixed send-back batch removes only what the broker accepted; the refused one stays cached
    /// and keeps the offset from passing it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mixed_send_back_removes_only_what_the_broker_took() {
        let messages = vec![message(0, 0), message(1, 0)];
        let fixture = Fixture::new("send-back-mixed", MessageModel::Clustering, 3, messages.clone()).await;
        queue_send_back(&fixture, &[true, false]);
        let listener = fixture.listener(ConsumeConcurrentlyStatus::ReconsumeLater, None);
        let mut request = fixture.request(handles(&messages), listener);
        request.run(fixture.service.clone()).await;
        drop(request);

        assert_eq!(fixture.pq.msg_count(), 1, "only the refused message stays cached");
        assert!(fixture.pq.contains_message(&messages[1]).await);
        assert_eq!(fixture.memory_offset().await, 1);
        fixture.shutdown().await;
    }

    /// The broker's answers decide what leaves the queue: an all-successful batch is finished with
    /// every message, and a mixed one only finishes the accepted subset.
    #[test]
    fn send_back_answers_decide_what_is_finished() {
        let build = |offsets: &[i64]| {
            offsets
                .iter()
                .map(|offset| ArcMut::new(message(*offset, 0)))
                .collect::<Vec<_>>()
        };

        let (failed, finalized) = partition_send_backs(build(&[0, 1, 2]), &[true, true, true]);
        assert!(failed.is_empty());
        assert_eq!(finalized.len(), 3, "an all-successful batch is fully finished");
        assert_eq!(finalized[0].reconsume_times(), 0);

        let (failed, finalized) = partition_send_backs(build(&[0, 1, 2]), &[true, false, true]);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].queue_offset, 1);
        assert_eq!(failed[0].reconsume_times(), 1, "a retried message counts its attempt");
        assert_eq!(finalized.len(), 2);

        let (failed, finalized) = partition_send_backs(build(&[0, 1]), &[false, false]);
        assert_eq!(failed.len(), 2);
        assert!(finalized.is_empty());

        let (failed, finalized) = partition_send_backs(Vec::new(), &[]);
        assert!(failed.is_empty() && finalized.is_empty());
    }
}
