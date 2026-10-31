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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_error::RocketmqError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::warn;

use crate::consumer::consumer_impl::consumer_lifecycle::PullFailureKind;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::pull_status::PullStatus;

/// What a **typed** response code says about the smallest repair that can help (plan 49f F3).
///
/// Judged from the code itself. A broker remark is presentation: its wording, language and
/// wrapping decide whether a substring match fires, so classifying by text could turn one fault
/// into whichever family its message happened to mention.
fn classify_response_code(code: ResponseCode) -> Option<PullFailureKind> {
    match code {
        // The address we hold for this queue is stale or the service behind it is not there: a
        // coalesced route refresh is the repair that can fix it.
        ResponseCode::TopicNotExist
        | ResponseCode::ServiceNotAvailable
        | ResponseCode::BrokerNotExist
        | ResponseCode::SlaveNotAvailable
        | ResponseCode::BrokerDispatchNotComplete
        | ResponseCode::NotLeaderForQueue => Some(PullFailureKind::RouteOrBrokerMiss),
        // The RPC did not answer inside its own window.
        ResponseCode::RpcTimeOut | ResponseCode::PollingTimeout => Some(PullFailureKind::RpcTimeout),
        _ => None,
    }
}

/// Maps a pull failure onto the smallest repair that can help (plan 49 section 4.2). Route and
/// broker misses are addressable by a coalesced route refresh; timeouts are expected while a
/// broker is slow and must not escalate on their own.
///
/// Typed codes and error variants are consulted first, display text only when the failure carries
/// no code with a family of its own (plan 49f F3).
fn classify_pull_failure(err: &(dyn std::error::Error + Send + 'static)) -> PullFailureKind {
    if let Some(error) = err.downcast_ref::<RocketmqError>() {
        match error {
            RocketmqError::RemotingTimeoutError(_, _) => return PullFailureKind::RpcTimeout,
            RocketmqError::RequestTimeoutError(_) => return PullFailureKind::RpcTimeout,
            // A channel that could not carry the request at all: the address book is what a route
            // refresh repairs, and none of these need their message read.
            RocketmqError::RemotingConnectError(_)
            | RocketmqError::ConnectionInvalid(_)
            | RocketmqError::ChannelSendRequestFailed(_)
            | RocketmqError::RemotingSendRequestError(_) => return PullFailureKind::RouteOrBrokerMiss,
            RocketmqError::MQClientBrokerError(broker_error) => {
                let code = ResponseCode::from(broker_error.response_code());
                return classify_response_code(code).unwrap_or_else(|| match code {
                    // An unrecognised code carries no typed meaning, so the remark is all there is.
                    ResponseCode::SystemError => {
                        classify_text(broker_error.error_message().map_or("", |text| text.as_str()))
                    }
                    _ => PullFailureKind::Other,
                });
            }
            // `MQBrokerError` carries (code, message, broker address); only the message can say more
            // than the code does.
            RocketmqError::MQBrokerError(code, remark, _) | RocketmqError::RpcError(code, remark) => {
                let code = ResponseCode::from(*code);
                return classify_response_code(code).unwrap_or_else(|| match code {
                    ResponseCode::SystemError => classify_text(remark),
                    _ => PullFailureKind::Other,
                });
            }
            _ => {}
        }
    }
    classify_text(&err.to_string())
}

fn is_route_text(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("not exist") || text.contains("route") || text.contains("no broker") || text.contains("connect to")
}

fn is_timeout_text(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("timeout") || text.contains("timed out")
}

fn classify_text(text: &str) -> PullFailureKind {
    if is_route_text(text) {
        PullFailureKind::RouteOrBrokerMiss
    } else if is_timeout_text(text) {
        PullFailureKind::RpcTimeout
    } else {
        PullFailureKind::Other
    }
}

pub type PullCallbackFn =
    Arc<dyn FnOnce(Option<PullResultExt>, Option<Box<dyn std::error::Error + Send>>) + Send + Sync>;

#[trait_variant::make(PullCallback: Send)]
pub trait PullCallbackLocal: Sync {
    async fn on_success(&mut self, pull_result: PullResultExt);
    fn on_exception(&mut self, e: Box<dyn std::error::Error + Send>);
    fn cancellation(&self) -> Option<tokio_util::sync::CancellationToken> {
        None
    }
}

pub(crate) struct DefaultPullCallback {
    pub(crate) push_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    pub(crate) message_queue_inner: Option<MessageQueue>,
    pub(crate) subscription_data: Option<SubscriptionData>,
    pub(crate) pull_request: Option<PullRequest>,
    pub(crate) work: tokio_util::task::task_tracker::TaskTrackerToken,
}

impl DefaultPullCallback {
    /// Whether the answer this callback holds still belongs to the acquisition that issued it
    /// (plan 49f F3).
    ///
    /// Every callback path asks this **before** any shared effect, because an answer whose
    /// acquisition ended is history: notifying the application, retiring another attempt,
    /// publishing messages or scheduling a retry would all act on a queue its replacement owns.
    fn holds_this_acquisition(&self) -> bool {
        match self.pull_request.as_ref() {
            Some(request) => self
                .push_consumer_impl
                .holds_acquisition(request.get_message_queue(), request.lease_epoch()),
            // Nothing names an acquisition, so nothing of ours may be touched on its behalf.
            None => false,
        }
    }
}

impl PullCallback for DefaultPullCallback {
    fn cancellation(&self) -> Option<tokio_util::sync::CancellationToken> {
        Some(self.push_consumer_impl.work.cancellation())
    }

    async fn on_success(&mut self, mut pull_result_ext: PullResultExt) {
        let Some(_work) = self.push_consumer_impl.work.enter() else {
            return;
        };
        if self
            .pull_request
            .as_ref()
            .is_none_or(|request| request.process_queue.is_dropped())
        {
            return;
        }
        // Plan 49f F3: an answer whose acquisition has ended is history, and being history it may
        // not touch anything shared - not the progress listener, not the attempt record, not the
        // process queue or delivery accounting below. Revocation ends an acquisition, so a late
        // success for a revoked queue used to notify the application and publish into whatever
        // process queue now answers under that key.
        if !self.holds_this_acquisition() {
            debug!("a pull answer arrived after its acquisition ended, dropping it before any shared state",);
            return;
        }
        if matches!(
            pull_result_ext.pull_result.pull_status,
            PullStatus::Found | PullStatus::NoNewMsg | PullStatus::NoMatchedMsg
        ) {
            if let Some(listener) = &self.push_consumer_impl.pull_progress_listener {
                listener(pull_result_ext.pull_result.pull_status == PullStatus::Found);
            }
        }
        /*let push_consumer_impl = self.push_consumer_impl.upgrade();
        if push_consumer_impl.is_none() {
            warn!("push_consumer_impl is None");
            return;
        }*/
        let mut push_consumer_impl = self.push_consumer_impl.clone();

        let message_queue_inner = self.message_queue_inner.take().unwrap();
        let subscription_data = self.subscription_data.take().unwrap();
        let mut pull_request = self.pull_request.take().unwrap();

        // Plan 49d R1: the RPC answered, so this attempt is over no matter what happens to the
        // answer below (expired acquisition, decode failure, immediate continuation).
        // Retiring it here keeps a completed loop from carrying an outstanding-RPC
        // deadline, while the lease stays held by the loop that owns the queue.
        push_consumer_impl.finish_pull_attempt(&pull_request);

        push_consumer_impl
            .pull_api_wrapper
            .as_mut()
            .unwrap()
            .process_pull_result(&message_queue_inner, &mut pull_result_ext, &subscription_data);

        // Plan 49a F4 section 4.3 item 2, plan 49f F3: attribution is asked again **after** the
        // answer has been decoded and its filter hooks have run, because that is the last point
        // before anything of this consumer's is touched. A success published after its acquisition
        // ended would land under a process queue this consumer no longer owns, advance offsets
        // nobody read, and - recorded as progress - clear the fault the queue's current owner has.
        // The current owner pulls for itself.
        if !push_consumer_impl.holds_acquisition(&message_queue_inner, pull_request.lease_epoch()) {
            debug!(
                "pull answer lost its acquisition while it was decoded, dropping it without publishing, mq={}",
                message_queue_inner
            );
            return;
        }

        // SDK-owned progress observation (plan 49 section 4.2, plan 49a F5 section 4.4 item 1): a
        // response for an owned queue is progress whether or not it carried messages.
        // Whether the pull "found" anything is read from the decoded result, because
        // process_pull_result is what decodes the wire bytes and drops messages this
        // subscription does not match; before that call the list does not exist yet, so a
        // delivered batch used to be recorded as an empty pull.
        {
            let found = matches!(pull_result_ext.pull_result.pull_status, PullStatus::Found)
                && pull_result_ext
                    .pull_result
                    .msg_found_list
                    .as_ref()
                    .is_some_and(|list| !list.is_empty());
            push_consumer_impl
                .progress
                .note_pull_completed(&message_queue_inner, found);
        }
        match pull_result_ext.pull_result.pull_status {
            PullStatus::Found => {
                let prev_request_offset = pull_request.next_offset;
                pull_request.set_next_offset(pull_result_ext.pull_result.next_begin_offset as i64);
                /*let pull_rt = get_current_millis() - begin_timestamp.elapsed().as_millis() as u64;
                self.client_instance.as_mut().unwrap().*/
                let mut first_msg_offset = i64::MAX;
                if pull_result_ext
                    .pull_result
                    .msg_found_list
                    .as_ref()
                    .is_none_or(|v| v.is_empty())
                {
                    push_consumer_impl.execute_pull_request_immediately(pull_request).await;
                } else {
                    first_msg_offset = pull_result_ext
                        .pull_result
                        .msg_found_list
                        .as_ref()
                        .unwrap()
                        .first()
                        .unwrap()
                        .queue_offset;
                    let vec = pull_result_ext.pull_result.msg_found_list.clone();
                    // Accounting ownership is taken here, from the very list handed to put_message
                    // and submit_consume_request below, because that batch
                    // travels from here through queueing, any delayed retry,
                    // listener execution and final commit. Counting at handover keeps a
                    // stalled listener visible even when its pulls are quiet; taking *ownership*
                    // here is what stops the consume path from releasing
                    // messages a sleeping retry still holds (plan 49a
                    // F5 section 4.4 item 1, plan 49c C4).
                    let accounting = push_consumer_impl.progress.admit_batch(
                        pull_request.get_message_queue(),
                        vec.as_ref().map(|list| list.len()).unwrap_or(0),
                    );
                    let dispatch_to_consume = pull_request.process_queue.put_message(vec.unwrap_or_default()).await;
                    push_consumer_impl
                        .consume_message_service
                        .as_mut()
                        .unwrap()
                        .submit_consume_request(
                            pull_result_ext.pull_result.msg_found_list.unwrap_or_default(),
                            pull_request.get_process_queue().clone(),
                            pull_request.get_message_queue().clone(),
                            dispatch_to_consume,
                            accounting,
                        )
                        .await;
                    let pull_interval = push_consumer_impl.consumer_config.pull_interval;
                    if pull_interval > 0 {
                        push_consumer_impl.execute_pull_request_later(pull_request, pull_interval);
                    } else {
                        push_consumer_impl.execute_pull_request_immediately(pull_request).await;
                    }
                }
                if pull_result_ext.pull_result.next_begin_offset < prev_request_offset as u64
                    || first_msg_offset < prev_request_offset
                {
                    warn!(
                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} \
                         prevRequestOffset: {}",
                        pull_result_ext.pull_result.next_begin_offset, prev_request_offset, prev_request_offset
                    );
                }
            }
            PullStatus::NoNewMsg | PullStatus::NoMatchedMsg => {
                pull_request.next_offset = pull_result_ext.pull_result.next_begin_offset as i64;
                push_consumer_impl.correct_tags_offset(&pull_request).await;
                push_consumer_impl.execute_pull_request_immediately(pull_request).await;
            }

            PullStatus::OffsetIllegal => {
                warn!(
                    "the pull request offset illegal, {},{}",
                    pull_result_ext.pull_result, pull_result_ext.pull_result.pull_status
                );
                pull_request.next_offset = pull_result_ext.pull_result.next_begin_offset as i64;
                pull_request.process_queue.set_dropped(true);

                let offset_store = push_consumer_impl.offset_store.as_mut().unwrap();
                offset_store
                    .update_and_freeze_offset(pull_request.get_message_queue(), pull_request.next_offset)
                    .await;
                offset_store.persist(pull_request.get_message_queue()).await;
                push_consumer_impl
                    .rebalance_impl
                    .remove_process_queue(pull_request.get_message_queue())
                    .await;
                push_consumer_impl
                    .rebalance_impl
                    .rebalance_impl_inner
                    .client_instance
                    .as_ref()
                    .unwrap()
                    .re_balance_immediately()
            }
        };
    }

    fn on_exception(&mut self, err: Box<dyn std::error::Error + Send>) {
        let Some(_work) = self.push_consumer_impl.work.enter() else {
            return;
        };
        let message_queue_inner = self.message_queue_inner.take().unwrap();
        // Plan 49f F3: the same gate as a successful answer. A failure whose acquisition ended is
        // history, and history may not retire another attempt, warn as current work, or schedule a
        // retry for whoever owns the queue now. It still counts in the lifetime tallies (plan 49e
        // E1 kept those honest about what happened), which is all this records before
        // returning.
        if !self.holds_this_acquisition() {
            if let Some(request) = self.pull_request.take() {
                self.push_consumer_impl.progress.note_pull_failed_for(
                    request.get_message_queue(),
                    request.lease_epoch(),
                    classify_pull_failure(err.as_ref()),
                );
                debug!(
                    "a pull failure arrived after its acquisition ended, counted as history and dropped without \
                     touching the queue's current owner",
                );
            }
            return;
        }
        let pull_request = self.pull_request.take().unwrap();
        // A failed attempt is finished work too: it must not stay outstanding while the retry timer
        // runs (plan 49d R1).
        self.push_consumer_impl.finish_pull_attempt(&pull_request);
        // Classifying the failure keeps recovery proportional: a route or broker miss is repaired
        // in place, an RPC timeout is only watched, and neither is reported as a bare count.
        // Plan 49e E1: identity is checked first, because a failure of an **expired** acquisition
        // is history - counting it as this queue's newest outcome would fault the owner
        // that replaced it with work it never ran. Its tallies still remember it.
        let recorded = self.push_consumer_impl.progress.note_pull_failed_for(
            &message_queue_inner,
            pull_request.lease_epoch(),
            classify_pull_failure(err.as_ref()),
        );
        if !recorded {
            // The acquisition is current yet reported no acquisition of its own: worth knowing, but
            // the queue stays owned and keeps pulling.
            debug!(
                "a pull failure could not be attributed to a tracked acquisition of this queue, mq={}",
                message_queue_inner
            );
        }
        let topic = message_queue_inner.get_topic();
        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if let Some(er) = err.downcast_ref::<RocketmqError>() {
                match er {
                    RocketmqError::MQClientBrokerError(broker_error) => {
                        if ResponseCode::from(broker_error.response_code()) == ResponseCode::SubscriptionNotLatest {
                            warn!(
                                "the subscription is not latest, group={}",
                                self.push_consumer_impl.consumer_config.consumer_group,
                            );
                        } else {
                            warn!(
                                "execute the pull request exception, group={}",
                                self.push_consumer_impl.consumer_config.consumer_group
                            );
                        }
                    }
                    _ => {
                        warn!(
                            "execute the pull request exception, group={}",
                            self.push_consumer_impl.consumer_config.consumer_group
                        );
                    }
                }
            } else {
                warn!(
                    "execute the pull request exception, group={}",
                    self.push_consumer_impl.consumer_config.consumer_group
                );
            }
        }
        let time_delay = if let Some(er) = err.downcast_ref::<RocketmqError>() {
            match er {
                RocketmqError::MQClientBrokerError(broker_error) => {
                    if ResponseCode::from(broker_error.response_code()) == ResponseCode::FlowControl {
                        PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL
                    } else {
                        self.push_consumer_impl.pull_time_delay_mills_when_exception
                    }
                }
                _ => self.push_consumer_impl.pull_time_delay_mills_when_exception,
            }
        } else {
            self.push_consumer_impl.pull_time_delay_mills_when_exception
        };

        self.push_consumer_impl
            .execute_pull_request_later(pull_request, time_delay);
    }
}

#[cfg(test)]
mod review49e {
    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use cheetah_string::CheetahString;
    #[test]
    fn a_stale_failure_must_not_publish_into_the_new_acquisition() {
        let push = ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            ArcMut::new(ConsumerConfig::default()),
            None,
        ));
        push.mark_managed_lifecycle();
        let q = MessageQueue::from_parts("T", "broker", 0);
        push.progress.note_queue_installed(&q);
        let old = push.progress.acquire_pull_lease(&q).unwrap();
        let mut request = PullRequest::new(
            CheetahString::from_static_str("review49e"),
            q.clone(),
            Arc::new(ProcessQueue::new()),
            0,
        );
        request.hold_lease(old);
        push.progress.note_queue_revoked(&q);
        push.progress.note_queue_installed(&q);
        push.progress.note_pull_completed(&q, false);
        let token = push.work.enter().unwrap();
        let mut callback = DefaultPullCallback {
            push_consumer_impl: push.clone(),
            message_queue_inner: Some(q),
            subscription_data: None,
            pull_request: Some(request),
            work: token,
        };
        PullCallback::on_exception(&mut callback, Box::new(std::io::Error::other("no broker route")));
        let s = push
            .progress
            .snapshot(std::time::Duration::from_secs(30), std::time::Duration::from_secs(35));
        eprintln!(
            "stale callback: route_miss_queues={} pull_failures={}",
            s.route_miss_queues, s.pull_failures
        );
        assert_eq!(
            s.route_miss_queues, 0,
            "an expired acquisition poisoned its replacement's evidence"
        );
    }
}

/// Plan 49f F3: an answer whose acquisition ended reaches nothing shared.
#[cfg(test)]
mod review49f {
    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::consumer_lifecycle::ConsumerProgress;
    use crate::consumer::consumer_impl::consumer_lifecycle::LifecyclePolicy;
    use crate::consumer::consumer_impl::consumer_lifecycle::Reason;
    use crate::consumer::consumer_impl::consumer_lifecycle::RecoveryMachine;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use crate::consumer::pull_result::PullResult;
    use crate::factory::mq_client_instance::MQClientInstance;
    use crate::hook::filter_message_context::FilterMessageContext;
    use crate::hook::filter_message_hook::FilterMessageHook;
    use cheetah_string::CheetahString;
    struct ReacquireInFilter {
        progress: Arc<ConsumerProgress>,
        queue: MessageQueue,
    }
    impl FilterMessageHook for ReacquireInFilter {
        fn hook_name(&self) -> &str {
            "review49f-reacquire"
        }
        fn filter_message(&self, _: &FilterMessageContext<'_>) {
            self.progress.note_queue_revoked(&self.queue);
            self.progress.note_queue_installed(&self.queue);
            self.progress.note_pull_failed_for(
                &self.queue,
                self.progress.current_epoch(&self.queue),
                PullFailureKind::RouteOrBrokerMiss,
            );
            std::thread::sleep(std::time::Duration::from_millis(3));
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn success_after_reacquisition_must_not_clear_the_new_owners_failure() {
        let cc = ClientConfig::default();
        let mut push = ArcMut::new(DefaultMQPushConsumerImpl::new(
            cc.clone(),
            ArcMut::new(ConsumerConfig::default()),
            None,
        ));
        push.mark_managed_lifecycle();
        let mut client = MQClientInstance::new_arc(cc, 0, "review49f-success-client", None);
        let capture = client.pull_message_service.review49e_capture();
        push.client_instance = Some(client.clone());
        let q = MessageQueue::from_parts("T", "broker", 0);
        push.progress.note_queue_installed(&q);
        let old = push.progress.acquire_pull_lease(&q).unwrap();
        let mut request = PullRequest::new(
            CheetahString::from_static_str("review49f"),
            q.clone(),
            Arc::new(ProcessQueue::new()),
            0,
        );
        request.hold_lease(old);
        let mut wrapper = PullAPIWrapper::new(client.clone(), CheetahString::from_static_str("review49f"), false);
        wrapper.register_filter_message_hook(vec![Arc::new(Box::new(ReacquireInFilter {
            progress: push.progress.clone(),
            queue: q.clone(),
        }))]);
        push.pull_api_wrapper = Some(ArcMut::new(wrapper));
        let token = push.work.enter().unwrap();
        let mut callback = DefaultPullCallback {
            push_consumer_impl: push.clone(),
            message_queue_inner: Some(q),
            subscription_data: Some(SubscriptionData::default()),
            pull_request: Some(request),
            work: token,
        };
        let result = PullResultExt {
            pull_result: PullResult::new(PullStatus::Found, 0, 0, 0, None),
            suggest_which_broker_id: 0,
            message_binary: None,
            offset_delta: None,
        };
        PullCallback::on_success(&mut callback, result).await;
        let s = push
            .progress
            .snapshot(std::time::Duration::from_secs(30), std::time::Duration::from_secs(35));
        eprintln!(
            "late success: current_route_faults={} completed={} queued_stale={}",
            s.route_miss_queues,
            s.pulls_completed,
            capture.len()
        );
        client.shutdown().await;
        assert_eq!(
            s.route_miss_queues, 1,
            "success attribution was checked before decoding, then wrote a replacement acquisition"
        );
        assert_eq!(
            capture.len(),
            0,
            "an expired success must not enqueue another continuation"
        );
    }
    #[test]
    fn a_revoked_queues_failure_must_not_fault_the_remaining_assignment() {
        let push = ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            ArcMut::new(ConsumerConfig::default()),
            None,
        ));
        push.mark_managed_lifecycle();
        let old_q = MessageQueue::from_parts("T", "old", 0);
        let good = MessageQueue::from_parts("T", "good", 0);
        push.progress.note_queue_installed(&old_q);
        let old = push.progress.acquire_pull_lease(&old_q).unwrap();
        let mut request = PullRequest::new(
            CheetahString::from_static_str("review49f"),
            old_q.clone(),
            Arc::new(ProcessQueue::new()),
            0,
        );
        request.hold_lease(old);
        push.progress.note_queue_revoked(&old_q);
        push.progress.note_queue_installed(&good);
        push.progress.note_pull_completed(&good, false);
        std::thread::sleep(std::time::Duration::from_millis(3));
        let token = push.work.enter().unwrap();
        let mut callback = DefaultPullCallback {
            push_consumer_impl: push.clone(),
            message_queue_inner: Some(old_q),
            subscription_data: None,
            pull_request: Some(request),
            work: token,
        };
        PullCallback::on_exception(&mut callback, Box::new(std::io::Error::other("no broker route")));
        let s = push
            .progress
            .snapshot(std::time::Duration::from_secs(30), std::time::Duration::from_secs(35));
        let mut machine = RecoveryMachine::new(LifecyclePolicy::default(), 1);
        machine.on_start_result(true, false, false, 0.5);
        let (state, _) = machine.on_tick(&s, std::time::Duration::from_secs(35), 0.5);
        eprintln!(
            "removed queue failure: tracked_faults={} aggregate_miss={} last_ok={} state={state:?}",
            s.route_miss_queues, s.pull_route_miss_ms, s.last_pull_ok_ms
        );
        assert_eq!(s.route_miss_queues, 0);
        assert_eq!(
            state.reason,
            Reason::IdleNoMessages,
            "an expired tracked request became fallback evidence for an untracked queue"
        );
    }
}
