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

/// Maps a pull failure onto the smallest repair that can help (plan 49 section 4.2). Route and
/// broker misses are addressable by a coalesced route refresh; timeouts are expected while a
/// broker is slow and must not escalate on their own.
fn classify_pull_failure(err: &(dyn std::error::Error + Send + 'static)) -> PullFailureKind {
    if let Some(error) = err.downcast_ref::<RocketmqError>() {
        match error {
            RocketmqError::RemotingTimeoutError(_, _) => return PullFailureKind::RpcTimeout,
            RocketmqError::MQClientBrokerError(broker_error) => {
                return match ResponseCode::from(broker_error.response_code()) {
                    // Both mean the address book is stale; a coalesced route refresh can fix it.
                    ResponseCode::TopicNotExist | ResponseCode::ServiceNotAvailable => {
                        PullFailureKind::RouteOrBrokerMiss
                    }
                    _ => classify_text(broker_error.error_message().map_or("", |text| text.as_str())),
                };
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

        // Plan 49a F4 section 4.3 item 2: an answer can arrive after its acquisition moved on - the
        // queue was revoked, or revoked and assigned again. The lease epoch names the
        // acquisition that issued the request, so publishing now would put messages under a
        // process queue this consumer no longer owns and advance offsets nobody read. Drop
        // the answer only; bookkeeping owned elsewhere is untouched and the current owner
        // pulls for itself.
        if !push_consumer_impl.holds_acquisition(&message_queue_inner, pull_request.lease_epoch()) {
            debug!(
                "pull answer belongs to an expired acquisition, dropping it without publishing, mq={}",
                message_queue_inner
            );
            return;
        }

        push_consumer_impl
            .pull_api_wrapper
            .as_mut()
            .unwrap()
            .process_pull_result(&message_queue_inner, &mut pull_result_ext, &subscription_data);

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
                    // Pending work is counted here, from the very list handed to put_message and
                    // submit_consume_request below, because that batch's delivery guard releases
                    // exactly this many messages. Counting the
                    // undecoded/filtered list instead either missed every delivered
                    // batch - hiding a stalled listener as idleness - or kept filtered-out messages
                    // pending forever (plan 49a F5 section 4.4 item 1).
                    if let Some(list) = vec.as_ref() {
                        if !list.is_empty() {
                            push_consumer_impl.progress.note_delivery_pending(list.len() as u64);
                        }
                    }
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
        let pull_request = self.pull_request.take().unwrap();
        // Classifying the failure keeps recovery proportional: a route or broker miss is repaired
        // in place, an RPC timeout is only watched, and neither is reported as a bare count.
        self.push_consumer_impl
            .progress
            .note_pull_failed(&message_queue_inner, classify_pull_failure(err.as_ref()));
        if !self
            .push_consumer_impl
            .holds_acquisition(&message_queue_inner, pull_request.lease_epoch())
        {
            // A failure of an expired acquisition must not schedule a retry against it; dropping
            // the request here releases its lease (plan 49a F4 section 4.3 item 2).
            debug!(
                "pull failure belongs to an expired acquisition, not scheduling a retry, mq={}",
                message_queue_inner
            );
            return;
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
