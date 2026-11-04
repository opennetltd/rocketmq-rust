//! Opt-in real-broker coverage for repeated pull batches, including an idle-to-active transition.
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::managed_consumer::ShutdownOutcome;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

async fn continuous(model: MessageModel, label: &str) {
    let nameserver = std::env::var("ROCKETMQ_PHASE3_NAMESERVER")
        .expect("set ROCKETMQ_PHASE3_NAMESERVER to a disposable NameServer/Broker");
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let topic = format!("Continuous_{label}_{nonce}");
    let mut producer = DefaultMQProducer::builder()
        .producer_group(format!("Producer_{label}_{nonce}"))
        .name_server_addr(nameserver.clone())
        .build();
    producer.start().await.unwrap();
    // Create the route before starting the consumer. The unique topic makes first-offset safe.
    // Broker boot can precede its first successful registration with the NameServer.
    let template = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if let Ok(queues) = producer.fetch_publish_message_queues("TBW102").await {
                if !queues.is_empty() {
                    break queues;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("broker must register its auto-create route with the NameServer");
    let queue = MessageQueue::from_parts(topic.clone(), template[0].get_broker_name().clone(), 0);
    producer
        .send_to_queue_with_timeout(Message::new(topic.clone(), b"seed"), queue.clone(), 5000)
        .await
        .unwrap();
    let seen = Arc::new(Mutex::new(HashSet::new()));
    let received = seen.clone();
    let handle = DefaultMQPushConsumer::builder()
        .consumer_group(format!("Consumer_{label}_{nonce}"))
        .name_server_addr(nameserver)
        .message_model(model)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_message_batch_max_size(2)
        .subscribe(topic.clone(), "*")
        .start_managed(move |msgs, _| {
            let mut received = received.lock().unwrap();
            for msg in msgs {
                received.insert(msg.message.body.as_ref().unwrap().to_vec());
            }
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        })
        .unwrap();
    let per_wave = std::env::var("ROCKETMQ_CONTINUOUS_MESSAGES_PER_WAVE")
        .map(|value| value.parse::<usize>().expect("positive message count"))
        .unwrap_or(64);
    assert!(per_wave > 0);
    let mut complete = true;
    for wave in 0..3 {
        for index in 0..per_wave {
            let body = format!("{wave}:{index}");
            producer
                .send_to_queue_with_timeout(Message::new(topic.clone(), body.as_bytes()), queue.clone(), 5000)
                .await
                .unwrap();
        }
        let expected = 1 + (wave + 1) * per_wave;
        complete = tokio::time::timeout(Duration::from_secs(35), async {
            while seen.lock().unwrap().len() != expected {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok();
        eprintln!(
            "{label} wave={wave} expected={expected} received={} status={}",
            seen.lock().unwrap().len(),
            handle.status()
        );
        if !complete {
            break;
        }
        // A new broker answer must dispatch after the prior batch has completely drained.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let outcome = handle
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(15))
        .await;
    producer.shutdown().await;
    assert!(complete, "repeated pull batches stopped reaching the listener");
    assert_eq!(seen.lock().unwrap().len(), 1 + 3 * per_wave);
    assert!(matches!(outcome, ShutdownOutcome::Complete), "{outcome:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable real RocketMQ NameServer/Broker"]
async fn broadcasting_consumes_every_wave() {
    continuous(MessageModel::Broadcasting, "broadcast").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable real RocketMQ NameServer/Broker"]
async fn clustering_consumes_every_wave() {
    continuous(MessageModel::Clustering, "cluster").await;
}
