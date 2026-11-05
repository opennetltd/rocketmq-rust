//! Plan 49 section 5: the managed consumer entry point must stand on its own. These tests use only
//! the public API, link nothing from the application side, send no health or control signals, and
//! run without a NameServer: autonomous startup retry, truthful stop reports and observer-free
//! status reporting have to hold in exactly that situation.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::managed_consumer::ConsumerHealth;
use rocketmq_client_rust::consumer::managed_consumer::ConsumerPhase;
use rocketmq_client_rust::consumer::managed_consumer::ShutdownOutcome;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

/// A listener that only counts invocations. Nothing here reads or writes lifecycle state, so a
/// passing test cannot be explained by an application-side supervisor reacting to it.
#[derive(Clone, Default)]
struct CountingListener {
    calls: Arc<AtomicU64>,
}

impl CountingListener {
    fn callback(
        &self,
    ) -> impl Fn(&[&MessageExt], &mut ConsumeConcurrentlyContext) -> RocketMQResult<ConsumeConcurrentlyStatus>
           + Send
           + Sync
           + 'static {
        let calls = Arc::clone(&self.calls);
        move |msgs, _context| {
            calls.fetch_add(msgs.len() as u64, Ordering::Relaxed);
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        }
    }
}

fn unique_group(suffix: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    format!("plan49_standalone_{suffix}_{nonce}")
}

/// A NameServer address that refuses connections: every startup attempt fails, which is precisely
/// the condition the old application-side controller turned into restart loops and exit requests.
const UNREACHABLE_NAMESERVER: &str = "127.0.0.1:1";

/// Invalid configuration is reported to the caller and never retried: no handle, no background
/// retry loop, no process exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_configuration_is_reported_and_never_retried() {
    let listener = CountingListener::default();
    let result = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("no_subscription"))
        .name_server_addr(UNREACHABLE_NAMESERVER)
        .start_managed(listener.callback());
    assert!(
        result.is_err(),
        "a consumer without a subscription must be rejected at the managed entry point"
    );
}

/// With no NameServer reachable the SDK keeps one owner, reports what it is doing instead of
/// claiming healthy idle, repairs in place, never exits and still honours a stop request. No
/// application code participates, and the status stream is optional.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_nameserver_is_reported_and_repaired_in_place() {
    let listener = CountingListener::default();
    let handle = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("unreachable"))
        .name_server_addr(UNREACHABLE_NAMESERVER)
        .subscribe("TopicTest", "*")
        .start_managed(listener.callback())
        .expect("managed start is asynchronous and must not fail because the NameServer is down");
    let started = tokio::time::Instant::now();

    let first = handle.status();
    assert_eq!(1, first.incarnation, "the first incarnation is 1");
    assert!(!first.reason.is_empty(), "a reason label is always present");

    // Section 3.2: a route or assignment miss is a fault that is repaired in place - never healthy
    // idle, never a replacement, never a stopped consumer. The window is far shorter than one
    // rebalance interval (20s) on purpose: the first pass must not wait for a confirmed
    // heartbeat, otherwise an outage at startup reads as healthy idle for a whole interval. An
    // observer that exists and is dropped immediately cannot change any of this.
    let mut observed = 0;
    let mut reported_refresh_failure = None;
    while reported_refresh_failure.is_none() && observed < 24 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let status = handle.status();
        assert_eq!(
            first.incarnation, status.incarnation,
            "the incarnation only changes after a proved cleanup: {status}"
        );
        assert!(
            !matches!(status.phase, ConsumerPhase::Stopped | ConsumerPhase::Stopping),
            "an unreachable NameServer never stops the consumer or asks for a restart: {status}"
        );
        assert!(
            !matches!(status.health, ConsumerHealth::Blocked),
            "a control-plane outage degrades, it does not block admission: {status}"
        );
        if status.reason == "assignment_refresh_failed" {
            assert_eq!(ConsumerPhase::Recovering, status.phase, "{status}");
            assert_eq!(ConsumerHealth::Degraded, status.health, "{status}");
            let elapsed = started.elapsed();
            assert!(
                elapsed <= Duration::from_secs(8),
                "a cold-start route failure must be reported promptly, not after a rebalance interval: {elapsed:?}"
            );
            reported_refresh_failure = Some(status);
        }
        if observed == 4 {
            drop(handle.subscribe_events());
        }
        observed += 1;
    }
    let status = reported_refresh_failure
        .expect("a consumer that cannot see a route reports a failed assignment refresh instead of healthy idle");
    assert_eq!(1, status.incarnation, "repair stayed in place: {status}");
    assert_eq!(0, listener.calls.load(Ordering::Relaxed));

    assert!(handle.request_stop(), "the first stop request is accepted");
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        handle.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(20)),
    )
    .await
    .expect("stop must answer within its own deadline instead of parking the caller");
    match outcome {
        ShutdownOutcome::Complete => {
            assert_eq!(ConsumerPhase::Stopped, handle.status().phase);
        }
        // Truthful: cleanup stayed SDK-owned past the caller's budget. The phase is still on the way
        // down and the caller was never blocked beyond its own deadline.
        ShutdownOutcome::Pending => assert!(matches!(
            handle.status().phase,
            ConsumerPhase::Stopping | ConsumerPhase::Stopped
        )),
    }

    // Stop is sticky: a second waiter receives a report rather than hanging or re-running cleanup.
    let again = tokio::time::timeout(
        Duration::from_secs(30),
        handle.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(20)),
    )
    .await
    .expect("a second waiter is answered too");
    assert!(matches!(again, ShutdownOutcome::Complete | ShutdownOutcome::Pending));
}

/// Requires a live RocketMQ NameServer and Broker. Run with:
/// `ROCKETMQ_PHASE3_NAMESERVER=127.0.0.1:9876 cargo test -p rocketmq-client-rust --test
/// managed_consumer_standalone -- --ignored`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live RocketMQ NameServer and Broker"]
async fn idle_consumer_keeps_one_incarnation_beyond_the_old_five_second_rule() {
    let nameserver = std::env::var("ROCKETMQ_PHASE3_NAMESERVER")
        .expect("set ROCKETMQ_PHASE3_NAMESERVER to a live NameServer address");
    let listener = CountingListener::default();
    let handle = DefaultMQPushConsumer::builder()
        .consumer_group(unique_group("idle"))
        .name_server_addr(nameserver)
        .subscribe("TopicTest", "*")
        .start_managed(listener.callback())
        .expect("managed start against a live cluster");

    // Nothing is produced for twenty seconds. The old stall detector would have recreated this
    // consumer at five seconds; here it stays one incarnation, running and healthy.
    let mut reached_running = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let status = handle.status();
        if status.phase == ConsumerPhase::Running {
            reached_running = true;
        }
        assert_eq!(
            1, status.incarnation,
            "idle traffic never triggers replacement: {status}"
        );
    }
    assert!(reached_running, "consumer reached Running with no messages at all");

    let outcome = handle
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(30))
        .await;
    assert!(matches!(outcome, ShutdownOutcome::Complete | ShutdownOutcome::Pending));
}
