use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

static PENDING_RESPONSES: AtomicUsize = AtomicUsize::new(0);
static SEND_TASKS: AtomicUsize = AtomicUsize::new(0);
static RECV_TASKS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportMetrics {
    pub pending_responses: usize,
    pub send_tasks: usize,
    pub recv_tasks: usize,
}

pub fn snapshot() -> TransportMetrics {
    TransportMetrics {
        pending_responses: PENDING_RESPONSES.load(Ordering::Relaxed),
        send_tasks: SEND_TASKS.load(Ordering::Relaxed),
        recv_tasks: RECV_TASKS.load(Ordering::Relaxed),
    }
}

pub(crate) fn pending_registered() {
    PENDING_RESPONSES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn pending_removed() {
    PENDING_RESPONSES.fetch_sub(1, Ordering::Relaxed);
}

pub(crate) struct TaskGuard {
    counter: &'static AtomicUsize,
}

pub(crate) fn send_task_started() -> TaskGuard {
    SEND_TASKS.fetch_add(1, Ordering::Relaxed);
    TaskGuard { counter: &SEND_TASKS }
}

pub(crate) fn recv_task_started() -> TaskGuard {
    RECV_TASKS.fetch_add(1, Ordering::Relaxed);
    TaskGuard { counter: &RECV_TASKS }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}
