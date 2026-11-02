/*
 * [File description]: one queue's installation/retirement transition, owned by one pass at a time
 * (plan 49i I2).
 *
 * Retirement is not a single instruction. A pass decides to drop a process queue, then waits on
 * local persistence, then erases the queue's offset, then removes the table entry and finally
 * ends the acquisition. Every one of those steps touches state shared with an installation of
 * the same key, and a replacement that appears in the middle owns them now. This module makes
 * one queue's transition somebody's job at a time - without ever making a competing pass wait on
 * it, because a rebalance pass that blocks behind slow disk I/O stops making progress for every
 * other queue too.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::consumer_impl::process_queue::ProcessQueue;

/// One key's transition state: who is retiring it, and whether an installation was turned away.
#[derive(Default)]
struct Transition {
    retiring: Option<Arc<ProcessQueue>>,
    deferred_install: bool,
}

/// Which queue is in the middle of being retired (plan 49i I2 item 1).
///
/// Claims never wait: a pass that finds a queue already retiring defers that one queue and carries
/// on with the rest, and the pass that owns the transition asks for another rebalance when it
/// finishes. Nothing here holds the process queue table or the progress map - those locks must stay
/// free while persistence runs - and no timeout is imposed, so a slow writer delays its own queue
/// rather than authorizing an overlapping installation.
#[derive(Default)]
pub(crate) struct QueueTransitions {
    queues: Mutex<HashMap<MessageQueue, Transition>>,
}

/// One queue's retirement, held until its side effects and bookkeeping have all finished (plan 49i
/// I2 item 2). It is released by [`RetirementGuard::release`], which reports whether an
/// installation of this key was deferred; dropping it without releasing - because the pass carrying
/// it was cancelled - ends the claim too, leaving nothing to block later transitions.
pub(crate) struct RetirementGuard {
    transitions: Arc<QueueTransitions>,
    queue: MessageQueue,
    released: bool,
}

impl QueueTransitions {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Take the right to retire `process_queue` under `queue`, or find that another pass is already
    /// retiring it. A second retirement of the same object is therefore a no-op rather than a
    /// second round of persistence and offset removal, and neither can act on an installation
    /// this pass does not own.
    pub(crate) fn try_retire(
        self: &Arc<Self>,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
    ) -> Option<RetirementGuard> {
        let mut queues = self.queues.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = queues.entry(queue.clone()).or_default();
        if entry.retiring.is_some() {
            return None;
        }
        entry.retiring = Some(Arc::clone(process_queue));
        Some(RetirementGuard {
            transitions: Arc::clone(self),
            queue: queue.clone(),
            released: false,
        })
    }

    /// Whether an installation of `queue` must wait for the retirement in flight, recording the
    /// deferral so that completion asks for another pass (plan 49i I2 item 1). Returns `false` when
    /// nothing is being retired and the installation may publish normally.
    pub(crate) fn install_deferred(&self, queue: &MessageQueue) -> bool {
        let mut queues = self.queues.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match queues.get_mut(queue) {
            Some(entry) if entry.retiring.is_some() => {
                entry.deferred_install = true;
                true
            }
            _ => false,
        }
    }

    /// Whether a retirement of `queue` is in flight at all. Test-only observation (plan 49i I2):
    /// the production paths ask by claiming, never by looking.
    #[cfg(test)]
    pub(crate) fn retiring(&self, queue: &MessageQueue) -> bool {
        let queues = self.queues.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        queues.get(queue).is_some_and(|entry| entry.retiring.is_some())
    }

    fn finish(&self, queue: &MessageQueue) -> bool {
        let mut queues = self.queues.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match queues.remove(queue) {
            Some(entry) => entry.deferred_install,
            None => false,
        }
    }
}

impl RetirementGuard {
    /// End this retirement, reporting whether an installation of the same key was deferred while it
    /// ran. Callers ask for another rebalance when the answer is yes; until then no other pass may
    /// retire or install this queue's transition here.
    pub(crate) fn release(mut self) -> bool {
        self.released = true;
        self.transitions.finish(&self.queue)
    }
}

impl Drop for RetirementGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        // A cancelled pass must not strand the key: its claim ends with it. The side effects it had
        // already performed were each revalidated against the object they named, so nothing here
        // can have touched a newer installation's state.
        let _ = self.transitions.finish(&self.queue);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(id: i32) -> MessageQueue {
        MessageQueue::from_parts("T", "broker", id)
    }

    /// One queue is retired by one pass at a time, and the claim ends when that pass finishes - or
    /// when it is cancelled mid-flight, which must never strand the key (plan 49i I2 item 1).
    #[test]
    fn one_queue_is_retired_by_one_pass_at_a_time() {
        let transitions = QueueTransitions::new();
        let q = queue(0);
        let pq = Arc::new(ProcessQueue::new());
        let other_pq = Arc::new(ProcessQueue::new());

        let guard = transitions.try_retire(&q, &pq).expect("nothing retires this queue yet");
        assert!(transitions.retiring(&q));
        assert!(
            transitions.try_retire(&q, &pq).is_none(),
            "the same process queue was retired twice at once"
        );
        assert!(
            transitions.try_retire(&q, &other_pq).is_none(),
            "a second pass started retiring a queue that is already transitioning"
        );

        let deferred = guard.release();
        assert!(!deferred, "nothing was deferred in this fixture");
        assert!(!transitions.retiring(&q));
        transitions
            .try_retire(&q, &other_pq)
            .expect("the key stayed claimed after its retirement ended");
    }

    /// A pass that is cancelled while its side effects run gives the queue back; nothing inherits
    /// its claim and nothing is left blocked (plan 49i I2 item 4).
    #[test]
    fn a_cancelled_pass_ends_its_own_claim() {
        let transitions = QueueTransitions::new();
        let q = queue(1);
        let pq = Arc::new(ProcessQueue::new());

        let guard = transitions.try_retire(&q, &pq).expect("nothing retires this queue yet");
        drop(guard);
        assert!(!transitions.retiring(&q), "a cancelled pass stranded its claim");
        assert!(
            !transitions.install_deferred(&q),
            "an installation was still deferred behind a retirement that had ended"
        );
    }

    /// Only the queue being retired waits; an unrelated queue installs normally, and the deferral
    /// is reported to the retiring pass so it can ask for another round (plan 49i I2 item 1).
    #[test]
    fn only_the_queue_being_retired_is_deferred() {
        let transitions = QueueTransitions::new();
        let retiring_q = queue(2);
        let sibling_q = queue(3);
        let pq = Arc::new(ProcessQueue::new());

        assert!(!transitions.install_deferred(&retiring_q));
        let guard = transitions
            .try_retire(&retiring_q, &pq)
            .expect("nothing retires this queue yet");
        assert!(
            transitions.install_deferred(&retiring_q),
            "an installation was published into a retirement of the same key"
        );
        assert!(
            !transitions.install_deferred(&sibling_q),
            "a retirement held up an unrelated queue"
        );

        let deferred = guard.release();
        assert!(deferred, "the deferred installation was not reported to its retirement");
        assert!(!transitions.retiring(&retiring_q));
        assert!(
            !transitions.install_deferred(&retiring_q),
            "the key stayed deferred after its retirement ended"
        );
    }
}
