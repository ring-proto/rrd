//! The discrete-event scheduler's payload and queue. Events are processed in non-decreasing
//! time order; ties break by insertion sequence (FIFO), giving a fully deterministic schedule.

use ring_core::{Message, NeighborId};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Duration;

/// One scheduled occurrence.
pub enum Event {
    /// Node runs one relax-and-emit on its own clock.
    Emit { node: usize },
    /// A message arrives at `to` from neighbour `from`, having traversed link `link`.
    /// `link` is carried so [`crate::fabric::Fabric::do_deliver`] can drop an in-flight
    /// message whose link was cut while it was in transit (#29): a cut must take effect
    /// the instant it is applied, not one delivery later.
    Deliver {
        to: usize,
        from: NeighborId,
        link: usize,
        msg: Message,
    },
}

/// An event stamped with its fire time and an insertion sequence number (the tie-break).
pub struct Scheduled {
    pub time: Duration,
    pub seq: u64,
    pub event: Event,
}

// Order so that the EARLIEST (time, then lowest seq) is the GREATEST — `BinaryHeap` is a
// max-heap, so `pop()` yields the earliest event. seq is globally unique, so the order is total.
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .time
            .cmp(&self.time)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}
impl Eq for Scheduled {}

/// The scheduler queue: a min-by-time heap with a monotonic insertion counter for tie-breaks.
#[derive(Default)]
pub struct EventQueue {
    heap: BinaryHeap<Scheduled>,
    next_seq: u64,
}

impl EventQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, time: Duration, event: Event) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Scheduled { time, seq, event });
    }

    /// The fire time of the earliest pending event, if any.
    pub fn peek_time(&self) -> Option<Duration> {
        self.heap.peek().map(|s| s.time)
    }

    pub fn pop(&mut self) -> Option<Scheduled> {
        self.heap.pop()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn pops_in_time_order() {
        let mut q = EventQueue::new();
        q.push(ms(30), Event::Emit { node: 3 });
        q.push(ms(10), Event::Emit { node: 1 });
        q.push(ms(20), Event::Emit { node: 2 });
        let mut order = Vec::new();
        while let Some(s) = q.pop() {
            if let Event::Emit { node } = s.event {
                order.push((s.time, node));
            }
        }
        assert_eq!(order, vec![(ms(10), 1), (ms(20), 2), (ms(30), 3)]);
    }

    #[test]
    fn ties_break_fifo_by_insertion() {
        let mut q = EventQueue::new();
        for node in [5, 6, 7] {
            q.push(ms(10), Event::Emit { node }); // all same time
        }
        let mut order = Vec::new();
        while let Some(s) = q.pop() {
            if let Event::Emit { node } = s.event {
                order.push(node);
            }
        }
        assert_eq!(
            order,
            vec![5, 6, 7],
            "same-time events keep insertion order"
        );
    }

    #[test]
    fn peek_time_reports_earliest() {
        let mut q = EventQueue::new();
        assert_eq!(q.peek_time(), None);
        q.push(ms(50), Event::Emit { node: 0 });
        q.push(ms(20), Event::Emit { node: 1 });
        assert_eq!(q.peek_time(), Some(ms(20)));
    }
}
