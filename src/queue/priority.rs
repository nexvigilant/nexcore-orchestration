//! Bounded priority queue with async backpressure.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Mutex;

use nexcore_chrono::DateTime;
use tokio::sync::Notify;

use crate::error::{OrcError, OrcResult};
use crate::types::Priority;

/// A work item wrapping a payload with priority and enqueue time.
#[derive(Debug)]
struct WorkItem<T> {
    payload: T,
    priority: Priority,
    _enqueued_at: DateTime,
    sequence: u64,
}

// BinaryHeap is a max-heap, so Ord must put highest priority first.
// For equal priority, earlier enqueue time (lower sequence) wins.
impl<T> PartialEq for WorkItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl<T> Eq for WorkItem<T> {}

impl<T> PartialOrd for WorkItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for WorkItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.sequence.cmp(&self.sequence)) // lower sequence = earlier = higher priority
    }
}

/// Bounded priority queue with async wake on push/pop.
///
/// - Priority ordering: Critical > High > Normal > Low
/// - Ties broken by enqueue order (FIFO within same priority)
/// - `push()` blocks when full; `try_push()` returns `QueueFull`
/// - `pop()` blocks when empty; `try_pop()` returns `None`
#[derive(Debug)]
pub struct BoundedPriorityQueue<T> {
    inner: Mutex<QueueInner<T>>,
    capacity: usize,
    /// Notified when an item is pushed (wakes poppers).
    item_available: Notify,
    /// Notified when an item is popped (wakes pushers).
    space_available: Notify,
}

#[derive(Debug)]
struct QueueInner<T> {
    heap: BinaryHeap<WorkItem<T>>,
    sequence_counter: u64,
    closed: bool,
}

impl<T: Send + 'static> BoundedPriorityQueue<T> {
    /// Create a queue with the given maximum capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(QueueInner {
                heap: BinaryHeap::with_capacity(capacity),
                sequence_counter: 0,
                closed: false,
            }),
            capacity,
            item_available: Notify::new(),
            space_available: Notify::new(),
        }
    }

    /// Try to enqueue an item. Returns `QueueFull` if at capacity.
    pub fn try_push(&self, item: T, priority: Priority) -> OrcResult<()> {
        self.try_push_inner(item, priority).map_err(|(_, e)| e)
    }

    /// Enqueue an item, waiting asynchronously if the queue is full.
    ///
    /// Unlike `try_push`, this will wait until space is available.
    /// The item is held until it can be inserted.
    pub async fn push(&self, item: T, priority: Priority) -> OrcResult<()> {
        let mut pending = Some(item);
        loop {
            if let Some(val) = pending.take() {
                match self.try_push_inner(val, priority) {
                    Ok(()) => return Ok(()),
                    Err((returned, OrcError::QueueFull { .. })) => {
                        pending = Some(returned);
                        self.space_available.notified().await;
                    }
                    Err((_, e)) => return Err(e),
                }
            }
        }
    }

    /// Internal try_push that returns the item back on failure.
    fn try_push_inner(&self, item: T, priority: Priority) -> Result<(), (T, OrcError)> {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(_) => return Err((item, OrcError::QueueClosed)),
        };
        if inner.closed {
            return Err((item, OrcError::QueueClosed));
        }
        if inner.heap.len() >= self.capacity {
            return Err((
                item,
                OrcError::QueueFull {
                    capacity: self.capacity,
                },
            ));
        }
        let seq = inner.sequence_counter;
        inner.sequence_counter = seq.wrapping_add(1);
        inner.heap.push(WorkItem {
            payload: item,
            priority,
            _enqueued_at: DateTime::now(),
            sequence: seq,
        });
        drop(inner);
        self.item_available.notify_one();
        Ok(())
    }

    /// Try to dequeue the highest-priority item. Returns `None` if empty.
    #[must_use]
    pub fn try_pop(&self) -> Option<T> {
        let mut inner = self.inner.lock().ok()?;
        let item = inner.heap.pop();
        if item.is_some() {
            drop(inner);
            self.space_available.notify_one();
        }
        item.map(|w| w.payload)
    }

    /// Dequeue the highest-priority item, waiting asynchronously if empty.
    pub async fn pop(&self) -> Option<T> {
        loop {
            if let Some(item) = self.try_pop() {
                return Some(item);
            }
            // Check if closed
            {
                let inner = self.inner.lock().ok()?;
                if inner.closed && inner.heap.is_empty() {
                    return None;
                }
            }
            self.item_available.notified().await;
        }
    }

    /// Close the queue. No new items accepted; remaining items can be drained.
    pub fn close(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.closed = true;
        }
        // Wake all waiters so they can observe closure
        self.item_available.notify_waiters();
        self.space_available.notify_waiters();
    }

    /// Current number of items in the queue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map(|inner| inner.heap.len()).unwrap_or(0)
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the queue has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.lock().map(|inner| inner.closed).unwrap_or(true)
    }

    /// Maximum capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_ordering_critical_first() {
        let q: BoundedPriorityQueue<&str> = BoundedPriorityQueue::new(10);
        q.try_push("low", Priority::Low).ok();
        q.try_push("critical", Priority::Critical).ok();
        q.try_push("normal", Priority::Normal).ok();
        q.try_push("high", Priority::High).ok();

        assert_eq!(q.try_pop(), Some("critical"));
        assert_eq!(q.try_pop(), Some("high"));
        assert_eq!(q.try_pop(), Some("normal"));
        assert_eq!(q.try_pop(), Some("low"));
    }

    #[test]
    fn fifo_within_same_priority() {
        let q: BoundedPriorityQueue<&str> = BoundedPriorityQueue::new(10);
        q.try_push("first", Priority::Normal).ok();
        q.try_push("second", Priority::Normal).ok();
        q.try_push("third", Priority::Normal).ok();

        assert_eq!(q.try_pop(), Some("first"));
        assert_eq!(q.try_pop(), Some("second"));
        assert_eq!(q.try_pop(), Some("third"));
    }

    #[test]
    fn backpressure_when_full() {
        let q: BoundedPriorityQueue<i32> = BoundedPriorityQueue::new(2);
        assert!(q.try_push(1, Priority::Normal).is_ok());
        assert!(q.try_push(2, Priority::Normal).is_ok());

        let result = q.try_push(3, Priority::Normal);
        assert!(result.is_err());
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn close_rejects_new_items() {
        let q: BoundedPriorityQueue<i32> = BoundedPriorityQueue::new(10);
        q.try_push(1, Priority::Normal).ok();
        q.close();

        let result = q.try_push(2, Priority::Normal);
        assert!(result.is_err());
        assert!(q.is_closed());

        // Existing items can still be drained
        assert_eq!(q.try_pop(), Some(1));
    }

    #[test]
    fn try_pop_empty_returns_none() {
        let q: BoundedPriorityQueue<i32> = BoundedPriorityQueue::new(10);
        assert_eq!(q.try_pop(), None);
        assert!(q.is_empty());
    }

    #[tokio::test]
    async fn async_pop_waits_for_item() {
        let q = std::sync::Arc::new(BoundedPriorityQueue::new(10));
        let q2 = q.clone();

        let handle = tokio::spawn(async move { q2.pop().await });

        // Small delay then push
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        q.try_push(42_i32, Priority::Normal).ok();

        let result = handle.await.ok().flatten();
        assert_eq!(result, Some(42));
    }

    #[tokio::test]
    async fn async_pop_returns_none_on_close() {
        let q = std::sync::Arc::new(BoundedPriorityQueue::<i32>::new(10));
        let q2 = q.clone();

        let handle = tokio::spawn(async move { q2.pop().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        q.close();

        let result = handle.await.ok().flatten();
        assert_eq!(result, None);
    }

    #[test]
    fn capacity_and_len() {
        let q: BoundedPriorityQueue<i32> = BoundedPriorityQueue::new(5);
        assert_eq!(q.capacity(), 5);
        assert_eq!(q.len(), 0);
        q.try_push(1, Priority::Normal).ok();
        assert_eq!(q.len(), 1);
    }
}
