// Package queue implements the bounded, priority-ordered queue of pending jobs.
package queue

import (
	"container/heap"
	"errors"
	"sync"
)

// ErrFull is returned by Submit when the queue is at capacity.
var ErrFull = errors.New("queue is full")

// ErrClosed is returned when pushing to a closed queue.
var ErrClosed = errors.New("queue is closed")

// Queue is a bounded priority queue with a blocking Pop.
//
// Ordering is: higher Priority first, ties broken first-in-first-out. Delayed retries are NOT
// modelled here — a retry is simply re-pushed later by a timer (see NOTES.md D-018), which
// keeps this type to one ordering concern.
//
// Concurrency shape: a mutex plus a sync.Cond rather than a channel. See NOTES.md D-017.
type Queue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	items    itemHeap
	seq      uint64
	capacity int
	closed   bool
}

func New(capacity int) *Queue {
	q := &Queue{capacity: capacity}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

// Submit enqueues a newly accepted job. It enforces capacity: this is the backpressure point
// that turns "too much work" into a 429 rather than into unbounded memory growth.
func (q *Queue) Submit(id string, priority int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if len(q.items) >= q.capacity {
		return ErrFull
	}
	q.push(id, priority)
	return nil
}

// Requeue puts an already-admitted job back on the queue for a retry attempt.
//
// It deliberately bypasses the capacity check. A retry is work the system already accepted
// responsibility for; dropping it because new submissions filled the queue would mean the
// scheduler silently loses jobs under load, which is a worse failure than briefly exceeding a
// soft limit. See NOTES.md D-018.
func (q *Queue) Requeue(id string, priority int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	q.push(id, priority)
	return nil
}

// push assumes q.mu is held.
func (q *Queue) push(id string, priority int) {
	q.seq++
	heap.Push(&q.items, item{id: id, priority: priority, seq: q.seq})
	q.notEmpty.Signal()
}

// Pop blocks until a job is available and returns it, or returns ok=false once the queue is
// closed and drained. Workers loop on this until it reports false.
func (q *Queue) Pop() (id string, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		if q.closed {
			return "", false
		}
		q.notEmpty.Wait()
	}
	it := heap.Pop(&q.items).(item)
	return it.id, true
}

// Len reports the number of pending jobs. Used by the API and, in Stage 2, by metrics.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Close stops the queue accepting work and wakes every blocked Pop. Already-queued items are
// still drained by any worker that asks for them, so Close is "no new work", not "drop work".
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notEmpty.Broadcast()
}

// item is one queued entry. Only the job ID is stored, not the whole job: the store is the
// single source of truth for job state, and holding a copy here would let the two disagree.
type item struct {
	id       string
	priority int
	seq      uint64
}

// itemHeap implements heap.Interface. Less defines the queue's ordering policy.
type itemHeap []item

func (h itemHeap) Len() int { return len(h) }

func (h itemHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority // higher priority first
	}
	return h[i].seq < h[j].seq // FIFO within a priority
}

func (h itemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *itemHeap) Push(x any) { *h = append(*h, x.(item)) }

func (h *itemHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
