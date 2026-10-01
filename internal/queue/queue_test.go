package queue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPriorityOrdering(t *testing.T) {
	q := New(10)
	// Submitted low-to-high; expected out high-to-low.
	q.Submit("low", 1)
	q.Submit("high", 10)
	q.Submit("mid", 5)

	want := []string{"high", "mid", "low"}
	for _, w := range want {
		got, ok := q.Pop()
		if !ok {
			t.Fatalf("queue drained early, wanted %q", w)
		}
		if got != w {
			t.Errorf("got %q, want %q", got, w)
		}
	}
}

func TestFIFOWithinSamePriority(t *testing.T) {
	q := New(10)
	for _, id := range []string{"a", "b", "c", "d"} {
		q.Submit(id, 0)
	}
	for _, want := range []string{"a", "b", "c", "d"} {
		got, _ := q.Pop()
		if got != want {
			t.Errorf("got %q, want %q — equal priorities must stay FIFO", got, want)
		}
	}
}

func TestCapacityRejectsSubmitButNotRequeue(t *testing.T) {
	q := New(2)
	if err := q.Submit("a", 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit("b", 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit("c", 0); !errors.Is(err, ErrFull) {
		t.Fatalf("submit past capacity: got %v, want ErrFull", err)
	}
	// D-018: retries are already-admitted work and must never be dropped for capacity.
	if err := q.Requeue("retry", 0); err != nil {
		t.Fatalf("requeue past capacity must succeed, got %v", err)
	}
	if q.Len() != 3 {
		t.Errorf("len = %d, want 3", q.Len())
	}
}

func TestPopBlocksUntilWorkArrives(t *testing.T) {
	q := New(10)
	got := make(chan string, 1)
	go func() {
		id, _ := q.Pop()
		got <- id
	}()

	select {
	case id := <-got:
		t.Fatalf("Pop returned %q on an empty queue; it must block", id)
	case <-time.After(50 * time.Millisecond):
	}

	q.Submit("late", 0)
	select {
	case id := <-got:
		if id != "late" {
			t.Errorf("got %q, want %q", id, "late")
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not wake after Submit")
	}
}

func TestCloseWakesAllBlockedPops(t *testing.T) {
	q := New(10)
	const waiters = 4
	var wg sync.WaitGroup
	wg.Add(waiters)
	for range waiters {
		go func() {
			defer wg.Done()
			if _, ok := q.Pop(); ok {
				t.Error("Pop returned ok=true after Close")
			}
		}()
	}

	time.Sleep(50 * time.Millisecond) // let them all block
	q.Close()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake every blocked Pop — workers would hang on shutdown")
	}
}

func TestCloseDrainsRemainingWork(t *testing.T) {
	q := New(10)
	q.Submit("pending", 0)
	q.Close()

	// Close means "no new work", not "discard queued work".
	if id, ok := q.Pop(); !ok || id != "pending" {
		t.Errorf("Pop after Close = (%q, %v), want (\"pending\", true)", id, ok)
	}
	if _, ok := q.Pop(); ok {
		t.Error("Pop on a closed, drained queue must report ok=false")
	}
	if err := q.Submit("new", 0); !errors.Is(err, ErrClosed) {
		t.Errorf("Submit after Close: got %v, want ErrClosed", err)
	}
}
