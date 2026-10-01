package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/LaithAlz/jobsched/internal/executor"
	"github.com/LaithAlz/jobsched/internal/job"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
)

// These tests use the fake executor (NOTES.md D-009), so they exercise the real queue, real
// store and real retry policy in milliseconds. The Docker-backed equivalents live in test/.

type harness struct {
	t     *testing.T
	q     *queue.Queue
	store *store.MemoryStore
	fake  *executor.Fake
	pool  *Pool
	mx    *metrics.Metrics
}

func newHarness(t *testing.T, workers int, base time.Duration, fake *executor.Fake) *harness {
	t.Helper()
	q := queue.New(100)
	st := store.NewMemory()
	// A real metrics instance, not a no-op: each has its own registry (D-039), so tests can
	// assert on exact counter values without interfering with each other (D-042).
	mx := metrics.New()
	pool := NewPool(Config{Workers: workers, BackoffBase: base, BackoffMax: 10 * time.Second},
		q, st, fake, mx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
	})
	return &harness{t: t, q: q, store: st, fake: fake, pool: pool, mx: mx}
}

func (h *harness) submit(id string, maxAttempts int) {
	h.t.Helper()
	j := job.Job{
		ID:        id,
		Spec:      job.Spec{Command: "test", Image: "none", CPUs: 1, MemoryMB: 64, MaxAttempts: maxAttempts, TimeoutSeconds: 30},
		Status:    job.StatusQueued,
		CreatedAt: time.Now(),
		Attempts:  []job.Attempt{},
	}
	if err := h.store.Create(j); err != nil {
		h.t.Fatal(err)
	}
	if err := h.q.Submit(id, 0); err != nil {
		h.t.Fatal(err)
	}
}

// waitTerminal polls until the job reaches succeeded or failed.
func (h *harness) waitTerminal(id string, within time.Duration) job.Job {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		j, err := h.store.Get(id)
		if err == nil && j.Status.Terminal() {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := h.store.Get(id)
	h.t.Fatalf("job %s did not reach a terminal state within %s (status=%s attempts=%d)",
		id, within, j.Status, j.AttemptsUsed)
	return job.Job{}
}

func TestExhaustsAttemptsThenFailsPermanently(t *testing.T) {
	h := newHarness(t, 2, 20*time.Millisecond, &executor.Fake{Handler: executor.AlwaysFails(1)})
	h.submit("j1", 4)

	j := h.waitTerminal("j1", 10*time.Second)

	if j.Status != job.StatusFailed {
		t.Errorf("status = %s, want failed", j.Status)
	}
	// D-021: max_attempts is a total, so 4 means exactly 4 runs — not 5.
	if j.AttemptsUsed != 4 {
		t.Errorf("attempts_used = %d, want exactly 4", j.AttemptsUsed)
	}
	if len(j.Attempts) != 4 {
		t.Errorf("len(attempts) = %d, want 4 — AttemptsUsed must mirror the slice", len(j.Attempts))
	}
	if j.ExitCode == nil || *j.ExitCode != 1 {
		t.Errorf("exit_code = %v, want 1", j.ExitCode)
	}
	if j.FinishedAt == nil {
		t.Error("finished_at must be set on a terminal job")
	}
	for i, a := range j.Attempts {
		if a.Number != i+1 {
			t.Errorf("attempt[%d].number = %d, want %d", i, a.Number, i+1)
		}
	}
}

func TestBackoffIsExponential(t *testing.T) {
	const base = 100 * time.Millisecond
	h := newHarness(t, 2, base, &executor.Fake{Handler: executor.AlwaysFails(1)})
	h.submit("j1", 4)

	j := h.waitTerminal("j1", 15*time.Second)
	if len(j.Attempts) != 4 {
		t.Fatalf("need 4 attempts to measure 3 gaps, got %d", len(j.Attempts))
	}

	// The gap between one attempt finishing and the next starting is the observed backoff.
	// Measuring it this way tests the behaviour end to end rather than the arithmetic.
	want := []time.Duration{base, 2 * base, 4 * base}
	for i, w := range want {
		got := j.Attempts[i+1].StartedAt.Sub(j.Attempts[i].FinishedAt)
		if got < w {
			t.Errorf("gap %d = %s, want at least %s", i+1, got, w)
		}
		// Upper bound catches a backoff that is exponential but with the wrong base, and
		// leaves room for scheduling slop.
		if got > w+500*time.Millisecond {
			t.Errorf("gap %d = %s, want ~%s (too long)", i+1, got, w)
		}
	}
}

func TestSucceedsAfterTransientFailures(t *testing.T) {
	h := newHarness(t, 2, 20*time.Millisecond, &executor.Fake{Handler: executor.FailsThenSucceeds(2)})
	h.submit("j1", 4)

	j := h.waitTerminal("j1", 10*time.Second)
	if j.Status != job.StatusSucceeded {
		t.Errorf("status = %s, want succeeded", j.Status)
	}
	if j.AttemptsUsed != 3 {
		t.Errorf("attempts_used = %d, want 3 (two failures then a success)", j.AttemptsUsed)
	}
	if j.LastError != "" {
		t.Errorf("last_error = %q, want empty on success", j.LastError)
	}
}

func TestMaxAttemptsOneNeverRetries(t *testing.T) {
	h := newHarness(t, 1, time.Second, &executor.Fake{Handler: executor.AlwaysFails(3)})
	h.submit("j1", 1)

	j := h.waitTerminal("j1", 5*time.Second)
	if j.Status != job.StatusFailed || j.AttemptsUsed != 1 {
		t.Errorf("got status=%s attempts=%d, want failed after exactly 1 attempt", j.Status, j.AttemptsUsed)
	}
}

// TestBackoffDoesNotOccupyAWorker is the direct test of NOTES.md D-018. With a single worker
// and a long backoff, a job waiting to retry must not block unrelated jobs. If retries were
// implemented as time.Sleep inside the worker, this test would time out.
func TestBackoffDoesNotOccupyAWorker(t *testing.T) {
	fake := &executor.Fake{Handler: func(req executor.Request) (executor.Result, error) {
		if req.JobID == "slow-retrier" {
			return executor.Result{ExitCode: 1}, nil
		}
		return executor.Result{ExitCode: 0}, nil
	}}
	// One worker, 2s backoff: plenty long enough to distinguish blocked from free.
	h := newHarness(t, 1, 2*time.Second, fake)

	h.submit("slow-retrier", 3)
	// Let the single worker pick up the failing job and enter backoff.
	time.Sleep(300 * time.Millisecond)

	if j, _ := h.store.Get("slow-retrier"); j.Status != job.StatusRetrying {
		t.Fatalf("setup: expected slow-retrier to be retrying, got %s", j.Status)
	}

	h.submit("quick", 1)
	start := time.Now()
	j := h.waitTerminal("quick", 1500*time.Millisecond) // < the 2s backoff
	elapsed := time.Since(start)

	if j.Status != job.StatusSucceeded {
		t.Errorf("quick job status = %s, want succeeded", j.Status)
	}
	if elapsed > time.Second {
		t.Errorf("quick job took %s; the worker was blocked by another job's backoff", elapsed)
	}
}

func TestRetryingJobRecordsNextAttemptAt(t *testing.T) {
	h := newHarness(t, 1, 2*time.Second, &executor.Fake{Handler: executor.AlwaysFails(1)})
	h.submit("j1", 3)
	time.Sleep(300 * time.Millisecond)

	j, err := h.store.Get("j1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != job.StatusRetrying {
		t.Fatalf("status = %s, want retrying", j.Status)
	}
	if j.NextAttemptAt == nil {
		t.Fatal("next_attempt_at must be set while retrying")
	}
	if !j.NextAttemptAt.After(time.Now()) {
		t.Error("next_attempt_at must be in the future")
	}
}

func TestBackoffCalculation(t *testing.T) {
	p := &Pool{cfg: Config{BackoffBase: time.Second, BackoffMax: 10 * time.Second}}
	cases := []struct {
		attemptsUsed int
		want         time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // capped
		{99, 10 * time.Second},
		// A huge attempt count must not overflow into a negative (immediate-fire) delay.
		{1 << 20, 10 * time.Second},
	}
	for _, c := range cases {
		if got := p.backoff(c.attemptsUsed); got != c.want {
			t.Errorf("backoff(%d) = %s, want %s", c.attemptsUsed, got, c.want)
		}
	}
}

// TestMetricsReflectRetryBehaviour checks the instrumentation against a known scenario: one
// job, four failing attempts, three retries. The numbers are exact, which is only possible
// because each Metrics instance owns its registry (D-039).
func TestMetricsReflectRetryBehaviour(t *testing.T) {
	h := newHarness(t, 2, 20*time.Millisecond, &executor.Fake{Handler: executor.AlwaysFails(1)})
	h.submit("j1", 4)
	h.waitTerminal("j1", 10*time.Second)

	if got := testutil.ToFloat64(h.mx.JobAttempts.WithLabelValues("failed")); got != 4 {
		t.Errorf("job_attempts{failed} = %v, want 4", got)
	}
	// D-021: 4 total attempts means 3 retries, not 4.
	if got := testutil.ToFloat64(h.mx.JobRetries); got != 3 {
		t.Errorf("job_retries = %v, want 3", got)
	}
	if got := testutil.ToFloat64(h.mx.JobsCompleted.WithLabelValues("failed")); got != 1 {
		t.Errorf("jobs_completed{failed} = %v, want 1 — the job terminates once", got)
	}
	if got := testutil.ToFloat64(h.mx.JobsCompleted.WithLabelValues("succeeded")); got != 0 {
		t.Errorf("jobs_completed{succeeded} = %v, want 0", got)
	}
}

// D-024's three-way outcome split must survive into the metrics: an infrastructure error is
// counted separately from a job that ran and exited non-zero.
func TestMetricsDistinguishInfraErrorFromJobFailure(t *testing.T) {
	fake := &executor.Fake{Handler: func(req executor.Request) (executor.Result, error) {
		if req.JobID == "broken-infra" {
			return executor.Result{}, errors.New("docker daemon unreachable")
		}
		return executor.Result{ExitCode: 1}, nil
	}}
	h := newHarness(t, 2, 20*time.Millisecond, fake)

	h.submit("broken-infra", 1)
	h.submit("bad-command", 1)
	h.waitTerminal("broken-infra", 10*time.Second)
	h.waitTerminal("bad-command", 10*time.Second)

	if got := testutil.ToFloat64(h.mx.JobAttempts.WithLabelValues("error")); got != 1 {
		t.Errorf("job_attempts{error} = %v, want 1 (the job never ran)", got)
	}
	if got := testutil.ToFloat64(h.mx.JobAttempts.WithLabelValues("failed")); got != 1 {
		t.Errorf("job_attempts{failed} = %v, want 1 (the job ran and exited non-zero)", got)
	}
}

// D-037: utilisation is accumulated busy time per worker, so a worker that ran a slow job must
// show a non-zero busy-seconds counter, and the pool's busy gauge must return to zero.
func TestWorkerBusySecondsAccumulate(t *testing.T) {
	h := newHarness(t, 1, 20*time.Millisecond, &executor.Fake{Delay: 200 * time.Millisecond})
	h.submit("j1", 1)
	h.waitTerminal("j1", 10*time.Second)

	busy := testutil.ToFloat64(h.mx.WorkerBusySeconds.WithLabelValues(metrics.WorkerLabel(0)))
	if busy < 0.15 {
		t.Errorf("worker 0 busy_seconds = %v, want >= 0.15 after a 200ms job", busy)
	}
	if got := h.pool.BusyWorkers(); got != 0 {
		t.Errorf("BusyWorkers() = %d after the job finished, want 0", got)
	}
}
