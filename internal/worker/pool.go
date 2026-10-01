// Package worker contains the pool that connects the queue to the executor, and the
// retry/backoff policy that decides what happens after a failed attempt.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaithAlz/jobsched/internal/executor"
	"github.com/LaithAlz/jobsched/internal/job"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
)

type Config struct {
	Workers     int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// Pool runs Config.Workers goroutines, each looping: pop a job ID, execute one attempt,
// record the outcome, and either finish the job or schedule its retry.
type Pool struct {
	cfg   Config
	q     *queue.Queue
	store store.Store
	exec  executor.Executor
	log   *slog.Logger
	mx    *metrics.Metrics

	// busy is read on every metrics scrape (D-038) while workers write it, so it is an
	// atomic rather than a mutex-guarded int: a single counter with no invariant tying it to
	// other state is exactly what atomics are for.
	busy atomic.Int64

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPool(cfg Config, q *queue.Queue, st store.Store, ex executor.Executor, mx *metrics.Metrics, log *slog.Logger) *Pool {
	return &Pool{cfg: cfg, q: q, store: st, exec: ex, mx: mx, log: log}
}

// BusyWorkers reports how many workers are currently executing a job.
func (p *Pool) BusyWorkers() int { return int(p.busy.Load()) }

// Start launches the worker goroutines. They run until the queue is closed and drained.
func (p *Pool) Start() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		// `go func()` starts a goroutine: this call returns immediately and the loop body
		// runs concurrently. Each worker gets its own index for logging; passing it as an
		// argument rather than closing over the loop variable is the habit to keep, even
		// though Go 1.22+ makes loop variables per-iteration.
		go func(id int) {
			defer p.wg.Done()
			p.run(id)
		}(i)
	}
	p.log.Info("worker pool started", "workers", p.cfg.Workers)
}

// Shutdown stops accepting work, lets in-flight attempts finish, and gives up once ctx
// expires. It is safe to call once.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.q.Close() // no new work; wakes every idle worker blocked in Pop

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cancel()
		p.log.Info("worker pool drained cleanly")
		return nil
	case <-ctx.Done():
		// Grace period expired: cancel the pool context, which aborts in-flight Docker
		// calls. Container cleanup still runs — it uses a detached context (D-029).
		p.log.Warn("shutdown grace expired, cancelling in-flight attempts")
		p.cancel()
		<-done
		return ctx.Err()
	}
}

func (p *Pool) run(workerID int) {
	log := p.log.With("worker", workerID)
	for {
		id, ok := p.q.Pop()
		if !ok {
			log.Debug("worker exiting: queue closed and drained")
			return
		}
		p.execute(log, workerID, id)
	}
}

// execute runs exactly one attempt of one job and records what happened.
func (p *Pool) execute(log *slog.Logger, workerID int, jobID string) {
	started := time.Now()

	// Busy accounting brackets the whole attempt. The counter accumulates wall time so
	// utilisation is rate() over it, immune to scrape aliasing (D-037).
	p.busy.Add(1)
	defer func() {
		p.busy.Add(-1)
		p.mx.WorkerBusySeconds.WithLabelValues(metrics.WorkerLabel(workerID)).
			Add(time.Since(started).Seconds())
	}()

	// Claim the job: mark it running and read back the snapshot we will act on.
	j, err := p.store.Update(jobID, func(j *job.Job) {
		j.Status = job.StatusRunning
		j.NextAttemptAt = nil
		if j.StartedAt == nil {
			j.StartedAt = &started
		}
	})
	if err != nil {
		// D-015: this should be unreachable, but concurrent code deserves a defined
		// behaviour for "impossible" rather than a nil dereference.
		log.Error("dequeued job not found in store", "job_id", jobID, "error", err)
		return
	}

	attemptNum := j.AttemptsUsed + 1
	log = log.With("job_id", jobID, "attempt", attemptNum)
	log.Info("attempt starting", "command", j.Spec.Command)

	// Queue wait is measured only for the first attempt. Later attempts are separated by
	// backoff, not by queue pressure, so including them would make the histogram report
	// retry policy rather than saturation.
	if attemptNum == 1 {
		p.mx.QueueWait.Observe(started.Sub(j.CreatedAt).Seconds())
	}

	// Everything above this line is the scheduler's own work for this attempt. Measured
	// separately from attempt duration because pod startup dwarfs it by ~3 orders of
	// magnitude and would otherwise hide it entirely (D-072).
	p.mx.DispatchOverhead.Observe(time.Since(started).Seconds())

	res, runErr := p.exec.Run(p.ctx, executor.Request{
		JobID:    j.ID,
		Attempt:  attemptNum,
		Command:  j.Spec.Command,
		Image:    j.Spec.Image,
		CPUs:     j.Spec.CPUs,
		MemoryMB: j.Spec.MemoryMB,
		Timeout:  time.Duration(j.Spec.TimeoutSeconds) * time.Second,
	})

	att := job.Attempt{
		Number:     attemptNum,
		StartedAt:  started,
		FinishedAt: time.Now(),
	}

	// D-024: a non-nil error means the job never ran; a zero error with a non-zero exit
	// code means it ran and failed. Both are failures, but they are recorded differently.
	switch {
	case runErr != nil:
		att.Error = runErr.Error()
	default:
		code := res.ExitCode
		att.ExitCode = &code
		att.OOMKilled = res.OOMKilled
		att.ContainerID = res.RunID
		att.Logs = res.Logs
		switch {
		case res.TimedOut:
			att.Error = fmt.Sprintf("attempt exceeded timeout of %ds", j.Spec.TimeoutSeconds)
		case res.OOMKilled:
			att.Error = fmt.Sprintf("container exceeded its %d MB memory limit and was OOM-killed", j.Spec.MemoryMB)
		case code != 0:
			att.Error = fmt.Sprintf("command exited with status %d", code)
		}
	}

	// Three outcomes, not two: D-024's distinction between "ran and failed" and "never ran"
	// is preserved in the metrics so an infrastructure outage is visibly different from a
	// spike of broken commands.
	succeeded := runErr == nil && res.ExitCode == 0
	outcome := "failed"
	switch {
	case runErr != nil:
		outcome = "error"
	case succeeded:
		outcome = "succeeded"
	}
	p.mx.ObserveAttempt(outcome, att.FinishedAt.Sub(att.StartedAt).Seconds())

	p.record(log, jobID, att, succeeded)
}

// record commits the attempt and decides the job's next state: succeeded, failed, or
// retrying with a scheduled re-enqueue.
func (p *Pool) record(log *slog.Logger, jobID string, att job.Attempt, succeeded bool) {
	var (
		retryIn  time.Duration
		priority int
		retry    bool
	)

	updated, err := p.store.Update(jobID, func(j *job.Job) {
		j.Attempts = append(j.Attempts, att)
		j.AttemptsUsed = len(j.Attempts)
		j.ExitCode = att.ExitCode
		j.LastError = att.Error

		now := att.FinishedAt
		switch {
		case succeeded:
			j.Status = job.StatusSucceeded
			j.LastError = ""
			j.FinishedAt = &now

		case j.AttemptsUsed >= j.Spec.MaxAttempts:
			// D-021: max_attempts is a total, so this is the last try.
			j.Status = job.StatusFailed
			j.FinishedAt = &now

		default:
			retry = true
			retryIn = p.backoff(j.AttemptsUsed)
			priority = j.Spec.Priority
			next := now.Add(retryIn)
			j.Status = job.StatusRetrying
			j.NextAttemptAt = &next
		}
	})
	if err != nil {
		log.Error("failed to record attempt", "error", err)
		return
	}

	switch updated.Status {
	case job.StatusSucceeded:
		p.mx.JobsCompleted.WithLabelValues(string(job.StatusSucceeded)).Inc()
		log.Info("job succeeded", "duration", att.FinishedAt.Sub(att.StartedAt))
	case job.StatusFailed:
		p.mx.JobsCompleted.WithLabelValues(string(job.StatusFailed)).Inc()
		log.Warn("job permanently failed", "attempts", updated.AttemptsUsed, "error", updated.LastError)
	case job.StatusRetrying:
		p.mx.JobRetries.Inc()
		log.Info("attempt failed, scheduling retry",
			"error", updated.LastError,
			"retry_in", retryIn,
			"attempts_used", updated.AttemptsUsed,
			"max_attempts", updated.Spec.MaxAttempts)
	}

	if !retry {
		return
	}

	// D-018: the retry is handed to a runtime timer rather than slept on here, so this
	// worker is immediately free for other jobs. A pending AfterFunc is not a goroutine —
	// the runtime keeps it in a timer heap — so this is cheap even with many jobs backing off.
	time.AfterFunc(retryIn, func() {
		if p.ctx.Err() != nil {
			return // shutting down; abandon the retry
		}
		if err := p.q.Requeue(jobID, priority); err != nil {
			if !errors.Is(err, queue.ErrClosed) {
				log.Error("failed to requeue for retry", "error", err)
			}
			return
		}
		_, _ = p.store.Update(jobID, func(j *job.Job) {
			// Only move back to queued if it is still waiting on this retry.
			if j.Status == job.StatusRetrying {
				j.Status = job.StatusQueued
			}
		})
	})
}

// backoff returns the delay before the next attempt: base * 2^(attemptsUsed-1), capped.
//
// With the default 1s base that is 1s, 2s, 4s, 8s... exactly as the brief specifies.
// See NOTES.md D-030 for why these parameters and why there is no jitter.
func (p *Pool) backoff(attemptsUsed int) time.Duration {
	if attemptsUsed < 1 {
		attemptsUsed = 1
	}
	// Shift rather than math.Pow, and cap the exponent, so a large max_attempts cannot
	// overflow the int64 nanosecond count into a negative duration.
	const maxShift = 32
	shift := attemptsUsed - 1
	if shift > maxShift {
		shift = maxShift
	}
	d := time.Duration(float64(p.cfg.BackoffBase) * math.Pow(2, float64(shift)))
	if d > p.cfg.BackoffMax || d <= 0 {
		return p.cfg.BackoffMax
	}
	return d
}
