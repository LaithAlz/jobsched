// Package job defines the core domain type of the scheduler: a unit of work, its
// specification, and its lifecycle state machine.
//
// This package has no dependencies on Docker, HTTP, or Kubernetes. It is the vocabulary every
// other package speaks, which is what lets the executor be swapped out (Docker in Stage 1,
// Kubernetes in Stage 3) without the queue, store, or API noticing.
package job

import (
	"slices"
	"time"
)

// Status is the lifecycle state of a job.
//
// Valid transitions:
//
//	queued   -> running                (a worker picked it up)
//	running  -> succeeded              (container exited 0)
//	running  -> retrying               (attempt failed, attempts remain)
//	running  -> failed                 (attempt failed, no attempts remain)
//	retrying -> queued                 (backoff elapsed, re-enqueued)
//
// succeeded and failed are terminal.
type Status string

const (
	StatusQueued  Status = "queued"
	StatusRunning Status = "running"
	// StatusRetrying means the last attempt failed and the job is waiting out its backoff.
	// It is NOT occupying a worker while in this state. See NOTES.md D-018.
	StatusRetrying  Status = "retrying"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Terminal reports whether the job has reached a state it can never leave.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed
}

// Spec is the immutable description of what to run, supplied by the client at submission.
// Nothing mutates a Spec after validation.
type Spec struct {
	// Command runs via `/bin/sh -c`, so pipes, && and redirection work.
	Command string `json:"command"`
	Image   string `json:"image"`
	// CPUs is the fractional CPU limit, as in `docker run --cpus`.
	CPUs float64 `json:"cpus"`
	// MemoryMB is a hard limit, as in `docker run --memory`. Exceeding it means the kernel
	// OOM-kills the process, not that it gets throttled.
	MemoryMB int64 `json:"memory_mb"`
	// Priority orders the ready queue: higher runs first, ties broken FIFO.
	Priority int `json:"priority"`
	// MaxAttempts is the total number of tries, not the number of retries: 1 means
	// "run once, never retry". See NOTES.md D-021.
	MaxAttempts    int `json:"max_attempts"`
	TimeoutSeconds int `json:"timeout_seconds"`
}

// Attempt records the outcome of one execution try. Keeping every attempt rather than only
// the latest is what makes retry/backoff externally observable, and therefore testable: the
// harness measures real backoff from the gap between one attempt's FinishedAt and the next
// attempt's StartedAt.
type Attempt struct {
	Number     int       `json:"number"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	// ExitCode is nil if the container never ran (an infrastructure error, not a job failure).
	ExitCode    *int   `json:"exit_code,omitempty"`
	OOMKilled   bool   `json:"oom_killed"`
	Error       string `json:"error,omitempty"`
	ContainerID string `json:"container_id,omitempty"`
	Logs        string `json:"logs,omitempty"`
}

// Job is the full record of a submitted unit of work: what to run, plus what has happened.
type Job struct {
	ID     string `json:"id"`
	Spec   Spec   `json:"spec"`
	Status Status `json:"status"`

	// AttemptsUsed counts completed attempts; it always equals len(Attempts).
	AttemptsUsed int `json:"attempts_used"`

	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// NextAttemptAt is set only while Status == retrying.
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`

	ExitCode  *int   `json:"exit_code,omitempty"`
	LastError string `json:"last_error,omitempty"`

	Attempts []Attempt `json:"attempts"`
}

// Clone returns a copy of the job that is safe to hand to another goroutine.
//
// Only the Attempts slice needs deep-copying: a plain struct assignment copies the slice
// header but shares its backing array, so a caller ranging over j.Attempts while a worker
// appends to the original would race.
//
// The pointer fields are shallow-copied on purpose, under a rule the whole codebase keeps:
// they are write-once. Code assigns a *new* pointer (j.FinishedAt = &t), never writes through
// an existing one (*j.FinishedAt = t). That makes the pointed-to values immutable and safe to
// share between copies. See NOTES.md D-012.
func (j Job) Clone() Job {
	out := j
	out.Attempts = slices.Clone(j.Attempts)
	return out
}
