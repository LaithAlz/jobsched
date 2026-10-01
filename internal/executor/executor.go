// Package executor abstracts "run this command somewhere isolated, tell me how it went".
//
// This interface is the seam that Stage 3 replaces: the Docker implementation here becomes a
// Kubernetes implementation without the queue, worker pool, retry logic, or API changing.
// See NOTES.md D-002 and D-009.
package executor

import (
	"context"
	"time"
)

// Request is one execution attempt.
type Request struct {
	JobID    string
	Attempt  int
	Command  string
	Image    string
	CPUs     float64
	MemoryMB int64
	Timeout  time.Duration
}

// Result describes an execution that actually happened.
//
// The split between a non-nil error and a Result with a non-zero ExitCode is deliberate and
// load-bearing (see NOTES.md D-024):
//
//	error != nil        -> the job never ran. Infrastructure problem: daemon unreachable,
//	                       image pull failed, container creation rejected.
//	err == nil, code!=0 -> the job ran and failed. This is the job's own fault.
//
// Both are retried, but they mean different things and are reported differently.
type Result struct {
	ExitCode  int
	OOMKilled bool
	// TimedOut is true if the attempt was killed for exceeding its timeout.
	TimedOut bool
	Logs     string
	// RunID identifies the underlying execution unit: a container ID in Stage 1, a
	// Kubernetes Job name in Stage 3.
	RunID string
}

// Executor runs a single attempt to completion. Implementations must be safe for concurrent
// use by multiple worker goroutines.
type Executor interface {
	Run(ctx context.Context, req Request) (Result, error)
	// Close releases any long-lived resources (an HTTP client to the Docker daemon).
	Close() error
}
