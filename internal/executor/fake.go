package executor

import (
	"context"
	"sync"
	"time"
)

// Fake is an in-memory Executor for tests. It is the second implementation that justifies the
// Executor interface existing at all (NOTES.md D-009): it makes retry, backoff and scheduling
// behaviour testable in milliseconds instead of requiring a real container per attempt.
//
// It lives in the non-test build rather than a _test.go file so that packages other than
// executor (worker, api) can use it.
type Fake struct {
	// Handler decides the outcome of each attempt. If nil, every attempt succeeds.
	Handler func(req Request) (Result, error)
	// Delay is how long each attempt takes.
	Delay time.Duration

	mu    sync.Mutex
	calls []Request
}

func (f *Fake) Run(ctx context.Context, req Request) (Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.mu.Unlock()

	if f.Delay > 0 {
		select {
		case <-time.After(f.Delay):
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	if f.Handler == nil {
		return Result{ExitCode: 0, RunID: "fake"}, nil
	}
	return f.Handler(req)
}

func (f *Fake) Close() error { return nil }

// Calls returns every request the fake has seen, in order.
func (f *Fake) Calls() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Request, len(f.calls))
	copy(out, f.calls)
	return out
}

// AlwaysFails returns a Handler that reports the given non-zero exit code every time.
func AlwaysFails(code int) func(Request) (Result, error) {
	return func(req Request) (Result, error) {
		return Result{ExitCode: code, RunID: "fake", Logs: "boom"}, nil
	}
}

// FailsThenSucceeds returns a Handler that fails for the first n attempts, then succeeds.
func FailsThenSucceeds(n int) func(Request) (Result, error) {
	return func(req Request) (Result, error) {
		if req.Attempt <= n {
			return Result{ExitCode: 1, RunID: "fake"}, nil
		}
		return Result{ExitCode: 0, RunID: "fake"}, nil
	}
}
