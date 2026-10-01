// Package integration is the end-to-end test harness for Stage 1.
//
// These tests run the real service — real HTTP API, real queue, real worker pool, real Docker
// containers — and drive it the way a client would. Nothing is mocked. They are the evidence
// for the two things the brief asks to be proven:
//
//   - retry with exponential backoff actually happens, with the right delays and the right
//     number of attempts (TestFailingJobRetriesWithExponentialBackoff);
//   - CPU and memory limits are actually *enforced* by the kernel, not merely requested
//     (TestMemoryLimitIsEnforcedByOOMKill, TestCgroupLimitsMatchTheRequest,
//     TestCPULimitActuallyThrottles).
//
// Run with:  go test ./test/ -v
// Skip with: go test ./test/ -short
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LaithAlz/jobsched/internal/api"
	"github.com/LaithAlz/jobsched/internal/config"
	"github.com/LaithAlz/jobsched/internal/executor"
	"github.com/LaithAlz/jobsched/internal/job"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
	"github.com/LaithAlz/jobsched/internal/worker"
)

const testImage = "alpine:3.20"

// service is a fully wired scheduler behind a real HTTP server. The same struct and helpers
// drive both the Docker suite (this file) and the Kubernetes suite (k8s_integration_test.go),
// which is only possible because the two executors are behind one interface (D-002).
type service struct {
	t   *testing.T
	url string
	mx  *metrics.Metrics
}

func startService(t *testing.T, workers int, backoffBase time.Duration) *service {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Docker integration test in -short mode")
	}

	cfg := config.Config{
		Workers: workers, QueueCapacity: 64,
		DefaultImage: testImage, DefaultCPUs: 1, DefaultMemoryMB: 128,
		DefaultMaxAttempts: 4, DefaultTimeout: 2 * time.Minute,
		MaxCPUs: 4, MaxMemoryMB: 2048, MaxAttempts: 10, MaxJobTimeout: 30 * time.Minute,
		BackoffBase: backoffBase, BackoffMax: time.Minute,
		JobNetwork: "none", LogTailBytes: 4096,
	}

	exec, err := executor.NewDocker(cfg.JobNetwork, cfg.LogTailBytes)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = exec.Ping(ctx)
		cancel()
	}
	if err != nil {
		// A skipped test reports as PASS, so an unreachable daemon would otherwise look
		// like these proofs ran and succeeded. Setting SCHED_REQUIRE_DOCKER=1 (which the
		// Makefile does) turns that into a hard failure.
		if os.Getenv("SCHED_REQUIRE_DOCKER") != "" {
			t.Fatalf("SCHED_REQUIRE_DOCKER is set but Docker is unusable: %v", err)
		}
		t.Skipf("docker unavailable, skipping integration test: %v", err)
	}

	st := store.NewMemory()
	q := queue.New(cfg.QueueCapacity)
	mx := metrics.New()
	pool := worker.NewPool(worker.Config{
		Workers: cfg.Workers, BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax,
	}, q, st, exec, mx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool.Start()
	mx.RegisterSources(metrics.Sources{
		QueueDepth: q.Len, StatusCounts: st.Counts,
		WorkersBusy: pool.BusyWorkers, WorkersTotal: cfg.Workers,
	})

	srv := httptest.NewServer(api.NewServer(cfg, st, q, mx, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes())

	t.Cleanup(func() {
		srv.Close()
		shutdownCtx, c := context.WithTimeout(context.Background(), 60*time.Second)
		defer c()
		_ = pool.Shutdown(shutdownCtx)
		_ = exec.Close()
	})

	return &service{t: t, url: srv.URL, mx: mx}
}

// submit posts a job and returns the accepted record.
func (s *service) submit(body map[string]any) job.Job {
	s.t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(s.url+"/jobs", "application/json", bytes.NewReader(raw))
	if err != nil {
		s.t.Fatalf("submit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		s.t.Fatalf("submit status = %d, want 202. body=%s", resp.StatusCode, b)
	}
	var j job.Job
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		s.t.Fatalf("decode: %v", err)
	}
	return j
}

func (s *service) get(id string) job.Job {
	s.t.Helper()
	resp, err := http.Get(s.url + "/jobs/" + id)
	if err != nil {
		s.t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var j job.Job
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		s.t.Fatalf("decode: %v", err)
	}
	return j
}

// waitTerminal polls the status endpoint — exactly as a real client would (D-007) — until the
// job succeeds or fails.
func (s *service) waitTerminal(id string, within time.Duration) job.Job {
	s.t.Helper()
	deadline := time.Now().Add(within)
	var last job.Job
	for time.Now().Before(deadline) {
		last = s.get(id)
		if last.Status.Terminal() {
			return last
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.t.Fatalf("job %s still %s after %s (attempts=%d, last_error=%q)",
		id, last.Status, within, last.AttemptsUsed, last.LastError)
	return job.Job{}
}

func TestSuccessfulJobRunsOnceAndCapturesOutput(t *testing.T) {
	s := startService(t, 2, time.Second)

	j := s.submit(map[string]any{"command": "echo hello-from-container", "max_attempts": 1})
	done := s.waitTerminal(j.ID, 90*time.Second)

	if done.Status != job.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (last_error=%q)", done.Status, done.LastError)
	}
	if done.AttemptsUsed != 1 {
		t.Errorf("attempts_used = %d, want 1 — a succeeding job must not retry", done.AttemptsUsed)
	}
	if done.ExitCode == nil || *done.ExitCode != 0 {
		t.Errorf("exit_code = %v, want 0", done.ExitCode)
	}
	if !strings.Contains(done.Attempts[0].Logs, "hello-from-container") {
		t.Errorf("logs = %q, want the container's stdout", done.Attempts[0].Logs)
	}
	if done.Attempts[0].ContainerID == "" {
		t.Error("container_id should be recorded for a job that ran")
	}
}

// TestFailingJobRetriesWithExponentialBackoff is the retry/backoff proof the brief asks for.
//
// A container that always exits 1, with max_attempts=4 and a 1s base, must produce exactly 4
// attempts separated by roughly 1s, 2s and 4s, and end permanently failed.
func TestFailingJobRetriesWithExponentialBackoff(t *testing.T) {
	const base = time.Second
	s := startService(t, 2, base)

	submitted := time.Now()
	j := s.submit(map[string]any{
		"command":      "echo failing-on-purpose >&2; exit 1",
		"max_attempts": 4,
		"memory_mb":    64,
		"cpus":         0.5,
	})
	done := s.waitTerminal(j.ID, 120*time.Second)
	total := time.Since(submitted)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	// D-021: max_attempts is a total. 4 means 4 runs, not 5.
	if done.AttemptsUsed != 4 || len(done.Attempts) != 4 {
		t.Fatalf("attempts_used = %d, len(attempts) = %d, want exactly 4",
			done.AttemptsUsed, len(done.Attempts))
	}
	if done.FinishedAt == nil {
		t.Error("finished_at must be set on a permanently failed job")
	}

	for i, a := range done.Attempts {
		if a.ExitCode == nil || *a.ExitCode != 1 {
			t.Errorf("attempt %d exit_code = %v, want 1", i+1, a.ExitCode)
		}
		if !strings.Contains(a.Logs, "failing-on-purpose") {
			t.Errorf("attempt %d logs = %q, want stderr captured", i+1, a.Logs)
		}
	}

	// The observed backoff: the gap between one attempt finishing and the next starting.
	wantGaps := []time.Duration{base, 2 * base, 4 * base}
	for i, want := range wantGaps {
		got := done.Attempts[i+1].StartedAt.Sub(done.Attempts[i].FinishedAt)
		t.Logf("backoff before attempt %d: %s (want ~%s)", i+2, got.Round(time.Millisecond), want)
		if got < want {
			t.Errorf("backoff before attempt %d = %s, want at least %s", i+2, got, want)
		}
		if got > want+2*time.Second {
			t.Errorf("backoff before attempt %d = %s, want ~%s (far too long)", i+2, got, want)
		}
	}

	// D-021's stated consequence: three waits, not four. Total must exceed 1+2+4 = 7s.
	if total < 7*time.Second {
		t.Errorf("total elapsed %s; 1s+2s+4s of backoff means it cannot finish faster than 7s", total)
	}
	t.Logf("job permanently failed after %d attempts in %s", done.AttemptsUsed, total.Round(time.Millisecond))
}

// TestMemoryLimitIsEnforcedByOOMKill proves the memory limit is a hard kernel-enforced wall.
//
// `tail /dev/zero` allocates without bound. If the limit were merely requested (or if swap
// were left enabled — D-025), this would grow past 64 MB and eventually succeed or thrash.
// Instead the kernel OOM-kills it, and the daemon records OOMKilled=true.
func TestMemoryLimitIsEnforcedByOOMKill(t *testing.T) {
	s := startService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command":      "tail /dev/zero",
		"memory_mb":    64,
		"max_attempts": 1,
		"cpus":         1,
	})
	done := s.waitTerminal(j.ID, 120*time.Second)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	a := done.Attempts[0]
	// OOMKilled is the load-bearing assertion. Exit 137 alone only means SIGKILL — it does
	// not distinguish "hit its memory limit" from "was killed for any other reason".
	if !a.OOMKilled {
		t.Errorf("oom_killed = false; the memory limit was not enforced by the kernel")
	}
	if a.ExitCode == nil || *a.ExitCode != 137 {
		t.Errorf("exit_code = %v, want 137 (128+SIGKILL)", a.ExitCode)
	}
	if !strings.Contains(done.LastError, "memory limit") {
		t.Errorf("last_error = %q, want it to name the memory limit", done.LastError)
	}
	t.Logf("container OOM-killed as expected: exit=%d oom_killed=%v", *a.ExitCode, a.OOMKilled)
}

// TestCgroupLimitsMatchTheRequest reads the kernel's own cgroup v2 files from inside the
// container. This is the difference between "we passed a flag" and "the kernel is enforcing
// the number we asked for": these values are written by the kernel, not by the scheduler.
func TestCgroupLimitsMatchTheRequest(t *testing.T) {
	s := startService(t, 2, time.Second)

	const (
		cpus     = 0.5
		memoryMB = 64
	)
	j := s.submit(map[string]any{
		"command":      "cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/memory.max",
		"cpus":         cpus,
		"memory_mb":    memoryMB,
		"max_attempts": 1,
	})
	done := s.waitTerminal(j.ID, 90*time.Second)

	if done.Status != job.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (logs=%q)", done.Status, done.Attempts[0].Logs)
	}
	logs := done.Attempts[0].Logs
	t.Logf("cgroup values reported by the kernel:\n%s", strings.TrimSpace(logs))

	// cpu.max is "<quota> <period>" in microseconds. 0.5 CPU over the default 100000us
	// period is a 50000us quota.
	wantCPU := fmt.Sprintf("%d 100000", int(cpus*100000))
	if !strings.Contains(logs, wantCPU) {
		t.Errorf("cpu.max does not contain %q; requested %g CPUs was not applied", wantCPU, cpus)
	}

	wantMem := strconv.Itoa(memoryMB * 1024 * 1024)
	if !strings.Contains(logs, wantMem) {
		t.Errorf("memory.max does not contain %q; requested %d MB was not applied", wantMem, memoryMB)
	}
}

// TestCPULimitActuallyThrottles proves the CPU limit has teeth. cpu.stat's nr_throttled is
// the kernel's count of scheduling periods in which this cgroup was stopped for exhausting
// its quota. A non-zero value cannot happen unless the limit is being enforced.
func TestCPULimitActuallyThrottles(t *testing.T) {
	s := startService(t, 2, time.Second)

	j := s.submit(map[string]any{
		// A CPU-bound shell loop, then dump the kernel's throttling counters.
		"command":         "i=0; while [ $i -lt 120000 ]; do i=$((i+1)); done; cat /sys/fs/cgroup/cpu.stat",
		"cpus":            0.25,
		"memory_mb":       64,
		"max_attempts":    1,
		"timeout_seconds": 120,
	})
	done := s.waitTerminal(j.ID, 180*time.Second)

	if done.Status != job.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (last_error=%q)", done.Status, done.LastError)
	}
	logs := done.Attempts[0].Logs
	t.Logf("cpu.stat after a CPU-bound loop at 0.25 CPU:\n%s", strings.TrimSpace(logs))

	throttled := parseCgroupStat(logs, "nr_throttled")
	throttledUsec := parseCgroupStat(logs, "throttled_usec")
	if throttled <= 0 {
		t.Errorf("nr_throttled = %d, want > 0 — the CPU limit was requested but never enforced", throttled)
	}
	if throttledUsec <= 0 {
		t.Errorf("throttled_usec = %d, want > 0", throttledUsec)
	}
	t.Logf("kernel throttled the container %d times for a total of %.2fs",
		throttled, float64(throttledUsec)/1e6)
}

// TestAttemptTimeoutIsEnforced covers D-029: a job that never exits is killed at its deadline
// and the failure is attributed to the timeout rather than to a generic error.
func TestAttemptTimeoutIsEnforced(t *testing.T) {
	s := startService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command":         "sleep 300",
		"timeout_seconds": 3,
		"max_attempts":    1,
		"memory_mb":       64,
	})
	start := time.Now()
	done := s.waitTerminal(j.ID, 90*time.Second)
	elapsed := time.Since(start)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	if !strings.Contains(done.LastError, "timeout") {
		t.Errorf("last_error = %q, want it to name the timeout", done.LastError)
	}
	if elapsed > 45*time.Second {
		t.Errorf("took %s; a 3s timeout should terminate the attempt promptly", elapsed)
	}
	t.Logf("timed out and cleaned up after %s: %s", elapsed.Round(time.Millisecond), done.LastError)
}

// TestJobContainerHasNoNetwork covers D-027: the default isolation posture.
func TestJobContainerHasNoNetwork(t *testing.T) {
	s := startService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command":         "ip addr show eth0",
		"max_attempts":    1,
		"memory_mb":       64,
		"timeout_seconds": 30,
	})
	done := s.waitTerminal(j.ID, 90*time.Second)

	if done.Status != job.StatusFailed {
		t.Errorf("status = %s, want failed: with NetworkMode=none there is no eth0", done.Status)
	}
	t.Logf("no network interface, as expected: %s", strings.TrimSpace(done.Attempts[0].Logs))
}

// TestConcurrentJobsAllComplete checks the pool drains a burst without losing or duplicating
// work — the property that makes the queue a load buffer rather than a bottleneck (D-005).
func TestConcurrentJobsAllComplete(t *testing.T) {
	const n = 8
	s := startService(t, 4, time.Second)

	ids := make([]string, n)
	for i := range n {
		j := s.submit(map[string]any{
			"command":      fmt.Sprintf("echo job-%d", i),
			"max_attempts": 1,
			"memory_mb":    64,
			"cpus":         0.5,
		})
		ids[i] = j.ID
	}

	for i, id := range ids {
		done := s.waitTerminal(id, 180*time.Second)
		if done.Status != job.StatusSucceeded {
			t.Errorf("job %d (%s) status = %s, want succeeded (last_error=%q)",
				i, id, done.Status, done.LastError)
		}
		if done.AttemptsUsed != 1 {
			t.Errorf("job %d ran %d times, want exactly 1 — no duplicate execution",
				i, done.AttemptsUsed)
		}
	}
}

// parseCgroupStat pulls a named counter out of a cgroup stat file's "key value" lines.
func parseCgroupStat(logs, key string) int64 {
	for _, line := range strings.Split(logs, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 2 && fields[0] == key {
			v, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return -1
			}
			return v
		}
	}
	return -1
}
