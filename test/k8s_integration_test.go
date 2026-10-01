package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/LaithAlz/jobsched/internal/api"
	"github.com/LaithAlz/jobsched/internal/config"
	"github.com/LaithAlz/jobsched/internal/executor"
	"github.com/LaithAlz/jobsched/internal/job"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
	"github.com/LaithAlz/jobsched/internal/worker"
)

// Stage 3 end-to-end tests against a real kind cluster.
//
// These deliberately mirror the Docker suite in integration_test.go, because the point of the
// Executor interface is that the *scheduler's* observable behaviour does not change when the
// backend does. The retry timings, attempt counts and enforced limits asserted here are the
// same ones Stage 1 asserted — only the mechanism underneath is different.
//
//	make cluster-up          create the kind cluster
//	make test-k8s            run this suite
//
// Skipped when no cluster is reachable, unless SCHED_REQUIRE_K8S is set (D-062).

func startK8sService(t *testing.T, workers int, backoffBase time.Duration) *service {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Kubernetes integration test in -short mode")
	}

	cfg := config.Config{
		Workers: workers, QueueCapacity: 64,
		DefaultImage: testImage, DefaultCPUs: 1, DefaultMemoryMB: 128,
		DefaultMaxAttempts: 4, DefaultTimeout: 3 * time.Minute,
		MaxCPUs: 4, MaxMemoryMB: 2048, MaxAttempts: 10, MaxJobTimeout: 30 * time.Minute,
		BackoffBase: backoffBase, BackoffMax: time.Minute,
		LogTailBytes: 4096,
		Executor:     "kubernetes",
		K8sNamespace: k8sNamespace(),
		K8sContext:   k8sContext(),
	}

	mx := metrics.New()
	exec, err := executor.NewKubernetes(executor.KubernetesOptions{
		Context:      cfg.K8sContext,
		Namespace:    cfg.K8sNamespace,
		TTLSeconds:   120,
		LogTailBytes: cfg.LogTailBytes,
	}, mx)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = exec.Ping(ctx)
		cancel()
	}
	if err != nil {
		// Same fail-loud rule as D-033: a skip reports as PASS, so where these proofs are
		// meant to run, a missing cluster must fail instead.
		if os.Getenv("SCHED_REQUIRE_K8S") != "" {
			t.Fatalf("SCHED_REQUIRE_K8S is set but the cluster is unusable: %v", err)
		}
		t.Skipf("kind cluster unavailable, skipping: %v", err)
	}

	st := store.NewMemory()
	q := queue.New(cfg.QueueCapacity)
	pool := worker.NewPool(worker.Config{
		Workers: cfg.Workers, BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax,
	}, q, st, exec, mx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool.Start()
	mx.RegisterSources(metrics.Sources{
		QueueDepth: q.Len, StatusCounts: st.Counts,
		WorkersBusy: pool.BusyWorkers, WorkersTotal: cfg.Workers,
		K8sActiveJobs: exec.ActiveJobs,
	})

	srv := httptest.NewServer(api.NewServer(cfg, st, q, mx, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes())

	t.Cleanup(func() {
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
		_ = exec.Close()
	})

	return &service{t: t, url: srv.URL, mx: mx}
}

func k8sContext() string {
	if v := os.Getenv("SCHED_K8S_CONTEXT"); v != "" {
		return v
	}
	return "kind-jobsched"
}

func k8sNamespace() string {
	if v := os.Getenv("SCHED_K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jobsched-test"
}

func TestK8sSuccessfulJobCapturesOutput(t *testing.T) {
	s := startK8sService(t, 2, time.Second)

	j := s.submit(map[string]any{"command": "echo hello-from-kubernetes", "max_attempts": 1, "memory_mb": 64, "cpus": 0.5})
	done := s.waitTerminal(j.ID, 3*time.Minute)

	if done.Status != job.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (last_error=%q)", done.Status, done.LastError)
	}
	if done.AttemptsUsed != 1 {
		t.Errorf("attempts_used = %d, want 1", done.AttemptsUsed)
	}
	if !strings.Contains(done.Attempts[0].Logs, "hello-from-kubernetes") {
		t.Errorf("logs = %q, want the pod's stdout", done.Attempts[0].Logs)
	}
	// RunID is the Kubernetes Job name here, where it was a container ID in Stage 1.
	if !strings.HasPrefix(done.Attempts[0].ContainerID, "jobsched-") {
		t.Errorf("run id = %q, want the kubernetes job name", done.Attempts[0].ContainerID)
	}
}

// TestK8sRetryIsDrivenByTheApplicationNotKubernetes is the central Stage 3 proof (D-051).
//
// Three things must hold together:
//   - the backoff intervals are the scheduler's (1s, 2s), not Kubernetes' hardcoded 10s floor;
//   - each attempt is a *separate* Job object, so the scheduler created them, not the controller;
//   - exactly one pod terminated per attempt, so the Job controller never retried.
func TestK8sRetryIsDrivenByTheApplicationNotKubernetes(t *testing.T) {
	const base = time.Second
	s := startK8sService(t, 2, base)

	j := s.submit(map[string]any{
		"command": "echo failing-in-k8s >&2; exit 7", "max_attempts": 3,
		"memory_mb": 64, "cpus": 0.25,
	})
	done := s.waitTerminal(j.ID, 5*time.Minute)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	if done.AttemptsUsed != 3 || len(done.Attempts) != 3 {
		t.Fatalf("attempts_used = %d, len(attempts) = %d, want exactly 3",
			done.AttemptsUsed, len(done.Attempts))
	}

	// Kubernetes' own Job backoff starts at 10s and is not configurable. Observing ~1s and ~2s
	// gaps is direct evidence the scheduler is driving the retries.
	wantGaps := []time.Duration{base, 2 * base}
	for i, want := range wantGaps {
		got := done.Attempts[i+1].StartedAt.Sub(done.Attempts[i].FinishedAt)
		t.Logf("backoff before attempt %d: %s (want ~%s; Kubernetes-native would be >= 10s)",
			i+2, got.Round(time.Millisecond), want)
		if got < want {
			t.Errorf("backoff before attempt %d = %s, want at least %s", i+2, got, want)
		}
		if got >= 10*time.Second {
			t.Errorf("backoff before attempt %d = %s — that is Kubernetes' native backoff, "+
				"not the application's (D-051)", i+2, got)
		}
	}

	// Each attempt must be its own Job object (D-050).
	seen := map[string]bool{}
	for i, a := range done.Attempts {
		if a.ExitCode == nil || *a.ExitCode != 7 {
			t.Errorf("attempt %d exit_code = %v, want 7", i+1, a.ExitCode)
		}
		if !strings.Contains(a.Logs, "failing-in-k8s") {
			t.Errorf("attempt %d logs = %q, want stderr captured", i+1, a.Logs)
		}
		if seen[a.ContainerID] {
			t.Errorf("attempt %d reused job name %q; attempts must be distinct Job objects",
				i+1, a.ContainerID)
		}
		seen[a.ContainerID] = true
		if want := fmt.Sprintf("-%d", i+1); !strings.HasSuffix(a.ContainerID, want) {
			t.Errorf("attempt %d job name = %q, want it to end in %q", i+1, a.ContainerID, want)
		}
	}

	// Exactly one Job per attempt, and exactly one pod termination per attempt. With
	// Kubernetes-native retry (backoffLimit > 0) the terminations would outnumber the Jobs.
	if got := testutil.ToFloat64(s.mx.K8sJobsCreated); got != 3 {
		t.Errorf("k8s_jobs_created_total = %v, want exactly 3", got)
	}
	if got := testutil.ToFloat64(s.mx.K8sPodTerminations.WithLabelValues("Error")); got != 3 {
		t.Errorf("k8s_pod_terminations{Error} = %v, want exactly 3 — more would mean the Job "+
			"controller retried on its own", got)
	}
	if got := testutil.ToFloat64(s.mx.JobRetries); got != 2 {
		t.Errorf("job_retries_total = %v, want 2 (3 attempts = 2 retries)", got)
	}
}

// TestK8sMemoryLimitIsEnforcedByOOMKill is the same proof as Stage 1, through pod resource
// limits instead of Docker flags. Identical mechanism underneath: a cgroup memory.max the
// kernel enforces (D-053).
func TestK8sMemoryLimitIsEnforcedByOOMKill(t *testing.T) {
	s := startK8sService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command": "tail /dev/zero", "memory_mb": 64, "max_attempts": 1, "cpus": 0.5,
	})
	done := s.waitTerminal(j.ID, 3*time.Minute)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	a := done.Attempts[0]
	// The kubelet reports the reason, which is what distinguishes "hit its memory limit" from
	// "was killed for some other reason". Exit 137 alone only means SIGKILL.
	if !a.OOMKilled {
		t.Errorf("oom_killed = false; the pod memory limit was not enforced")
	}
	if a.ExitCode == nil || *a.ExitCode != 137 {
		t.Errorf("exit_code = %v, want 137", a.ExitCode)
	}
	if got := testutil.ToFloat64(s.mx.K8sPodTerminations.WithLabelValues("OOMKilled")); got != 1 {
		t.Errorf("k8s_pod_terminations{OOMKilled} = %v, want 1", got)
	}
	t.Logf("pod OOM-killed as expected: exit=%d oom_killed=%v", *a.ExitCode, a.OOMKilled)
}

// TestK8sCgroupLimitsMatchTheRequest reads the kernel's cgroup files from inside the pod.
// The values must be identical to the ones the Docker executor produced in Stage 1 — that is
// the evidence that resource requests/limits translate to the same enforcement.
func TestK8sCgroupLimitsMatchTheRequest(t *testing.T) {
	s := startK8sService(t, 2, time.Second)

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
	done := s.waitTerminal(j.ID, 3*time.Minute)

	if done.Status != job.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (logs=%q)", done.Status, done.Attempts[0].Logs)
	}
	logs := done.Attempts[0].Logs
	t.Logf("cgroup values inside the pod:\n%s", strings.TrimSpace(logs))

	wantCPU := fmt.Sprintf("%d 100000", int(cpus*100000))
	if !strings.Contains(logs, wantCPU) {
		t.Errorf("cpu.max does not contain %q; the pod cpu limit was not applied", wantCPU)
	}
	// 64Mi == 67108864 bytes. A "64M" (decimal) limit would read 64000000 here.
	wantMem := strconv.Itoa(memoryMB * 1024 * 1024)
	if !strings.Contains(logs, wantMem) {
		t.Errorf("memory.max does not contain %q; the pod memory limit was not applied", wantMem)
	}
}

// TestK8sCPULimitActuallyThrottles: nr_throttled is the kernel's own counter of periods in
// which it stopped the cgroup for exhausting quota. Non-zero is only possible under real
// enforcement (D-034, applied to pod limits).
func TestK8sCPULimitActuallyThrottles(t *testing.T) {
	s := startK8sService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command":         "i=0; while [ $i -lt 120000 ]; do i=$((i+1)); done; cat /sys/fs/cgroup/cpu.stat",
		"cpus":            0.25,
		"memory_mb":       64,
		"max_attempts":    1,
		"timeout_seconds": 180,
	})
	done := s.waitTerminal(j.ID, 5*time.Minute)

	if done.Status != job.StatusSucceeded {
		t.Fatalf("status = %s, want succeeded (last_error=%q)", done.Status, done.LastError)
	}
	logs := done.Attempts[0].Logs
	t.Logf("cpu.stat after a CPU-bound loop at 0.25 CPU:\n%s", strings.TrimSpace(logs))

	throttled := parseCgroupStat(logs, "nr_throttled")
	if throttled <= 0 {
		t.Errorf("nr_throttled = %d, want > 0 — the pod cpu limit was requested but not enforced", throttled)
	}
	t.Logf("kernel throttled the pod %d times (%.2fs)", throttled,
		float64(parseCgroupStat(logs, "throttled_usec"))/1e6)
}

func TestK8sAttemptTimeoutIsEnforced(t *testing.T) {
	s := startK8sService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command": "sleep 600", "timeout_seconds": 10, "max_attempts": 1, "memory_mb": 64,
	})
	start := time.Now()
	done := s.waitTerminal(j.ID, 4*time.Minute)
	elapsed := time.Since(start)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	if !strings.Contains(done.LastError, "timeout") {
		t.Errorf("last_error = %q, want it to name the timeout", done.LastError)
	}
	// The client-side context deadline should fire well before the server-side
	// activeDeadlineSeconds backstop (D-058).
	if elapsed > 90*time.Second {
		t.Errorf("took %s; the client-side deadline should terminate the attempt promptly", elapsed)
	}
	t.Logf("timed out after %s: %s", elapsed.Round(time.Millisecond), done.LastError)
}

// TestK8sPodThatNeverStartsIsAnInfraErrorNotAJobFailure covers D-056's diagnosis path: when an
// attempt runs out of time, the executor must work out whether the command ran too long or
// never ran at all. Reporting the latter as "the job failed" would be wrong — nothing ever
// executed, so it is an infrastructure error (D-024).
//
// The trigger here is an unpullable image, which leaves the pod Pending indefinitely. An
// oversized resource request would exercise the same path, but it cannot be produced through
// the API: D-020 caps requests at SCHED_MAX_CPUS, and any value under that cap is schedulable
// on a normal node. An unpullable image is also independent of how large the test machine is.
func TestK8sPodThatNeverStartsIsAnInfraErrorNotAJobFailure(t *testing.T) {
	s := startK8sService(t, 2, time.Second)

	j := s.submit(map[string]any{
		"command":         "echo never runs",
		"image":           "jobsched.invalid/does-not-exist:v1",
		"cpus":            0.25,
		"memory_mb":       64,
		"max_attempts":    1,
		"timeout_seconds": 30,
	})
	done := s.waitTerminal(j.ID, 5*time.Minute)

	if done.Status != job.StatusFailed {
		t.Fatalf("status = %s, want failed", done.Status)
	}
	// No exit code, because no container ever ran. That absence is the signal that separates
	// an infrastructure error from a job that ran and exited non-zero.
	a := done.Attempts[0]
	if a.ExitCode != nil {
		t.Errorf("exit_code = %v, want nil — the container never ran", a.ExitCode)
	}
	if got := testutil.ToFloat64(s.mx.JobAttempts.WithLabelValues("error")); got != 1 {
		t.Errorf("job_attempts{error} = %v, want 1 (an infra error, not a job failure)", got)
	}
	if got := testutil.ToFloat64(s.mx.JobAttempts.WithLabelValues("failed")); got != 0 {
		t.Errorf("job_attempts{failed} = %v, want 0", got)
	}
	// The message must explain the cause rather than just saying "timed out".
	if !strings.Contains(done.LastError, "never started") {
		t.Errorf("last_error = %q, want it to explain that the pod never started", done.LastError)
	}
	t.Logf("pod that never started, correctly reported as infrastructure error:\n  %s", done.LastError)
}

func TestK8sConcurrentJobsAllComplete(t *testing.T) {
	const n = 6
	s := startK8sService(t, 3, time.Second)

	ids := make([]string, n)
	for i := range n {
		j := s.submit(map[string]any{
			"command": fmt.Sprintf("echo k8s-job-%d", i), "max_attempts": 1,
			"memory_mb": 64, "cpus": 0.25,
		})
		ids[i] = j.ID
	}
	for i, id := range ids {
		done := s.waitTerminal(id, 5*time.Minute)
		if done.Status != job.StatusSucceeded {
			t.Errorf("job %d (%s) status = %s, want succeeded (last_error=%q)",
				i, id, done.Status, done.LastError)
		}
		if done.AttemptsUsed != 1 {
			t.Errorf("job %d ran %d times, want exactly 1", i, done.AttemptsUsed)
		}
	}

	if got := testutil.ToFloat64(s.mx.K8sJobsCreated); got != n {
		t.Errorf("k8s_jobs_created_total = %v, want %d", got, n)
	}
	// Scheduling latency is a Kubernetes-only concept: the gap between the object existing and
	// a node accepting it. It has no Docker equivalent.
	if got := testutil.CollectAndCount(s.mx.K8sPodScheduleLatency); got == 0 {
		t.Error("k8s_pod_schedule_latency_seconds recorded no observations")
	}
}
