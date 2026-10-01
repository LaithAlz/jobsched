package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/LaithAlz/jobsched/internal/job"
)

func TestScrapeTimeGaugesReadLiveValues(t *testing.T) {
	m := New()

	// Mutable backing values stand in for the real queue/store/pool.
	depth, busy := 0, 0
	counts := map[job.Status]int{}
	m.RegisterSources(Sources{
		QueueDepth:   func() int { return depth },
		StatusCounts: func() map[job.Status]int { return counts },
		WorkersBusy:  func() int { return busy },
		WorkersTotal: 4,
	})

	body := scrape(t, m)
	if !strings.Contains(body, "jobsched_queue_depth 0") {
		t.Errorf("queue_depth should start at 0:\n%s", body)
	}

	// The point of D-038: change the underlying value, scrape again, and the gauge follows
	// without anything having called Set().
	depth, busy = 7, 3
	counts[job.StatusRunning] = 3
	counts[job.StatusQueued] = 7

	body = scrape(t, m)
	for _, want := range []string{
		"jobsched_queue_depth 7",
		"jobsched_workers_busy 3",
		"jobsched_workers_total 4",
		`jobsched_jobs_by_status{status="running"} 3`,
		`jobsched_jobs_by_status{status="queued"} 7`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in scrape output:\n%s", want, body)
		}
	}
}

// A status with no jobs must still be reported as 0. If it vanished from the output, its
// dashboard series would show a gap rather than a zero.
func TestAllStatusesReportedEvenWhenZero(t *testing.T) {
	m := New()
	m.RegisterSources(Sources{
		QueueDepth:   func() int { return 0 },
		StatusCounts: func() map[job.Status]int { return map[job.Status]int{job.StatusFailed: 2} },
		WorkersBusy:  func() int { return 0 },
		WorkersTotal: 1,
	})

	body := scrape(t, m)
	for _, s := range []job.Status{
		job.StatusQueued, job.StatusRunning, job.StatusRetrying,
		job.StatusSucceeded, job.StatusFailed,
	} {
		if !strings.Contains(body, `jobsched_jobs_by_status{status="`+string(s)+`"}`) {
			t.Errorf("status %q missing from output entirely:\n%s", s, body)
		}
	}
	if !strings.Contains(body, `jobsched_jobs_by_status{status="failed"} 2`) {
		t.Error("failed count should be 2")
	}
	if !strings.Contains(body, `jobsched_jobs_by_status{status="queued"} 0`) {
		t.Error("queued should be present as an explicit 0")
	}
}

func TestCountersRecordEvents(t *testing.T) {
	m := New()

	m.JobsSubmitted.Inc()
	m.JobsSubmitted.Inc()
	m.JobsCompleted.WithLabelValues("succeeded").Inc()
	m.JobsCompleted.WithLabelValues("failed").Inc()
	m.JobRetries.Inc()
	m.SubmissionsRejected.WithLabelValues("queue_full").Inc()
	m.ObserveAttempt("succeeded", 1.5)
	m.ObserveAttempt("error", 0.2)

	if got := testutil.ToFloat64(m.JobsSubmitted); got != 2 {
		t.Errorf("jobs_submitted = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.JobsCompleted.WithLabelValues("succeeded")); got != 1 {
		t.Errorf("jobs_completed{succeeded} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.JobAttempts.WithLabelValues("error")); got != 1 {
		t.Errorf("job_attempts{error} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.SubmissionsRejected.WithLabelValues("queue_full")); got != 1 {
		t.Errorf("submissions_rejected{queue_full} = %v, want 1", got)
	}
}

// D-039: each Metrics owns its registry, so two instances in one process do not collide. With
// the default global registry this would panic on duplicate registration.
func TestTwoInstancesAreIndependent(t *testing.T) {
	a, b := New(), New()
	a.JobsSubmitted.Inc()

	if got := testutil.ToFloat64(a.JobsSubmitted); got != 1 {
		t.Errorf("a = %v, want 1", got)
	}
	if got := testutil.ToFloat64(b.JobsSubmitted); got != 0 {
		t.Errorf("b = %v, want 0 — instances must not share state", got)
	}
}

// D-039: the Go and process collectors are re-added by hand, since we are off the default
// registry. go_goroutines is the leak check for the worker pool and retry timers.
func TestRuntimeCollectorsArePresent(t *testing.T) {
	body := scrape(t, New())
	for _, want := range []string{"go_goroutines", "go_memstats_alloc_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("%q missing; the Go collector was not registered", want)
		}
	}
}

func TestHistogramBucketsCoverContainerStartup(t *testing.T) {
	m := New()
	m.ObserveAttempt("succeeded", 0.3) // typical container startup
	body := scrape(t, m)

	// D-043: buckets must bracket the real distribution. 0.25 is the first bucket a
	// container-backed attempt can plausibly land in.
	for _, want := range []string{
		`jobsched_attempt_duration_seconds_bucket{outcome="succeeded",le="0.25"} 0`,
		`jobsched_attempt_duration_seconds_bucket{outcome="succeeded",le="0.5"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics returned %d", rec.Code)
	}
	return rec.Body.String()
}

// A CounterVec with no observations emits no series at all, so an error panel would read
// "No data" when the system is healthy — indistinguishable from a missing metric or a dead
// scheduler. The known label values are initialised to zero so the healthy state is visible.
func TestKubernetesAPIErrorsStartAtExplicitZero(t *testing.T) {
	body := scrape(t, New())
	for _, op := range k8sAPIOps {
		want := `jobsched_k8s_api_errors_total{op="` + op + `"} 0`
		if !strings.Contains(body, want) {
			t.Errorf("missing %q — an error metric must show a flat zero when healthy, not "+
				"vanish from the output:\n%s", want, body)
		}
	}
}
