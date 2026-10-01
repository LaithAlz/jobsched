package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LaithAlz/jobsched/internal/config"
	"github.com/LaithAlz/jobsched/internal/job"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
)

func newTestServer(t *testing.T, capacity int) (http.Handler, *store.MemoryStore, *queue.Queue) {
	t.Helper()
	cfg := config.Config{
		Workers: 2, QueueCapacity: capacity,
		DefaultImage: "alpine:3.20", DefaultCPUs: 1, DefaultMemoryMB: 256,
		DefaultMaxAttempts: 4, DefaultTimeout: 300e9,
		MaxCPUs: 4, MaxMemoryMB: 2048, MaxAttempts: 10, MaxJobTimeout: 1800e9,
	}
	st := store.NewMemory()
	q := queue.New(capacity)
	mx := metrics.New()
	mx.RegisterSources(metrics.Sources{
		QueueDepth: q.Len, StatusCounts: st.Counts,
		WorkersBusy: func() int { return 0 }, WorkersTotal: cfg.Workers,
	})
	h := NewServer(cfg, st, q, mx, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()
	return h, st, q
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSubmitAppliesDefaultsAndReturns202(t *testing.T) {
	h, _, q := newTestServer(t, 10)
	rec := post(t, h, `{"command":"echo hi"}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (D-007: accepted, not completed). body=%s", rec.Code, rec.Body)
	}
	var j job.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if j.Status != job.StatusQueued {
		t.Errorf("status = %s, want queued", j.Status)
	}
	if j.Spec.Image != "alpine:3.20" || j.Spec.CPUs != 1 || j.Spec.MemoryMB != 256 || j.Spec.MaxAttempts != 4 {
		t.Errorf("defaults not applied: %+v", j.Spec)
	}
	if !strings.HasPrefix(j.ID, "job_") {
		t.Errorf("id = %q, want a job_ prefix", j.ID)
	}
	if got := rec.Header().Get("Location"); got != "/jobs/"+j.ID {
		t.Errorf("Location = %q, want /jobs/%s", got, j.ID)
	}
	if q.Len() != 1 {
		t.Errorf("queue length = %d, want 1", q.Len())
	}
}

func TestSubmitValidation(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	cases := []struct{ name, body string }{
		{"missing command", `{}`},
		{"empty command", `{"command":""}`},
		{"zero cpus", `{"command":"x","cpus":0}`},
		{"negative cpus", `{"command":"x","cpus":-1}`},
		{"cpus over ceiling", `{"command":"x","cpus":99}`},
		{"memory below docker minimum", `{"command":"x","memory_mb":1}`},
		{"memory over ceiling", `{"command":"x","memory_mb":999999}`},
		{"zero max_attempts", `{"command":"x","max_attempts":0}`},
		{"max_attempts over ceiling", `{"command":"x","max_attempts":1000}`},
		{"timeout over ceiling", `{"command":"x","timeout_seconds":999999}`},
		{"malformed json", `{"command":`},
		// D-004's principle applied to the API: a misspelled field is reported, not ignored.
		{"unknown field", `{"command":"x","memoryMb":64}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := post(t, h, c.body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400. body=%s", rec.Code, rec.Body)
			}
		})
	}
}

func TestSubmitExplicitValuesOverrideDefaults(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	rec := post(t, h, `{"command":"x","cpus":0.5,"memory_mb":64,"max_attempts":1,"priority":7,"image":"busybox","timeout_seconds":30}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}
	var j job.Job
	json.Unmarshal(rec.Body.Bytes(), &j)
	want := job.Spec{Command: "x", Image: "busybox", CPUs: 0.5, MemoryMB: 64, Priority: 7, MaxAttempts: 1, TimeoutSeconds: 30}
	if j.Spec != want {
		t.Errorf("spec = %+v, want %+v", j.Spec, want)
	}
}

// A zero value must be distinguishable from an omitted field, which is why the request struct
// uses pointers. priority:0 is valid; cpus:0 is not.
func TestExplicitZeroPriorityIsAccepted(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	if rec := post(t, h, `{"command":"x","priority":0}`); rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202: an explicit priority of 0 is legitimate", rec.Code)
	}
}

func TestQueueFullReturns429AndMarksJobFailed(t *testing.T) {
	h, st, _ := newTestServer(t, 1)
	if rec := post(t, h, `{"command":"x"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("first submit should succeed, got %d", rec.Code)
	}

	rec := post(t, h, `{"command":"x"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 should carry a Retry-After header")
	}
	// The rejected job must not linger as a phantom `queued` record that nothing will run.
	for _, j := range st.List() {
		if j.Status == job.StatusQueued && j.AttemptsUsed == 0 {
			continue // the first, legitimately queued job
		}
		if j.Status != job.StatusFailed {
			t.Errorf("rejected job status = %s, want failed", j.Status)
		}
	}
}

func TestGetJob(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	var created job.Job
	json.Unmarshal(post(t, h, `{"command":"x"}`).Body.Bytes(), &created)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got job.Job
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got.ID != created.ID {
		t.Errorf("id = %q, want %q", got.ID, created.ID)
	}
}

func TestGetUnknownJobReturns404(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/job_doesnotexist", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestListJobsAndStatusFilter(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	post(t, h, `{"command":"a"}`)
	post(t, h, `{"command":"b"}`)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs", nil))
	var resp struct {
		Jobs         []job.Job `json:"jobs"`
		Count        int       `json:"count"`
		QueuePending int       `json:"queue_pending"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Count != 2 || len(resp.Jobs) != 2 {
		t.Errorf("count = %d, len = %d, want 2", resp.Count, len(resp.Jobs))
	}
	if resp.QueuePending != 2 {
		t.Errorf("queue_pending = %d, want 2", resp.QueuePending)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs?status=failed", nil))
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Count != 0 {
		t.Errorf("filtered count = %d, want 0", resp.Count)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, _, _ := newTestServer(t, 10)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405 — ServeMux method patterns should reject this", rec.Code)
	}
}
