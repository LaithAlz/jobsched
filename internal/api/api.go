// Package api exposes the HTTP surface: submit a job, read a job, list jobs.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/LaithAlz/jobsched/internal/config"
	"github.com/LaithAlz/jobsched/internal/job"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
)

// maxListLimit caps GET /jobs?limit= so a client cannot ask for an unbounded response.
const maxListLimit = 500

type Server struct {
	cfg   config.Config
	store store.Store
	queue *queue.Queue
	mx    *metrics.Metrics
	log   *slog.Logger
}

func NewServer(cfg config.Config, st store.Store, q *queue.Queue, mx *metrics.Metrics, log *slog.Logger) *Server {
	return &Server{cfg: cfg, store: st, queue: q, mx: mx, log: log}
}

// Routes builds the HTTP handler.
//
// Go 1.22+ ServeMux patterns carry the method and path parameters directly, which is why no
// third-party router is needed here (NOTES.md D-003).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.submitJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /healthz", s.health)
	// {$} matches the root path exactly, so this does not swallow every unrouted path.
	mux.HandleFunc("GET /{$}", s.index)
	// D-040: metrics share the API's port and mux.
	mux.Handle("GET /metrics", s.mx.Handler())
	return recoverPanic(s.log, mux)
}

// submitRequest is the POST /jobs body. Pointers mark optional fields so "absent" can be
// distinguished from "explicitly zero" — without that, `"cpus": 0` and an omitted `cpus` look
// identical and the server cannot tell "use the default" from an invalid explicit request.
type submitRequest struct {
	Command        string   `json:"command"`
	Image          *string  `json:"image"`
	CPUs           *float64 `json:"cpus"`
	MemoryMB       *int64   `json:"memory_mb"`
	Priority       *int     `json:"priority"`
	MaxAttempts    *int     `json:"max_attempts"`
	TimeoutSeconds *int     `json:"timeout_seconds"`
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	// Reject unknown fields: a client that misspells "memory_mb" as "memoryMb" should be
	// told, not silently given the default and a job that ignores its limit.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.mx.SubmissionsRejected.WithLabelValues("invalid").Inc()
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	spec, err := s.buildSpec(req)
	if err != nil {
		s.mx.SubmissionsRejected.WithLabelValues("invalid").Inc()
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	id, err := newJobID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not generate job id")
		return
	}

	j := job.Job{
		ID:        id,
		Spec:      spec,
		Status:    job.StatusQueued,
		CreatedAt: time.Now(),
		Attempts:  []job.Attempt{},
	}
	if err := s.store.Create(j); err != nil {
		writeError(w, http.StatusInternalServerError, "could not store job")
		return
	}

	// Enqueue after storing, never before: a worker that pops the ID must be able to find
	// the record (D-015).
	if err := s.queue.Submit(j.ID, spec.Priority); err != nil {
		if errors.Is(err, queue.ErrFull) {
			// Mark it failed rather than leaving a phantom `queued` job that nothing will
			// ever run.
			_, _ = s.store.Update(j.ID, func(u *job.Job) {
				now := time.Now()
				u.Status = job.StatusFailed
				u.LastError = "rejected: queue is full"
				u.FinishedAt = &now
			})
			s.mx.SubmissionsRejected.WithLabelValues("queue_full").Inc()
			w.Header().Set("Retry-After", "5")
			writeError(w, http.StatusTooManyRequests,
				fmt.Sprintf("queue is full (capacity %d), retry shortly", s.cfg.QueueCapacity))
			return
		}
		writeError(w, http.StatusServiceUnavailable, "scheduler is shutting down")
		return
	}

	s.mx.JobsSubmitted.Inc()
	s.log.Info("job accepted", "job_id", j.ID, "command", spec.Command,
		"cpus", spec.CPUs, "memory_mb", spec.MemoryMB, "max_attempts", spec.MaxAttempts)

	// 202, not 201: the job is accepted for later execution, not completed (NOTES.md D-007).
	w.Header().Set("Location", "/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

// buildSpec applies defaults to omitted fields and enforces the admission ceilings (D-020).
func (s *Server) buildSpec(req submitRequest) (job.Spec, error) {
	spec := job.Spec{
		Command:        req.Command,
		Image:          s.cfg.DefaultImage,
		CPUs:           s.cfg.DefaultCPUs,
		MemoryMB:       s.cfg.DefaultMemoryMB,
		MaxAttempts:    s.cfg.DefaultMaxAttempts,
		TimeoutSeconds: int(s.cfg.DefaultTimeout.Seconds()),
	}
	if req.Image != nil {
		spec.Image = *req.Image
	}
	if req.CPUs != nil {
		spec.CPUs = *req.CPUs
	}
	if req.MemoryMB != nil {
		spec.MemoryMB = *req.MemoryMB
	}
	if req.Priority != nil {
		spec.Priority = *req.Priority
	}
	if req.MaxAttempts != nil {
		spec.MaxAttempts = *req.MaxAttempts
	}
	if req.TimeoutSeconds != nil {
		spec.TimeoutSeconds = *req.TimeoutSeconds
	}

	switch {
	case spec.Command == "":
		return job.Spec{}, errors.New("command is required")
	case spec.Image == "":
		return job.Spec{}, errors.New("image must not be empty")
	case spec.CPUs <= 0:
		return job.Spec{}, fmt.Errorf("cpus must be > 0, got %g", spec.CPUs)
	case spec.CPUs > s.cfg.MaxCPUs:
		return job.Spec{}, fmt.Errorf("cpus must be <= %g, got %g", s.cfg.MaxCPUs, spec.CPUs)
	// Docker rejects memory limits below 6 MB, so catch it here with a clearer message
	// than the daemon's.
	case spec.MemoryMB < 6:
		return job.Spec{}, fmt.Errorf("memory_mb must be >= 6 (Docker's minimum), got %d", spec.MemoryMB)
	case spec.MemoryMB > s.cfg.MaxMemoryMB:
		return job.Spec{}, fmt.Errorf("memory_mb must be <= %d, got %d", s.cfg.MaxMemoryMB, spec.MemoryMB)
	case spec.MaxAttempts < 1:
		return job.Spec{}, fmt.Errorf("max_attempts must be >= 1 (1 means run once, never retry), got %d", spec.MaxAttempts)
	case spec.MaxAttempts > s.cfg.MaxAttempts:
		return job.Spec{}, fmt.Errorf("max_attempts must be <= %d, got %d", s.cfg.MaxAttempts, spec.MaxAttempts)
	case spec.TimeoutSeconds < 1:
		return job.Spec{}, fmt.Errorf("timeout_seconds must be >= 1, got %d", spec.TimeoutSeconds)
	case time.Duration(spec.TimeoutSeconds)*time.Second > s.cfg.MaxJobTimeout:
		return job.Spec{}, fmt.Errorf("timeout_seconds must be <= %d, got %d",
			int(s.cfg.MaxJobTimeout.Seconds()), spec.TimeoutSeconds)
	}
	return spec, nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs := s.store.List()
	if status := r.URL.Query().Get("status"); status != "" {
		filtered := jobs[:0]
		for _, j := range jobs {
			if string(j.Status) == status {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}
	total := len(jobs)

	// D-068: without a limit, a scheduler holding a few hundred jobs serialises every attempt
	// of every job — including captured log tails — on every poll.
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("limit must be a positive integer, got %q", raw))
			return
		}
		if n > maxListLimit {
			n = maxListLimit
		}
		if n < len(jobs) {
			jobs = jobs[:n] // List() is already newest-first
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"jobs": jobs,
		// count is the number returned; total is how many matched before limiting.
		"count":         len(jobs),
		"total":         total,
		"queue_pending": s.queue.Len(),
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		// D-068: reporting the executor lets the UI show which backend is running rather
		// than asserting it in a caption. Same page, same API, either backend.
		"executor":       s.cfg.Executor,
		"workers":        s.cfg.Workers,
		"queue_capacity": s.cfg.QueueCapacity,
		"queue_pending":  s.queue.Len(),
		"backoff_base":   s.cfg.BackoffBase.String(),
		"max_attempts":   s.cfg.MaxAttempts,
		"default_image":  s.cfg.DefaultImage,
	})
}

func newJobID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "job_" + hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// recoverPanic keeps one broken handler from killing the whole process. A panic in a
// goroutine that nothing recovers takes down the entire Go program, and every HTTP request is
// served on its own goroutine — so without this, a nil map write in a handler stops the
// scheduler and abandons every running job.
func recoverPanic(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic in handler", "path", r.URL.Path, "panic", rec)
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
