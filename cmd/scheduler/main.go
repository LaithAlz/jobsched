// Command scheduler is the job scheduler service: an HTTP API in front of a bounded priority
// queue, drained by a pool of workers that run each job in an isolated Docker container.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LaithAlz/jobsched/internal/api"
	"github.com/LaithAlz/jobsched/internal/config"
	"github.com/LaithAlz/jobsched/internal/executor"
	"github.com/LaithAlz/jobsched/internal/metrics"
	"github.com/LaithAlz/jobsched/internal/queue"
	"github.com/LaithAlz/jobsched/internal/store"
	"github.com/LaithAlz/jobsched/internal/worker"
)

func main() {
	// main() does wiring and nothing else. Every decision it depends on lives in a package
	// that can be tested without starting a process.
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	// D-004: log the fully-resolved config so "what is this process running with" is always
	// answerable from the logs.
	log.Info("configuration loaded",
		"addr", cfg.Addr,
		"workers", cfg.Workers,
		"queue_capacity", cfg.QueueCapacity,
		"default_image", cfg.DefaultImage,
		"default_max_attempts", cfg.DefaultMaxAttempts,
		"backoff_base", cfg.BackoffBase,
		"backoff_max", cfg.BackoffMax,
		"executor", cfg.Executor,
		"job_network", cfg.JobNetwork)

	mx := metrics.New()

	// The executor is chosen at startup, not at build time: one binary contains both, and the
	// Executor interface (D-002, D-009) is what makes the swap a single switch statement
	// rather than a diff across the program. See NOTES.md D-061.
	exec, k8sActive, err := buildExecutor(cfg, mx, log)
	if err != nil {
		return err
	}
	defer exec.Close()

	// Drives the background reconciler; cancelled on shutdown.
	ctxReconcile, stopReconcile := context.WithCancel(context.Background())
	defer stopReconcile()

	st := store.NewMemory()
	q := queue.New(cfg.QueueCapacity)

	pool := worker.NewPool(worker.Config{
		Workers:     cfg.Workers,
		BackoffBase: cfg.BackoffBase,
		BackoffMax:  cfg.BackoffMax,
	}, q, st, exec, mx, log)
	pool.Start()

	// Scrape-time gauges are registered last, because they close over the queue, store and
	// pool. Reading the live values on each scrape is what makes them impossible to drift
	// (D-038).
	mx.RegisterSources(metrics.Sources{
		QueueDepth:    q.Len,
		StatusCounts:  st.Counts,
		WorkersBusy:   pool.BusyWorkers,
		WorkersTotal:  cfg.Workers,
		K8sActiveJobs: k8sActive, // nil for the Docker executor, so the gauge is not registered
	})

	// Periodic orphan sweep, for Jobs whose delete failed or that outlived their attempt.
	if k8s, ok := exec.(*executor.Kubernetes); ok && cfg.K8sReconcileInterval > 0 {
		go func() {
			t := time.NewTicker(cfg.K8sReconcileInterval)
			defer t.Stop()
			for {
				select {
				case <-ctxReconcile.Done():
					return
				case <-t.C:
					n, err := k8s.ReconcileOrphans(ctxReconcile)
					if err != nil {
						log.Warn("reconciliation sweep failed", "error", err)
					} else if n > 0 {
						log.Warn("reclaimed orphaned jobs", "count", n)
					}
				}
			}
		}()
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.NewServer(cfg, st, q, mx, log).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// signal.NotifyContext gives a context that is cancelled on SIGINT/SIGTERM, which turns
	// signal handling into the same cancellation primitive everything else already uses.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The HTTP server runs on its own goroutine so main can wait on the signal context.
	// A buffered channel of size 1 means this send never blocks even if nobody is reading
	// (which is the case once shutdown wins the race below).
	serverErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// Shutdown order matters: stop accepting new jobs first, then drain the workers.
	// Doing it the other way round would let a request enqueue work into a pool that has
	// already stopped, leaving a job permanently queued.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown did not complete cleanly", "error", err)
	}
	if err := pool.Shutdown(shutdownCtx); err != nil {
		log.Warn("worker pool did not drain within grace period", "error", err)
	}
	log.Info("shutdown complete")
	return nil
}

// buildExecutor constructs the configured backend and verifies it is reachable before any jobs
// are accepted, so a misconfigured environment fails at startup rather than once per job.
//
// The second return value is a live count of in-flight Kubernetes Jobs, or nil for Docker.
func buildExecutor(cfg config.Config, mx *metrics.Metrics, log *slog.Logger) (executor.Executor, func() int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch cfg.Executor {
	case "docker":
		ex, err := executor.NewDocker(cfg.JobNetwork, cfg.LogTailBytes)
		if err != nil {
			return nil, nil, err
		}
		if err := ex.Ping(ctx); err != nil {
			return nil, nil, err
		}
		log.Info("executor ready", "executor", "docker", "job_network", cfg.JobNetwork)
		return ex, nil, nil

	case "kubernetes":
		ex, err := executor.NewKubernetes(executor.KubernetesOptions{
			KubeconfigPath: cfg.Kubeconfig,
			Context:        cfg.K8sContext,
			Namespace:      cfg.K8sNamespace,
			TTLSeconds:     int32(cfg.K8sJobTTLSecs),
			LogTailBytes:   cfg.LogTailBytes,
			QPS:            float32(cfg.K8sQPS),
			Burst:          cfg.K8sBurst,
		}, mx)
		if err != nil {
			return nil, nil, err
		}
		// Ping also creates the namespace if it is missing (D-055).
		if err := ex.Ping(ctx); err != nil {
			return nil, nil, err
		}
		// Startup sweep: this process owns nothing yet, so anything still labelled in the
		// namespace was left by a previous one (D-071).
		if n, err := ex.ReconcileOrphans(ctx); err != nil {
			log.Warn("startup reconciliation failed", "error", err)
		} else if n > 0 {
			log.Warn("reclaimed orphaned jobs from a previous process", "count", n)
		}

		log.Info("executor ready", "executor", "kubernetes",
			"namespace", ex.Namespace(), "context", cfg.K8sContext,
			"job_ttl_seconds", cfg.K8sJobTTLSecs,
			"client_qps", cfg.K8sQPS, "client_burst", cfg.K8sBurst,
			"reconcile_interval", cfg.K8sReconcileInterval)
		return ex, ex.ActiveJobs, nil
	}
	// Unreachable: config.Load validates the value.
	return nil, nil, fmt.Errorf("unknown executor %q", cfg.Executor)
}
