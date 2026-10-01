// Package config loads all runtime tunables from the environment exactly once, at startup.
//
// Every value has a documented default. A malformed value is a startup error, never a silent
// fallback to the default — see NOTES.md D-004.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Addr is the HTTP listen address.
	Addr string
	// Workers is the number of concurrent worker goroutines, and therefore the maximum
	// number of containers running at once.
	Workers int
	// QueueCapacity bounds the number of pending jobs. Submissions past it get 429.
	QueueCapacity int

	// Defaults applied to a submitted job when it omits a field.
	DefaultImage       string
	DefaultCPUs        float64
	DefaultMemoryMB    int64
	DefaultMaxAttempts int
	DefaultTimeout     time.Duration

	// Admission ceilings. A submission asking for more is rejected with 400.
	MaxCPUs       float64
	MaxMemoryMB   int64
	MaxAttempts   int
	MaxJobTimeout time.Duration

	// Retry backoff: delay before attempt N+1 is BackoffBase * 2^(N-1), capped at BackoffMax.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// Executor selects the backend: "docker" (Stage 1) or "kubernetes" (Stage 3). One
	// binary contains both; the choice is made at startup. See NOTES.md D-061.
	Executor string

	// Kubernetes executor settings, ignored when Executor is "docker".
	K8sNamespace  string
	K8sContext    string
	Kubeconfig    string
	K8sJobTTLSecs int
	// K8sQPS/K8sBurst bound client-go's request rate. Left unset, client-go applies
	// DefaultQPS=5 / DefaultBurst=10, which caps dispatch at ~1.25 jobs/s. See NOTES.md D-070.
	K8sQPS   float64
	K8sBurst int
	// K8sReconcileInterval is how often orphaned Jobs are swept (D-071).
	K8sReconcileInterval time.Duration

	// JobNetwork is the Docker network mode for job containers ("none" isolates them).
	JobNetwork string
	// LogTailBytes caps how much container output is retained per attempt.
	LogTailBytes int
	// ShutdownTimeout bounds graceful shutdown before in-flight work is abandoned.
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	c := Config{
		Addr:                 ":8080",
		Workers:              4,
		QueueCapacity:        256,
		DefaultImage:         "alpine:3.20",
		DefaultCPUs:          1.0,
		DefaultMemoryMB:      256,
		DefaultMaxAttempts:   4,
		DefaultTimeout:       5 * time.Minute,
		MaxCPUs:              4.0,
		MaxMemoryMB:          2048,
		MaxAttempts:          10,
		MaxJobTimeout:        30 * time.Minute,
		BackoffBase:          time.Second,
		BackoffMax:           60 * time.Second,
		Executor:             "docker",
		K8sNamespace:         "jobsched",
		K8sJobTTLSecs:        300,
		K8sQPS:               100,
		K8sBurst:             200,
		K8sReconcileInterval: 60 * time.Second,
		JobNetwork:           "none",
		LogTailBytes:         4096,
		ShutdownTimeout:      30 * time.Second,
	}

	var err error
	get := func(key string, target any) {
		if err != nil {
			return
		}
		if e := parseInto(key, target); e != nil {
			err = e
		}
	}

	get("SCHED_ADDR", &c.Addr)
	get("SCHED_WORKERS", &c.Workers)
	get("SCHED_QUEUE_CAPACITY", &c.QueueCapacity)
	get("SCHED_DEFAULT_IMAGE", &c.DefaultImage)
	get("SCHED_DEFAULT_CPUS", &c.DefaultCPUs)
	get("SCHED_DEFAULT_MEMORY_MB", &c.DefaultMemoryMB)
	get("SCHED_DEFAULT_MAX_ATTEMPTS", &c.DefaultMaxAttempts)
	get("SCHED_DEFAULT_TIMEOUT", &c.DefaultTimeout)
	get("SCHED_MAX_CPUS", &c.MaxCPUs)
	get("SCHED_MAX_MEMORY_MB", &c.MaxMemoryMB)
	get("SCHED_MAX_ATTEMPTS", &c.MaxAttempts)
	get("SCHED_MAX_JOB_TIMEOUT", &c.MaxJobTimeout)
	get("SCHED_BACKOFF_BASE", &c.BackoffBase)
	get("SCHED_BACKOFF_MAX", &c.BackoffMax)
	get("SCHED_EXECUTOR", &c.Executor)
	get("SCHED_K8S_NAMESPACE", &c.K8sNamespace)
	get("SCHED_K8S_CONTEXT", &c.K8sContext)
	get("SCHED_KUBECONFIG", &c.Kubeconfig)
	get("SCHED_K8S_JOB_TTL_SECONDS", &c.K8sJobTTLSecs)
	get("SCHED_K8S_QPS", &c.K8sQPS)
	get("SCHED_K8S_BURST", &c.K8sBurst)
	get("SCHED_K8S_RECONCILE_INTERVAL", &c.K8sReconcileInterval)
	get("SCHED_JOB_NETWORK", &c.JobNetwork)
	get("SCHED_LOG_TAIL_BYTES", &c.LogTailBytes)
	get("SCHED_SHUTDOWN_TIMEOUT", &c.ShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	if c.Workers < 1 {
		return Config{}, fmt.Errorf("SCHED_WORKERS must be >= 1, got %d", c.Workers)
	}
	if c.QueueCapacity < 1 {
		return Config{}, fmt.Errorf("SCHED_QUEUE_CAPACITY must be >= 1, got %d", c.QueueCapacity)
	}
	if c.BackoffBase <= 0 {
		return Config{}, fmt.Errorf("SCHED_BACKOFF_BASE must be > 0, got %s", c.BackoffBase)
	}
	if c.Executor != "docker" && c.Executor != "kubernetes" {
		return Config{}, fmt.Errorf("SCHED_EXECUTOR must be \"docker\" or \"kubernetes\", got %q", c.Executor)
	}
	return c, nil
}

// parseInto reads one environment variable into a typed target, leaving the target untouched
// if the variable is unset or empty. A present-but-malformed value is an error.
func parseInto(key string, target any) error {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return nil
	}
	switch t := target.(type) {
	case *string:
		*t = raw
	case *int:
		v, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("%s: %q is not an integer", key, raw)
		}
		*t = v
	case *int64:
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%s: %q is not an integer", key, raw)
		}
		*t = v
	case *float64:
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("%s: %q is not a number", key, raw)
		}
		*t = v
	case *time.Duration:
		v, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("%s: %q is not a duration (want e.g. 1s, 500ms, 5m)", key, raw)
		}
		*t = v
	default:
		return fmt.Errorf("%s: unsupported config target type %T", key, target)
	}
	return nil
}
