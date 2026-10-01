// Package metrics owns the Prometheus instrumentation: metric definitions, the registry they
// live in, and the /metrics handler.
//
// The design rules are in NOTES.md: counters for events and scrape-time gauges for levels
// (D-036, D-038), per-worker utilisation as busy-seconds (D-037), and no unbounded labels
// (D-041).
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/LaithAlz/jobsched/internal/job"
)

const namespace = "jobsched"

// Kubernetes API operations the executor instruments. Declared here rather than as string
// literals at the call sites so the executor and the zero-initialisation below cannot drift.
const (
	OpCreateJob = "create_job"
	OpWatchPods = "watch_pods"
	OpPodLogs   = "pod_logs"
	OpDeleteJob = "delete_job"
	OpListJobs  = "list_jobs"
)

var k8sAPIOps = []string{OpCreateJob, OpWatchPods, OpPodLogs, OpDeleteJob, OpListJobs}

// durationBuckets brackets the range actually observed in Stage 1: container startup puts a
// floor near 250ms, and job timeouts reach into minutes. See NOTES.md D-043.
var durationBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// Metrics holds every instrument. Callers hold this concrete type rather than an interface —
// see NOTES.md D-042.
type Metrics struct {
	registry *prometheus.Registry

	// Event counters.
	JobsSubmitted       prometheus.Counter
	JobsCompleted       *prometheus.CounterVec // status: succeeded|failed
	JobAttempts         *prometheus.CounterVec // outcome: succeeded|failed|error
	JobRetries          prometheus.Counter
	SubmissionsRejected *prometheus.CounterVec // reason: invalid|queue_full

	// Distributions.
	AttemptDuration *prometheus.HistogramVec // outcome
	QueueWait       prometheus.Histogram

	// Per-worker utilisation, as accumulated busy time (D-037).
	WorkerBusySeconds *prometheus.CounterVec // worker: "0".."N-1"

	// Kubernetes-specific instruments. Zero unless the Kubernetes executor is in use, which
	// is why they are separate metrics rather than labels on the existing ones (D-060).
	K8sJobsCreated        prometheus.Counter
	K8sPodTerminations    *prometheus.CounterVec // reason: Completed|Error|OOMKilled|...
	K8sAPIErrors          *prometheus.CounterVec // op: create_job|watch_pods|pod_logs|delete_job
	K8sPodScheduleLatency prometheus.Histogram
	K8sOrphansReclaimed   prometheus.Counter

	// DispatchOverhead is the scheduler's own cost per attempt, excluding everything the
	// platform does. Needs its own microsecond buckets: durationBuckets starts at 100ms, so
	// every observation would land in the first bucket and measure nothing (D-072).
	DispatchOverhead prometheus.Histogram
}

// New builds an isolated registry and registers every instrument on it.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	// Re-added by hand because we are not using the default registry (D-039). go_goroutines
	// in particular is close to a correctness check for the worker pool and retry timers.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: reg,

		JobsSubmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_submitted_total",
			Help: "Jobs accepted onto the queue.",
		}),
		JobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_completed_total",
			Help: "Jobs that reached a terminal state, by final status.",
		}, []string{"status"}),
		JobAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "job_attempts_total",
			Help: "Individual execution attempts. 'error' means the job never ran (D-024).",
		}, []string{"outcome"}),
		JobRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "job_retries_total",
			Help: "Retries scheduled after a failed attempt.",
		}),
		SubmissionsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "submissions_rejected_total",
			Help: "Submissions refused before reaching the queue.",
		}, []string{"reason"}),

		AttemptDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "attempt_duration_seconds",
			Help:    "Wall-clock duration of a single execution attempt.",
			Buckets: durationBuckets,
		}, []string{"outcome"}),
		QueueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "queue_wait_seconds",
			Help:    "Time from submission to the start of the first attempt.",
			Buckets: durationBuckets,
		}),

		WorkerBusySeconds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "worker_busy_seconds_total",
			Help: "Seconds each worker spent executing. rate() of this is utilisation in [0,1].",
		}, []string{"worker"}),

		K8sJobsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "k8s_jobs_created_total",
			Help: "batch/v1 Job objects created. One per attempt (D-050).",
		}),
		K8sPodTerminations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "k8s_pod_terminations_total",
			Help: "Job pods by how they ended, as reported by the kubelet.",
		}, []string{"reason"}),
		K8sAPIErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "k8s_api_errors_total",
			Help: "Failed Kubernetes API calls, by operation.",
		}, []string{"op"}),
		K8sPodScheduleLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "k8s_pod_schedule_latency_seconds",
			Help: "Job creation until its pod leaves Pending. Has no Docker equivalent: it is " +
				"the time the cluster took to place the pod on a node.",
			Buckets: durationBuckets,
		}),
		K8sOrphansReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "k8s_orphans_reclaimed_total",
			Help: "Orphaned Jobs deleted by the reconciler. Non-zero after startup means the " +
				"previous process exited uncleanly (D-071).",
		}),
		DispatchOverhead: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "dispatch_overhead_seconds",
			Help: "Scheduler-side cost between dequeuing a job and issuing the executor call.",
			Buckets: []float64{
				0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
				0.001, 0.0025, 0.005, 0.01, 0.05,
			},
		}),
	}

	reg.MustRegister(
		m.JobsSubmitted, m.JobsCompleted, m.JobAttempts, m.JobRetries,
		m.SubmissionsRejected, m.AttemptDuration, m.QueueWait, m.WorkerBusySeconds,
		m.K8sJobsCreated, m.K8sPodTerminations, m.K8sAPIErrors, m.K8sPodScheduleLatency,
		m.K8sOrphansReclaimed, m.DispatchOverhead,
	)

	// Initialise every known label value to zero. A CounterVec that has never been observed
	// emits no series at all, so an error panel would read "No data" while the system is
	// perfectly healthy — indistinguishable from the metric being missing or broken. Same
	// reasoning as the status collector emitting explicit zeros below.
	for _, op := range k8sAPIOps {
		m.K8sAPIErrors.WithLabelValues(op)
	}
	return m
}

// Sources are the live values read on every scrape rather than tracked incrementally, so they
// cannot drift out of sync with reality. See NOTES.md D-038.
type Sources struct {
	QueueDepth   func() int
	StatusCounts func() map[job.Status]int
	WorkersBusy  func() int
	WorkersTotal int
	// K8sActiveJobs is nil when the Docker executor is in use.
	K8sActiveJobs func() int
}

// RegisterSources wires the scrape-time gauges. Called once at startup, after the queue, store
// and worker pool exist.
func (m *Metrics) RegisterSources(s Sources) {
	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Name: "queue_depth",
		Help: "Jobs currently waiting in the queue.",
	}, func() float64 { return float64(s.QueueDepth()) }))

	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Name: "workers_busy",
		Help: "Workers currently executing a job.",
	}, func() float64 { return float64(s.WorkersBusy()) }))

	m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Name: "workers_total",
		Help: "Configured size of the worker pool.",
	}, func() float64 { return float64(s.WorkersTotal) }))

	if s.K8sActiveJobs != nil {
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "k8s_active_jobs",
			Help: "batch/v1 Jobs currently in flight.",
		}, func() float64 { return float64(s.K8sActiveJobs()) }))
	}

	// One gauge per status rather than one GaugeFunc per status, because all five come from a
	// single pass over the store. statusGauge is a small custom Collector for that.
	m.registry.MustRegister(&statusCollector{counts: s.StatusCounts})
}

// Handler serves the metrics endpoint for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the registry for tests that want to gather and assert on values.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ObserveAttempt records one finished attempt.
func (m *Metrics) ObserveAttempt(outcome string, seconds float64) {
	m.JobAttempts.WithLabelValues(outcome).Inc()
	m.AttemptDuration.WithLabelValues(outcome).Observe(seconds)
}

// WorkerLabel is the label value for worker N. Kept in one place so the pool and any test
// agree on the format.
func WorkerLabel(id int) string { return strconv.Itoa(id) }

// statusCollector reports jobsched_jobs_by_status{status="..."} from one pass over the store.
//
// Implementing prometheus.Collector directly (rather than five GaugeFuncs) means the store is
// walked once per scrape instead of five times.
type statusCollector struct {
	counts func() map[job.Status]int
}

var jobsByStatusDesc = prometheus.NewDesc(
	namespace+"_jobs_by_status",
	"Jobs currently in each lifecycle state.",
	[]string{"status"}, nil,
)

func (c *statusCollector) Describe(ch chan<- *prometheus.Desc) { ch <- jobsByStatusDesc }

func (c *statusCollector) Collect(ch chan<- prometheus.Metric) {
	counts := c.counts()
	// Emit every status explicitly, including zeros. Without this, a status with no jobs
	// would vanish from the output entirely and its dashboard line would show a gap rather
	// than a zero.
	for _, s := range []job.Status{
		job.StatusQueued, job.StatusRunning, job.StatusRetrying,
		job.StatusSucceeded, job.StatusFailed,
	} {
		ch <- prometheus.MustNewConstMetric(
			jobsByStatusDesc, prometheus.GaugeValue, float64(counts[s]), string(s),
		)
	}
}
