package executor

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/LaithAlz/jobsched/internal/metrics"
)

// Label keys applied to every Job and Pod this scheduler creates. See NOTES.md D-054.
const (
	labelJobID     = "jobsched.io/job-id"
	labelAttempt   = "jobsched.io/attempt"
	labelManagedBy = "app.kubernetes.io/managed-by"
	managedByValue = "jobsched"

	// containerName is fixed: one container per Pod (D-050), so it never needs to vary.
	containerName = "job"
)

// Kubernetes runs each attempt as a batch/v1 Job containing exactly one Pod.
//
// The retry/backoff policy stays in the worker pool — every Job is created with backoffLimit 0
// so the Job controller never retries on our behalf. See NOTES.md D-051.
type Kubernetes struct {
	client       kubernetes.Interface
	namespace    string
	ttlSeconds   int32
	logTailBytes int
	mx           *metrics.Metrics

	// active counts Jobs currently in flight, read at scrape time (D-038).
	active atomic.Int64

	// owned is the set of Job names this process is responsible for. The reconciler treats
	// any labelled Job absent from this set as an orphan, so entries are added BEFORE the
	// Create call and removed only after deletion — registering intent before acting is what
	// makes the sweep safe to run concurrently with dispatch (D-071).
	mu    sync.Mutex
	owned map[string]struct{}
}

type KubernetesOptions struct {
	// KubeconfigPath overrides the default lookup (KUBECONFIG, then ~/.kube/config).
	KubeconfigPath string
	// Context selects a kubeconfig context, e.g. "kind-jobsched".
	Context      string
	Namespace    string
	TTLSeconds   int32
	LogTailBytes int
	// QPS/Burst bound client-go's request rate. Zero leaves client-go's defaults of 5/10 in
	// place, which caps sustained dispatch at roughly 1.25 jobs/s (D-070).
	QPS   float32
	Burst int
}

// NewKubernetes builds a client from a kubeconfig, exactly as kubectl does.
//
// The scheduler runs outside the cluster and authenticates as an ordinary API client, which is
// what lets Stage 3 avoid ServiceAccounts and RBAC entirely. See NOTES.md D-049.
func NewKubernetes(opts KubernetesOptions, mx *metrics.Metrics) (*Kubernetes, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if opts.KubeconfigPath != "" {
		rules.ExplicitPath = opts.KubeconfigPath
	}
	overrides := &clientcmd.ConfigOverrides{}
	if opts.Context != "" {
		overrides.CurrentContext = opts.Context
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w (is a cluster running? try: kind create cluster --config deploy/kind/cluster.yaml)", err)
	}

	// Explicit rate limits. Leaving these zero is NOT "unlimited" — client-go substitutes
	// DefaultQPS=5 and DefaultBurst=10, a client-side token bucket that silently queues
	// requests before they reach the network. At ~4 API calls per attempt that caps dispatch
	// at ~1.25 jobs/s regardless of worker count. See NOTES.md D-070.
	if opts.QPS > 0 {
		cfg.QPS = opts.QPS
		cfg.Burst = opts.Burst
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}

	ns := opts.Namespace
	if ns == "" {
		ns = "jobsched"
	}
	ttl := opts.TTLSeconds
	if ttl <= 0 {
		ttl = 300
	}
	return &Kubernetes{
		client: client, namespace: ns, ttlSeconds: ttl,
		logTailBytes: opts.LogTailBytes, mx: mx,
		owned: make(map[string]struct{}),
	}, nil
}

// claim records that this process owns a Job name, so the reconciler will not reclaim it.
func (k *Kubernetes) claim(name string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	// Initialised lazily so a zero-valued Kubernetes is usable. Reads and deletes on a nil map
	// are already safe; only writes panic, and this is the sole write site.
	if k.owned == nil {
		k.owned = make(map[string]struct{})
	}
	k.owned[name] = struct{}{}
}

func (k *Kubernetes) release(name string) {
	k.mu.Lock()
	delete(k.owned, name)
	k.mu.Unlock()
}

func (k *Kubernetes) ownsJob(name string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	_, ok := k.owned[name]
	return ok
}

// ReconcileOrphans deletes every labelled Job this process does not own.
//
// At startup the owned set is empty, so the first sweep reclaims everything left behind by a
// previous process — the same code path serves startup and steady state (D-071).
//
// This assumes a single scheduler owns the namespace. Two schedulers sharing one would each
// treat the other's Jobs as orphans; give each its own namespace, or see D-071 for the fix.
func (k *Kubernetes) ReconcileOrphans(ctx context.Context) (int, error) {
	list, err := k.client.BatchV1().Jobs(k.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelManagedBy + "=" + managedByValue,
	})
	if err != nil {
		k.mx.K8sAPIErrors.WithLabelValues(metrics.OpListJobs).Inc()
		return 0, fmt.Errorf("list jobs for reconciliation: %w", err)
	}

	reclaimed := 0
	for _, j := range list.Items {
		if k.ownsJob(j.Name) {
			continue
		}
		// TTL only starts once a Job finishes, so a Job still running when its scheduler died
		// would hold Guaranteed-QoS capacity until its activeDeadlineSeconds expired. This is
		// the gap the sweep exists to close.
		policy := metav1.DeletePropagationBackground
		err := k.client.BatchV1().Jobs(k.namespace).Delete(ctx, j.Name, metav1.DeleteOptions{
			PropagationPolicy: &policy,
		})
		if err != nil && !apierrors.IsNotFound(err) {
			k.mx.K8sAPIErrors.WithLabelValues(metrics.OpDeleteJob).Inc()
			continue
		}
		reclaimed++
		k.mx.K8sOrphansReclaimed.Inc()
	}
	return reclaimed, nil
}

// Ping verifies the API server is reachable and ensures the namespace exists, so a broken
// cluster fails at startup rather than once per job.
func (k *Kubernetes) Ping(ctx context.Context) error {
	if _, err := k.client.Discovery().ServerVersion(); err != nil {
		return fmt.Errorf("kubernetes api unreachable: %w", err)
	}
	return k.ensureNamespace(ctx)
}

// ensureNamespace creates the namespace if absent. AlreadyExists is success, not an error —
// this must be safe to run on every start and from concurrent schedulers (D-055).
func (k *Kubernetes) ensureNamespace(ctx context.Context) error {
	_, err := k.client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   k.namespace,
			Labels: map[string]string{labelManagedBy: managedByValue},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %q: %w", k.namespace, err)
	}
	return nil
}

func (k *Kubernetes) Close() error { return nil }

// ActiveJobs reports how many Kubernetes Jobs are currently in flight.
func (k *Kubernetes) ActiveJobs() int { return int(k.active.Load()) }

// Namespace exposes the target namespace for tests and diagnostics.
func (k *Kubernetes) Namespace() string { return k.namespace }

func (k *Kubernetes) Run(ctx context.Context, req Request) (Result, error) {
	runCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	name := jobName(req.JobID, req.Attempt)
	selector := fmt.Sprintf("%s=%s,%s=%d", labelJobID, req.JobID, labelAttempt, req.Attempt)

	spec := k.buildJob(name, req)

	k.active.Add(1)
	defer k.active.Add(-1)

	// Claimed before Create so a concurrent sweep can never see this Job unowned (D-071).
	k.claim(name)
	defer k.release(name)

	created := time.Now()
	if _, err := k.client.BatchV1().Jobs(k.namespace).Create(runCtx, spec, metav1.CreateOptions{}); err != nil {
		k.mx.K8sAPIErrors.WithLabelValues(metrics.OpCreateJob).Inc()
		return Result{}, fmt.Errorf("create kubernetes job %s: %w", name, err)
	}
	k.mx.K8sJobsCreated.Inc()

	// Cleanup on every path, on a context detached from cancellation — deleting after a
	// timeout is exactly when runCtx is already dead (same reasoning as D-029).
	defer k.deleteJob(ctx, name)

	pod, terminated, err := k.waitForTermination(runCtx, selector, created)
	if err != nil {
		// The attempt deadline expiring is not automatically a job timeout: the Pod may never
		// have been scheduled at all. Diagnose before attributing blame (D-056).
		if runCtx.Err() != nil && ctx.Err() == nil {
			return k.diagnoseTimeout(ctx, name, selector, req)
		}
		return Result{}, err
	}

	res := Result{
		ExitCode:  int(terminated.ExitCode),
		OOMKilled: terminated.Reason == "OOMKilled",
		RunID:     name,
	}
	// Logs are read before the deferred delete removes the Pod — the D-026 ordering lesson,
	// which applies identically here.
	res.Logs = k.podLogs(ctx, pod.Name)

	k.mx.K8sPodTerminations.WithLabelValues(terminationReason(terminated)).Inc()
	return res, nil
}

func (k *Kubernetes) buildJob(name string, req Request) *batchv1.Job {
	labels := map[string]string{
		// Label VALUES may contain underscores even though object NAMES may not, which is why
		// the untransformed job ID survives here (D-054).
		labelJobID:     req.JobID,
		labelAttempt:   fmt.Sprint(req.Attempt),
		labelManagedBy: managedByValue,
	}

	// Server-side deadline sits above the client deadline so the client's own timeout normally
	// fires first and produces the more precise error; this is the backstop (D-058).
	serverDeadline := int64(req.Timeout.Seconds()) + 30

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: batchv1.JobSpec{
			// backoffLimit 0 is the whole of D-051: Kubernetes must never retry on our behalf,
			// because the retry policy belongs to the scheduler and must behave identically
			// whichever executor is running.
			BackoffLimit:            ptr(int32(0)),
			Completions:             ptr(int32(1)),
			Parallelism:             ptr(int32(1)),
			TTLSecondsAfterFinished: ptr(k.ttlSeconds),
			ActiveDeadlineSeconds:   ptr(serverDeadline),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// Never, not OnFailure: an in-place container restart would reuse the Pod
					// and make the terminated state racy to read (D-052).
					RestartPolicy: corev1.RestartPolicyNever,
					// Job pods have no reason to talk to the Kubernetes API, and not mounting
					// a token means a compromised job cannot use one (D-059).
					AutomountServiceAccountToken: ptr(false),
					Containers: []corev1.Container{{
						Name:    containerName,
						Image:   req.Image,
						Command: []string{"/bin/sh", "-c", req.Command},
						// requests == limits gives Guaranteed QoS, so the job gets exactly what
						// it asked for regardless of what else is on the node (D-053).
						Resources: corev1.ResourceRequirements{
							Requests: resourceList(req),
							Limits:   resourceList(req),
						},
						ImagePullPolicy: corev1.PullIfNotPresent,
					}},
				},
			},
		},
	}
}

// resourceList converts the scheduler's units into Kubernetes quantities.
//
// Note Mi (2^20) rather than M (10^6): using M would silently make every memory limit 4.8%
// smaller than requested and break the enforcement assertions.
func resourceList(req Request) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(int64(req.CPUs*1000), resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(req.MemoryMB*1024*1024, resource.BinarySI),
	}
}

// waitForTermination watches the attempt's Pod until its container terminates.
//
// A watch is used rather than polling because the exit code and the OOMKilled reason both live
// on the Pod, and a watch reports the transition when it happens rather than quantising the
// measured duration to a poll interval (D-056).
func (k *Kubernetes) waitForTermination(ctx context.Context, selector string, created time.Time) (*corev1.Pod, *corev1.ContainerStateTerminated, error) {
	scheduled := false

	for {
		w, err := k.client.CoreV1().Pods(k.namespace).Watch(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			k.mx.K8sAPIErrors.WithLabelValues(metrics.OpWatchPods).Inc()
			return nil, nil, fmt.Errorf("watch pods: %w", err)
		}

		for event := range w.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			// Scheduling latency is a Kubernetes-specific concept with no Docker equivalent:
			// the gap between "the object exists" and "a node accepted it".
			if !scheduled && pod.Status.Phase != corev1.PodPending {
				scheduled = true
				k.mx.K8sPodScheduleLatency.Observe(time.Since(created).Seconds())
			}

			for _, cs := range pod.Status.ContainerStatuses {
				if cs.Name == containerName && cs.State.Terminated != nil {
					w.Stop()
					return pod, cs.State.Terminated, nil
				}
			}
		}
		w.Stop()

		// The API server may close a watch at any time; that is normal, not an error, so
		// re-establish unless the caller is done waiting.
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
}

// diagnoseTimeout works out why an attempt ran out of time, distinguishing "the command ran too
// long" from "the Pod never started". Reporting the latter as a timeout would be misleading:
// the command never ran at all, which is an infrastructure error, not a job failure (D-024).
func (k *Kubernetes) diagnoseTimeout(ctx context.Context, name, selector string, req Request) (Result, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	// A Job that hit its server-side deadline has its Pod deleted, so check the Job first.
	if j, err := k.client.BatchV1().Jobs(k.namespace).Get(readCtx, name, metav1.GetOptions{}); err == nil {
		for _, c := range j.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Reason == "DeadlineExceeded" {
				k.mx.K8sPodTerminations.WithLabelValues("DeadlineExceeded").Inc()
				return Result{ExitCode: 137, TimedOut: true, RunID: name}, nil
			}
		}
	}

	pods, err := k.client.CoreV1().Pods(k.namespace).List(readCtx, metav1.ListOptions{LabelSelector: selector})
	if err != nil || len(pods.Items) == 0 {
		k.mx.K8sPodTerminations.WithLabelValues("NoPod").Inc()
		return Result{}, fmt.Errorf("job %s produced no pod within %s", name, req.Timeout)
	}

	pod := pods.Items[0]
	if pod.Status.Phase == corev1.PodPending {
		// Unschedulable, ImagePullBackOff, etc. The command never ran, so this is an
		// infrastructure error rather than a job failure.
		k.mx.K8sPodTerminations.WithLabelValues("Pending").Inc()
		return Result{}, fmt.Errorf("pod %s never started (phase=%s): %s",
			pod.Name, pod.Status.Phase, pendingReason(&pod))
	}

	// The Pod did run; it just did not finish in time.
	k.mx.K8sPodTerminations.WithLabelValues("Timeout").Inc()
	res := Result{ExitCode: 137, TimedOut: true, RunID: name}
	res.Logs = k.podLogs(ctx, pod.Name)
	return res, nil
}

// pendingReason extracts a human-readable explanation for a Pod stuck in Pending.
func pendingReason(pod *corev1.Pod) string {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
			return fmt.Sprintf("%s: %s", c.Reason, c.Message)
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			return fmt.Sprintf("%s: %s", cs.State.Waiting.Reason, cs.State.Waiting.Message)
		}
	}
	return "no scheduling condition reported"
}

// podLogs returns the tail of the pod's output, bounded the same way as the Docker executor.
func (k *Kubernetes) podLogs(ctx context.Context, podName string) string {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()

	rc, err := k.client.CoreV1().Pods(k.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		TailLines: ptr(int64(200)),
	}).Stream(readCtx)
	if err != nil {
		k.mx.K8sAPIErrors.WithLabelValues(metrics.OpPodLogs).Inc()
		return ""
	}
	defer rc.Close()

	// Unlike Docker, the kubelet returns stdout and stderr already merged as plain text, so
	// there is no stream demultiplexing to do here.
	b, err := io.ReadAll(io.LimitReader(rc, 1<<20))
	if err != nil && len(b) == 0 {
		return ""
	}
	return tail(b, k.logTailBytes)
}

// deleteJob removes the Job and, through its ownerReference, the Pod it created.
func (k *Kubernetes) deleteJob(parent context.Context, name string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 20*time.Second)
	defer cancel()

	policy := metav1.DeletePropagationBackground
	err := k.client.BatchV1().Jobs(k.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &policy,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		// Not fatal: ttlSecondsAfterFinished is the server-side backstop for exactly this
		// case, so a failed delete costs cleanup latency rather than a permanent leak (D-057).
		k.mx.K8sAPIErrors.WithLabelValues(metrics.OpDeleteJob).Inc()
	}
}

func terminationReason(t *corev1.ContainerStateTerminated) string {
	if t.Reason != "" {
		return t.Reason
	}
	if t.ExitCode == 0 {
		return "Completed"
	}
	return "Error"
}

// jobName derives a DNS-1123-legal object name from a job ID.
//
// Job IDs look like "job_a3f2..." and underscores are NOT valid in Kubernetes object names, so
// the prefix is stripped rather than passed through (D-054).
func jobName(jobID string, attempt int) string {
	base := strings.ToLower(strings.TrimPrefix(jobID, "job_"))
	base = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, base)

	name := fmt.Sprintf("jobsched-%s-%d", base, attempt)
	// Names are capped at 63 characters. Truncating defensively rather than trusting the ID
	// format to stay short forever.
	if len(name) > 63 {
		name = name[:63]
		name = strings.TrimRight(name, "-")
	}
	return name
}

// ptr returns a pointer to v. The Kubernetes API uses pointer fields throughout to distinguish
// "unset" from "set to the zero value" — the same problem the submit API solves the same way.
func ptr[T any](v T) *T { return &v }
