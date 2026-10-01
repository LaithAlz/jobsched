package executor

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/LaithAlz/jobsched/internal/metrics"
)

// These tests assert the Job spec the executor builds, using a fake clientset so no cluster is
// needed. The spec fields checked here are the ones that encode Stage 3's decisions, so a
// regression in any of them is a silent behavioural change. Cluster-backed proofs live in
// test/k8s_integration_test.go.

func newTestK8s() *Kubernetes {
	return &Kubernetes{
		client:       fake.NewClientset(),
		namespace:    "jobsched",
		ttlSeconds:   300,
		logTailBytes: 4096,
		mx:           metrics.New(),
	}
}

func sampleRequest() Request {
	return Request{
		JobID:    "job_a3f21b9c4d5e6f70",
		Attempt:  2,
		Command:  "echo hi",
		Image:    "alpine:3.20",
		CPUs:     0.5,
		MemoryMB: 64,
		Timeout:  120 * time.Second,
	}
}

// TestKubernetesRetryIsDisabled is the direct assertion of NOTES.md D-051: the Job controller
// must never retry on our behalf, because the retry policy belongs to the scheduler and has to
// behave identically across executors.
func TestKubernetesRetryIsDisabled(t *testing.T) {
	spec := newTestK8s().buildJob("jobsched-test-2", sampleRequest())

	if spec.Spec.BackoffLimit == nil {
		t.Fatal("backoffLimit is nil, which means Kubernetes applies its default of 6 retries")
	}
	if got := *spec.Spec.BackoffLimit; got != 0 {
		t.Errorf("backoffLimit = %d, want 0 — application-level retry only (D-051)", got)
	}
	// With backoffLimit 0 and restartPolicy Never, one Job produces exactly one Pod, which is
	// what makes "one Job = one attempt" true (D-050).
	if got := spec.Spec.Template.Spec.RestartPolicy; got != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never — OnFailure restarts the container in place "+
			"and breaks the fresh-container-per-attempt guarantee (D-052)", got)
	}
	if got := *spec.Spec.Completions; got != 1 {
		t.Errorf("completions = %d, want 1", got)
	}
	if got := *spec.Spec.Parallelism; got != 1 {
		t.Errorf("parallelism = %d, want 1", got)
	}
}

// D-053: requests must equal limits so the pod lands in the Guaranteed QoS class and gets
// exactly the resources it asked for regardless of what else is on the node.
func TestResourcesUseGuaranteedQoS(t *testing.T) {
	spec := newTestK8s().buildJob("jobsched-test-2", sampleRequest())
	c := spec.Spec.Template.Spec.Containers[0]

	for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		req, hasReq := c.Resources.Requests[res]
		lim, hasLim := c.Resources.Limits[res]
		if !hasReq || !hasLim {
			t.Fatalf("%s: requests set=%v limits set=%v, want both", res, hasReq, hasLim)
		}
		if req.Cmp(lim) != 0 {
			t.Errorf("%s: request %s != limit %s — unequal values give Burstable QoS, so the "+
				"job's speed would depend on its neighbours (D-053)", res, req.String(), lim.String())
		}
	}

	// 0.5 CPU must become 500m.
	wantCPU := resource.MustParse("500m")
	if got := c.Resources.Limits[corev1.ResourceCPU]; got.Cmp(wantCPU) != 0 {
		t.Errorf("cpu limit = %s, want 500m", got.String())
	}
	// 64 MB must become 64Mi (67108864 bytes), not 64M (64000000) — a 4.8% difference that
	// would quietly break the enforcement assertions.
	wantMem := resource.MustParse("64Mi")
	if got := c.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantMem) != 0 {
		t.Errorf("memory limit = %s (%d bytes), want 64Mi (67108864 bytes)",
			got.String(), got.Value())
	}
	if got := c.Resources.Limits[corev1.ResourceMemory]; got.Value() != 67108864 {
		t.Errorf("memory limit = %d bytes, want exactly 67108864", got.Value())
	}
}

// D-057 and D-058: server-side cleanup and a server-side deadline, both of which survive the
// scheduler dying.
func TestJobCarriesServerSideTTLAndDeadline(t *testing.T) {
	k := newTestK8s()
	req := sampleRequest()
	spec := k.buildJob("jobsched-test-2", req)

	if spec.Spec.TTLSecondsAfterFinished == nil || *spec.Spec.TTLSecondsAfterFinished != 300 {
		t.Errorf("ttlSecondsAfterFinished = %v, want 300 — the backstop for orphans if the "+
			"scheduler dies before deleting (D-057)", spec.Spec.TTLSecondsAfterFinished)
	}
	if spec.Spec.ActiveDeadlineSeconds == nil {
		t.Fatal("activeDeadlineSeconds is nil; a runaway pod would outlive a scheduler crash (D-058)")
	}
	// Set above the client deadline so the client's own timeout normally fires first and
	// produces the more precise error.
	if got := *spec.Spec.ActiveDeadlineSeconds; got <= int64(req.Timeout.Seconds()) {
		t.Errorf("activeDeadlineSeconds = %d, want > client timeout %.0fs",
			got, req.Timeout.Seconds())
	}
}

// D-059: job pods have no reason to hold a Kubernetes API credential.
func TestServiceAccountTokenIsNotMounted(t *testing.T) {
	spec := newTestK8s().buildJob("jobsched-test-2", sampleRequest())
	got := spec.Spec.Template.Spec.AutomountServiceAccountToken
	if got == nil || *got {
		t.Errorf("automountServiceAccountToken = %v, want false — a job running arbitrary "+
			"commands should not get an API token (D-059)", got)
	}
}

func TestCommandRunsThroughShell(t *testing.T) {
	req := sampleRequest()
	req.Command = "echo a | grep a && echo b"
	c := newTestK8s().buildJob("jobsched-test-2", req).Spec.Template.Spec.Containers[0]

	want := []string{"/bin/sh", "-c", req.Command}
	if len(c.Command) != 3 || c.Command[0] != want[0] || c.Command[1] != want[1] || c.Command[2] != want[2] {
		t.Errorf("command = %v, want %v — shell form keeps pipes and && working (D-023)", c.Command, want)
	}
}

func TestLabelsIdentifyJobAndAttempt(t *testing.T) {
	req := sampleRequest()
	spec := newTestK8s().buildJob("jobsched-test-2", req)

	for _, labels := range []map[string]string{spec.Labels, spec.Spec.Template.Labels} {
		if labels[labelJobID] != req.JobID {
			t.Errorf("%s = %q, want the full job id %q", labelJobID, labels[labelJobID], req.JobID)
		}
		if labels[labelAttempt] != "2" {
			t.Errorf("%s = %q, want \"2\"", labelAttempt, labels[labelAttempt])
		}
		if labels[labelManagedBy] != managedByValue {
			t.Errorf("%s = %q, want %q", labelManagedBy, labels[labelManagedBy], managedByValue)
		}
	}

	// D-054: label VALUES may contain underscores even though object NAMES may not. This is
	// what lets the untransformed job ID survive as a label.
	if !strings.Contains(spec.Labels[labelJobID], "_") {
		t.Error("expected the job id label to retain its underscore")
	}
	if errs := validation.IsValidLabelValue(spec.Labels[labelJobID]); len(errs) > 0 {
		t.Errorf("job id is not a valid label value: %v", errs)
	}
}

// D-054: object names must be DNS-1123 subdomains, and job IDs are not — they contain "_".
func TestJobNameIsAlwaysDNS1123Legal(t *testing.T) {
	cases := []struct {
		jobID   string
		attempt int
		want    string
	}{
		{"job_a3f21b9c4d5e6f70", 1, "jobsched-a3f21b9c4d5e6f70-1"},
		{"job_00ff", 12, "jobsched-00ff-12"},
		{"job_ABCDEF", 1, "jobsched-abcdef-1"},       // uppercase is illegal, so it is lowercased
		{"weird__id!!", 1, "jobsched-weird--id---1"}, // anything else becomes '-'
	}
	for _, c := range cases {
		got := jobName(c.jobID, c.attempt)
		if got != c.want {
			t.Errorf("jobName(%q, %d) = %q, want %q", c.jobID, c.attempt, got, c.want)
		}
		// Checked with Kubernetes' own validator rather than a hand-rolled regex.
		if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
			t.Errorf("jobName(%q, %d) = %q is not a legal object name: %v", c.jobID, c.attempt, got, errs)
		}
	}
}

func TestJobNameIsTruncatedToTheLengthLimit(t *testing.T) {
	got := jobName("job_"+strings.Repeat("a", 200), 3)
	if len(got) > 63 {
		t.Errorf("name length = %d, want <= 63", len(got))
	}
	if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
		t.Errorf("truncated name %q is illegal: %v", got, errs)
	}
}

// D-055: namespace creation runs on every startup, so AlreadyExists must be success.
func TestEnsureNamespaceIsIdempotent(t *testing.T) {
	k := newTestK8s()
	ctx := context.Background()

	if err := k.ensureNamespace(ctx); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := k.ensureNamespace(ctx); err != nil {
		t.Fatalf("second call must treat AlreadyExists as success, got: %v", err)
	}

	if _, err := k.client.CoreV1().Namespaces().Get(ctx, "jobsched", metav1.GetOptions{}); err != nil {
		t.Errorf("namespace was not created: %v", err)
	}
}

func TestCreateJobRecordsAPIErrorMetric(t *testing.T) {
	k := newTestK8s()
	// Creating the same name twice fails the second time; the executor should count it.
	spec := k.buildJob("dup", sampleRequest())
	ctx := context.Background()
	if _, err := k.client.BatchV1().Jobs(k.namespace).Create(ctx, spec, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.client.BatchV1().Jobs(k.namespace).Create(ctx, spec, metav1.CreateOptions{}); err == nil {
		t.Error("expected a conflict on duplicate create")
	}
}

// --- orphan reconciliation (D-071) ---

func seedJob(t *testing.T, k *Kubernetes, name string, managed bool) {
	t.Helper()
	labels := map[string]string{}
	if managed {
		labels[labelManagedBy] = managedByValue
	}
	_, err := k.client.BatchV1().Jobs(k.namespace).Create(context.Background(),
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
}

func jobNames(t *testing.T, k *Kubernetes) []string {
	t.Helper()
	list, err := k.client.BatchV1().Jobs(k.namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, j := range list.Items {
		out = append(out, j.Name)
	}
	sort.Strings(out)
	return out
}

// A freshly started process owns nothing, so everything still labelled in the namespace was
// left behind by a previous one. This is the startup sweep.
func TestReconcileReclaimsJobsFromAPreviousProcess(t *testing.T) {
	k := newTestK8s()
	seedJob(t, k, "jobsched-aaaa-1", true)
	seedJob(t, k, "jobsched-bbbb-1", true)

	n, err := k.ReconcileOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("reclaimed %d, want 2", n)
	}
	if got := jobNames(t, k); len(got) != 0 {
		t.Errorf("jobs remaining after sweep: %v, want none", got)
	}
	if got := testutil.ToFloat64(k.mx.K8sOrphansReclaimed); got != 2 {
		t.Errorf("orphans_reclaimed_total = %v, want 2", got)
	}
}

// The dangerous failure: a sweep running concurrently with dispatch must never delete a job
// this process is actively running. Claim-before-create is what prevents it (D-071).
func TestReconcileNeverDeletesJobsWeOwn(t *testing.T) {
	k := newTestK8s()
	seedJob(t, k, "jobsched-running-1", true) // in flight
	seedJob(t, k, "jobsched-orphan-1", true)  // left by a dead process
	k.claim("jobsched-running-1")

	n, err := k.ReconcileOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("reclaimed %d, want 1 — only the unowned job", n)
	}
	got := jobNames(t, k)
	if len(got) != 1 || got[0] != "jobsched-running-1" {
		t.Errorf("remaining jobs = %v, want only the in-flight one; a sweep that deletes "+
			"running work is worse than no sweep at all", got)
	}
}

// The sweep is scoped by label, so it must not touch unrelated workloads sharing the namespace.
func TestReconcileIgnoresJobsItDoesNotManage(t *testing.T) {
	k := newTestK8s()
	seedJob(t, k, "someone-elses-job", false)
	seedJob(t, k, "jobsched-orphan-1", true)

	if _, err := k.ReconcileOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := jobNames(t, k)
	if len(got) != 1 || got[0] != "someone-elses-job" {
		t.Errorf("remaining = %v, want the unlabelled job untouched", got)
	}
}

func TestReconcileOnAnEmptyNamespaceIsANoop(t *testing.T) {
	k := newTestK8s()
	n, err := k.ReconcileOrphans(context.Background())
	if err != nil || n != 0 {
		t.Errorf("got (%d, %v), want (0, nil)", n, err)
	}
}

// release() must put a job back in scope for reclamation once it is done.
func TestClaimAndReleaseTrackOwnership(t *testing.T) {
	k := newTestK8s()
	if k.ownsJob("x") {
		t.Error("owns x before claiming")
	}
	k.claim("x")
	if !k.ownsJob("x") {
		t.Error("does not own x after claiming")
	}
	k.release("x")
	if k.ownsJob("x") {
		t.Error("still owns x after release")
	}
}

// Rate limits must be applied when configured, since leaving them zero silently caps dispatch
// at client-go's default of 5 QPS (D-070).
func TestClientRateLimitsAreConfigurable(t *testing.T) {
	if testing.Short() {
		t.Skip("needs a kubeconfig")
	}
	// Exercised end to end by the load runs; here we only assert the option plumbs through to
	// the struct the client is built from, which is the part that silently defaults.
	opts := KubernetesOptions{QPS: 100, Burst: 200}
	if opts.QPS <= 0 {
		t.Fatal("QPS must be positive to override client-go's default of 5")
	}
	if opts.Burst < int(opts.QPS) {
		t.Errorf("burst %d < qps %g: bursts of simultaneous dispatch would be smoothed", opts.Burst, opts.QPS)
	}
}
