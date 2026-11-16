# jobsched: a distributed job scheduler

A Go service that accepts jobs (a shell command plus CPU/memory limits), queues them by priority,
runs each in an isolated Docker container with kernel-enforced resource limits, and retries failures
with exponential backoff.

**Read [NOTES.md](./NOTES.md) first.** It is the decision log: every architectural choice, the
alternatives considered, and why each was chosen. The code implements those decisions.

All three stages are complete and tested: the core job runner, Prometheus/Grafana observability, and
Kubernetes execution via client-go against a local kind cluster.

**See it running:** `make stack-up`, then open <http://localhost:8080> for the control UI and
<http://localhost:3000> for the Grafana dashboard.

## What the measurements showed

I expected the scheduler to be the bottleneck. It was not.

- The scheduler's own cost is **38 µs per job** (p50). Starting a pod takes about **12 s**.
- My first suspect was client-go's default rate limit. Raising it removed the throttling warnings and did not change throughput.
- The control experiment: the same workload through Kubernetes' own Job controller, with this scheduler out of the path, ran at the same rate. The ceiling is pod creation on the node, not the scheduling code.

The full numbers are in [Measured performance](#measured-performance).

---

## Prerequisites

| What | Version used here | Needed for | Install |
|---|---|---|---|
| Go | 1.26.7 | everything | `brew install go` |
| Docker | 27.3.1 | Stages 1–2, and kind itself | Docker Desktop |
| kubectl | 1.30.2 | Stage 3 | `brew install kubectl` |
| kind | 0.32.0 | Stage 3 | `brew install kind` |

Go must be 1.22+ for the `ServeMux` routing patterns (D-003). Docker must be running: check with
`docker info`. kubectl and kind are only needed for Stage 3.

The scheduler finds the daemon socket automatically, including Docker Desktop's
`~/.docker/run/docker.sock` (NOTES.md D-032). If you use a remote or non-default Docker context, set
`DOCKER_HOST` explicitly — it takes precedence.

---

## Quickstart

```bash
make build          # compile to bin/scheduler
make run            # start on :8080
```

In another terminal:

```bash
# Submit a job. Returns 202 immediately with a job ID — execution is asynchronous (D-007).
curl -s localhost:8080/jobs -d '{"command":"echo hello","cpus":0.5,"memory_mb":64}'

# Check status. Poll this until status is "succeeded" or "failed".
curl -s localhost:8080/jobs/job_<id> | jq

# List everything, optionally filtered.
curl -s 'localhost:8080/jobs?status=failed' | jq
```

Watch retry and backoff happen live:

```bash
curl -s localhost:8080/jobs -d '{"command":"exit 1","max_attempts":4,"memory_mb":64}'
```

The scheduler log shows four attempts separated by 1s, 2s and 4s, then a permanent failure.

---

## Demo UI

The scheduler serves a single-page control panel at `/`. It is compiled into the binary
(`go:embed`), so there is nothing to install, build, or serve separately — `make run` and open
<http://localhost:8080>.

It exists for one reason: **Grafana shows aggregates, and this shows an individual job's retry
behaviour as a picture.** Clicking a job draws its attempts as bars on a shared linear time axis,
with each backoff span hatched and labelled. Because the axis is linear and shared, each wait is
visibly twice the width of the one before it — the exponential backoff is seen rather than inferred
from timestamps (D-067).

Six preset buttons dispatch the behaviours worth demonstrating:

| Preset | What it shows |
|---|---|
| `succeeds` | baseline: one attempt, exit 0, stdout captured |
| `retry ×4` | the timeline staircase — 1s, 2s, 4s measured backoff |
| `oom kill` | 64 MB limit, `tail /dev/zero` → killed by the kernel, `oom_killed` flag set |
| `cgroup proof` | prints `cpu.max` and `memory.max` as the kernel sees them |
| `timeout` | `sleep 300` under a 5s deadline |
| `rejected` | asks for 99 CPUs → `400` naming the ceiling, never queued (D-020) |

The header reads the active backend from `/healthz`, so it shows **DOCKER** or **KUBERNETES**
depending on which executor is running — the same page, API and behaviour on either (D-068).
A job's timeline is linkable: the URL carries `#job_<id>`.

`make test-ui` verifies the timeline's geometry — that consecutive backoff spans really are drawn at
2.00× the previous width. A miscomputed scale would still render and still look plausible, so it is
checked rather than eyeballed.

---

## API

### `POST /jobs` → `202 Accepted`

Only `command` is required; everything else falls back to a server default.

| Field | Type | Default | Notes |
|---|---|---|---|
| `command` | string | *required* | Run via `/bin/sh -c`, so pipes and `&&` work (D-023) |
| `image` | string | `alpine:3.20` | Must contain `/bin/sh` |
| `cpus` | number | `1.0` | Fractional CPU limit, as `docker run --cpus` |
| `memory_mb` | integer | `256` | Hard limit; exceeding it means an OOM kill (D-025) |
| `priority` | integer | `0` | Higher runs first; ties are FIFO |
| `max_attempts` | integer | `4` | **Total** tries, not retries. `1` = never retry (D-021) |
| `timeout_seconds` | integer | `300` | Per-attempt wall-clock limit |

Requests exceeding the configured ceilings are rejected with `400` and a message naming the limit —
never silently clamped (D-020). Unknown fields are rejected, so a typo like `memoryMb` is reported
rather than ignored.

Returns `429` with `Retry-After` when the queue is full.

### `GET /jobs/{id}` → `200` / `404`

Returns the full record, including a per-attempt history with timestamps, exit codes, OOM flags and
captured log tails — this is what makes retry behaviour externally observable (D-014).

```json
{
  "id": "job_a3f2...",
  "status": "failed",
  "attempts_used": 4,
  "exit_code": 1,
  "last_error": "command exited with status 1",
  "attempts": [
    {"number": 1, "started_at": "...", "finished_at": "...", "exit_code": 1, "logs": "..."}
  ]
}
```

`status` is one of `queued`, `running`, `retrying`, `succeeded`, `failed`. While `retrying`, the
record carries `next_attempt_at` (D-011).

### `GET /jobs` → `200`

All jobs, newest first. Supports `?status=<status>` and `?limit=<n>` (capped at 500). Returns
`count` (returned) alongside `total` (matched before limiting) and `queue_pending`.

### `GET /healthz` → `200`

Reports `executor` (`docker` or `kubernetes`), `workers`, `queue_pending`, and the configured
backoff base — enough for a client to describe the running scheduler without being told.

### `GET /` → the demo UI

---

## Configuration

Every value is an environment variable with a default. A malformed value is a **startup error**, not
a silent fallback (D-004). The resolved configuration is logged at boot.

| Variable | Default | Purpose |
|---|---|---|
| `SCHED_ADDR` | `:8080` | HTTP listen address |
| `SCHED_WORKERS` | `4` | Worker goroutines = max concurrent containers |
| `SCHED_QUEUE_CAPACITY` | `256` | Pending-job bound; past it, submissions get 429 |
| `SCHED_BACKOFF_BASE` | `1s` | First retry delay; doubles each attempt |
| `SCHED_BACKOFF_MAX` | `60s` | Backoff ceiling |
| `SCHED_DEFAULT_IMAGE` | `alpine:3.20` | Image when a job omits one |
| `SCHED_DEFAULT_CPUS` | `1.0` | |
| `SCHED_DEFAULT_MEMORY_MB` | `256` | |
| `SCHED_DEFAULT_MAX_ATTEMPTS` | `4` | |
| `SCHED_DEFAULT_TIMEOUT` | `5m` | |
| `SCHED_MAX_CPUS` | `4.0` | Admission ceiling |
| `SCHED_MAX_MEMORY_MB` | `2048` | Admission ceiling |
| `SCHED_MAX_ATTEMPTS` | `10` | Admission ceiling |
| `SCHED_MAX_JOB_TIMEOUT` | `30m` | Admission ceiling |
| `SCHED_JOB_NETWORK` | `none` | Docker network mode; `none` isolates jobs (D-027). Docker only |
| `SCHED_LOG_TAIL_BYTES` | `4096` | Retained output per attempt |
| `SCHED_SHUTDOWN_TIMEOUT` | `30s` | Grace period for in-flight jobs |
| `SCHED_EXECUTOR` | `docker` | `docker` or `kubernetes` (D-061) |
| `SCHED_K8S_NAMESPACE` | `jobsched` | Namespace for Jobs; created at startup (D-055) |
| `SCHED_K8S_CONTEXT` | *(current)* | kubeconfig context, e.g. `kind-jobsched` |
| `SCHED_KUBECONFIG` | *(default lookup)* | Overrides `KUBECONFIG` / `~/.kube/config` |
| `SCHED_K8S_JOB_TTL_SECONDS` | `300` | `ttlSecondsAfterFinished` backstop (D-057) |
| `SCHED_K8S_QPS` | `100` | client-go request rate. **Unset means 5**, which caps dispatch (D-070) |
| `SCHED_K8S_BURST` | `200` | client-go burst allowance |
| `SCHED_K8S_RECONCILE_INTERVAL` | `60s` | Orphaned-Job sweep interval (D-071) |

---

## Tests

Three tiers, for three different questions (D-033, D-062).

```bash
make test-unit          # ~7s, no Docker. Queue, retry policy, backoff, metrics, K8s job specs.
make test-integration   # ~35s, real containers. Docker enforcement proofs.
make test-k8s           # ~60s, real kind cluster. Kubernetes enforcement proofs.
make test-ui            # <1s, needs node. Demo UI timeline geometry.
make test-all           # everything, with the race detector
```

`make test-integration` sets `SCHED_REQUIRE_DOCKER=1` and `make test-k8s` sets `SCHED_REQUIRE_K8S=1`,
so a missing backend **fails** instead of skipping. That matters: a skipped Go test reports as
`PASS`, which once made the suite look green while proving nothing.

### What the integration suite actually proves

Resource limits are verified from **kernel state**, not from wall-clock timing (D-034):

| Test | Evidence |
|---|---|
| `TestFailingJobRetriesWithExponentialBackoff` | 4 attempts, measured gaps of 1s / 2.002s / 4.001s, then permanently failed |
| `TestCgroupLimitsMatchTheRequest` | `cpu.max` = `50000 100000` for 0.5 CPU; `memory.max` = `67108864` for 64 MB |
| `TestCPULimitActuallyThrottles` | `cpu.stat` showed 28 throttling events, 2.05s of enforced idle at 0.25 CPU |
| `TestMemoryLimitIsEnforcedByOOMKill` | `OOMKilled=true`, exit 137 under a 64 MB limit |
| `TestAttemptTimeoutIsEnforced` | `sleep 300` killed 3.27s into a 3s timeout |
| `TestJobContainerHasNoNetwork` | no `eth0` with `NetworkMode=none` |
| `TestConcurrentJobsAllComplete` | 8 jobs, 4 workers, each ran exactly once |

Inspecting `HostConfig` would only prove what was *requested*. `nr_throttled` and `OOMKilled` are
counters the kernel and daemon write, so they can only be non-zero if the limits are real.

### What the Kubernetes suite proves

| Test | Evidence |
|---|---|
| `TestK8sRetryIsDrivenByTheApplicationNotKubernetes` | backoff of 1.001s / 2.001s (Kubernetes-native cannot go below 10s), 3 distinct Job objects, exactly 3 pod terminations |
| `TestK8sCgroupLimitsMatchTheRequest` | `cpu.max` = `50000 100000`, `memory.max` = `67108864` — **byte-identical to the Docker executor** |
| `TestK8sCPULimitActuallyThrottles` | kernel throttled the pod 10 times at 0.25 CPU |
| `TestK8sMemoryLimitIsEnforcedByOOMKill` | kubelet reported `OOMKilled`, exit 137 |
| `TestK8sAttemptTimeoutIsEnforced` | `sleep 600` killed 10.07s into a 10s timeout |
| `TestK8sPodThatNeverStartsIsAnInfraErrorNotAJobFailure` | unpullable image → recorded as an infra `error` with no exit code, not a job failure |

The cgroup values matching Stage 1 exactly is the point: pod resource limits and Docker flags reach
the same kernel mechanism.

Spec-level assertions live in `internal/executor/kubernetes_test.go` and use a fake clientset, so
they need no cluster. They exist because removing `backoffLimit: 0` would let Kubernetes silently
retry underneath while every integration test still passed (D-062).

---

## Observability (Stage 2)

```bash
make stack-up      # scheduler + Prometheus + Grafana, one command
make stack-status  # container status and Prometheus target health
make stack-reset   # tear down and discard metric history
```

| Service | URL | Notes |
|---|---|---|
| Scheduler | <http://localhost:8080> | API and `/metrics` on the same port (D-040) |
| Prometheus | <http://localhost:9090> | Scrapes the scheduler every 5s (D-047) |
| Grafana | <http://localhost:3000> | Opens directly on the dashboard, no login (D-045) |

Everything binds to `127.0.0.1`. Give it ~30 seconds of job traffic before the rate-based panels
have anything to show.

### Metrics

Counters for events, gauges for levels, histograms for distributions (D-036).

| Metric | Type | Meaning |
|---|---|---|
| `jobsched_jobs_submitted_total` | counter | Jobs accepted onto the queue |
| `jobsched_jobs_completed_total{status}` | counter | Terminal jobs, by `succeeded`/`failed` |
| `jobsched_job_attempts_total{outcome}` | counter | Attempts, by `succeeded`/`failed`/`error` |
| `jobsched_job_retries_total` | counter | Retries scheduled after a failed attempt |
| `jobsched_submissions_rejected_total{reason}` | counter | `invalid` (400) or `queue_full` (429) |
| `jobsched_queue_depth` | gauge | Jobs waiting for a worker |
| `jobsched_jobs_by_status{status}` | gauge | Jobs in each of the five states |
| `jobsched_workers_busy` / `_total` | gauge | Pool occupancy and size |
| `jobsched_worker_busy_seconds_total{worker}` | counter | Per-worker busy time |
| `jobsched_attempt_duration_seconds` | histogram | Per-attempt wall clock |
| `jobsched_queue_wait_seconds` | histogram | Submission → first attempt |

Two of these are worth understanding, because they are not what you would write first:

- **`outcome` has three values, not two.** `failed` means the container ran and exited non-zero;
  `error` means it never ran at all — an infrastructure problem. Watching them diverge is how you
  tell "everyone's jobs are broken" from "Docker is broken" (D-024).
- **Worker utilisation is a counter of seconds, not a busy/idle gauge.** Utilisation is the query
  `rate(jobsched_worker_busy_seconds_total[1m])`, which yields a fraction in `[0,1]`. A 0/1 gauge
  would be sampled once per scrape and would miss every job shorter than the scrape interval
  (D-037).

Level metrics (`queue_depth`, `jobs_by_status`, `workers_busy`) are read from the live queue, store
and pool **at scrape time** rather than incremented at each transition, so they cannot drift out of
sync with reality (D-038).

### Dashboard

14 panels answering the three questions the brief asked, plus supporting context. Every panel
carries a description in Grafana citing the decision behind it.

- **Throughput** — `rate()` of completion counters, split by status.
- **Failure rate** — a ratio of counters computed in PromQL over a 5m window, not a stored
  percentage. Reads "No data" when nothing has completed, because 0/0 is undefined, not zero
  (D-046).
- **Worker load** — per-worker utilisation, so uneven distribution is visible rather than averaged
  away.
- Plus queue depth vs pool saturation, non-terminal job states, attempt-duration percentiles,
  attempts by outcome, retries and rejections, queue wait, and scheduler goroutines (a leak check
  for the pool and retry timers).

Editing a panel in the browser will not persist — the JSON file is the source of truth. Change
`deploy/grafana/dashboards/jobsched.json` and restart Grafana.

---

## Kubernetes execution (Stage 3)

Same scheduler, same retry policy, same API — jobs run as Kubernetes `batch/v1` Jobs instead of
Docker containers. Selected at startup with `SCHED_EXECUTOR=kubernetes`; both backends are compiled
into one binary (D-061).

### Setup

```bash
brew install kind kubectl     # if you don't have them
make cluster-up               # ~4 min on first run: it downloads a node image
make run-k8s                  # scheduler in Kubernetes mode on :8081
```

`make cluster-up` creates a single-node cluster named `jobsched` and sets the kubectl context to
`kind-jobsched`. Nothing needs to be built or side-loaded: the scheduler runs on your host as an
ordinary API client (D-049), and job images are pulled by the cluster's own nodes from the registry.

Submit a job exactly as before, on port 8081:

```bash
curl -s localhost:8081/jobs -d '{"command":"echo hi","cpus":0.5,"memory_mb":64}'

# Watch the objects appear and disappear
kubectl --context kind-jobsched get jobs,pods -n jobsched -w
```

Useful targets: `make cluster-status`, `make clean-k8s-jobs`, `make cluster-down`.

### What changes, and what deliberately does not

| | Stage 1 (Docker) | Stage 3 (Kubernetes) |
|---|---|---|
| Unit of execution | container | `batch/v1` Job → one Pod (D-050) |
| Limits | `--cpus`, `--memory` | pod `resources.requests` == `limits` (D-053) |
| Enforcement | cgroup CFS quota + `memory.max` | **identical** — same cgroups, same kernel |
| Retry | application-level | **still application-level** (D-051) |
| Cleanup | explicit `docker rm` | explicit delete **plus** `ttlSecondsAfterFinished` (D-057) |
| Network isolation | `--network=none` | **none — see the limitation below** (D-059) |

The queue, retry policy, store, worker pool and HTTP API were **not modified** to add Kubernetes
support. It was one new file implementing `executor.Executor`, one `switch` in `main.go`, and some
config. That was the bet made in D-002 before any of it was written.

### Why retry stays in the application (D-051)

The brief asks specifically about this, so briefly: Kubernetes can retry Jobs for you via
`backoffLimit`, but its backoff **starts at 10 seconds and is not configurable**. The required
1s/2s/4s/8s sequence is therefore impossible natively. Beyond that, the Job controller reports a
*count* of failures rather than a history, so the per-attempt records that make retries debuggable
would be lost, and `backoffLimit` counts Pod failures including ones the job did not cause (node
drain, eviction). So every Job is created with `backoffLimit: 0` and the scheduler creates a fresh
Job per attempt.

**What that costs:** Kubernetes-native retry is *more durable* than ours — its state lives in etcd
and survives a scheduler crash, while ours lives in an in-process timer and does not. The fix is to
make our state durable (D-008), not to hand retry to the controller.

### Limitation: job pods are not network-isolated

Stage 1 ran containers with `--network=none`. **Kubernetes has no per-Pod equivalent** — every Pod
gets a network interface. Real isolation needs a `NetworkPolicy`, and kind's default CNI does not
enforce NetworkPolicy, so shipping one would look like isolation while enforcing nothing. Job pods
in Stage 3 can therefore reach the cluster network and the internet. This is a real regression
against Stage 1 and the main reason this setup is local-development-only. Pods do at least run
without a mounted ServiceAccount token (D-059).

---

## How it fits together

```
POST /jobs ──> validate ──> store (memory) ──> priority queue (bounded)
                                                      │
                                                 worker pool
                                              (N goroutines)
                                                      │
                                              Docker executor
                                         (one container per attempt)
                                                      │
                                    ┌─────────────────┴─────────────────┐
                                 exit 0                            exit != 0
                                    │                                   │
                              succeeded              attempts left ? retrying : failed
                                                                        │
                                                          time.AfterFunc(backoff) ──> requeue
```

The pieces, and the decisions behind them:

- **`internal/queue`** — bounded priority heap with a blocking `Pop`. A channel would be idiomatic
  but is strictly FIFO, and priority is a requirement (D-017).
- **`internal/worker`** — the pool bounds concurrent containers. A failed attempt does **not** sleep
  in its worker; it is handed to a timer and re-queued, so backoff never consumes pool capacity
  (D-018).
- **`internal/executor`** — the seam Stage 3 replaces. Swapping Docker for Kubernetes should be one
  new file plus one line in `main.go` (D-002).
- **`internal/store`** — in-memory, behind an interface, returning snapshot copies so API reads
  cannot race worker writes (D-008, D-012).
- **`internal/api/ui`** — the embedded demo UI, one self-contained HTML file (D-064, D-065).
- **`internal/metrics`** — owns the Prometheus registry. Each instance has its own registry rather
  than using the global default, so two schedulers can run in one test binary (D-039).

---

## Known limitations

These are deliberate, scoped-out, or deferred — each is argued in NOTES.md rather than overlooked.

- **No durability.** A restart loses all job state, queued and running. The store is an interface, so
  a durable backend is a new implementation plus one wiring change (D-008).
- **Single process.** Stage 1 is concurrent, not distributed. Multiple replicas would need shared
  state and jitter in the backoff (D-005, D-030).
- **Job records accumulate.** Nothing evicts terminal jobs; a TTL reaper is the obvious fix (D-008).
- **No auth.** Out of scope by instruction. Note that submitting a job means running arbitrary shell
  code in a container — the container and `NetworkMode=none` are the boundary (D-023, D-027).
- **Orphaned containers after a `SIGKILL`.** Normal shutdown and timeouts clean up; a hard kill in a
  millisecond-wide window can leave one behind. `make clean-containers` sweeps them (D-026).
- **Retry policy is uniform.** Infrastructure errors and non-zero exits are distinguished in the
  record but retried identically (D-024).
- **cgroup v2 assumed** by the enforcement tests (D-034).
- **Kubernetes job pods have no network isolation** (D-059). Stage 1's `--network=none` has no Pod
  equivalent, and kind's default CNI does not enforce NetworkPolicy. This is the sharpest reason the
  Stage 3 setup is local-only.
- **The Stage 3 scheduler runs outside the cluster** with an admin kubeconfig, to avoid the RBAC that
  is out of scope. A real deployment runs in-cluster with a least-privilege Role (D-049).
- **Retry state is less durable than Kubernetes would give you.** `backoffLimit: 0` means a scheduler
  restart abandons pending retries, where the Job controller would have continued. Deliberate, and
  fixed by persisting job state rather than by delegating retry (D-051).
- **The compose stack is a local development tool, not a deployment.** The scheduler container
  bind-mounts the host Docker socket, which is equivalent to giving it root on the host, and neither
  the API nor Grafana has any authentication. Everything binds to `127.0.0.1`; do not expose it
  (D-044, D-045). Stage 3 removes the socket mount entirely.
- **The 5s scrape interval is a demo setting**, tuned so short-lived jobs are visible on a laptop.
  Raise it for anything larger (D-047).
- **Metrics scrape cost grows with total job count**, because the status tally walks the store on
  each scrape. Same root cause as the unbounded job map above (D-038).
- **Docker SDK is pinned**, including `go-connections` at v0.5.0. `go get -u ./...` will break the
  build (D-031).

---

## Measured performance

Numbers from this machine (single-node kind on Docker Desktop, 8 shared CPUs). Reproduce with
`make run-k8s` in one shell and `make load-test N=1000` in another. Full analysis in NOTES.md D-074.

| | |
|---|---|
| Sustained run | **1,000 / 1,000 jobs succeeded, 0 failed** |
| Throughput | **1.32 jobs/sec** over 765 s, 16 concurrent pods |
| **Scheduler dispatch overhead** | **38 µs p50, 750 µs p99** |
| Pod startup (platform) | 12.40 s p50, 28.30 s p95 |
| Kubernetes API errors | 0 |

The ratio is the point: the scheduler's own cost per job is ~38 µs against ~12 s of pod startup —
about **300,000× smaller**. Whatever limits this system, it is not the scheduling code.

### The bottleneck hunt, including the wrong guess

client-go defaults to **`QPS=5, Burst=10`** when left unset, which caps dispatch at roughly 1.25
jobs/sec regardless of worker count. That looked like the obvious culprit. Under load at 30 workers
the scheduler did log six `client-side throttling` waits, and raising the limit eliminated them —
**but throughput did not change** (1.84 → 1.76 jobs/sec, inside noise).

The reason is arithmetic: the limiter was adding ~1 s to a small fraction of requests, while every
job already spent 12–21 s waiting for a pod to start. The real constraint is **pod sandbox creation
on the node**. Confirmed by the failure mode at 30 workers under sustained load — 41 pods sat in
`ContainerCreating` past their attempt deadline while the node reported no memory, disk, or PID
pressure.

The fix was kept anyway: the limit is invisible, unrelated to the workload, and would bind on a
faster cluster. Knowing it is *not* the current constraint is what stops effort being spent there.

| Workers | Throughput | Pod startup p50 |
|---:|---:|---:|
| 8 | 1.04 jobs/s | 6.69 s |
| 16 | 1.52 jobs/s | 8.58 s |
| 30 | 1.82 jobs/s | 21.23 s |

Throughput rises sublinearly while latency degrades sharply — 16 concurrent pods is the practical
saturation point here; at 30, sustained load pushes pod startup past the attempt deadline.

**The control experiment.** Running the same workload through Kubernetes' *own* Job controller
(`parallelism: 16`, scheduler removed from the path) gives **1.23 pods/sec** against our **1.32
jobs/sec** — within noise of each other. The scheduler is running at the platform's ceiling, so
there is no headroom to recover in application code. At 38 µs of scheduler time per job against
~760 ms per pod, optimising the scheduler could improve throughput by at most 0.005%.

### Orphan reclamation

Kubernetes' `ttlSecondsAfterFinished` only starts counting when a Job *finishes*, so a Job still
running when its scheduler dies is never collected — it holds Guaranteed-QoS capacity until its
deadline expires. The reconciler closes that gap with a label-scoped sweep at startup and on a
ticker, keyed on a set of Jobs the process owns.

Verified by killing a process rather than by asserting: a scheduler with 8 running pods was
`SIGKILL`ed, leaving 8 orphaned Jobs in `Running`; a fresh process reclaimed all 8 at startup, and a
subsequently dispatched running job survived two sweeps untouched.

---

## If you continue

Nothing here is required; all three stages are done. The gaps that would matter most, in order:

1. **Persist job state.** One change fixes the largest limitation in the project: jobs survive a
   restart, retries become durable (closing the honest cost admitted in D-051), and the unbounded
   in-memory job map goes away. The store is already an interface (D-009).
2. **Enforce network isolation for Kubernetes jobs** — a policy-enforcing CNI in `cluster.yaml` plus
   a default-deny NetworkPolicy, with a test that proves it is enforced (D-059).
3. **Run the scheduler in-cluster** with a least-privilege Role, which is the real deployment
   topology D-049 traded away.
