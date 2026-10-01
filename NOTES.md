# NOTES.md — Architectural Decision Log

Every architectural decision made while building this distributed job scheduler, recorded at the
moment it was made. Each entry states **the decision**, **the alternatives considered**, and **why
this option won over those alternatives**.

This file is the primary artifact of the project. The code is an implementation of these decisions.

Format of each entry:

- **ID** — sequential, never reused, so entries can be cited (`see D-014`).
- **Stage** — which build stage the decision belongs to.
- **Status** — `active`, or `superseded by D-0XX` if a later stage changed it.

---

## Index

**Foundations** — [D-001](#d-001) Go via Homebrew · [D-002](#d-002) `cmd/`+`internal/` layout ·
[D-003](#d-003) stdlib `net/http` over chi · [D-004](#d-004) env-var config ·
[D-010](#d-010) *governing principle: simplest still-correct implementation*

**Concurrency model** — [D-005](#d-005) goroutine pool over worker processes ·
[D-017](#d-017) heap+`sync.Cond` over a channel · [D-018](#d-018) retries re-queued by timer, never
slept in a worker · [D-035](#d-035) shutdown ordering

**Job model & state** — [D-006](#d-006) server-generated random IDs · [D-007](#d-007) async
`202`+poll · [D-011](#d-011) five states, `retrying` explicit · [D-014](#d-014) full per-attempt
history · [D-021](#d-021) `max_attempts` = total tries · [D-023](#d-023) `/bin/sh -c`

**Storage & data races** — [D-008](#d-008) in-memory map + `RWMutex` · [D-009](#d-009) interfaces at
point of use · [D-012](#d-012) snapshot copies + write-once pointer rule · [D-015](#d-015) queue
holds IDs · [D-016](#d-016) `Update(id, func)` closure · [D-013](#d-013) JSON tags, no DTO

**Retry & failure** — [D-024](#d-024) infra error vs job exit code · [D-030](#d-030) backoff
parameters, and why no jitter · [D-020](#d-020) admission ceilings

**Docker execution** — [D-022](#d-022) fresh container per attempt · [D-025](#d-025) `MemorySwap` =
`Memory` · [D-026](#d-026) explicit removal, not `AutoRemove` · [D-027](#d-027) no network by
default · [D-028](#d-028) lazy image pull · [D-029](#d-029) context-based timeouts ·
[D-031](#d-031) pinned SDK versions · [D-032](#d-032) socket discovery · [D-019](#d-019) bounded log
tail

**Testing** — [D-033](#d-033) fake executor vs real Docker, and fail-loud ·
[D-034](#d-034) enforcement proven from kernel state

**Observability (Stage 2)** — [D-036](#d-036) counters vs gauges vs histograms ·
[D-037](#d-037) utilisation as busy-seconds · [D-038](#d-038) scrape-time level metrics ·
[D-039](#d-039) dedicated registry · [D-040](#d-040) `/metrics` on the API port ·
[D-041](#d-041) label cardinality rules · [D-042](#d-042) concrete metrics type, no interface ·
[D-043](#d-043) duration buckets

**Deployment & dashboard (Stage 2)** — [D-044](#d-044) compose runs the scheduler, socket mount and
its security cost · [D-045](#d-045) Grafana provisioned from files · [D-046](#d-046) dashboard built
on `rate()` · [D-047](#d-047) 5s scrape interval · [D-048](#d-048) dashboard queries verified
programmatically

**Kubernetes execution (Stage 3)** — [D-049](#d-049) scheduler runs outside the cluster ·
[D-050](#d-050) one Job per attempt · [D-051](#d-051) **`backoffLimit: 0` — app-level retry vs
Kubernetes-native** · [D-052](#d-052) `restartPolicy: Never` · [D-053](#d-053) requests == limits,
Guaranteed QoS · [D-054](#d-054) DNS-1123 object names · [D-055](#d-055) dedicated namespace ·
[D-056](#d-056) watch the Pod, not the Job · [D-057](#d-057) delete plus TTL ·
[D-058](#d-058) two-layer timeouts · [D-059](#d-059) no token mount, and the network-isolation gap ·
[D-060](#d-060) separate K8s metrics · [D-061](#d-061) runtime executor selection ·
[D-062](#d-062) fake clientset plus kind · [D-063](#d-063) scraping the K8s-mode scheduler

**Demo UI (Stage 4, unplanned)** — [D-064](#d-064) embedded with `go:embed` ·
[D-065](#d-065) vanilla JS, no build step · [D-066](#d-066) polling, consistent with D-007 ·
[D-067](#d-067) **the proportional attempt timeline — why backoff is drawn, not tabulated** ·
[D-068](#d-068) `?limit=` and executor identity · [D-069](#d-069) the one CDN dependency

**Throughput & reclamation (Stage 5)** — [D-070](#d-070) client-go's silent 5 QPS default ·
[D-071](#d-071) orphan reconciliation by label sweep · [D-072](#d-072) dispatch overhead measured
apart from platform latency · [D-073](#d-073) measure completion, not submission ·
[D-074](#d-074) **the measured findings — including the hypothesis that was wrong**

**If you read only one entry, read [D-051](#d-051)** — application-level retry versus Kubernetes'
native Job retry, why each mechanism works the way it does, and what choosing ours actually costs.

**Entries revisited across stages:** D-013 (DTOs — still not needed), D-018 and D-008 (durable retry
state — now the top outstanding gap), D-024 (the third failure category earned its keep in D-056),
D-026 (the orphan leak is closed server-side by D-057), D-027 (superseded in Kubernetes mode by
D-059's gap), D-044 (the socket mount is gone in Stage 3, per D-049).

---

## Stage 1 — Core Job Runner

<a id="d-001"></a>

### D-001 — Environment: install Go 1.x via Homebrew rather than a version manager or Docker-based toolchain

**Decision.** Go was not installed on this machine. Installed the current stable Go via
`brew install go`, which puts a single system-wide toolchain on `PATH`.

**Alternatives considered.**

1. **A version manager (`gvm`, `asdf`, `mise`).** Lets multiple Go versions coexist per project.
2. **Build inside a `golang:1.x` Docker container.** Zero host install; the toolchain is pinned by
   an image tag, perfectly reproducible.
3. **Download the official tarball from go.dev into `/usr/local/go`.** What the Go team documents.

**Why Homebrew won.** This project has exactly one Go component and no version-conflict pressure, so
a version manager solves a problem we don't have and adds a shell-init dependency to every future
session. Option 2 is genuinely tempting for reproducibility, but this project *drives Docker itself* —
building inside a container means either mounting the host Docker socket into the build container
(docker-out-of-docker) or nesting daemons, and that complexity would be paid on every single
`go test` run for the entire life of the project. Option 3 is fine but leaves an unmanaged directory
that nothing will ever upgrade or remove. Homebrew is already installed here, `brew upgrade go` is a
real upgrade path, and `brew uninstall go` is a real removal path.

**Cost of being wrong.** Low and reversible. The `go.mod` file pins the language version
independently of how the toolchain got installed, so switching to any of the alternatives later
changes nothing about the code.

---

<a id="d-002"></a>

### D-002 — Module layout: `cmd/` + `internal/` multi-package, not a single `main.go`

**Decision.** Lay the repo out as:

```
cmd/scheduler/main.go      process entry point, wiring only
internal/config/           configuration loading
internal/job/              the Job domain type and its state machine
internal/store/            job state storage
internal/queue/            the pending-work queue
internal/executor/         the "run this job somewhere" abstraction + Docker impl
internal/worker/           the worker pool that connects queue -> executor -> store
internal/api/              HTTP handlers
test/                      integration test harness
```

**Alternatives considered.**

1. **Single `main.go`** (or a handful of files in `package main`). Stage 1 is genuinely small —
   maybe 800 lines. A flat layout would work and would be faster to write.
2. **Flat `pkg/` with everything public.** Common in older Go repos.
3. **Group by layer across one package** (`handlers.go`, `models.go`, `services.go`) — the
   Rails/Django-shaped layout.

**Why the multi-package layout won.** The decisive factor is Stage 3, which is *already known* to
require ripping out Docker execution and replacing it with Kubernetes Job creation. If execution
logic lives in the same package as queueing and HTTP handling, that swap is a diff across the whole
program and there is no compiler-enforced guarantee that queue/retry logic didn't quietly grow a
dependency on something Docker-specific. Putting a package boundary around `executor` means the
Stage 3 change is: add one file, change one line of wiring in `main.go`. The boundary is the
architecture.

`internal/` (rather than `pkg/`) because Go's toolchain *enforces* that nothing outside this module
can import `internal/...`. That is a real, compiler-checked guarantee that these are implementation
details, and it costs nothing. Option 3 was rejected because grouping by layer means every feature
change touches every file, and it produces import cycles in Go far more readily than grouping by
domain concept.

**Cost of being wrong.** If this turns out to be over-engineering, collapsing packages later is a
mechanical, low-risk refactor. Splitting a tangled single package later is not. The asymmetry favors
splitting now.

---

<a id="d-003"></a>

### D-003 — HTTP layer: `net/http` with Go 1.22+ `ServeMux` patterns, not chi/gin/echo

**Decision.** Use the standard library's `net/http` and the enhanced `http.ServeMux` introduced in
Go 1.22, which supports method-scoped, wildcard route patterns:

```go
mux.HandleFunc("POST /jobs", h.submitJob)
mux.HandleFunc("GET /jobs/{id}", h.getJob)
```

Zero third-party HTTP dependencies.

**Alternatives considered.**

1. **`chi`** — explicitly offered in the brief. ~1k lines, stdlib-compatible (`http.Handler` in,
   `http.Handler` out), gives `chi.URLParam`, sub-routers, and a middleware stack.
2. **`gin`** — fastest to write, huge ecosystem, but introduces its own `gin.Context` type that is
   *not* `http.Handler`-compatible.
3. **`echo`** — similar tradeoffs to gin.

**Why stdlib won.** Before Go 1.22, this decision went the other way: the old `ServeMux` couldn't do
path parameters or method routing, so `GET /jobs/{id}` required either a router or hand-rolled
string splitting, and every real project reached for chi. Go 1.22 removed that reason. The API
surface here is three routes with one path parameter — precisely the case the new `ServeMux` covers.

The concrete win is not "one less dependency" in the abstract; it is that `chi`'s value is
*middleware composition* (request IDs, structured logging, panic recovery, timeouts, CORS) and this
service, by explicit scope constraint, has no authentication, no RBAC, and no multi-tenant concerns.
We need exactly one piece of middleware (panic recovery), which is ~10 lines of stdlib code.

Gin was rejected on a stronger principle: its non-`http.Handler` context type is *contagious*. Once
handlers take `*gin.Context`, every helper they call is coupled to gin, and standard `httptest`
patterns stop working cleanly. `chi` does not have this problem, which is why it was the runner-up.

**The honest tradeoff.** If this service later grows real middleware needs — per-route auth, rate
limiting, versioned sub-routers — `chi` becomes the better answer, and because chi is
`http.Handler`-native the migration is nearly mechanical (swap the mux, change `r.PathValue("id")`
to `chi.URLParam(r, "id")`). We are choosing the smaller thing now precisely *because* the escape
hatch is cheap.

**What we give up right now.** `ServeMux` returns bare `404`/`405` with no body on route misses,
where chi would let us render a JSON error consistently. Accepted: a JSON API returning an empty 404
for an unrouted path is ugly but not incorrect.

---

<a id="d-004"></a>

### D-004 — Configuration: environment variables with defaults, not flags or a config file

**Decision.** All tunables (`SCHED_ADDR`, `SCHED_WORKERS`, `SCHED_QUEUE_CAPACITY`,
`SCHED_BACKOFF_BASE`, `SCHED_MAX_ATTEMPTS`, ...) are read from the environment at startup into a
single `config.Config` struct, each with a documented default, and the resolved config is logged on
boot.

**Alternatives considered.**

1. **`flag` package.** Idiomatic Go, self-documenting via `-h`, type-checked for free.
2. **A YAML/TOML config file** (via `viper` or `koanf`). Structured, supports nesting and comments.
3. **Hardcoded constants.** Nothing to configure, nothing to get wrong.

**Why env vars won.** Stage 2 puts this service in `docker-compose.yml` and Stage 3 puts it in
Kubernetes. Both platforms inject configuration as environment variables as their native mechanism —
`environment:` in compose, `env:`/`ConfigMap` in a pod spec. Choosing flags means every deployment
manifest has to encode an argv array, and changing one value edits a command line. Choosing a config
file means shipping a file into a container image or mounting a ConfigMap volume, which is strictly
more machinery for ~8 scalar values.

Option 3 was rejected because the brief explicitly requires the worker count and max-attempts to be
configurable, and because the test harness needs to shrink the backoff base (1s → ~50ms) to run in
reasonable time — that alone forces runtime configurability.

**The tradeoff accepted.** Env vars are stringly-typed and have no built-in validation or discovery.
Mitigated by: parsing into a typed struct in exactly one place, failing fast with a clear error on a
malformed value rather than silently falling back to a default, and logging the fully-resolved
config at startup so "what is this process actually running with" is always answerable from the logs.
Silent fallback on a typo (`SCHED_WORKERS=eight` quietly becoming 4) is the classic env-var failure
mode and is specifically rejected here.

---

<a id="d-005"></a>

### D-005 — Concurrency unit: goroutines in one process, not worker processes or OS threads

**Decision.** The worker pool is *N* goroutines inside a single scheduler process, where *N* is
configurable. They coordinate over channels.

**Alternatives considered.**

1. **Worker processes** — fork/exec *N* copies of a worker binary, coordinate via an external broker
   (Redis, NATS, RabbitMQ).
2. **A `sync.WaitGroup` over one goroutine per submitted job** — unbounded concurrency, no pool.
3. **Explicit OS threads** — not really available in Go; `runtime.LockOSThread` is the closest and
   is for a different purpose entirely.

**Why goroutines won.** This is *the* Go-specific decision in the project, so the reasoning matters.

A goroutine is not an OS thread. It is a function call with its own stack, scheduled by the Go
runtime onto a small pool of real OS threads (`GOMAXPROCS` of them, defaulting to core count). Its
initial stack is ~2–8 KB and grows on demand, versus ~1–8 MB reserved for an OS thread. Spawning
10,000 is routine. Crucially, when a goroutine blocks on I/O — and *every single thing our workers
do is blocking I/O against the Docker daemon* — the runtime parks that goroutine and runs another on
the same OS thread. A pool of 8 workers waiting on 8 containers consumes essentially zero CPU.

Against option 1: worker processes buy true fault isolation (a panicking worker can't take down its
siblings) and horizontal scaling across machines. Both are real. Both are also unnecessary here,
because the actual isolation boundary for untrusted work in this design is *the container*, not the
worker — a job that segfaults or eats all memory kills its container, and the worker goroutine just
reads a non-zero exit code. The blast radius is already contained by the thing we were going to
build anyway. In exchange, option 1 would require a message broker, a serialization format, and a
distributed job-state store on day one — for Stage 1, that is a large amount of infrastructure
bought with no isolation gained.

Against option 2 (goroutine per job, no pool): this is the more seductive mistake, because it looks
simpler and is *less* code. It is rejected because an unbounded pool means 500 submitted jobs
launches 500 concurrent containers, and the failure mode is not a slow scheduler — it is the Docker
daemon exhausting host memory and the kernel OOM-killer choosing victims arbitrarily. A fixed worker
pool is a **concurrency limiter**: it converts "the host falls over" into "the queue gets deeper,"
which is an observable, recoverable condition. This is the central argument for the pool's existence
and it is worth being able to state plainly.

**Note on "distributed."** With this decision, Stage 1 is a *concurrent* scheduler, not a
*distributed* one — all state is in one process's memory. That is a deliberate staging choice, not
an oversight; see D-008 for the state-storage consequence and how it is contained.

---

<a id="d-006"></a>

### D-006 — Job identity: server-generated random IDs, not client-supplied or sequential

**Decision.** The server generates each job's ID as 128 bits from `crypto/rand`, hex-encoded, with a
`job_` prefix. Clients cannot choose an ID.

**Alternatives considered.**

1. **Sequential integers** (`1`, `2`, `3`) from an atomic counter — smallest, human-readable,
   naturally ordered.
2. **A UUID library** (`google/uuid`) — the standard answer, RFC-4122 formatted.
3. **Client-supplied IDs** — enables idempotent submission: resubmitting the same ID is a no-op.
4. **ULID / KSUID** — random *and* lexicographically sortable by creation time.

**Why server-generated random won.** Sequential IDs leak total job volume to anyone who submits a
job, and — more practically — they are guaranteed to collide the moment there is more than one
scheduler process, which is the explicit direction of this project. `crypto/rand` IDs are
collision-free across processes without any coordination, which keeps the door open for Stage 3.

`google/uuid` was rejected only because 16 bytes of `crypto/rand` hex-encoded is ~5 lines of stdlib
code with identical collision properties, and D-003 already established a preference for not taking
dependencies that replace trivial stdlib usage. This is a weak preference, not a strong one — if a
UUID dependency arrives for another reason, switching is fine.

Option 3 (client-supplied) is genuinely valuable — idempotency keys are how you make job submission
safe to retry across a network timeout — but it requires deciding what a duplicate submission
*means* (reject? return the existing job? re-run it?), which is a semantics question the brief
doesn't ask us to answer. Deliberately deferred rather than half-implemented.

Option 4 (ULID) loses to plain random for a specific reason: its selling point is index locality in
a sorted database, and D-008 stores jobs in a hash map where sort order is irrelevant. We would pay
a dependency for a property we cannot use.

**Simplicity/correctness tradeoff taken.** IDs are opaque and carry no ordering, so "list jobs in
creation order" requires sorting on the `CreatedAt` timestamp rather than on the ID. Accepted —
that's one `sort.Slice` call in the list handler.

---

<a id="d-007"></a>

### D-007 — Job submission is asynchronous: `202 Accepted` + poll, not a blocking request

**Decision.** `POST /jobs` validates, enqueues, and immediately returns `202 Accepted` with the job
ID and a `queued` status. The caller polls `GET /jobs/{id}` for the outcome. The HTTP request
lifetime is fully decoupled from the job lifetime.

**Alternatives considered.**

1. **Synchronous execution** — hold the HTTP connection open until the container exits, return the
   exit code and logs in the response.
2. **Webhook callback** — accept a `callback_url`, POST the result there on completion.
3. **Server-Sent Events / WebSocket** — stream status transitions to a connected client.

**Why async + poll won.** Option 1 collapses under this project's *core feature*. A job with
`max_attempts: 4` and 1s/2s/4s backoff can legitimately take 7+ seconds of pure waiting before its
final attempt even starts, plus execution time. Holding an HTTP connection open across that means
the request outlives default proxy/load-balancer idle timeouts (commonly 30–60s), and a client
disconnect creates a genuinely ambiguous state: is the job cancelled, or orphaned and still running?
Async submission makes that question disappear — the job's life was never tied to the connection.

It also makes the queue *real*. If submission blocks, the client's concurrency limit and the
server's queue depth are the same number, and the queue stops being a load-absorbing buffer.

Options 2 and 3 are both better *user experiences* than polling and both are out of scope: webhooks
require outbound-request retry logic (a second, independent retry system), and SSE requires
connection lifecycle management. Polling is the lowest-machinery option that is fully correct, and
Stage 2's `/metrics` endpoint will cover the "watch it live" need at the fleet level.

**Tradeoff accepted.** Polling is chatty and adds latency equal to the poll interval. For a
scheduler whose jobs run in seconds-to-minutes, that is irrelevant.

---

<a id="d-008"></a>

### D-008 — Job state: in-memory `map` guarded by `sync.RWMutex`, returning copies

**Decision.** Job records live in a `map[string]job.Job` guarded by a `sync.RWMutex`. The store's
API takes and returns **values, not pointers** — every read returns a defensive copy of the record.

**Alternatives considered.**

1. **`map[string]*Job`** — store pointers, hand them to callers. Fewer allocations, no copying.
2. **`sync.Map`** — the stdlib's lock-free-ish concurrent map.
3. **SQLite / Postgres / Redis** — real durable storage, survives restart.
4. **A single goroutine owning the map, accessed by channel** — "share memory by communicating."

**Why RWMutex + copies won.**

*Against option 1, which is the important one.* If the store hands out `*Job`, the HTTP handler
serializing a job to JSON and the worker goroutine updating that job's status are touching the same
struct at the same time with no synchronization. That is a textbook data race: `go test -race` flags
it, and in production it produces torn reads — a response showing `status: "running"` alongside a
populated `finished_at`, or worse, a corrupted string header. The mutex would *look* like it was
protecting the map, while actually protecting nothing about the records inside it. Copying on read
makes the lock's guarantee complete: what a caller receives is an immutable snapshot that was
internally consistent at some instant. A `Job` is a small flat struct, so the copy is cheap, and
this is the single most important correctness decision in the storage layer.

*Against option 2.* `sync.Map` is optimized for a specific access pattern — keys written once and
read many times, or disjoint key sets per goroutine. Our pattern is the opposite: every job record
is written repeatedly (queued → running → retrying → running → succeeded) throughout its life. Under
write-heavy load `sync.Map` is *slower* than a plain map with a mutex, and it costs static type
safety (`any` in, type assertion out). Wrong tool.

*Against option 3.* This is the real, acknowledged limitation: **if the scheduler restarts, every
queued and running job is lost.** For a system that will eventually be called distributed, that is a
serious gap. It is deferred deliberately, because durability is not on the Stage 1–3 list, and
because adding a database now means schema migrations, connection pooling, and a container
dependency in the test harness — before the core execution semantics are even proven. The mitigation
is architectural rather than aspirational: the store is defined as **an interface**, so the swap to
a durable backend is a new implementation and one wiring change, exactly like the executor in D-002.

*Against option 4.* The channel-owned-state pattern is very idiomatic Go and would also be correct.
It loses on ergonomics: every read becomes a request/reply channel round-trip with a reply channel
allocated per call, which is more code and more allocation than `mu.RLock()` for no additional
safety. Go's own proverb has a matching counterpart — "share memory by communicating" is advice for
transferring *ownership*, and a lookup table that many readers query concurrently is the case a
mutex was designed for.

**Bounded memory note.** The map grows without limit as jobs complete. Not addressed in Stage 1; a
TTL-based reaper or capped ring buffer of terminal jobs is the obvious fix and is noted here so it
isn't mistaken for something that was overlooked.

---

<a id="d-009"></a>

### D-009 — Store is defined as an interface at its point of use

**Decision.** `worker` and `api` depend on a `store.Store` **interface**, not on the concrete
`*store.MemoryStore`. Same for `executor.Executor`.

**Alternatives considered.**

1. **Depend on the concrete type directly.** Simplest; Go's implicit interfaces mean you can
   introduce the abstraction later without touching the implementation.
2. **Define the interface in the implementing package** (the Java/C# habit).

**Why an interface now.** The general Go guidance is genuinely "don't add an interface until you
have a second implementation" — premature interfaces are a real Go smell. This case clears that bar
on evidence, not speculation: the second implementations are *scheduled*. Stage 3 replaces the
executor outright, and the test harness needs a fake executor to test retry/backoff logic without
burning real container startup time on every assertion. Two known consumers of the seam is not
speculative generality.

Regarding option 2: in Go, interfaces belong to the **consumer**, not the producer. Because Go
interfaces are satisfied structurally, `store.MemoryStore` doesn't need to know that `worker` has an
interface it happens to satisfy. Declaring the interface next to the code that *calls* it means the
interface contains exactly the methods that caller needs, and it keeps the dependency arrow pointing
inward. (Pragmatic exception noted: the shared `Job` type still lives in `internal/job` so both
sides can name it without an import cycle.)

**Kept honest.** The interfaces stay small — the worker's view of the store is a handful of methods,
not the full CRUD surface. A 12-method interface with one implementation would be the smell this
decision is trying to avoid.

---

<a id="d-010"></a>

### D-010 — Governing principle: the simplest implementation that is still correct

**Decision.** Where a simpler and a more elaborate implementation are both *correct*, take the
simpler one. Complexity has to be paid for by a correctness or observability requirement that is
actually on the list, not by a hypothetical future one. Every later entry in this file is decided
under this rule, and several entries below record things that were built and then deliberately
removed.

**Alternatives considered.**

1. **Build for the anticipated end state** — write Stage 1 already shaped for Stages 2 and 3, with
   the extension points, wrappers, and indirection those stages will want.
2. **Simplest thing that could possibly work** — no abstraction at all until something breaks.

**Why the middle position.** Option 1 is what produced the first draft of the domain type: a custom
`Duration` JSON wrapper, per-attempt backoff bookkeeping, defensive deep-copies of pointer fields
that are never mutated. None of it was wrong; all of it was code with no requirement behind it, and
every line of it is code that has to be read and defended later. Option 2 is rejected where a seam
is *already known* to be needed — D-002 and D-009 keep the executor and store interfaces because
Stage 3 is a scheduled requirement, not a guess.

The dividing line this project uses: **abstraction needs a named, scheduled consumer.** The executor
interface has one (Stage 3 Kubernetes, plus a fake for tests). A `Duration` wrapper had none — the
only thing it bought was prettier JSON, and `timeout_seconds` as a plain integer buys the same
readability for zero code.

**Consequences accepted.** Some things will be rewritten rather than extended. That is the intended
trade: rewriting 40 lines of simple code later is cheaper than carrying 200 lines of general code
through three stages, especially when the general code encodes guesses about requirements that may
change.

---

<a id="d-011"></a>

### D-011 — Five job states, with `retrying` as a first-class state rather than a derived one

**Decision.** `queued | running | retrying | succeeded | failed`, exactly as the brief lists them,
with `retrying` stored explicitly on the record alongside a `next_attempt_at` timestamp.

**Alternatives considered.**

1. **Four states, deriving "retrying"** — keep the job in `queued` during backoff and let clients
   infer retrying from `attempts_used > 0`.
2. **More states** — add `cancelled`, `timed_out`, `pending` (accepted but not yet queued).
3. **Separate status and sub-status fields** — e.g. `state: waiting, reason: backoff`.

**Why five, with retrying explicit.** Option 1 is a real simplification and it was close. It loses
on the thing this project is actually being built to demonstrate: a job in backoff and a job waiting
for a free worker are *operationally different situations*. One is the system being healthy under
load, the other is a job failing repeatedly. Collapsing them means a queue-depth graph in Stage 2
can't distinguish "we are saturated" from "we are thrashing on a broken job", and that is exactly
the distinction the dashboard exists to show. Storing `next_attempt_at` alongside it makes the state
self-explaining: a client can see not just *that* it is retrying but *when* it resumes.

Option 2 was rejected on scope. `cancelled` implies a cancellation endpoint and a decision about
whether to kill a running container mid-flight — real work, not requested. `timed_out` is
deliberately folded into `failed`, because a timeout is just a failed attempt with a particular
`last_error`, and it should consume an attempt and be retried like any other failure.

Option 3 is the most *expressive* model and is what a mature system converges on, but two orthogonal
fields means clients must understand a 2-D state space to answer "did it work?". One flat enum with
five values keeps the common question a single comparison.

**Tradeoff accepted.** `failed` is overloaded: it covers "exited non-zero four times", "timed out",
and "the Docker daemon was unreachable". They are distinguished only by the `last_error` string and
the per-attempt records, not by the status enum. That is a real loss of machine-readability, and the
mitigation is D-014's per-attempt history — the structured detail is there, just not in the enum.

---

<a id="d-012"></a>

### D-012 — Snapshot copies use a shallow struct copy plus one slice clone, under a write-once pointer rule

**Decision.** `Job.Clone()` copies the struct and deep-copies only the `Attempts` slice. The pointer
fields (`StartedAt`, `FinishedAt`, `NextAttemptAt`, `ExitCode`) are shared between copies, protected
by a codebase-wide invariant: **pointers are assigned, never written through.** Code does
`j.FinishedAt = &t`, never `*j.FinishedAt = t`.

**Alternatives considered.**

1. **Full deep copy** — allocate a new `time.Time` and `int` for every pointer field on every read.
   Correct under any future mutation pattern; ~30 lines of mechanical copying.
2. **Plain struct assignment, no clone at all** — rely on the store's mutex alone.
3. **No pointers in the model** — use zero `time.Time` values and `.IsZero()` checks, which removes
   the question entirely.

**Why the middle option.** Option 2 is straightforwardly a data race, and a subtle one: the mutex
protects the *map*, but a shared `Attempts` backing array means a JSON encoder ranging over the
slice can observe a worker's `append` in progress. `go test -race` catches this; production catches
it as intermittently truncated API responses. The slice clone is not optional.

Option 1 was the first implementation and was removed under D-010. It defends against a mutation
pattern the code does not use and would not adopt — nothing anywhere has a reason to write through
one of these pointers, because they represent facts that become true once (a job starts once,
finishes once). Paying 30 lines to defend against a hypothesis is the exact trade D-010 rejects.

Option 3 is genuinely appealing and nearly won. It fails on the API surface: Go's `encoding/json`
does not omit a zero `time.Time`, so every queued job's response would carry
`"finished_at": "0001-01-01T00:00:00Z"`, and the alternative is a separate DTO type purely for
rendering. A pointer plus `omitempty` gets a clean, honest JSON document for one line of struct tag.

**The risk, stated plainly.** This decision is a convention enforced by review, not by the compiler.
If someone later writes `*j.ExitCode = 0`, the race comes back silently. That risk is accepted and
documented in the `Clone` doc comment so it is discoverable at the place it would be violated. If
the model grows more pointer fields, revisit and take option 1.

---

<a id="d-013"></a>

### D-013 — JSON tags on the domain type; no separate API DTO

**Decision.** `job.Job` and `job.Spec` carry `json:"..."` struct tags and are serialized directly by
the HTTP handlers. There is no parallel `api.JobResponse` type.

**Alternatives considered.**

1. **Separate DTO types in `internal/api`** with explicit mapping functions between them and the
   domain type.
2. **A hand-written `MarshalJSON`** on the domain type.

**Why tags on the domain type.** This is the clearest simplicity-versus-correctness trade in the
project, and simplicity wins *for now* on a specific argument: the domain model and the wire model
are currently identical, so a DTO layer would be a pure identity mapping — two structs and two
mapping functions that must be kept in sync, providing decoupling that nothing is currently pulling
on.

**What this genuinely costs, and it is not nothing.** Renaming an internal field silently changes
the public API. Adding an internal field silently exposes it. There is no compile-time barrier
between "how we store it" and "what we promise clients"; the only barrier is the struct tag, and
forgetting one leaks a Go field name into the API.

**The trigger to revisit, decided now rather than later.** Add DTOs the first time any of these
becomes true: (a) a field must exist internally but not be exposed — a credential, an internal
error, a Kubernetes object reference from Stage 3; (b) the wire format must stay stable while the
internal model changes; (c) request and response shapes diverge. Stage 3 will add Kubernetes
identifiers to the record, so (a) is the likely trigger and this decision is expected to be revisited
there rather than to hold forever.

---

<a id="d-014"></a>

### D-014 — Full per-attempt history is retained, not just the latest attempt

**Decision.** Every `Job` carries an `Attempts []Attempt` slice with one entry per try, each holding
its own start/finish timestamps, exit code, OOM flag, container ID, and captured logs.

**Alternatives considered.**

1. **Latest attempt only** — overwrite `exit_code`, `logs`, `error` on each retry. Constant memory
   per job regardless of attempt count.
2. **A counter only** — track `attempts_used` and nothing else per attempt.
3. **Emit attempts as log lines** rather than storing them on the record.

**Why full history.** The brief requires a test harness that "confirms the retry/backoff behavior
works correctly." With option 1 or 2, there is no way to verify backoff *from outside the process* —
the delays would only be observable by instrumenting internals or by polling the status endpoint
fast enough to catch each transition, which is a flaky test. With per-attempt timestamps, the
assertion is direct and deterministic: the gap between `Attempts[0].FinishedAt` and
`Attempts[1].StartedAt` must be at least the first backoff interval. **The observability requirement
is what justifies the storage cost**, which is precisely the standard D-010 sets.

It also answers the question operators actually ask about a flaky job — "did it fail the same way
every time, or differently?" — which option 1 destroys by overwriting.

Option 3 keeps memory flat and is what a production system does at scale, but it moves the data out
of the API and into a log aggregator that this project does not have.

**Cost accepted, and bounded.** Memory per job now scales with attempt count. Bounded in practice by
two things: `max_attempts` is capped server-side (D-020), and captured logs are truncated to a
configurable tail per attempt (D-019) rather than retained in full. A job cannot grow without limit.

---

<a id="d-015"></a>

### D-015 — The queue holds job IDs, not job records

**Decision.** A queue entry is `{id, priority, seq}`. To act on a job, a worker pops the ID and
reads the record from the store.

**Alternatives considered.**

1. **Queue whole `job.Job` values** — the worker gets everything it needs in one pop, no store
   lookup, no second failure path.
2. **Queue `*job.Job` pointers** — no copying at all.

**Why IDs won.** Option 1 creates two copies of mutable state that can disagree. A job sits in the
queue while its record continues to exist in the store; if anything ever mutates the stored record
(a future cancel endpoint, a status correction, a Stage 3 reconciliation loop), the queued copy is
stale and the worker acts on out-of-date information. Storing only the ID makes the store the single
source of truth by construction — there is no second copy that *can* drift.

Option 2 is worse than either: it reintroduces exactly the shared-mutable-struct race that D-012
exists to prevent, across a goroutine boundary.

**Cost accepted.** One extra map lookup per dispatch, and a new failure mode — the ID could be
missing from the store. In this design that is impossible (records are created before enqueue and
never deleted), but the worker handles it by logging and dropping rather than panicking, because
"impossible" states in concurrent code deserve a defined behaviour.

---

<a id="d-016"></a>

### D-016 — Mutation API is `Update(id, func(*Job))`, not `Get` then `Put`

**Decision.** The store exposes a single mutating method that takes a closure and applies it while
holding the write lock.

**Alternatives considered.**

1. **`Get(id)` → mutate the copy → `Put(id, j)`.** The obvious CRUD shape, and what most people
   write first.
2. **Fine-grained setters** — `SetStatus(id, s)`, `AppendAttempt(id, a)`, `SetFinished(id, t)`.
3. **Hand the caller the lock** — expose `Lock()`/`Unlock()` and a raw accessor.

**Why the closure won.** Option 1 has a lost-update race that the mutex does *not* prevent, and this
is the important point: the lock protects each individual call, but the read-modify-write spans two
calls, so two goroutines can both read version N and both write version N+1, with one update
silently vanishing. In this scheduler that shows up as a dropped attempt record — a job reporting 3
attempts when 4 ran. Passing the mutation *into* the lock makes read-modify-write a single atomic
operation, which is the only way to make it correct without pushing lock discipline onto callers.

Option 2 is correct too, but each new state transition needs a new method, and a transition touching
three fields either needs three calls (reintroducing the same race across calls) or a bespoke
combined method. It scales badly.

Option 3 is rejected outright: exporting a lock makes every caller responsible for the invariant,
and a single early `return` past an `Unlock` deadlocks the whole service.

**Cost accepted.** Callers can do arbitrary things inside the closure, including something slow —
and it runs with the store's write lock held, blocking every reader. The rule the codebase follows:
**closures passed to `Update` do field assignment only** — no I/O, no channel operations, no calls
back into the store. Kept honest by review, not by the compiler.

---

<a id="d-017"></a>

### D-017 — The queue is a heap guarded by a mutex and `sync.Cond`, not a Go channel

**Decision.** `container/heap` + `sync.Mutex` + `sync.Cond`. Workers call a blocking `Pop()`;
shutdown is signalled by `Close()`, which broadcasts and makes `Pop` return `ok=false`.

**Alternatives considered.**

1. **A buffered channel `chan string`** — the idiomatic Go work queue. Blocking receive, bounded
   capacity, and `select` with a context for free. The brief explicitly says "channel-based is fine."
2. **Multiple channels, one per priority level**, with workers `select`ing across them.
3. **A third-party priority queue library.**

**Why the heap won, despite channels being the idiomatic answer.** A Go channel is strictly FIFO and
that is not configurable. The brief asks for "simple priority-based queuing," and priority is
precisely the one thing a channel cannot express. Everything else about the channel option is
better, which is why this decision is worth recording rather than assuming.

Option 2 is the interesting near-miss: `select` over a high- and a low-priority channel *looks* like
it gives priority ordering, but Go's `select` chooses **uniformly at random** among ready cases —
by language specification, to prevent starvation. So two channels give you a coin flip, not
priority. Getting real precedence requires the nested-select idiom (try high-priority alone with a
`default`, fall through to a blocking select over both), which is subtle, easy to get wrong, and
caps you at a fixed number of discrete levels rather than an integer priority.

**What choosing the heap costs — and this is the real price.** Channels compose with `select`, so a
worker could wait on work *and* context cancellation simultaneously. `sync.Cond` has no `select`
integration and `Cond.Wait` cannot be cancelled. The mitigation is `Close()`: shutdown flips a flag
and broadcasts, every blocked `Pop` wakes and returns `ok=false`, and workers exit their loop. That
recovers cancellability, but it means **shutdown is signalled through the queue rather than through
the context**, which is a genuine deviation from idiomatic Go and is why it is documented here. It
also means `Cond.Wait` must sit inside a `for` loop rather than an `if` — a bare `if` is the classic
condition-variable bug, since `Wait` can return spuriously and the state must be rechecked.

**Complexity actually added.** ~25 lines implementing `heap.Interface`. The ordering policy ends up
concentrated in one three-line `Less` method, which is a decent place for it to live.

---

<a id="d-018"></a>

### D-018 — Retries are re-enqueued by a timer and bypass the capacity limit

**Decision.** When an attempt fails with attempts remaining, the worker records the failure, sets
status `retrying` with a `next_attempt_at`, and hands the job to `time.AfterFunc(delay, ...)` which
calls `queue.Requeue`. `Requeue` skips the capacity check that `Submit` enforces.

**Alternatives considered.**

1. **Sleep in the worker** — `time.Sleep(backoff)` and retry in place. Fewer moving parts, and the
   job never leaves the goroutine that owns it.
2. **A delayed-item heap inside the queue**, ordered by ready-time, with a dispatcher goroutine
   moving matured items into the ready set.
3. **A dedicated retry-scanner goroutine** that polls the store every N ms for jobs whose backoff
   has expired.
4. **Retries respect capacity** like any other submission.

**Why timer + requeue won.** Option 1 is the one to reject loudly, because it is the obvious
implementation and it is quietly broken. A sleeping worker still *occupies a worker slot*. With 4
workers and 4 jobs in their 8-second backoff, throughput is zero — the pool is fully subscribed to
doing nothing, while healthy jobs sit in the queue behind them. The pool's entire purpose (D-005) is
to bound concurrent containers, and a sleeping worker consumes that budget without running a
container. This is also what makes `retrying` a meaningful distinct state (D-011) rather than a
label on a busy worker.

Option 2 is the "proper" design and is what a production scheduler does. It was rejected under D-010
as unjustified complexity *at this stage*: a second heap, a dispatcher goroutine, and timer
management, all to replace one stdlib call. `time.AfterFunc` is not a goroutine while it waits — the
runtime timer heap handles it — so the cheap option has essentially the same runtime cost as the
elaborate one. **This is expected to be revisited if retry state ever needs to survive a restart**,
because in-process timers vanish on exit while a persisted ready-time does not.

Option 3 (polling) trades timer precision for a busy loop and is worse on both axes.

**Why retries bypass capacity (option 4 rejected).** If a retry could be rejected for a full queue,
a burst of new submissions would cause the scheduler to *silently drop jobs it had already accepted
and reported as `retrying`* — the job would sit in a non-terminal state forever with nothing
scheduled to advance it. Accepting a job is a promise to reach a terminal state. Capacity therefore
applies at the admission boundary only. The bound is soft rather than hard: with capacity *C* and
*W* workers, pending work can reach *C + W* in the worst case, which is a bounded and acceptable
overshoot.

**Shutdown consequence.** A pending `AfterFunc` fires into a closed queue. The closure checks the
shutdown context and no-ops, so a retry scheduled across a shutdown is simply abandoned — consistent
with D-008's existing accepted limitation that in-memory state does not survive a restart.

---

<a id="d-019"></a>

### D-019 — Container output is captured as a bounded tail, not in full

**Decision.** After a container exits, read its logs with a server-side cap of 200 lines, then
truncate to the last `SCHED_LOG_TAIL_BYTES` (default 4 KiB), prefixing a `...[truncated]...` marker
when anything was cut.

**Alternatives considered.**

1. **Capture everything** into the job record.
2. **Capture nothing** — leave logs in Docker and expose the container ID for `docker logs`.
3. **Stream logs to a file or a log aggregator**, referenced by path/URL from the record.

**Why a bounded tail.** Option 1 is a memory-exhaustion bug with a friendly face: a job that prints
in a loop can produce gigabytes, and D-014 keeps *every attempt*, so the worst case multiplies by
`max_attempts`. Since job state is in-memory (D-008), that is a direct path to killing the
scheduler with a `yes` command.

Option 2 is tempting and nearly free, but it breaks under this project's own design — D-026 removes
the container immediately after it exits, so by the time anyone asks, `docker logs` has nothing.
Keeping containers around instead would leak disk.

The tail (rather than the head) is chosen because the diagnostically useful part of a failed
command's output — the error, the stack trace, the last thing it did — is at the end.

**Both caps exist for different reasons.** The 200-line server-side cap stops a huge log crossing the
socket at all. The byte cap bounds what is retained, since 200 lines could still be enormous. The
line cap is the network protection; the byte cap is the memory protection.

**Cost accepted.** Verbose jobs lose their early output, and there is no way to retrieve it later.
For a system whose job is to report success/failure with enough context to debug, the tail is the
right 4 KiB to keep. Full log retention is a Stage-2+ concern that belongs in a log pipeline, not in
a job record.

---

<a id="d-020"></a>

### D-020 — Server-side ceilings on every resource a client can request

**Decision.** Submissions are validated against configured maxima — `SCHED_MAX_CPUS` (4),
`SCHED_MAX_MEMORY_MB` (2048), `SCHED_MAX_ATTEMPTS` (10), `SCHED_MAX_JOB_TIMEOUT` (30m). Over-asks
are rejected with `400`, not silently clamped.

**Alternatives considered.**

1. **No ceilings** — honour whatever the client asks for. Simplest, and there is no auth in scope
   anyway, so clients are already trusted.
2. **Clamp silently** to the maximum instead of rejecting.
3. **Reject only obviously invalid values** (negative CPUs, zero memory) with no upper bound.

**Why ceilings.** The argument against option 1 is not security, it is blast radius. `max_attempts:
100000` with exponential backoff is a job that never terminates and never frees its queue slot.
`memory_mb: 999999` on an 8 GB host is a container the daemon may accept and then let the kernel
resolve by OOM-killing something else — possibly the scheduler. A single malformed request should
not be able to degrade the host, and that is true whether or not the caller is authenticated. The
ceilings make the failure mode "your request was rejected" instead of "the machine got unhealthy."

**Why reject rather than clamp (option 2).** Silent clamping means a client asks for 8 GB, gets 2 GB,
and its job gets OOM-killed with no indication that the limit it requested was not the limit it got.
Debugging that means reading scheduler source. An explicit `400` naming the ceiling is immediately
actionable. General principle applied here: **never silently give someone something different from
what they asked for** — the same reasoning as D-004's refusal to fall back on a malformed env var.

**Tradeoff accepted.** The ceilings are global, not per-caller. Real multi-tenant quota is a
different feature and out of scope; these values are configured for the host the scheduler runs on.

---

<a id="d-021"></a>

### D-021 — `max_attempts` counts total tries, not retries

**Decision.** `max_attempts: 4` means the job runs **at most 4 times total** — one initial attempt
plus up to 3 retries. `max_attempts: 1` means run once, never retry. Values below 1 are rejected.

**Alternatives considered.**

1. **`max_retries`** — count only the re-tries, so `max_retries: 3` also yields 4 total runs.
2. **Both fields**, with validation that they agree.

**Why total attempts.** This is a small decision with an outsized bug surface, which is why it gets
an entry. The two conventions differ by exactly one, and off-by-one in retry logic is the single
most common bug in this kind of code — it shows up as a job that runs 5 times when the operator
expected 4, or as `max_retries: 0` ambiguously meaning "never run" or "run once".

"Total attempts" wins because **`max_attempts: 1` is unambiguous** — it can only mean "run once" —
whereas `max_retries: 0` reads as either "no retries" or "no attempts" depending on the reader.
The loop condition also reads naturally and is self-checking: `for attemptsUsed < maxAttempts`.

The field is named `max_attempts`, not `retries`, so the name states the convention. `AttemptsUsed`
on the record is maintained as an exact mirror of `len(Attempts)`, so the invariant is verifiable
rather than assumed.

**Backoff consequence, stated explicitly.** With `max_attempts: 4` and a 1s base, the delays are
1s, 2s, 4s — **three** waits, not four. There is no backoff after the final attempt, because there
is nothing to wait for. Worst-case wall-clock for a fast-failing job is ~7s plus execution time.

---

<a id="d-022"></a>

### D-022 — One fresh container per attempt; containers are never reused

**Decision.** Each attempt creates a new container from the image, runs it, and destroys it. Attempt
2 of a job shares nothing with attempt 1.

**Alternatives considered.**

1. **Restart the same container** via `ContainerRestart` — much faster (no create/pull overhead) and
   the container ID stays stable across attempts.
2. **A pool of warm containers** with commands injected via `docker exec` — fastest of all,
   amortising container startup across many jobs.

**Why fresh containers.** Retry exists to recover from *transient* failure. If attempt 1 left the
filesystem dirty — a half-written output file, a stale lock, a corrupted temp directory — then
option 1 makes attempt 2 start from that corrupted state, and the retry is strictly less likely to
succeed than the first try was. A fresh container makes every attempt start from the identical
known-good image state, which is what makes retrying meaningful rather than superstitious.

Option 2 breaks isolation outright, which is a stated requirement: `docker exec` into a shared
container means jobs see each other's processes, files, and environment, and one job's runaway
memory use is charged against the shared container's limit — so **per-job resource limits stop being
enforceable**, which defeats a core requirement of the project.

**Cost accepted, and it is real.** Container creation and startup is roughly 100–300 ms, paid on
every attempt. For jobs measured in seconds or minutes that is noise; for sub-second jobs it would
dominate. This scheduler is explicitly designed for the former.

---

<a id="d-023"></a>

### D-023 — Commands run via `/bin/sh -c`, accepting shell semantics

**Decision.** A submitted command string is executed as `["/bin/sh", "-c", command]`, so pipes,
`&&`, redirection, globbing and variable expansion all work.

**Alternatives considered.**

1. **Exec form** — accept a JSON array `["echo", "hello"]` passed straight to `Cmd` with no shell.
2. **Support both** — a string means shell, an array means exec.

**Why the shell form.** The brief specifies a job is "a shell command," and the ergonomic difference
is large: `cat f | grep x > out` is one string versus an array the client must decompose, and the
exec form simply cannot express a pipe (there is no process to interpret `|`). Every job would
otherwise have to be manually wrapped in `sh -c` by the caller anyway.

**What this costs, stated honestly.** The shell form means submitted text is interpreted by a shell,
so there is no meaningful boundary between "command" and "shell injection" — a caller that can
submit a job can already run arbitrary code, by design. That is acceptable *only* because of two
things that are load-bearing rather than incidental: the container is the security boundary
(D-005), and it runs with no network by default (D-027). Notably, if authentication were ever added
to this service, this decision would need revisiting — an unauthenticated caller with shell access
to a container is a very different risk profile.

Option 2 was rejected under D-010: two input shapes, two validation paths, and a polymorphic JSON
field, for a capability nobody has asked for.

**Dependency introduced.** The job image must contain `/bin/sh`. True for `alpine`, `busybox`,
`debian`, and nearly every general-purpose image; false for `scratch` and distroless images. Since
the failure is a clear container-create error naming the missing path, this is left as a documented
constraint rather than a validated one.

---

<a id="d-024"></a>

### D-024 — Infrastructure errors and job failures are different return values

**Decision.** `Executor.Run` returns `(Result, error)` where they mean different things:
a non-nil `error` means *the job never ran* (daemon unreachable, image pull failed, create
rejected); a nil error with `Result.ExitCode != 0` means *the job ran and failed*.

**Alternatives considered.**

1. **Fold everything into the error** — return an error for a non-zero exit too, since "the job
   failed" is arguably an error.
2. **Fold everything into the Result** — add an `InfraError string` field and never return an error.
3. **Distinct error types** the caller inspects with `errors.As`.

**Why the split.** Option 1 destroys information the retry logic and the operator both need. "Exited
1" means the command is broken and retrying probably will not help. "Cannot reach the Docker daemon"
means nothing is wrong with the command and retrying probably *will* help once the daemon returns.
Collapsing them means the job record reports `last_error: "exit status 1"` for a daemon outage, and
an operator debugging a spike of failures cannot tell whether their jobs broke or their
infrastructure did. Go's convention supports this reading: an error means *this function could not
do its work*, and successfully running a program that returns 1 is the function doing its work.

Option 2 makes every caller check a string field, which is easy to forget and impossible for `go
vet` to catch — an unchecked `error` return is at least conventionally noticed.

Option 3 is the more scalable design and is deliberately deferred: with exactly two categories, a
type hierarchy is machinery without a payoff. Revisit if a third category appears (a likely one from
Stage 3: "the Kubernetes API rejected the Job spec," which is neither transient infrastructure nor
job fault).

**Retry consequence.** Both categories are retried with the same backoff. That is a *simplification*
and is recorded as such: a smarter policy would retry infrastructure errors more aggressively (they
are transient) and non-zero exits less (they are usually deterministic). Not implemented, because
distinguishing them properly needs failure data this project has not gathered, and guessing at
per-category policy would be inventing requirements. The distinction is *recorded* in the job record
either way, so the data to make that call later is being collected now.

---

<a id="d-025"></a>

### D-025 — `MemorySwap` is pinned equal to `Memory`, so the memory limit is a hard wall

**Decision.** Set `Resources.MemorySwap` to the same byte value as `Resources.Memory` on every
container.

**Alternatives considered.**

1. **Leave `MemorySwap` unset** (zero), which is what `docker run --memory=X` alone does.
2. **`MemorySwap: -1`** — unlimited swap.

**Why this matters enough to be its own entry.** The brief requires limits to be *actually enforced,
not just requested*, and option 1 is the trap that makes them look enforced when they are not. When
`--memory=X` is set and swap is unset, the daemon grants the container swap equal to its memory
limit — a total allowance of **2X**. A container with a 64 MB limit can therefore touch 128 MB
before anything kills it, and in between it is merely slow. A test asserting "the container is
OOM-killed at 64 MB" would fail, and the natural conclusion would be that limits are not working,
when in fact swap is silently doubling them.

Setting `MemorySwap == Memory` sets the swap allowance to zero, so exceeding the memory limit
triggers the kernel OOM-killer immediately. That is the behaviour the requirement describes and the
behaviour the test harness verifies (via `State.OOMKilled`, not merely exit code 137 — see D-032).

Option 2 removes the memory limit's teeth entirely.

**Tradeoff accepted.** A job that briefly exceeds its limit gets killed rather than degraded into
swap. That is the intended semantic for a hard limit, and a job that needs more memory should
request more memory.

---

<a id="d-026"></a>

### D-026 — `AutoRemove` is off; containers are removed explicitly after inspection

**Decision.** Create containers with `AutoRemove: false` and remove them in a deferred cleanup, after
exit code, logs, and OOM status have been read.

**Alternatives considered.**

1. **`AutoRemove: true`** — the daemon deletes the container the moment it exits. One less API call,
   and no leak if the scheduler crashes mid-job.
2. **No removal at all**, plus a periodic reaper filtering on the `jobsched.job-id` label.

**Why explicit removal.** `AutoRemove` races the very reads this scheduler exists to perform. The
container's exit code, its `OOMKilled` flag, and its logs all live in daemon state that is destroyed
along with the container, so `AutoRemove` creates a window where `ContainerInspect` and
`ContainerLogs` return "no such container" *sometimes* — a genuinely nasty intermittent failure that
would show up as jobs randomly losing their logs under load. Ordering removal after the reads makes
it deterministic.

Option 2 is what a large deployment does (batched removal is cheaper than per-container), but it
needs a reaper goroutine and a retention policy, and it lets disk usage grow between sweeps.

**The leak this accepts.** If the scheduler is `SIGKILL`ed between a container exiting and the
cleanup running, that container is orphaned in `Exited` state and nothing will ever remove it. The
mitigations are that the window is milliseconds wide, that removal uses a context detached from
cancellation (`context.WithoutCancel`) so ordinary shutdown and attempt-timeouts still clean up, and
that every container is labelled `jobsched.job-id`, making manual cleanup a one-liner:
`docker rm $(docker ps -aq -f label=jobsched.job-id)`. A startup reaper that sweeps orphans from a
previous process is the obvious fix and is noted as a known gap rather than built.

---

<a id="d-027"></a>

### D-027 — Job containers get no network by default

**Decision.** `NetworkMode` defaults to `"none"`, configurable via `SCHED_JOB_NETWORK`.

**Alternatives considered.**

1. **Default bridge networking** — what `docker run` does; jobs can reach the internet and other
   containers.
2. **`host` networking** — the container shares the host's network stack.

**Why no network by default.** The brief asks for jobs to run "in an isolated Docker container," and
network access is the largest hole in that isolation: with bridge networking a job can reach the
host's other services, the local network, and the internet. Combined with D-023 (arbitrary shell
commands), a default-networked container is a straightforward pivot point. `none` makes the default
posture "this job computes and returns a result."

Option 2 is rejected outright — `host` networking removes network isolation entirely and lets a job
bind ports on the host.

**Why it is configurable rather than hardcoded.** Plenty of legitimate jobs need to fetch an input
or call an API, and forcing a code change for that would be wrong. Making it an env var means the
*deployment* chooses the posture. The value is set per-scheduler rather than per-job deliberately:
a per-job network toggle would let any submitter opt themselves out of the isolation, which makes
the default meaningless.

**Consequence to be aware of in Stage 1 testing.** With `none`, a job cannot pull anything at
runtime, and images must be pullable by the *daemon* (which has its own network) rather than by the
job. The test harness therefore uses images and commands that need no network — see D-032.

---

<a id="d-028"></a>

### D-028 — Images are pulled lazily, triggered by a failed create

**Decision.** Attempt `ContainerCreate` first. If it fails with a not-found error, pull the image and
retry the create exactly once.

**Alternatives considered.**

1. **Check-then-create** — call `ImageInspect` (or `ImageList`) before every create and pull if
   absent.
2. **Pre-pull at startup** — pull the configured default image on boot; reject jobs asking for
   anything else.
3. **Always pull** before every run, guaranteeing image freshness.

**Why create-first.** Option 1 adds a round-trip to the daemon on *every single job*, in order to
avoid a failed create on the rare cold-image case. Since the overwhelmingly common case is "the
image is already local," that is the wrong operation to optimise. It also has a time-of-check /
time-of-use race — the image can be deleted between the check and the create — so the create path
still needs the not-found branch. Handling it in one place is both faster and simpler.

Option 2 makes per-job images impossible, which the API explicitly supports. Option 3 makes every
job pay a registry round-trip and makes execution depend on registry availability — and with a
tag like `alpine:3.20` there is nothing to refresh.

**Cost accepted.** One wasted create round-trip the first time an image is used, and a possible
thundering herd if *N* workers all pull the same missing image simultaneously. The Docker daemon
deduplicates concurrent pulls of the same reference internally, so the herd costs some redundant API
calls but not redundant downloads. A `singleflight` guard would remove even that; not worth the
dependency for a once-per-image event.

---

<a id="d-029"></a>

### D-029 — Attempt timeouts are enforced by a `context` deadline, not a timer plus a kill

**Decision.** Wrap the attempt in `context.WithTimeout`, pass that context to every Docker call, and
identify a timeout by checking which context expired.

**Alternatives considered.**

1. **A `time.AfterFunc` that kills the container**, with the wait left unbounded.
2. **Docker's `StopTimeout`** on the container spec.
3. **A watchdog goroutine** per attempt polling elapsed time.

**Why context.** This is the Go-specific pattern worth understanding, since it recurs throughout the
program. A `context.Context` is a cancellation signal that composes: deriving `runCtx` from the
scheduler's shutdown context and passing it to `ContainerCreate`, `ContainerStart` and
`ContainerWait` means **either** the attempt deadline **or** a process shutdown unblocks all of them,
with no per-call timer bookkeeping. Options 1 and 3 both reimplement, less well, what the runtime
already provides, and they leave the wait itself unbounded so a hung daemon call still blocks a
worker forever.

Option 2 is a different mechanism than it appears: `StopTimeout` governs how long the daemon waits
after `SIGTERM` before escalating to `SIGKILL` during a *stop*. It does not bound total runtime.

**The subtlety this created, worth understanding.** When a context is cancelled, `ContainerWait`
reports it as a generic error on its error channel — it does not say "timeout". So the code compares
two contexts to attribute the cause: if `runCtx` is done but the parent is not, it was this attempt's
deadline (report `TimedOut`, exit code 137); if the parent is also done, the scheduler is shutting
down and this is not the job's fault. Without that distinction, every job in flight during a
shutdown would be recorded as having timed out.

**Cleanup consequence.** Once `runCtx` is cancelled, it cannot be used to clean up the container it
just abandoned. All cleanup paths therefore use `context.WithoutCancel(parent)`, which keeps context
values but drops cancellation — the removal, the kill, and the log/inspect reads all run on detached
contexts with their own short deadlines.

---

<a id="d-030"></a>

### D-030 — Backoff is `base * 2^(n-1)` with a cap, no jitter, and the same policy for all failures

**Decision.** Delay before attempt *n+1* is `BackoffBase * 2^(n-1)`, capped at `BackoffMax`. With
the defaults (base 1s, cap 60s) that is 1s, 2s, 4s, 8s — exactly the sequence the brief specifies.
Both parameters are configurable. No randomised jitter.

**Alternatives considered.**

1. **Fixed delay** — retry every 5s regardless of attempt number.
2. **Linear backoff** — 1s, 2s, 3s, 4s.
3. **Exponential with jitter** — multiply by a random factor, the standard distributed-systems
   recommendation.
4. **No cap** — let it double forever.

**Why exponential.** Retries exist for failures that may clear given time. Options 1 and 2 make a
weak assumption about how long that takes: too short and you hammer a struggling dependency with the
retry traffic that is keeping it down; too long and you add pointless latency to failures that would
have cleared instantly. Exponential covers both regimes with one parameter — it retries almost
immediately in case the failure was a blip, and backs off fast enough that a genuinely broken
dependency sees exponentially decreasing pressure.

**Why the cap (option 4 rejected).** Without one, `max_attempts: 10` produces a final wait of
2^9 = 512 seconds, and the doubling eventually overflows `int64` nanoseconds into a *negative*
duration — a `time.AfterFunc` with a negative delay fires immediately, silently converting the
backoff into a hot retry loop at exactly the point it was supposed to be most patient. The code
guards this twice: the cap, and a bounded shift exponent.

**Why no jitter — the interesting one.** Jitter is the textbook-correct answer and it is deliberately
not implemented. Its purpose is to break up *synchronised* retries: thousands of clients that failed
at the same instant retrying at the same instant, re-creating the thundering herd that caused the
outage. That failure mode needs a large population of independent retriers sharing one dependency.
This scheduler has a fixed, small worker pool draining a single queue, so retries are already
serialised through *W* workers — the pool itself is the rate limiter that jitter would otherwise
provide. Adding randomness would also make the backoff timing non-deterministic, and **the test
harness asserts on observed backoff intervals**; jitter would force those assertions into loose
ranges and make the retry behaviour harder to demonstrate, which is a real cost for a project whose
point is to be explainable.

**When this becomes wrong, stated now.** If this ever runs as multiple scheduler replicas sharing a
backing store and a common dependency, jitter stops being optional. Noted as the trigger to revisit.

**Simplification recorded (see also D-024).** One policy applies to every failure type. A more
correct system would treat "Docker daemon unreachable" (transient, retry soon) differently from
"exit code 1" (deterministic, retrying likely pointless). Not implemented because choosing sensible
per-category parameters requires failure data this project does not have; the per-attempt records
capture the data needed to make that decision later.

---

<a id="d-031"></a>

### D-031 — Docker SDK pinned to v27.3.1, with `go-connections` pinned to v0.5.0

**Decision.** Pin `github.com/docker/docker` at `v27.3.1+incompatible` — matching the daemon on this
machine — and pin the transitive `github.com/docker/go-connections` to `v0.5.0`.

**Alternatives considered.**

1. **Track latest** for both, letting `go mod tidy` resolve.
2. **Use the Docker CLI as a subprocess** (`os/exec` around `docker run`) instead of the SDK.
3. **Use a lighter third-party Docker client.**

**Why pinned.** Option 1 does not build. `go mod tidy` selected `go-connections v0.8.1`, which
removed `sockets.DialPipe`, a function `docker/docker v27.3.1` still calls — so the *dependency of a
dependency* broke compilation of code we do not own. That is worth recording because the error
(`undefined: sockets.DialPipe`, in a file inside the module cache) points at Docker's source, not
ours, and looks like a broken SDK rather than a version-resolution problem. `docker/docker` is
distributed as `+incompatible` (its `go.mod` predates semantic import versioning), so Go's minimal
version selection has no major-version boundary to protect against exactly this.

Option 2 — shelling out to the `docker` CLI — deserves a fair hearing, because it is genuinely
simpler to write and to debug (you can copy the command out of a log and run it). It was rejected on
three counts: parsing human-oriented CLI output to get an exit code and an OOM flag is brittle in a
way an API response is not; it adds a runtime dependency on the CLI binary being installed and
version-compatible; and the brief explicitly asks for the official SDK. The SDK also gives typed
access to `State.OOMKilled`, which D-032 relies on to prove memory limits are enforced.

Option 3 was rejected because the official SDK is the reference implementation and third-party
clients lag on API changes.

**Cost accepted.** Pinning means no automatic security or bugfix updates for the Docker SDK, and
`go get -u ./...` will re-break the build by floating `go-connections` forward again. Recorded here
so the fix (`go get github.com/docker/go-connections@v0.5.0`) is findable. Upgrading `docker/docker`
to v28.x later would likely allow un-pinning; not done now because v27.3.1 matches the local daemon
and matching versions removes a variable while the core behaviour is being proven.

---

<a id="d-032"></a>

### D-032 — The daemon socket is discovered by probing known paths, with `DOCKER_HOST` as the override

**Decision.** If `DOCKER_HOST` is set, use it. Otherwise probe, in order,
`~/.docker/run/docker.sock` then `/var/run/docker.sock`, and use the first that exists and is
actually a Unix socket.

**Alternatives considered.**

1. **`client.FromEnv` alone** — what the SDK documentation shows, and what the first version did.
2. **Parse the Docker CLI's context store** — read `currentContext` from `~/.docker/config.json`,
   then resolve the endpoint from `~/.docker/contexts/meta/<sha256(name)>/meta.json`, exactly as
   the `docker` CLI does.
3. **Require `DOCKER_HOST` explicitly** and fail fast if it is unset.

**Why probing.** Option 1 was implemented first and it did not work here, in a way worth recording
because the symptom is misleading. `docker ps` worked fine from the shell, but the Go service
reported *"Cannot connect to the Docker daemon at unix:///var/run/docker.sock"* — so the daemon
looked simultaneously up and down. The cause: `client.FromEnv` reads only `DOCKER_HOST` and then
falls back to a hardcoded `/var/run/docker.sock`, whereas the `docker` CLI reads its **context**
configuration. On Docker Desktop for macOS the active context is `desktop-linux`, pointing at
`~/.docker/run/docker.sock`, and `/var/run/docker.sock` does not exist at all. The SDK and the CLI
were looking in different places, and only the CLI was right.

This mattered concretely: it made every integration test `t.Skip`, and **a skipped Go test reports
as PASS** — so the suite was green while proving nothing (see D-033).

Option 2 is the fully correct fix — it is what the CLI does, so it agrees with the CLI by
construction, including for remote and SSH contexts. It was rejected under D-010 as roughly 60 lines
coupled to Docker's on-disk layout (an undocumented directory structure, a SHA-256-of-name path
scheme, a JSON schema that can change) to cover cases this project does not have.

Option 3 is honest but pushes an environment quirk onto every user and every CI config.

**Limitation, stated plainly.** If someone's active Docker context points at a *remote* daemon while
a local socket also exists, probing picks the local one and silently disagrees with `docker` CLI.
The fix is to set `DOCKER_HOST`, which is why the env var is checked first and takes precedence.
Documented in the README rather than solved. If this project ever targets remote daemons for real,
implement option 2.

---

<a id="d-033"></a>

### D-033 — Two test tiers: a fake executor for logic, real Docker for enforcement

**Decision.** Two distinct suites.

- **`internal/**/*_test.go`** — the queue, retry policy, backoff arithmetic, state machine and HTTP
  validation, all against the `Fake` executor. Runs in ~5s under `-race`, needs nothing installed.
- **`test/integration_test.go`** — the whole service, real HTTP, real containers, no mocks. Runs in
  ~25s and requires Docker.

**Alternatives considered.**

1. **Only integration tests.** Maximum realism, nothing faked.
2. **Only unit tests**, with the Docker executor tested by hand.
3. **A mocking framework** (`gomock`/`testify`) generating an Executor mock.

**Why two tiers.** Option 1 cannot test the thing that most needs testing. `TestBackoffIsExponential`
needs four failing attempts; with real containers each attempt costs ~250ms of startup on top of the
backoff, so testing a 100ms base is impossible — container noise exceeds the signal. The fake makes
attempts instant, so backoff timing is measured cleanly. Worse, option 1 makes the whole suite
un-runnable without Docker, which discourages running it at all.

Option 2 is what the enforcement requirement forbids. The brief specifically asks to prove limits
are *"actually applied (not just requested)"*, and no amount of faking can demonstrate that — only a
real kernel enforcing a real cgroup can (D-034).

Option 3 was rejected under D-010: `Fake` is ~60 lines of plain Go with two ready-made handlers
(`AlwaysFails`, `FailsThenSucceeds`), it is easier to read than generated mock DSL, and it avoids a
dependency and a code-generation step.

**The dividing rule.** Fake the executor to test *scheduling* decisions (what runs, when, how often,
in what order). Use real Docker to test *execution* facts (exit codes, OOM kills, cgroup values,
log capture). Two suites because there are genuinely two questions.

**Fail-loud, because of what D-032 exposed.** `t.Skip` reports as PASS. When the socket bug made
Docker unreachable, the suite printed `ok` while every enforcement proof was skipped — precisely the
false confidence a test suite exists to prevent. So the harness now skips only when Docker is
genuinely absent, and setting `SCHED_REQUIRE_DOCKER=1` (which `make test-integration` does) converts
that skip into a hard failure. **Default to skip for a developer without Docker; fail loudly wherever
the proofs are supposed to run.**

---

<a id="d-034"></a>

### D-034 — Limit enforcement is proven from kernel state, not from wall-clock timing

**Decision.** Prove resource limits three ways, all reading state the kernel wrote:

1. **`cpu.max` and `memory.max`** read from inside the container — the cgroup v2 files the kernel
   uses. Asserted to equal the requested values (`0.5 CPU` → `50000 100000`; `64 MB` → `67108864`).
2. **`cpu.stat`'s `nr_throttled` / `throttled_usec`** after a CPU-bound loop — the kernel's own count
   of scheduling periods in which it stopped the container for exhausting quota.
3. **`State.OOMKilled`** from the daemon after `tail /dev/zero` under a 64 MB limit.

**Alternatives considered.**

1. **Wall-clock comparison** — run identical CPU work at 1.0 and 0.25 CPUs and assert the second
   takes ~4× longer.
2. **Inspect the container's `HostConfig`** and assert `NanoCPUs`/`Memory` are what we sent.
3. **Assert exit code 137** for the memory test and call it proven.

**Why kernel state.** Option 1 is the obvious approach and it is the weakest. It is a *timing* test
on a shared machine: it fails spuriously under load, its threshold has to be loosened until it
barely asserts anything, and it depends on host core count and CPU speed. Reading `cpu.stat` gets a
strictly stronger result deterministically — `nr_throttled` is a counter the kernel increments *only*
when it actually throttles, so a non-zero value cannot occur unless the limit is being enforced. The
run recorded 28 throttling events totalling 2.05s of enforced idle time.

Option 2 is the trap the requirement is specifically written against. `HostConfig` echoes back
*what we asked for* — it would pass identically if the daemon ignored the values entirely. It proves
the request, not the enforcement, which is exactly the distinction the brief draws.

Option 3 is insufficient for the same reason in a subtler form: **137 is just `128 + SIGKILL`**. It
means "something sent SIGKILL", not "the memory limit killed it" — a manual `docker kill` produces
the same code. `State.OOMKilled` is the daemon recording *why*. The test asserts both, with
`OOMKilled` as the load-bearing one.

**Consequence to be aware of.** These assertions read cgroup **v2** paths, so they are specific to a
cgroup-v2 host (Docker Desktop's Linux VM, and any modern Linux distribution). On a cgroup-v1 host
the paths differ (`/sys/fs/cgroup/memory/memory.limit_in_bytes`) and these tests would fail rather
than skip. Accepted: v1 is end-of-life, and a loud failure on an unexpected host is better than a
silently weakened assertion — the same principle as D-033.

---

<a id="d-035"></a>

### D-035 — Shutdown stops the HTTP server first, then drains the pool, with a bounded grace period

**Decision.** On `SIGINT`/`SIGTERM`: (1) stop the HTTP server, (2) close the queue so no new work is
accepted but already-queued work is still drained, (3) wait for workers to finish in-flight
attempts, bounded by `SCHED_SHUTDOWN_TIMEOUT` (default 30s), (4) if the grace period expires, cancel
the pool context, which aborts in-flight Docker calls — while container cleanup still runs on
detached contexts (D-029).

**Alternatives considered.**

1. **Exit immediately** on signal. Simplest; the OS reclaims everything.
2. **Drain the pool first, then stop the HTTP server.**
3. **Wait indefinitely** for every job, with no grace bound.
4. **Kill in-flight containers** on shutdown rather than letting attempts finish.

**Why this order.** Option 2 is the specific ordering bug worth recording: if workers are drained
before the HTTP server stops, a request arriving during the drain enqueues work into a pool that has
already exited, so the job sits `queued` forever with nothing to run it. Closing the front door
before draining the room makes "accepted" mean "will be worked on."

Option 1 loses every running container's result — the container keeps running (it is the daemon's
child, not ours), finishes, and nobody records the outcome, so the record is stuck at `running` and
the container is orphaned (D-026). Since attempts are typically seconds long, waiting is cheap and
buys correct final states for most in-flight work.

Option 3 is how a deploy hangs: one job with a 30-minute timeout blocks the rolling restart for 30
minutes. A bounded grace period keeps shutdown predictable, and orchestrators (`docker stop`,
Kubernetes) enforce their own bound anyway — better to lose in-flight work deliberately at 30s than
to be `SIGKILL`ed arbitrarily at whatever the platform's limit is.

Option 4 was rejected because attempts are usually short; killing them converts work that was about
to succeed into a failure.

**What is deliberately given up.** Anything still queued at shutdown is gone — this is D-008's
no-durability limitation, not a separate one. Pending retry timers also vanish (D-018), so a job in
backoff across a restart is abandoned mid-flight. Both disappear once the store is durable, which is
the single change that would fix them together.

**Go note.** `signal.NotifyContext` turns a signal into a `context.Context`, so shutdown uses the
same cancellation primitive as attempt timeouts rather than a separate signal-channel mechanism.
One concept covers both.

---

## Stage 1 status

Complete, and verified against real Docker. Evidence is in `README.md` under *What the integration
suite actually proves*. Decisions D-001 to D-035 are all `active`; none have been superseded yet.

Stage 2 (Prometheus metrics, docker-compose with Prometheus and Grafana) and Stage 3 (Kubernetes
`batch/v1` Jobs via client-go against kind) are not started. Entries D-013, D-018 and D-024 each
name Stage 3 as their expected revisit trigger.

---

## Stage 2 — Observability

<a id="d-036"></a>

### D-036 — Metric taxonomy: counters for events, scrape-time gauges for levels, histograms for distributions

**Decision.** Classify every metric by what it measures, and let that pick the type:

| Question | Type | Metric |
|---|---|---|
| How many things happened? | Counter | `jobs_submitted_total`, `jobs_completed_total{status}`, `job_attempts_total{outcome}`, `job_retries_total`, `submissions_rejected_total{reason}` |
| How many things are there *right now*? | Gauge | `queue_depth`, `jobs_by_status{status}`, `workers_busy`, `workers_total` |
| How long did things take? | Histogram | `attempt_duration_seconds{outcome}`, `queue_wait_seconds` |
| How busy was each worker? | Counter of seconds | `worker_busy_seconds_total{worker}` (see D-037) |

**Alternatives considered.**

1. **Gauges for everything**, sampled periodically — one mental model, no counter-vs-gauge decision.
2. **Counters for everything**, deriving levels by subtracting (`submitted_total - completed_total`).
3. **Summaries instead of histograms** for durations.

**Why the split.** Option 1 is the classic monitoring mistake: a gauge sampled every 15s *loses
events between samples*. If 40 jobs succeed and 40 fail between two scrapes, a "jobs succeeded"
gauge shows whatever the instantaneous value was and the throughput information is simply gone.
Counters are monotonic, so `rate()` reconstructs throughput correctly regardless of scrape interval,
and — critically — a counter survives a missed scrape, because the next scrape still carries the
total. Throughput and failure rate, two of the three dashboard panels the brief asks for, are only
computable from counters.

Option 2 fails in the other direction. `submitted - completed` looks like queue depth but silently
drifts: it counts jobs that are running, retrying, *and* rejected as though they were queued, and
after a restart the counters reset while real queue state does not. Levels should be *observed*, not
*inferred* (D-038).

Option 3: summaries compute quantiles client-side, which means they **cannot be aggregated across
instances** — averaging p95s from two schedulers is statistically meaningless. Histograms ship
bucket counts, so `histogram_quantile()` aggregates correctly. This project is one process today but
explicitly heading toward more; picking summaries would silently poison any future aggregation.

**Naming.** Prometheus convention throughout: `jobsched_` prefix, `_total` suffix on counters, base
units (seconds, not milliseconds) so the unit is never ambiguous.

---

<a id="d-037"></a>

### D-037 — Per-worker utilization is a counter of busy-seconds, not a busy/idle gauge

**Decision.** Each worker increments `jobsched_worker_busy_seconds_total{worker="N"}` by the wall
time it spent executing. Utilization is a query, not a stored value:
`rate(jobsched_worker_busy_seconds_total[1m])` yields a fraction between 0 and 1.

**Alternatives considered.**

1. **A 0/1 gauge per worker** — `worker_busy{worker="3"} 1` while running.
2. **A pre-computed utilization gauge** — the pool calculates a rolling percentage itself.
3. **Only the aggregate `workers_busy`** gauge, with no per-worker breakdown.

**Why busy-seconds.** Option 1 is the intuitive design and it is aliased by the scrape interval. A
0/1 gauge only reports the instant the scrape landed, so a worker running 200ms jobs back-to-back at
100% utilization reads as 1 or 0 essentially at random — and averaging those samples estimates
utilization from a handful of coin flips per minute. The counter integrates over *all* time between
scrapes, so no busy period is missed however short. This is the same argument as D-036, applied to
one metric.

Option 2 puts the time window inside the application, so changing "utilization over 1m" to "over 5m"
becomes a code change and a redeploy. Keeping it a counter pushes the window into PromQL, where the
dashboard can offer both, and it means the application stores no derived state that could drift.

Option 3 is what the brief's "worker load" panel minimally needs, but it cannot answer "is the load
*even*?" — one pegged worker and three idle ones looks identical to four half-busy workers. Since
the pool assigns work by whichever goroutine reaches `Pop` first, skew is a real thing to be able to
see.

**Cardinality is bounded and small.** One series per worker, fixed at startup by `SCHED_WORKERS`
(default 4). Workers are indexed integers, not dynamic identities, so this cannot grow at runtime —
which is what makes a per-worker label acceptable here where a per-job label would not be (D-041).

**Tradeoff accepted.** Utilization is no longer readable from the raw `/metrics` output; you need
`rate()` to interpret it. That is the correct place for the computation, but it does mean
`curl /metrics` shows a large ever-growing number rather than a percentage.

---

<a id="d-038"></a>

### D-038 — Level metrics are computed at scrape time from the real data structures

**Decision.** `queue_depth`, `jobs_by_status{status}` and `workers_busy` are registered as
`GaugeFunc` collectors. On each scrape, Prometheus calls a closure that reads the live queue length,
the store's status tally, and the pool's busy count.

**Alternatives considered.**

1. **Increment/decrement gauges at each state transition** — `queueDepth.Inc()` on submit,
   `.Dec()` on pop, and so on.
2. **A background goroutine** sampling the values into gauges every few seconds.
3. **Expose the values only on the API's existing endpoints** and skip the gauges.

**Why scrape-time.** Option 1 is the standard approach and it is a drift machine. Correctness
requires that *every* path which changes a level also touches the gauge — and this scheduler has
many: submit, pop, retry-requeue, queue-full rejection, permanent failure, shutdown abandonment. Miss
one branch, or return early on an error path, and the gauge diverges from reality permanently, with
nothing to correct it. The failure is silent and cumulative: a dashboard that reads "12 jobs queued"
forever while the queue is empty.

Reading `q.Len()` at scrape time makes drift structurally impossible — there is no second copy of
the number to disagree with the first. It is the same reasoning as D-015 (the queue stores IDs so
job state cannot have two versions), applied to metrics.

Option 2 has the same self-healing property but adds a goroutine, a sampling interval that is
unrelated to the scrape interval, and staleness of up to one sampling period. Strictly worse than
letting the scrape drive it.

**What it costs, and how that is bounded.** The work now happens on the scrape path, so a slow
collector slows or times out scrapes. `q.Len()` is a mutex-guarded integer read. `workers_busy` is an
atomic load. The one with real cost is the store tally, which is O(number of jobs) — so the store
got a dedicated `Counts()` method that tallies statuses under a read lock **without cloning
records**, rather than reusing `List()`, whose defensive copying (D-012) would allocate the entire
job history on every scrape. Correct metric, wrong cost; the cheap path is a separate method.

**Known limitation.** `Counts()` walks the whole map, and the map grows without bound (D-008). Scrape
cost therefore grows with total jobs ever submitted. The fix is the same TTL reaper D-008 already
names, so this adds urgency to that gap rather than creating a new one.

---

<a id="d-039"></a>

### D-039 — A dedicated registry, with Go and process collectors registered explicitly

**Decision.** Build a `prometheus.NewRegistry()` rather than using
`prometheus.DefaultRegisterer`, and explicitly add `collectors.NewGoCollector()` and
`collectors.NewProcessCollector()`.

**Alternatives considered.**

1. **The default global registry** — `promauto` registers into it implicitly; `promhttp.Handler()`
   serves it. Least code.
2. **A custom registry with no runtime collectors** — only our own metrics.

**Why a dedicated registry.** The global registry is process-global mutable state, and its failure
mode is hostile: registering the same metric name twice panics. With a global registry, two tests
that each construct a scheduler panic the second time, so tests end up sharing one metrics instance
and leaking counter values into each other — assertions then depend on test execution order. A
per-instance registry makes `metrics.New()` return an isolated object, so every test gets clean
counters and `metrics_test.go` can assert exact values.

It also means anything a *dependency* registers into the global registry cannot silently appear on
our endpoint.

**Why keep the Go/process collectors (option 2 rejected).** They are the cheapest useful telemetry
available: goroutine count, heap size, GC pause, open file descriptors, CPU. For this service
specifically, `go_goroutines` is close to a correctness check — a leak in the worker pool or in the
retry timers shows up there first, and D-018's timer-based retries are exactly the kind of thing
worth being able to watch. Losing them to use a custom registry would be a bad trade, so they are
re-added by hand: two lines.

---

<a id="d-040"></a>

### D-040 — `/metrics` is served on the main HTTP port

**Decision.** `GET /metrics` sits on the same `ServeMux`, and therefore the same port, as `/jobs`.

**Alternatives considered.**

1. **A second listener on a separate admin port** (e.g. `:9090`), so metrics are never exposed on
   whatever network serves the API.
2. **A Unix socket** for metrics.

**Why the same port.** Option 1 is the better production posture and is genuinely what a real
deployment should do — metrics often expose internal detail, and operators want them on a network
the public cannot reach. It is rejected here on scope: a second listener means a second
`http.Server`, a second address to configure, second shutdown handling, and a second port to publish
in compose and later in a Kubernetes Service — machinery whose only benefit is a network boundary
this project explicitly does not have (no auth, no RBAC, by instruction).

**Consequence, stated plainly.** Anyone who can reach the API can read the metrics. Given that
anyone who can reach the API can already *run arbitrary shell commands in containers* (D-023), the
metrics endpoint is not the weak link. If authentication is ever added, split the ports at the same
time — recorded as the trigger.

---

<a id="d-041"></a>

### D-041 — No unbounded labels: nothing is labelled by job ID, command, or image

**Decision.** Labels are restricted to small closed sets: `status` (5 values), `outcome` (3),
`reason` (2), `worker` (`SCHED_WORKERS`, fixed at startup). Job IDs, command strings, image names,
container IDs and error messages are never labels.

**Alternatives considered.**

1. **Label metrics by `job_id`** — makes per-job timing queryable directly in Prometheus.
2. **Label by `image`** — plausibly bounded in practice, and genuinely useful ("which image fails
   most?").

**Why closed sets only.** Option 1 is the canonical way to destroy a Prometheus server. Every
distinct label value creates a **new time series**, retained for the full retention period, held in
memory in the head block. Job IDs are unique by construction (D-006), so a metric labelled by job ID
creates one series per job forever — the failure is not a slow dashboard, it is the Prometheus
process OOMing, and it keeps failing after the offending jobs are long gone. Per-job detail belongs
in the job record (D-014), which is exactly where it already is; `GET /jobs/{id}` answers per-job
questions and metrics answer aggregate ones.

Option 2 is the tempting middle case and is deliberately declined. `image` is *user-supplied* through
the API, so its cardinality is controlled by clients, not by us. It is bounded only by convention,
and a client submitting jobs with unique image tags would produce the same unbounded growth as
option 1 with no warning. **A label's cardinality must be bounded by the server, not by a client's
good behaviour.**

**Cost accepted.** "Which image fails most often?" is not answerable from metrics. It is answerable
by listing jobs from the API. If it becomes a routine question, the right fix is a bounded
allow-list of known images, not a free-form label.

---

<a id="d-042"></a>

### D-042 — Instrumented packages take a concrete `*metrics.Metrics`, not an interface

**Decision.** `worker.Pool` and `api.Server` hold a `*metrics.Metrics` struct directly. There is no
`Observer`/`Recorder` interface, and no no-op implementation.

**Alternatives considered.**

1. **An `Observer` interface** in `worker`, with a Prometheus implementation and a no-op for tests —
   symmetric with the `Executor` and `Store` seams (D-009).
2. **Callback function fields** on the pool config (`OnJobCompleted func(...)`).
3. **Package-level `promauto` metrics** referenced directly, with no injection at all.

**Why the concrete type.** This is D-010's rule applied against my own precedent. D-009 kept
interfaces for the executor and store because each had a *named, scheduled* second implementation.
Metrics has none: there is no plan for a StatsD backend, and tests do not need a no-op because a real
`metrics.New()` is cheap, isolated per-instance (D-039), and — more useful — lets tests assert on
actual counter values rather than merely verifying that a method was called. An interface here would
buy substitutability nobody has asked for and would make tests *weaker*.

Option 3 is the one to actively avoid: package-level metrics are global state, they panic on
double registration, and they make it impossible to run two schedulers in one test binary.

**Coupling accepted, and its bound.** `internal/worker` and `internal/api` now import
`internal/metrics`, which imports `client_golang`. Swapping metrics libraries would touch those
packages. That is a real cost, and it is smaller than it looks: instrumentation is a handful of call
sites, all of the form `m.X.Inc()`. If a second backend ever appears, extracting an interface then is
mechanical — and by then it will have a named consumer, which is the standard D-010 sets.

---

<a id="d-043"></a>

### D-043 — Explicit duration buckets tuned to container startup, not `DefBuckets`

**Decision.** `attempt_duration_seconds` and `queue_wait_seconds` use explicit buckets:
`0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300`.

**Alternatives considered.**

1. **`prometheus.DefBuckets`** — the library default: `.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10`.
2. **`ExponentialBuckets`** — generated powers, wide coverage with few buckets.
3. **Native histograms** — Prometheus's newer sparse-bucket format, no bucket choice needed.

**Why explicit.** `DefBuckets` is tuned for *HTTP handler latency*, where sub-millisecond responses
matter, and it spends its first six buckets below 100ms. Nothing here can finish that fast: D-022
gives every attempt a fresh container, and container create+start measured ~250ms–1s in the Stage 1
runs. So six of eleven default buckets would always be empty, while everything above 10s — a normal
range for a real job, and the range where `SCHED_MAX_JOB_TIMEOUT` (30m) lives — would collapse into
`+Inf`, making p95 uncomputable exactly where it matters. **Buckets must bracket the distribution
you actually have**, and Stage 1's measurements are what that distribution looks like.

Option 2 spreads buckets evenly in log space, which wastes resolution in ranges we know are empty.
Option 3 is the future-correct answer, but it needs opt-in on both client and server and is still
stabilising; not worth the setup risk for a demo dashboard.

**Cost.** Ten buckets per label value, per histogram — each bucket is a separate series. With three
`outcome` values that is ~36 series for attempt duration. Bounded and small, consistent with D-041.

**Known imprecision.** The top bucket is 300s while the maximum permitted job timeout is 1800s, so
anything over five minutes lands in `+Inf` and its p99 is a lower bound. Accepted deliberately:
adding buckets to cover a ceiling that few jobs approach costs series on every scrape. Revisit if
long jobs become normal.

---

<a id="d-044"></a>

### D-044 — Compose runs the scheduler too, with the Docker socket bind-mounted

**Decision.** `docker compose up` starts all three services: the scheduler (built from a
`Dockerfile` in this repo), Prometheus, and Grafana. The scheduler container bind-mounts
`/var/run/docker.sock` so it can create job containers on the host daemon.

**Alternatives considered.**

1. **Compose runs only Prometheus and Grafana**; the scheduler stays on the host and Prometheus
   scrapes it via `host.docker.internal:8080`. No socket mount, no Dockerfile.
2. **Docker-in-Docker** — run a nested `dind` daemon inside the scheduler container.
3. **Compose runs everything, scheduler included, with no Docker access** — jobs would not run.

**Why all three in compose.** The brief asks for this so the metrics can be *seen*, and one command
producing a working dashboard is materially better for that than a command plus "now also start the
scheduler in another terminal with the right port." It also makes the compose file document how the
service is actually deployed, which Stage 3 will need anyway.

Option 1 is genuinely the safer setup and is kept as a documented fallback in the README, because
`host.docker.internal` is a Docker Desktop convenience that does not exist on plain Linux, so the
"simpler" option is actually the less portable one.

Option 2 (dind) gives true daemon isolation — job containers would be invisible to the host — but it
requires `--privileged`, which is a *strictly larger* grant than the socket mount, plus a nested
storage driver and image cache. More privilege for isolation this project does not need.

**The security fact, stated plainly rather than buried.** Bind-mounting the Docker socket into a
container is equivalent to **granting that container root on the host**. Anything that can talk to
the socket can start a privileged container that mounts the host filesystem. Combined with D-023
(jobs are arbitrary shell commands) and no authentication, this compose stack must be treated as a
**local development tool only**. It is not a deployment posture and should never be exposed to a
network. This is written down here so the tradeoff is a decision rather than an accident.

It is acceptable here for a specific reason: the scheduler already needed the daemon's full API from
the host in Stage 1 — running it in a container does not grant it any capability it did not already
have. What changes is only *what else* shares that container. Stage 3 removes this entirely: the
Kubernetes executor talks to an API server with a service account, not to a root-equivalent socket.

**Implementation note.** The socket path works because of D-032's discovery order: inside the
container `$HOME/.docker/run/docker.sock` does not exist, so the probe falls through to
`/var/run/docker.sock`, which is where the bind mount lands. The host-side path differs on Docker
Desktop, but Docker resolves `/var/run/docker.sock` inside its own VM, so the same compose file
works on macOS and Linux without change.

**Sibling containers, not children.** Job containers are created on the *host* daemon, so they are
siblings of the scheduler container, not nested inside it. They therefore survive
`docker compose down` if the scheduler is killed uncleanly — the D-026 orphan case, and why
`make clean-containers` exists.

---

<a id="d-045"></a>

### D-045 — Grafana is provisioned from files, with anonymous access enabled

**Decision.** The datasource and the dashboard are provisioned by YAML/JSON files mounted into the
container at startup. Anonymous access is enabled with the Admin role, so the dashboard opens with
no login.

**Alternatives considered.**

1. **Configure Grafana through the UI** after it starts — add the datasource, import a dashboard.
2. **Push the dashboard via the Grafana HTTP API** from a setup script.
3. **Keep the default `admin/admin` login.**

**Why provisioning.** Option 1 makes the dashboard a manual setup step that is not captured
anywhere, so it is lost on `docker compose down -v` and cannot be reviewed or diffed. Provisioning
makes the dashboard part of the repository: it comes up identically every time, and a change to a
panel is a reviewable diff rather than a click nobody witnessed.

Option 2 needs a script that waits for Grafana's API to be ready, handles auth, and is idempotent —
more moving parts than a mounted file, for the same result.

**Why anonymous access (option 3 rejected).** Default `admin/admin` means every `compose up` is
followed by a login form and a forced password-change prompt before anything is visible. For a
local, single-command demo, that friction is pure cost. This is safe *only* under the same condition
as D-044: this stack binds to localhost and is a development tool. A deployed Grafana must have
authentication; the compose file carries a comment saying so at the point where it is enabled.

**Tradeoff accepted.** Provisioned dashboards are read-only-ish in the UI: edits made in the browser
do not persist across restarts, because the file is the source of truth. That is the intended
behaviour — it prevents dashboard drift — but it does mean the workflow for changing a panel is
"edit in the UI, export JSON, commit it", which is clunkier than clicking Save.

---

<a id="d-046"></a>

### D-046 — The dashboard answers the three questions the brief asked, and is built on `rate()`

**Decision.** One dashboard with panels for throughput, failure rate, and worker load — plus queue
depth, job states, and duration percentiles as supporting context. Every rate-based panel uses
`rate(...[1m])` over counters, and the failure-rate panel is a ratio of counters, not a stored
percentage.

**Alternatives considered.**

1. **Many small dashboards**, one per concern.
2. **Raw counter values** on the graphs (`jobsched_jobs_completed_total`) rather than rates.
3. **`increase()` instead of `rate()`.**

**Why one dashboard.** The brief asks for a starter dashboard to *see the system working*. Splitting
across dashboards means correlating a throughput dip with a queue-depth spike requires switching
views, which is exactly the correlation this is meant to make obvious.

**Why rates, not raw counters (option 2 rejected).** A counter graph is a monotonically rising line;
every interesting event — a burst of failures, a throughput collapse — appears as a subtle change in
*slope*, which the eye reads badly. `rate()` differentiates it into jobs-per-second, where the same
event is an obvious spike. This is the payoff for D-036's insistence on counters: the raw metric is
chosen for correctness, and the dashboard does the transformation.

`rate()` over `increase()` because `rate()` is per-second and therefore independent of the window,
so panels stay comparable if the window changes. Both handle counter resets from a restart.

**Failure rate as a ratio of counters.** Computed as
`sum(rate(jobs_completed_total{status="failed"}[5m])) / sum(rate(jobs_completed_total[5m]))`. Doing
the division in PromQL rather than storing a percentage means the window is a dashboard setting, and
it aggregates correctly across replicas later. A 5m window is used here rather than 1m because a
ratio over a short window is noisy when the denominator is small — one failure out of two jobs is
50%, which is technically true and practically meaningless.

**Known rough edge.** With no traffic, the failure-rate expression is 0/0 and renders as `NaN`/"No
data" rather than 0. Left as is: fabricating a 0 would imply "everything is fine" when the truth is
"nothing is happening", which the throughput panel already shows correctly.

---

<a id="d-047"></a>

### D-047 — 5-second scrape interval, with Prometheus and Grafana state on named volumes

**Decision.** `scrape_interval: 5s` (against Prometheus's 15s default), and both services keep their
data in named Docker volumes.

**Alternatives considered.**

1. **The default 15s interval.**
2. **1s**, for a near-live view.
3. **No volumes** — all state is discarded on `compose down`.

**Why 5s.** This is a demo whose jobs finish in seconds. At 15s, a burst of ten jobs can begin and
end entirely between two scrapes, so the throughput graph shows a single unrepresentative point and
the queue-depth gauge may never observe a non-zero value at all — the dashboard would suggest
nothing happened. 5s makes short-lived activity visible while keeping the sample count sane.

Option 2 (1s) is counterproductive: `rate()` over a 1m window needs only a handful of samples, and
every scrape runs the scrape-time collectors (D-038) including the O(n) store walk. Faster scraping
would increase the cost of the very thing being measured.

**Consequence to be honest about.** 5s is a **demo** setting, tuned to make short jobs visible on a
laptop. A production deployment with thousands of jobs should return to 15s or higher; the value is
in `prometheus.yml` where it can be changed without touching code.

**Why volumes (option 3 rejected).** Without them, every restart of the stack starts the graphs
empty, which makes it impossible to look at anything that happened before the last restart. Volumes
cost nothing and are removable with `docker compose down -v`, which the README documents as the
reset command.

**Not addressed.** Prometheus retention is left at its default (15 days). For a laptop demo the data
volume is trivial, and tuning it would be configuration without a purpose.

---

<a id="d-048"></a>

### D-048 — Dashboard queries are verified programmatically against a live Prometheus

**Decision.** Treat the dashboard's PromQL as code that can be wrong. A script extracts every `expr`
from `jobsched.json`, runs each against the Prometheus HTTP API under real load, and reports which
returned data, which returned nothing, and which errored.

**Alternatives considered.**

1. **Open the dashboard and look at it.** The obvious check, and what most people do.
2. **Validate the JSON schema only** — confirm it parses and the panel types exist.
3. **Don't verify.** Prometheus and Grafana are configured declaratively; if the target is `up` and
   the dashboard provisions, assume the panels work.

**Why this earned its place: it caught a real bug that all three alternatives would have missed.**
The "Pool utilisation" panel used
`sum(rate(jobsched_worker_busy_seconds_total[1m])) / jobsched_workers_total`, which is valid PromQL,
returns no error, and silently produces **nothing at all**. The reason is vector matching:
`sum()` strips every label, so the left operand has an empty label set while the right still carries
`instance` and `job` from the scrape. A binary operation between two instant vectors joins on
identical label sets, finds no pairs, and returns an empty result. The fix is to wrap both sides:
`… / sum(jobsched_workers_total)`.

Option 1 would have shown "No data" — indistinguishable from "the system is idle", which is exactly
what a freshly started demo looks like. The bug survives casual inspection precisely because its
symptom is the normal empty state. Option 2 passes: the JSON was well-formed and both panel types
(`stat`, `timeseries`) are registered core plugins — both of which were separately confirmed, and
neither of which says anything about whether a query returns rows.

**The general principle.** A query that returns an empty result is not an error in PromQL; it is a
valid answer. So "no error" is not evidence of correctness, and **the only way to know a panel works
is to run its query against real data and check that rows come back.** This is the same reasoning as
D-033's fail-loud rule: a green signal that can also mean "nothing happened" is not a green signal.

**What is verified and what is not.** The script confirms all 21 expressions return rows under load.
Panel *rendering* was checked separately and more cheaply — the dashboard page returns 200, the
provisioned JSON matches the file, the datasource reports healthy, and both panel types are
registered plugins. Rendering was **not** verified with a real browser: doing so needs a headless
Chromium install, which is a large dependency for a check that only guards against a panel-plugin
mismatch already covered above. If the dashboard grows panels with plugin-specific options, revisit.

---

## Stage 2 status

Complete and verified against the running stack. Decisions D-036 to D-048 are `active`; nothing from
Stage 1 was superseded — Stage 2 added instrumentation without changing the scheduler's behaviour.

Evidence: `make test-unit` covers the metric definitions, scrape-time gauges, and the pool's
instrumentation with exact counter assertions. Against the live stack, the Prometheus target reports
`up`, all 21 dashboard queries return data under load, and the counters are self-consistent —
`jobs_submitted_total` equalled `completed + queued + running + retrying` exactly throughout the run.

Stage 3 (Kubernetes `batch/v1` Jobs via client-go against kind) is not started. D-044 is the entry
Stage 3 changes most: the Docker socket mount disappears entirely in favour of an API server and a
service account.

---

## Stage 3 — Kubernetes Integration

<a id="d-049"></a>

### D-049 — The scheduler runs *outside* the cluster and authenticates with a kubeconfig

**Decision.** The scheduler stays a host process (or a container with a kubeconfig) and talks to the
kind cluster's API server as a normal client. It is not deployed into the cluster.

**Alternatives considered.**

1. **Run the scheduler as a Deployment inside the cluster**, using the in-cluster config
   (`rest.InClusterConfig()`) and a mounted ServiceAccount token.
2. **Run in-cluster with `cluster-admin`** bound to its ServiceAccount, to avoid writing a Role.

**Why out-of-cluster.** Option 1 is how you would actually deploy this, and it is ruled out by the
brief's own scope constraint. Creating Jobs from inside a Pod requires a ServiceAccount plus a Role
granting `create/get/list/watch/delete` on `batch/v1` Jobs and `get/list/watch` on Pods, plus a
RoleBinding — that is **RBAC**, which the brief explicitly excludes. Running outside sidesteps the
entire authorization surface: `kind` writes an admin kubeconfig to `~/.kube/config`, and client-go
picks it up the same way `kubectl` does.

Option 2 would technically satisfy "no RBAC work" by granting everything, which is worse than doing
RBAC properly — it is the anti-pattern, not the shortcut.

**What this costs, stated plainly.** This is *not* a production topology. A real deployment runs
in-cluster with a least-privilege Role, so the credential is short-lived, rotated, and scoped to one
namespace. Here the scheduler holds a long-lived cluster-admin credential belonging to a human. That
is acceptable for a local kind cluster on a laptop and would not be acceptable anywhere else.

**A secondary benefit that made this easier.** Because the scheduler is a client rather than a
workload, there is no image to build, push, or side-load into the cluster to iterate on scheduler
code — `go run` against the cluster works immediately. That removes what would otherwise be the
slowest part of the Stage 3 development loop.

**Consequence for the Stage 2 stack.** The compose scheduler cannot easily reach kind's API server
(kind's kubeconfig points at `127.0.0.1:<random port>`, which means something different inside a
container). So Kubernetes mode runs on the host; see D-062 for how Prometheus still scrapes it.

---

<a id="d-050"></a>

### D-050 — One `batch/v1` Job per attempt, rather than a bare Pod

**Decision.** Each attempt creates a `batch/v1` Job containing exactly one Pod. Attempt 2 of a job
creates a *different* Kubernetes Job.

**Alternatives considered.**

1. **Create a bare Pod** with `restartPolicy: Never` and manage it directly.
2. **One Kubernetes Job per scheduler job**, letting the Job controller produce the retries as
   multiple Pods.

**Why a Job per attempt.** The brief asks for `batch/v1` Jobs, but the choice is worth defending
anyway, because option 1 is genuinely simpler and there is a real irony here: **we disable the Job
controller's headline feature (retry, see D-051), so we are using Job mostly for its lifecycle
management.** That management is still worth having:

- **Garbage collection by ownership.** The Job owns its Pod through an `ownerReference`, so deleting
  the Job deletes the Pod. With bare Pods, cleanup is entirely ours to get right.
- **`ttlSecondsAfterFinished`.** The Job controller deletes finished Jobs server-side after a TTL.
  This is a *server-side* safety net for orphans, which is strictly better than what Stage 1 had:
  D-026 accepted that a `SIGKILL`ed scheduler leaks containers, and here the cluster cleans up after
  us even if the scheduler never comes back.
- **A terminal status to watch.** A Job reports `Complete` or `Failed` conditions, including cases a
  Pod alone would not surface, such as `DeadlineExceeded`.

Option 2 is the arrangement that *looks* most natural — a scheduler job maps to a Kubernetes Job —
and it is rejected because it hands retry policy to the Job controller (D-051). Keeping the mapping
at **one Kubernetes Job = one attempt** means `backoffLimit: 0` is honest, the attempt's Pod is
unambiguous, and the per-attempt record (D-014) has exactly one Kubernetes object behind it.

**The invariant this buys.** One Job → one Pod → one container → one exit code. No case where a
"Job" has three Pods and we have to work out which failure the user is looking at.

---

<a id="d-051"></a>

### D-051 — `backoffLimit: 0`: retry stays at the application level, not Kubernetes'

**Decision.** Every Job is created with `backoffLimit: 0`, disabling the Job controller's retry
entirely. When an attempt fails, the *scheduler* records it, waits out its own exponential backoff
(D-030), and creates a **new Job** for the next attempt.

**This is the decision the brief specifically asks to have explained, so the two mechanisms are set
out in full.**

#### How Kubernetes-native Job retry works

Set `backoffLimit: N` and the Job controller recreates the Pod on failure, up to *N* retries. Its
backoff is exponential — but the parameters are **hardcoded in the controller**: it starts at 10
seconds and doubles (10s, 20s, 40s, 80s…) with a cap of 6 minutes. There is no field to change the
base, the multiplier, or the cap. The Job's status reports `.status.failed` as a count, and the
controller decides when the Job is finally `Failed`.

#### How application-level retry works here

The scheduler treats a failed attempt exactly as it did with Docker: it appends an `Attempt` record
with timestamps, exit code, OOM flag and logs; computes `base * 2^(n-1)`; and schedules a re-enqueue
via a timer (D-018) that eventually creates a fresh Job.

#### Why application-level won

1. **The required backoff is literally impossible natively.** The brief specifies 1s, 2s, 4s, 8s.
   Kubernetes' floor is 10s and is not configurable. Native retry cannot produce the requested
   behaviour at all — this alone is decisive.
2. **Executor-independent semantics.** `max_attempts: 4` must mean the same thing whether the
   executor is Docker (Stage 1) or Kubernetes (Stage 3). If Kubernetes mode delegated retry to the
   controller, the same API request would produce different timing and different attempt-counting
   rules depending on a server-side deployment detail. The retry policy *is* the product here; it
   cannot live in a component we swap out.
3. **Observability.** D-014 keeps a full per-attempt history because that is what makes retry
   testable and debuggable. The Job controller gives a *counter* (`.status.failed`), not a history —
   no per-attempt start/end times, no per-attempt logs (Pods from earlier attempts may be gone), no
   way to see that attempt 1 failed differently from attempt 3.
4. **`backoffLimit` counts the wrong thing.** It counts *Pod* failures, which includes causes that
   have nothing to do with the submitted command: a node draining, a Pod evicted under memory
   pressure, a `kubectl delete pod`. Those consume the retry budget as though the job had failed.
   Application-level retry counts *attempts we actually made and observed*.
5. **Uniform metrics.** `jobsched_job_retries_total` means one thing across both executors.

#### What is genuinely given up — the honest cost

**Kubernetes-native retry is more durable than ours.** The Job controller's retry state lives in
etcd and is driven by a controller with its own leader election, so a Job with `backoffLimit: 4`
keeps retrying through a scheduler crash, a redeploy, or a network partition. Our retry lives in an
in-process `time.AfterFunc` (D-018), so **a scheduler restart abandons every pending retry** — the
same limitation as D-008 and D-018, but sharper here because Kubernetes was *offering* the durable
alternative and we declined it.

We also give up the controller's handling of infrastructure-caused Pod failures: if a node dies
mid-attempt, native retry reschedules automatically, whereas we observe a failed attempt and apply
our own policy to it.

**The resolution is not to switch, it is to make ours durable.** Persisting job state (the D-008
gap) fixes retry durability for both executors at once and keeps the policy uniform. Adopting native
retry would fix durability for one executor while breaking requirements 1–4 above.

#### Second-order consequence

With `backoffLimit: 0` and `restartPolicy: Never`, a Job creates exactly one Pod and reaches a
terminal state after that Pod terminates. That is what makes D-050's one-Job-one-attempt invariant
hold, and it removes any ambiguity about which Pod's exit code belongs to which attempt.

---

<a id="d-052"></a>

### D-052 — `restartPolicy: Never`, not `OnFailure`

**Decision.** Pod template uses `restartPolicy: Never`. (`Always` is not permitted for Jobs.)

**Alternatives considered.**

1. **`OnFailure`** — the kubelet restarts the failed *container in place*, inside the same Pod.

**Why Never.** `OnFailure` looks like a cheaper retry — no new Pod, no re-scheduling — and it
quietly breaks two things this project has already decided:

- It violates **D-022** (a fresh container per attempt). A restarted container reuses the Pod's
  environment and any `emptyDir` state, so attempt 2 can start from attempt 1's mess — exactly the
  failure mode D-022 exists to prevent.
- It makes exit-code observation racy. With `OnFailure`, `containerStatuses[0].state` moves to
  `Running` again on restart and the previous exit code slides into `lastState`. A scheduler reading
  the Pod is then racing the kubelet to observe the terminated state before it is overwritten. With
  `Never`, the terminated state is final and stays readable until the Pod is deleted.

`OnFailure` also interacts confusingly with `backoffLimit`: restarts are counted by the kubelet, so
the two retry mechanisms compose in ways that are hard to reason about. Since D-051 disabled Job
retry anyway, `Never` is the setting that makes "one Pod, one terminal result" true.

---

<a id="d-053"></a>

### D-053 — `requests` set equal to `limits`, giving Guaranteed QoS

**Decision.** Every job container sets `resources.requests` and `resources.limits` to the **same**
values: `cpu: <cpus>` as milliCPU, `memory: <memory_mb>Mi`.

**Alternatives considered.**

1. **Limits only, no requests.** Kubernetes then defaults requests to equal limits anyway — but
   implicitly, which is worth avoiding.
2. **Requests below limits** (Burstable QoS) — better bin-packing; a job can burst above its request
   when the node has spare capacity.
3. **Requests only, no limits** — no ceiling; jobs can use whatever is free.

**Why requests == limits.** Kubernetes assigns a Pod a **QoS class** from this relationship:

| QoS | Condition | Eviction order |
|---|---|---|
| `BestEffort` | no requests or limits | evicted first |
| `Burstable` | requests < limits | evicted second |
| `Guaranteed` | requests == limits, for every container | evicted last |

Setting them equal gets `Guaranteed`, which matters for a **correctness** reason, not a performance
one: the brief requires limits to be enforced, and the Stage 1 tests (D-034) assert that a job gets
*exactly* the CPU and memory it asked for. Under `Burstable` (option 2), a job's actual throughput
depends on what else happens to be running on the node, so the same job runs at different speeds at
different times and a CPU-throttling assertion becomes flaky. `Guaranteed` also means the kubelet
evicts these Pods last under node pressure, so an unrelated memory-hungry neighbour cannot get our
job killed and recorded as a spurious failure.

It also preserves Stage 1's semantics exactly. `docker run --cpus=0.5 --memory=64m` is a hard cap;
`Burstable` would silently make Kubernetes mode *more* permissive than Docker mode for the same API
request, which would violate the executor-independence argument of D-051.

Option 3 removes enforcement entirely and fails the brief outright.

**The mechanism is identical underneath, which is why Stage 1's proofs carry over.** A CPU limit
becomes a CFS quota on the container's cgroup; a memory limit becomes `memory.max`. Exceeding memory
gets the process OOM-killed by the same kernel path as D-025, and CPU overuse produces the same
`nr_throttled` counters as D-034. The API surface changed; the enforcement did not.

**Cost accepted.** Guaranteed QoS is the least efficient for cluster utilisation — reserved capacity
sits idle whenever a job under-uses its request. For a batch scheduler where predictable, verifiable
limits are the point, that is the right trade.

**Unit conversion, since it is a real source of bugs.** `cpus: 0.5` becomes `500m` (milliCPU) and
`memory_mb: 64` becomes `64Mi` (mebibytes, 64 × 2^20). Using `64M` instead would mean 64 × 10^6 —
a 6.9% smaller limit — which is exactly the kind of silent discrepancy that makes an enforcement
test fail confusingly.

---

<a id="d-054"></a>

### D-054 — Kubernetes object names are derived from the job ID, which is not itself a legal name

**Decision.** The Job object is named `jobsched-<hex>-<attempt>`, built by stripping the `job_`
prefix from the scheduler's job ID. The full original ID is preserved in a label,
`jobsched.io/job-id`.

**Alternatives considered.**

1. **Use the job ID directly** as the object name.
2. **Use `generateName`** and let the API server append a random suffix.
3. **Change the job ID format** in D-006 so it is always a legal Kubernetes name.

**Why a derived name.** Option 1 does not work, and the reason is a genuine trap: Kubernetes object
names must be **DNS-1123 subdomains** — lowercase alphanumerics, `-` and `.` only. Our IDs are
`job_a3f2…` (D-006), and **`_` is not permitted**, so every single Job creation would be rejected by
the API server with a validation error. Notably, label *values* have different rules and *do* allow
underscores, which is why the untransformed ID can be kept as a label — the two namespaces have
different character sets, and assuming they match is an easy mistake.

Option 2 (`generateName`) is idiomatic and avoids name collisions, but it means the scheduler does
not know the object's name until the API server replies, and the name carries no information. A
deterministic name means `kubectl get job jobsched-a3f2-2` works directly from a job ID in a log
line, which is worth a lot when debugging.

Option 3 was rejected because D-006's ID format is part of the public API and should not be
distorted by the naming rules of one particular executor. The transformation belongs in the executor
that has the constraint.

**Length is checked, not assumed.** Names are capped at 63 characters. `jobsched-` (9) + 16 hex + `-`
+ attempt digits is ~28, so there is comfortable headroom — but the code truncates defensively rather
than relying on the ID format never changing.

**Labels applied to every Job and Pod:** `jobsched.io/job-id`, `jobsched.io/attempt`, and
`app.kubernetes.io/managed-by: jobsched`. These make cleanup and debugging a one-liner
(`kubectl delete jobs -l app.kubernetes.io/managed-by=jobsched`) and are the mechanism behind the
label-selector lookups used to find an attempt's Pod.

---

<a id="d-055"></a>

### D-055 — A dedicated `jobsched` namespace, created at startup if absent

**Decision.** All Jobs go into a `jobsched` namespace (configurable via `SCHED_K8S_NAMESPACE`),
which the scheduler creates on startup if it does not exist.

**Alternatives considered.**

1. **`default`** — no setup, works everywhere.
2. **Require the namespace to pre-exist**, created by the setup instructions.
3. **A namespace per submitted job** — maximum isolation.

**Why a dedicated namespace.** Option 1 puts untrusted, arbitrary-command workloads next to whatever
else the developer is running, and makes bulk cleanup dangerous: there is no safe
`kubectl delete jobs --all` in `default`. A dedicated namespace makes the blast radius explicit and
cleanup trivial — `kubectl delete ns jobsched` removes every trace, including anything leaked.

Option 2 is a reasonable stance (the application does not create infrastructure), and it was rejected
on setup friction: it adds a manual step that, if forgotten, fails at the first job submission rather
than at startup. Creating it at boot means the failure mode is "the scheduler could not start and
said why", which is the better time to find out. The creation is idempotent — `AlreadyExists` is
treated as success, not an error.

Option 3 is real isolation but it is per-tenant machinery the brief scopes out, and namespaces are
not free (each carries default ServiceAccounts and controller work).

**Note.** Namespace creation needs cluster-scoped permission, which the admin kubeconfig of D-049
has. An in-cluster deployment with a namespaced Role would not, and would need option 2 instead —
another way the out-of-cluster choice simplifies things.

---

<a id="d-056"></a>

### D-056 — Completion is detected by watching the attempt's Pod, not by polling and not by watching the Job

**Decision.** After creating the Job, open a `watch` on Pods filtered by the attempt's label
selector, and wait for the container to reach a terminated state. The attempt's context deadline
bounds the wait. If the watch channel closes early (which the API server is entitled to do), it is
re-established.

**Alternatives considered.**

1. **Poll `Get` on the Job** every few hundred milliseconds until it reports `Complete`/`Failed`.
2. **Watch the *Job*** rather than the Pod.
3. **Use a shared informer** from `client-go/informers`.

**Why watch the Pod.** The decisive detail is *where the exit code lives*. A Job's status carries
`.status.succeeded` / `.status.failed` counts and conditions — it never carries an exit code. The
exit code, and the `OOMKilled` reason that D-034 relies on to prove memory enforcement, live in
`pod.status.containerStatuses[0].state.terminated`. So option 2 detects completion and then still
has to fetch the Pod; watching the Pod gets both facts from one stream, at the moment they become
true.

Option 1 (polling) is simpler and was seriously considered under D-010, since concurrency is bounded
by the worker count. It loses on measurement quality: attempt duration is a recorded metric
(D-036/D-043), and polling quantises every measurement to the poll interval — a 200ms job measured
on a 500ms poll is indistinguishable from a 700ms one. A watch delivers the transition as it happens.

Option 3 (informers) is the right tool at scale — one shared cache serving many consumers instead of
one watch per attempt. It is rejected here as over-machinery for a bounded pool: informers add a
cache, a resync period, and event-handler registration/deregistration per attempt, to optimise a
watch count that is capped at `SCHED_WORKERS`. **If the pool grew to hundreds of concurrent
attempts, this is the first thing to change.**

**The failure mode this had to handle explicitly.** A Pod that never starts — unschedulable because
the node cannot satisfy its resource request, or stuck `ImagePullBackOff` — produces *no* terminated
state, so the watch simply waits until the attempt deadline. A bare timeout there would report
"attempt exceeded timeout", which is actively misleading: the command never ran. So on timeout the
executor inspects the Pod's phase and conditions and reports the real reason (for example
`Unschedulable: insufficient cpu`), classifying it as an infrastructure error (D-024's `error`
outcome) rather than a job failure. **This is precisely the distinction D-024 was built for**, and
Kubernetes produces this case far more readily than Docker did.

---

<a id="d-057"></a>

### D-057 — Cleanup is both explicit deletion and a server-side TTL

**Decision.** After reading the result, delete the Job with `PropagationPolicy: Background` (which
also deletes its Pod via ownership). Additionally set `ttlSecondsAfterFinished` on every Job as a
server-side backstop.

**Alternatives considered.**

1. **`ttlSecondsAfterFinished` only** — let the cluster clean up; write no deletion code.
2. **Explicit deletion only** — mirror Stage 1's Docker behaviour exactly (D-026).
3. **Keep finished Jobs** for post-hoc `kubectl` inspection.

**Why both.** They fail in different situations, which is what makes the combination worth its small
cost rather than redundant:

- **Explicit deletion** reclaims resources immediately, so a burst of jobs does not leave hundreds of
  completed Pods holding etcd objects and node disk. But it does not run if the scheduler dies
  between the Pod terminating and the delete call — the exact orphan window D-026 accepted.
- **The TTL** covers precisely that gap, because it is enforced by a controller in the cluster and
  does not care whether the scheduler is alive.

**This closes a Stage 1 limitation rather than repeating it.** D-026 had to accept leaked containers
after a `SIGKILL`, mitigated only by a manual `docker rm` one-liner. Kubernetes provides server-side
garbage collection, so the Stage 3 executor has no equivalent permanent leak. Worth noting as a real
advantage of the platform, not just a different way of doing the same thing.

**Ordering matters and is the same lesson as D-026.** Deletion happens *after* the exit code, logs
and OOM status have been read. Deleting first — or setting a very short TTL — would race the reads
and intermittently lose job output, which is exactly the `AutoRemove` trap from Stage 1 in new
clothing.

**TTL value.** 300 seconds: long enough to `kubectl describe` a failure while debugging, short enough
that a burst does not accumulate. Configurable via `SCHED_K8S_JOB_TTL_SECONDS`; setting it to a large
value effectively gives option 3.

---

<a id="d-058"></a>

### D-058 — Timeouts are enforced twice: `activeDeadlineSeconds` server-side and a context deadline client-side

**Decision.** Set `activeDeadlineSeconds` on the Job spec **and** keep the client-side
`context.WithTimeout` that Stage 1 used (D-029).

**Alternatives considered.**

1. **Client-side context only**, exactly as the Docker executor does.
2. **`activeDeadlineSeconds` only** — let the cluster enforce it.

**Why both.** They cover different failures, and this is the same belt-and-braces reasoning as D-057:

- The **context deadline** gives a prompt, local reaction, keeps timeout behaviour identical between
  the Docker and Kubernetes executors, and unblocks the worker goroutine even if the API server
  becomes unreachable. Option 2 alone would leave a worker blocked on a watch against an API server
  that may never answer.
- **`activeDeadlineSeconds`** is enforced by the Job controller, so a runaway Pod is terminated even
  if the scheduler crashes. Option 1 alone leaves a `sleep 3600` Pod running forever after a
  scheduler restart, consuming Guaranteed-QoS capacity that nothing will reclaim — a worse leak than
  a stray container, because reserved capacity blocks other work from scheduling.

**Implementation detail worth knowing.** When `activeDeadlineSeconds` fires, the Job gets a `Failed`
condition with reason `DeadlineExceeded` and its Pod is deleted — so the Pod's terminated state may
be *gone* by the time we look. The executor therefore checks the Job's conditions as well as the
Pod's status when attributing a failure, rather than assuming a terminated Pod is always available.
Missing this produces a confusing "pod not found" error for what is really a timeout.

The server-side deadline is set slightly *above* the client deadline, so in the normal case the
client's own timeout fires first and produces the more precise, consistent error message; the
server-side one only takes over when the scheduler is not around to react.

---

<a id="d-059"></a>

### D-059 — Job pods do not mount a ServiceAccount token; network isolation is *not* preserved

**Decision.** Set `automountServiceAccountToken: false` on every job Pod. Accept, and document, that
Stage 1's network isolation (D-027) has **no equivalent** in Stage 3 as built.

**Alternatives considered for the network gap.**

1. **A default-deny `NetworkPolicy`** in the `jobsched` namespace.
2. **`hostNetwork: false` alone** — which is already the default and does nothing for this.
3. **Accept the gap and document it.**

**Why the token is not mounted.** By default Kubernetes mounts a ServiceAccount token into every
Pod at `/var/run/secrets/kubernetes.io/serviceaccount`. Our Pods run *arbitrary submitted shell
commands* (D-023), so that default hands every submitted job a credential it can use against the
Kubernetes API. Even the `default` ServiceAccount's minimal permissions are more than a batch job
needs, and it is a free thing to remove: one field.

**The network regression, stated plainly rather than glossed.** Stage 1 ran every container with
`NetworkMode: none`, so a job had no network at all (D-027), and that was load-bearing for the
argument that arbitrary shell execution was acceptable. **Kubernetes has no per-Pod equivalent**:
every Pod gets a network interface and a cluster IP, and there is no `network: none` field. Real
isolation requires a `NetworkPolicy` — and NetworkPolicy is only *enforced* if the cluster's CNI
implements it. kind's default CNI (kindnet) does not enforce NetworkPolicy, so shipping one would be
worse than shipping nothing: it would look like isolation while enforcing nothing.

So Stage 3 job pods **can reach the cluster network and the internet**. This is a genuine reduction
in isolation relative to Stage 1, and it is the strongest reason the Stage 3 configuration is a
local development setup rather than something to expose.

**Why option 1 was not done anyway.** Making it real means replacing kind's CNI (Calico or Cilium)
in the cluster config, which is cluster infrastructure work beyond the brief's scope and adds a
significant, fragile setup step to the "clear setup instructions" the brief asks for. The honest
trade is to name the gap loudly here and in the README rather than to paper over it.

**If this needed fixing**, the order would be: install a policy-enforcing CNI in `cluster.yaml`, add
a default-deny-egress NetworkPolicy to the `jobsched` namespace, and add an integration test that
asserts a job *cannot* reach an external address — because per the D-048 lesson, an unenforced
policy and an enforced one look identical until something checks.

---

<a id="d-060"></a>

### D-060 — Kubernetes metrics are separate instruments, not new labels on the existing ones

**Decision.** Add `jobsched_k8s_jobs_created_total`, `jobsched_k8s_pod_terminations_total{reason}`,
`jobsched_k8s_api_errors_total{op}`, `jobsched_k8s_pod_schedule_latency_seconds`, and a
`jobsched_k8s_active_jobs` gauge. The existing Stage 1/2 metrics are unchanged.

**Alternatives considered.**

1. **Add an `executor` label** to the existing metrics (`jobs_completed_total{status,executor}`).
2. **Rename the generic metrics** to be executor-specific.
3. **Reuse the Docker-era names** for Kubernetes concepts (e.g. count Jobs under a "containers
   created" metric).

**Why separate instruments.** The generic metrics answer questions about *the scheduler* —
throughput, failure rate, worker load — and those questions have identical meaning regardless of
backend. That is the whole premise of the Executor interface, and it means the Stage 2 dashboard
keeps working unchanged in Kubernetes mode, which is the strongest evidence the abstraction is real.

Option 1 would break that. Adding a label changes every existing series' identity, so all Stage 2
panels would need `sum by()` adjustments, and historical data would not join with new data across
the rename. A label is the right answer when one process runs both executors simultaneously — it
cannot (the executor is chosen once at startup, D-061), so the `job`/`instance` labels Prometheus
already attaches distinguish a Docker-mode scrape from a Kubernetes-mode one for free.

Option 3 is worse than either: it would force Kubernetes concepts into Docker vocabulary and lose
the things that are genuinely new.

**What is genuinely new, and why each earns a metric.**

- **`k8s_pod_schedule_latency_seconds`** has *no Docker analogue at all*. With Docker, "create" and
  "start" are one local operation. In Kubernetes there is a real interval between the Job object
  existing and a node accepting its Pod — scheduling, image pull, sandbox creation. In the first
  test run this averaged ~5s, dominated by the initial image pull, and dropped sharply once the
  image was cached on the node. That interval is invisible to `attempt_duration_seconds` and is
  frequently the thing that makes Kubernetes feel slow.
- **`k8s_pod_terminations_total{reason}`** records the kubelet's *own* verdict — `Completed`,
  `Error`, `OOMKilled`, `DeadlineExceeded`. That is third-party evidence rather than our
  interpretation of an exit code, the same reasoning as D-034.
- **`k8s_api_errors_total{op}`** separates "the cluster is unhealthy" from "jobs are failing",
  which is D-024's distinction pushed down to the API-call level.

**Cardinality.** All label sets are small and closed (D-041): four operations, a handful of
termination reasons the kubelet emits. Nothing is labelled by Job or Pod name.

**Zero-initialisation, found by the D-048 query check.** `k8s_api_errors_total` was the one panel
that returned nothing under load — correctly, since no API call had failed. But a `CounterVec` with
no observations emits **no series at all**, so the panel reads "No data" precisely when the system
is healthiest, which is indistinguishable from the metric being missing or the scheduler being down.
The four operation labels are therefore initialised to zero at construction, so the healthy state is
a visible flat line at zero. This is the same rule the status collector already follows for job
statuses (D-038) — worth applying consistently, and worth noting that an *error* metric is exactly
where the ambiguity is most dangerous.

**Cost accepted.** These series are always registered but sit at zero when the Docker executor is
running. Registering them conditionally was considered and rejected — a metric that sometimes does
not exist is worse to query than one that is zero, because `absent()` and "no data" are harder to
reason about on a dashboard than a flat line at zero. The one exception is the `k8s_active_jobs`
gauge, which is only registered when the Kubernetes executor is in use, because a *gauge* reading
zero forever would falsely imply it is measuring something.

---

<a id="d-061"></a>

### D-061 — The executor is chosen at runtime by config, not by build tag or separate binaries

**Decision.** `SCHED_EXECUTOR=docker|kubernetes` selects the backend at startup. One binary contains
both. An invalid value is a startup error.

**Alternatives considered.**

1. **Build tags** — `go build -tags kubernetes`, producing a binary containing only one backend.
2. **Two separate commands** — `cmd/scheduler-docker` and `cmd/scheduler-k8s`.
3. **Replace the Docker executor entirely**, since Stage 3 supersedes Stage 1.

**Why runtime selection.** Option 3 is what "refactor so job execution happens via Kubernetes" could
be read to mean, and it is rejected because the brief's own framing is that each stage should remain
*"resume-worthy"* — deleting the Docker executor would make Stage 1 unrunnable and would silently
discard its test suite, which is the evidence that the retry and limit semantics are correct in the
first place. Keeping both is what allows the **same assertions to be run against both backends**,
which is how Stage 3 demonstrates that the abstraction holds rather than merely asserting it.

Option 1 (build tags) means the two backends are never compiled together, so a change that breaks
the Kubernetes executor passes CI on a Docker build. It also makes `go vet ./...` and IDE tooling
see only one variant. The cost is a larger binary containing the Kubernetes client libraries even in
Docker mode — real (client-go is heavy) but irrelevant next to losing compile-time coverage.

Option 2 duplicates the entire wiring in `main.go`, so every future change must be made twice.

**This is where D-002 and D-009 pay off.** Adding Kubernetes support was: one new file implementing
`executor.Executor`, one `switch` in `main.go`, and config fields. The queue, retry policy, store,
worker pool, and HTTP API were **not modified at all** — the retry/backoff code has no idea which
backend ran the attempt. That was the explicit bet made in D-002 before any of this was written, and
it is the concrete payoff.

**Validation placement.** The value is validated in `config.Load` rather than at the switch, so a
typo (`SCHED_EXECUTOR=k8s`) fails immediately at startup with a message naming the valid values —
consistent with D-004's refusal to silently fall back.

---

<a id="d-062"></a>

### D-062 — A third test tier for Kubernetes: fake clientset for spec, real kind cluster for behaviour

**Decision.** Two new suites:

- **`internal/executor/kubernetes_test.go`** — uses `client-go`'s fake clientset to assert the *Job
  spec* the executor builds. Runs in ~1s, needs no cluster.
- **`test/k8s_integration_test.go`** — runs the whole scheduler against the kind cluster. ~45s.
  Skips when no cluster is reachable, unless `SCHED_REQUIRE_K8S=1`, which converts the skip into a
  hard failure — the same fail-loud rule as D-033.

**Alternatives considered.**

1. **Integration tests only.** The cluster is the truth; asserting on a struct proves nothing.
2. **Spec tests only.** Fast, deterministic, no cluster dependency in CI.
3. **`envtest`** (a real API server + etcd, no kubelet) as a middle tier.

**Why both tiers.** They catch different classes of bug, and the split falls out of what each can
see:

- The spec tests catch **silent policy regressions**. If someone removes `BackoffLimit: 0`,
  Kubernetes applies its default of 6 retries and *everything still passes* an integration test that
  only checks the final job status — the job would still fail after its attempts, just with the
  cluster secretly retrying underneath. A spec assertion catches that immediately, which matters
  because D-051 is the central decision of this stage.
- The integration tests catch what a struct cannot express: that a memory limit really OOM-kills,
  that cgroup values match, that a watch actually observes termination.

Option 3 (`envtest`) is genuinely the right middle tier for controller work — a real API server
means real validation and defaulting. It is rejected here because envtest has **no kubelet**, so
Pods are created but never run: it could not verify a single one of the enforcement claims, which
are the point. It would add a heavyweight dependency to cover a gap that barely exists for this code.

**A test-design constraint worth recording.** The "pod never starts" test (D-056's diagnosis path)
was first written to request more CPU than the node has. That does not work: D-020 caps requests at
`SCHED_MAX_CPUS` (4), and any value under that cap schedules fine on a normal machine — so the
admission ceilings make resource-unschedulability *unreachable through the API*. It was rewritten to
use an unpullable image, which reaches the same code path, and has the side benefit of being
independent of how many cores the test machine has. **The first version of that test passed for the
wrong reason** (the job succeeded), which is exactly the failure mode a green suite is supposed to
prevent.

---

<a id="d-063"></a>

### D-063 — Prometheus scrapes the Kubernetes-mode scheduler as a separate target on port 8081

**Decision.** Kubernetes mode runs on the host on port `8081`; the compose stack keeps the
Docker-mode scheduler on `8080`. `prometheus.yml` scrapes both, as separate jobs
(`jobsched` and `jobsched-k8s`).

**Alternatives considered.**

1. **Reuse port 8080** and stop the compose scheduler when running in Kubernetes mode.
2. **Run the Kubernetes-mode scheduler inside compose**, scraped as `scheduler:8080`.
3. **Do not scrape it** — Kubernetes mode is a CLI exercise.

**Why a separate port and target.** Option 2 does not work without extra machinery: kind writes a
kubeconfig pointing at `127.0.0.1:<random port>`, which means something different inside a container,
so the container would need a rewritten kubeconfig or to join kind's Docker network (D-049).

Option 1 works but forces the two modes to be mutually exclusive, and the far more interesting thing
to do is run both at once and compare them on the same dashboard — the metric names are identical
(D-060), so the `job` label separates them and the Stage 2 panels show both backends side by side.
That comparison is the clearest demonstration that the executor swap preserved behaviour.

**Consequence to be aware of.** Whichever scheduler is not running shows as a **DOWN target** in
Prometheus, which looks like a fault but is expected. It is documented in `prometheus.yml` at the
target definition. The alternative — silently having no target — would be worse, because then
"nothing is being scraped" and "the scheduler is broken" would look the same.

**Dashboard consequence.** The Stage 2 panels aggregate across both targets, so with both running
the throughput figures are combined. Left as is deliberately: adding a `job` filter to fourteen
panels to support a case that only arises when deliberately comparing backends is not worth it, and
the panels remain correct for the normal single-scheduler case.

---

## Stage 3 status

Complete and verified against a real kind cluster (Kubernetes v1.36.1, client-go v0.36.4).
Decisions D-049 to D-063 are `active`.

**Nothing from Stages 1 or 2 was superseded.** That is the headline result: adding Kubernetes
execution required one new file implementing `executor.Executor`, one `switch` in `main.go`, and new
config fields. The queue, retry policy, store, worker pool and HTTP API were not modified — the
retry code cannot tell which backend ran an attempt. D-002 and D-009 made that bet before any of this
existed; this is the payoff.

**Evidence.** 16 integration tests pass — 8 against Docker, 8 against Kubernetes — asserting the same
observable behaviour through both backends:

- Application-level retry produces 1.001s / 2.001s gaps in Kubernetes mode. Kubernetes' own Job
  backoff cannot go below 10s, so those timings are themselves proof the controller is not driving
  retries (D-051). Three distinct Job objects, exactly three pod terminations — one per attempt.
- `cpu.max` reads `50000 100000` and `memory.max` reads `67108864` inside the pod: **byte-identical
  to the values Stage 1 produced through Docker flags.** Pod resource limits and `--cpus`/`--memory`
  reach the same cgroup.
- The kubelet reports `OOMKilled` under a 64Mi limit, and the kernel throttled a 0.25-CPU pod 10
  times.
- An unpullable image is recorded as an infrastructure `error` with no exit code rather than a job
  failure — D-024's distinction, which Kubernetes produces far more readily than Docker did.

**The one real regression** is D-059: job pods are not network-isolated, because Kubernetes has no
per-Pod equivalent of `--network=none` and kind's default CNI does not enforce NetworkPolicy.
Documented rather than papered over.

**Known costs carried forward, in the order worth fixing:** durable job state (D-008), which would
also make retries survive a restart and close the honest cost admitted in D-051; enforced network
isolation (D-059); in-cluster deployment with a least-privilege Role (D-049).

---

## Stage 4 (unplanned) — Demo UI

Not part of the original brief. Added because the Grafana dashboard shows *aggregates*, and the
thing this project is actually about — an individual job's retry and backoff behaviour — has no
visual representation anywhere.

<a id="d-064"></a>

### D-064 — The demo UI is embedded in the scheduler binary with `go:embed`

**Decision.** A single `index.html` lives in `internal/api/ui/`, is compiled into the binary via
`go:embed`, and is served at `GET /`. No separate process, no static file server, no files to
deploy alongside the binary.

**Alternatives considered.**

1. **Serve from disk** with `http.FileServer(http.Dir("web/"))`.
2. **A separate static site** (its own container in compose, or opened as a `file://` page).
3. **No UI** — the API plus Grafana is enough.

**Why embedding.** Option 1 makes the binary depend on its working directory: `./bin/scheduler` works
from the repo root and silently 404s from anywhere else, which is exactly the kind of "works on my
machine" failure a demo cannot afford. Embedding makes `bin/scheduler` a single self-sufficient
artifact, which also means the compose image needs no extra `COPY` and no volume mount.

Option 2 reintroduces a cross-origin boundary — the page would need CORS headers on the API, which
is a real change to the server for no benefit. Serving the page from the same origin as the API
means `fetch('/jobs')` just works.

Option 3 was the position until now, and it is wrong for one specific reason: **the per-attempt
history from D-014 has never been visible.** Grafana answers "what is the failure rate", which is an
aggregate question. It cannot show that *this* job waited 1s, then 2s, then 4s. That is the single
most characteristic behaviour of the system and there was no way to look at it.

**Cost accepted.** Changing the UI requires recompiling. For a file that changes rarely, that is
preferable to the deployment fragility of option 1. `go:embed` is stdlib, so no dependency is added.

---

<a id="d-065"></a>

### D-065 — Vanilla HTML/CSS/JS in one file, no framework and no build step

**Decision.** One self-contained `index.html`: no React, no bundler, no `package.json`, no
`node_modules`.

**Alternatives considered.**

1. **React (or Vue/Svelte) with Vite.** Component model, better state handling as the UI grows.
2. **A CDN-loaded framework** (React via unpkg, or Alpine.js) — framework ergonomics, no build.
3. **HTMX** — server-rendered fragments, minimal JS.

**Why vanilla.** The decisive argument is that a build step would make this the *only* part of the
repo that cannot be built with the Go toolchain alone. Right now `make build` is sufficient to
produce everything; option 1 adds Node, a lockfile, a `dist/` directory, and a second thing that can
be out of date relative to the binary. For roughly 400 lines of view logic driven by one JSON
endpoint, that is a poor trade — and it would sit oddly beside D-003 and D-006, which declined much
smaller dependencies.

Option 2 avoids the build but adds a runtime CDN dependency, so the demo breaks without internet —
unacceptable for something whose purpose is being shown to people in unknown network conditions.

Option 3 is a genuinely good fit for "server has the state, client renders it", but the timeline
visualisation needs real client-side computation (scaling durations to pixels), which is the case
HTMX handles least well.

**Where this would stop being right.** If the UI grew multiple views, routing, or forms with real
validation state, hand-rolled DOM updates would become the wrong tool. It is correct *at this size*,
and the size is bounded by it being a demo.

---

<a id="d-066"></a>

### D-066 — The UI polls once per second rather than using SSE or WebSockets

**Decision.** `setInterval` fetching `GET /jobs?limit=40` every 1000 ms, rendering the result.

**Alternatives considered.**

1. **Server-Sent Events** — the server pushes job state changes; no wasted requests.
2. **WebSocket** — bidirectional, the usual answer for live dashboards.
3. **Poll faster (250 ms)** for a smoother feel.

**Why polling.** This is the same decision as D-007, for the same reason, and it is worth noting the
consistency: the API was deliberately built as async-submit-then-poll, so a polling UI is the
client that API was designed for. Adding SSE would mean adding a *second* delivery mechanism —
change notification, per-connection lifecycle, reconnect logic — to a server that currently has no
concept of a subscriber.

At one request per second against a local process, the cost is irrelevant. The honest observation is
that polling is only defensible *because* of scale: with many concurrent viewers it would be wrong,
and there is exactly one viewer.

Option 3 was rejected because job state changes on the order of hundreds of milliseconds at best
(container startup, D-022), so a 250 ms poll would mostly re-render identical data. 1 s is faster
than the state actually changes.

**Consequence.** Up to one second of staleness, and the *displayed* backoff countdown is derived
from `next_attempt_at` client-side rather than being pushed — so the countdown ticks smoothly even
though data arrives once a second.

---

<a id="d-067"></a>

### D-067 — The centrepiece is a proportional attempt timeline, not a table

**Decision.** A job's attempts are drawn as bars on a **shared, linearly-scaled time axis**: solid
bars for attempts, hatched spans for backoff waits, all positioned by real elapsed time from the
job's first attempt.

**Alternatives considered.**

1. **A table of attempts** with start/end timestamps and durations — the obvious representation, and
   what the JSON already is.
2. **A list of attempts with a duration label each**, unscaled.
3. **A log-scaled axis**, so long backoffs do not dominate.

**Why proportional bars.** Exponential backoff is a claim about *time*, and a table asks the reader
to do arithmetic on ISO-8601 timestamps to verify it. On a shared linear axis, each backoff span is
literally twice the width of the previous one — the doubling is perceived directly rather than
computed. That single property is the reason this view exists, and it is why option 1 (which the API
response already effectively is) does not suffice.

Option 2 loses exactly the property that matters: without a common scale, "1.0s" and "2.0s" labels
sit on identically-sized rows and communicate nothing.

Option 3 was tempting — with `BackoffMax` at 60s a long-running job's early attempts get very narrow
— and was rejected because **a log axis would destroy the doubling**, turning geometric growth into
evenly-spaced steps. The whole point is that the reader sees each wait double. Linear scaling with a
horizontally scrollable axis keeps that true.

**Detail that carries the argument.** Each backoff span is labelled with its measured duration taken
from the gap between one attempt's `finished_at` and the next's `started_at` — the *observed* delay,
not the configured one. So the picture is evidence rather than an illustration of intent. This is
the same distinction as D-034: show what happened, not what was requested.

**The geometry is tested, because a chart can lie silently.** `timeline()` is a pure function — job
object in, HTML string out — so `test/ui_check.js` runs it under Node's `vm` with a stubbed DOM,
parses the inline styles it emits, and asserts that consecutive backoff spans are drawn at exactly
2.00× the previous width (measured: 11.63% → 23.26% → 46.51% of the axis). A miscomputed scale would
still *render*, and would still look plausible, which is precisely the D-048 failure mode: an output
that is wrong but not broken. `make test-ui` skips cleanly when Node is absent, so it never becomes
a required dependency — Node is used as a JS interpreter for a test, not introduced as a build step
(D-065 stands).

---

<a id="d-068"></a>

### D-068 — Two small API additions driven by the UI: `?limit=` and executor identity in `/healthz`

**Decision.** `GET /jobs` accepts an optional `limit`, and `GET /healthz` reports the active executor
and configured worker count.

**Alternatives considered.**

1. **Change nothing** and have the UI fetch everything, then slice client-side.
2. **A dedicated `/ui/state` endpoint** shaped exactly for the page.

**Why extend the existing endpoints.** Option 1 has a concrete failure already present in this repo:
after the Stage 2 load runs, the scheduler holds ~150 jobs, and `GET /jobs` serialises **every
attempt of every job including captured log tails** — hundreds of kilobytes, re-sent every second, to
display 40 rows. `limit` is a normal, useful API parameter that any client benefits from, not a
UI-specific hack.

Option 2 is the "backend for frontend" pattern and is rejected under D-010: it would duplicate the
job serialisation, and the moment the UI needed a new field there would be two places to change.

`/healthz` gaining `executor` is what lets the page display **DOCKER** or **KUBERNETES** without
being told — which matters because the whole demo point of Stage 3 is that the same UI, API and
behaviour sit on top of either backend. The page proving that by *observation* is better than a
caption asserting it.

**Bounds.** `limit` is clamped to a sane maximum and ignored when absent, so existing clients are
unaffected. Both changes are additive; no response field was removed or renamed.

---

<a id="d-069"></a>

### D-069 — Web fonts load from a CDN, with a fallback chain that degrades gracefully

**Decision.** Load *Instrument Serif* (masthead) and *JetBrains Mono* (everything else) from Google
Fonts, behind a fallback chain ending in `ui-monospace` / system serif.

**Alternatives considered.**

1. **System fonts only** — zero external requests, guaranteed offline.
2. **Embed the font files** in the binary alongside the HTML.

**Why the CDN, stated as the exception it is.** This is the **only external runtime dependency in
the entire project**, which otherwise pins every version (D-031) and refuses CDN-loaded libraries
(D-065). It is accepted for one reason: this file's purpose is to be *looked at*, and typography is
most of what makes it worth looking at rather than being another default-styled admin panel.

The risk is bounded by design: fonts are the only thing loaded remotely, `font-display: swap` means
text renders immediately regardless, and the fallback is `ui-monospace` — which on the Mac this is
demoed from resolves to SF Mono, a genuinely good face. **Offline, the page loses some character but
loses no function and no legibility.**

Option 2 removes the dependency entirely and was rejected on weight: two variable font families is
several hundred kilobytes compiled into a binary, for a cosmetic gain, and it would need a licence
review before redistribution.

**Reconsider if** this is ever demoed somewhere with no network. In that case, embed the two `.woff2`
files — the change is local to one `<link>` and one `go:embed` line.

---

---

## Stage 5 — Throughput, reclamation, and measurement

Added after the first three stages were complete, to replace assumptions about scale with
measurements, and to close the orphan-cleanup gap D-026 and D-057 had only partially covered.

<a id="d-070"></a>

### D-070 — client-go's request rate limit is set explicitly, because its default silently caps dispatch

**Decision.** Set `rest.Config.QPS` and `Burst` explicitly from configuration
(`SCHED_K8S_QPS`, default 100; `SCHED_K8S_BURST`, default 200) instead of leaving them at zero.

**Alternatives considered.**

1. **Leave them unset** — which is what every client-go example does, and what this code did until
   now.
2. **Disable client-side rate limiting entirely** (`QPS: -1`), relying on the API server's own
   Priority and Fairness to shed load.
3. **Set a very high fixed value** in code, not configurable.

**Why explicit.** Option 1 is not "no rate limit" — it is a rate limit of **5 requests per second
with a burst of 10**, because `rest.Config` substitutes `DefaultQPS = 5.0` and `DefaultBurst = 10`
when the fields are zero. That is a client-side token bucket that silently queues requests before
they ever reach the network.

This matters here because of how many API calls one job costs. Each attempt issues roughly four:
create the Job, watch its Pods, read the Pod's logs, delete the Job. So the theoretical ceiling on
sustained dispatch with the default limiter is about **5/4 ≈ 1.25 jobs per second**, no matter how
many workers the pool has or how much capacity the cluster has. Adding workers past that point adds
*queueing*, not throughput.

The reason this went unnoticed through all of Stage 3 is that the worker pool defaulted to 4, and 4
concurrent jobs never sustained enough request rate to drain the bucket. **The bottleneck was
invisible at the concurrency the tests happened to use** — which is the general hazard of never
running a system near its limit, and the reason D-073 exists.

Option 2 (`QPS: -1`) removes the guard entirely and pushes all backpressure onto the API server.
That is increasingly the modern recommendation for controllers on clusters with APF enabled, and it
is declined here because a client-side limit is a useful blast-radius bound: a bug in the retry loop
should degrade this scheduler, not the cluster's API server. A high explicit ceiling keeps the
guard while lifting it above the real workload.

Option 3 was rejected because the right value depends on worker count and cluster size, which are
already configuration.

**How the value was chosen.** 100 QPS is roughly 25 jobs/second of API budget at ~4 calls per job,
comfortably above what a 30-worker pool can generate given pod startup time. Burst is set to 2× QPS
so a batch of simultaneous dispatches is absorbed rather than smoothed.

**Cost accepted.** A misconfigured scheduler can now issue substantially more load against the API
server than before. That is the intended trade — the previous behaviour was safe and quietly wrong.

---

<a id="d-071"></a>

### D-071 — Orphaned Jobs are reclaimed by a label-scoped sweep, at startup and periodically

**Decision.** The Kubernetes executor tracks the set of Job names it currently owns. A reconciler
lists Jobs carrying `app.kubernetes.io/managed-by=jobsched` and deletes any that are **not in that
set**. It runs once at startup and then on a ticker (`SCHED_K8S_RECONCILE_INTERVAL`, default 60s).

**Alternatives considered.**

1. **Rely on `ttlSecondsAfterFinished` alone**, which is what D-057 did.
2. **Track owned Jobs in a file or database** so a restarted process can tell its own leftovers from
   another scheduler's.
3. **Owner references to a parent object**, letting Kubernetes garbage-collect the whole tree.

**Why a sweep.** Option 1 covers less than it appears. The TTL only starts counting when a Job
*finishes*, so it cleans up completed work — but a Job whose scheduler died **while it was still
running** has not finished, has no TTL clock, and sits there holding Guaranteed-QoS reservations
until `activeDeadlineSeconds` expires (up to the job's full timeout, 30 minutes at the ceiling) and
only *then* starts its TTL. Reserved capacity that nothing will reclaim for half an hour is a real
cost, because Guaranteed QoS means the scheduler cannot place other work in it.

The in-flight set makes startup and steady state the same code path: a freshly started process owns
nothing, so the first sweep reclaims everything left by its predecessor. No special-casing.

Option 3 is the idiomatic Kubernetes answer — make Jobs children of a parent object and let the GC
cascade — but there is no natural parent here, and creating one purely to own Jobs would be
inventing an object to solve a problem a label selector already solves.

Option 2 is what makes this correct under multiple schedulers, and is deliberately not built.

**The limitation, stated loudly because it is a real one.** This assumes **a single scheduler owns
the namespace**. Two schedulers sharing one namespace would each classify the other's Jobs as
orphans and delete them mid-run — a genuinely destructive failure. The mitigations, in order of
cost: give each scheduler its own namespace (already configurable, costs nothing); or label Jobs
with a scheduler instance ID and reclaim only Jobs whose owner holds no live Lease. The second is
the correct fix and requires coordination this project does not have (D-005, D-008).

**Ordering hazard, and how it is avoided.** A Job name is added to the in-flight set **before** the
Create call and removed only after deletion. Adding it afterwards would leave a window in which a
concurrent sweep sees a live Job that its own process does not yet claim, and deletes work in
progress. Registering intent before acting is what makes the sweep safe to run concurrently with
dispatch.

**Observability.** Reclaimed Jobs increment `jobsched_k8s_orphans_reclaimed_total`. A non-zero
value after startup means the previous process exited uncleanly, which is worth being able to see.

---

<a id="d-072"></a>

### D-072 — Dispatch overhead is measured separately from platform latency

**Decision.** Add `jobsched_dispatch_overhead_seconds`, covering only the scheduler's own work
between a worker dequeuing a job and issuing the executor call — the store update, attempt
bookkeeping, and request construction. It deliberately excludes everything the platform does.

**Alternatives considered.**

1. **Rely on `attempt_duration_seconds`**, which already exists.
2. **Measure nothing** — the overhead is obviously small.

**Why a separate metric.** `attempt_duration_seconds` is dominated by container or pod startup —
measured at roughly 1.1s p50 for Kubernetes pods (D-060). Our own scheduling cost is buried three
orders of magnitude beneath that and cannot be recovered from it. Two very different questions get
conflated: *"is the scheduler slow?"* and *"is the platform slow?"*

Separating them makes the honest claim possible: **the scheduler's own contribution is negligible
next to the cost of starting a sandbox**, and the way to make jobs faster is not to optimise this
code. Without the measurement that is an assumption; with it, it is a number.

Option 2 is the position that produced D-070's blind spot. "Obviously small" was also true of the
API rate limit right up until it was the bottleneck.

**Bucket choice.** This histogram cannot reuse `durationBuckets` (D-043), whose smallest bucket is
100 ms — every observation would land in it and the metric would report nothing. It gets its own
microsecond-scale buckets from 10 µs upward. **A histogram whose buckets do not bracket the
distribution is not a measurement**, which is the same lesson D-043 recorded from the other end of
the scale.

---

<a id="d-073"></a>

### D-073 — Load is generated by a dedicated command, measuring completion rather than submission

**Decision.** A separate binary, `cmd/loadgen`, submits *N* jobs, polls until every one reaches a
terminal state, and reports **completion throughput** (jobs finished per second) alongside
submission rate and end-to-end latency percentiles.

**Alternatives considered.**

1. **A shell script with `curl` in a loop**, which is what the earlier ad-hoc load runs used.
2. **A Go test** in the existing suite.
3. **A real load tool** (k6, vegeta, hey).

**Why completion, not submission — this is the important part.** Option 1's `curl` loop measures how
fast jobs can be *accepted*, and acceptance is nearly free: the API validates, stores a record, and
returns `202` without doing any work (D-007). A submission-rate number would have looked excellent
while the pool ran at 1.25 jobs/second behind it. **The queue would absorb the difference and the
measurement would report the queue's write speed, not the system's throughput.** Measuring the rate
at which jobs reach a terminal state is the only number that cannot be inflated by buffering.

Option 3 is built for exactly this and is declined for one reason: general HTTP load tools measure
request/response latency, and here the response is `202 Accepted` — meaningless. Correct measurement
requires following each job to completion through a second endpoint, which is application-specific
enough that a purpose-built client is simpler than configuring a general one.

Option 2 was rejected so a load run cannot be triggered by `go test ./...`.

**Methodology, fixed before running anything so the numbers are not chosen after the fact.**

- The workload is a trivial command (`true`), so the measurement reflects scheduling and sandbox
  startup rather than the job's own runtime.
- Jobs request minimal resources so cluster capacity is not the binding constraint — the point is to
  find *our* limits.
- `max_attempts: 1`, so no retry backoff inflates completion time.
- Throughput is measured over the steady-state window, from first completion to last, excluding
  the initial ramp.
- Every run reports the configuration that produced it (workers, QPS, burst) so results are
  reproducible and comparable.
- **The measured numbers are reported as observed, including if they are unflattering.**

---

<a id="d-074"></a>

### D-074 — Measured findings: the API rate limiter was throttling but was not the bottleneck

**What was measured.** A worker-count sweep, an A/B of client-go's rate limiter, a sustained
1,000-job run, and a kill-and-recover test of the reconciler. All on one node: kind v1.36.1 inside
Docker Desktop, 8 CPUs shared with the host.

**Worker sweep** — 100 jobs, `command: true`, 50m CPU / 32Mi per job, QPS 100:

| Workers | Throughput | Pod startup p50 | Pod startup p95 |
|---:|---:|---:|---:|
| 8 | 1.04 jobs/s | 6.69 s | 25.77 s |
| 16 | 1.52 jobs/s | 8.58 s | 26.68 s |
| 30 | 1.82 jobs/s | 21.23 s | 46.29 s |

Throughput rises sublinearly while pod startup degrades sharply — the signature of a saturating
resource. Tripling concurrency bought 75% more throughput and tripled startup latency.

**The rate-limiter A/B** — 30 workers, 120 jobs, the only variable being QPS/Burst:

| Run | QPS/Burst | Throughput | Throttling events logged |
|---|---|---:|---:|
| A | 5 / 10 (client-go default) | 1.84 jobs/s | **6** |
| B | 100 / 200 | 1.76 jobs/s | **0** |

**The hypothesis was half right, and the half that was wrong is the interesting part.** The
throttling was real: run A logged six `"client-side throttling, not priority and fairness"` waits
of ~1 s each, exactly as D-070 predicted. Raising the limit eliminated them completely. **And
throughput did not improve** — 1.76 vs 1.84 jobs/s is inside run-to-run noise, and the "fixed" run
was nominally *slower*.

The reason is visible in the same numbers: at ~1.8 jobs/s and ~4 API calls per job, demand is about
7 QPS against a limit of 5. The limiter was clipping a small fraction of requests by ~1 s each,
while every job was already spending **12–21 seconds** waiting for a pod to start. Removing a
one-second delay from a twenty-second critical path is not measurable.

**The real bottleneck is pod sandbox creation on the node**, not the scheduler and not the API
client. Confirmed by the failure mode at 30 workers under sustained load: pods sat in
`ContainerCreating` past their 120 s attempt deadline, and **41 pods never started at all** while the
node reported no memory, disk, or PID pressure. Saturation was purely in kubelet/containerd's rate
of sandbox creation.

**Why the fix was kept anyway.** It removes a limit that is invisible, is not a property of the
workload, and *would* bind on a cluster whose nodes start pods faster. The value of the experiment
is knowing it is not currently the constraint — which means effort spent optimising the client would
have been wasted.

**Sustained run** — 1,000 jobs, 16 workers (the highest concurrency that did not blow the attempt
deadline), Prometheus/Grafana stopped to remove CPU contention:

| | |
|---|---|
| Completed | **1,000 / 1,000 succeeded, 0 failed** |
| Wall clock | 765 s |
| Throughput | **1.32 jobs/s sustained** |
| Peak concurrent pods | 16 |
| **Scheduler dispatch overhead** | **38 µs p50, 750 µs p99** |
| Pod startup | 12.40 s p50, 28.30 s p95 |
| Kubernetes API errors | 0 |

**The headline number is the ratio, not the throughput.** The scheduler's own cost per job is ~38
microseconds against ~12 seconds of pod startup — **roughly 300,000× smaller**. Whatever limits this
system, it is not the scheduling code, and D-072 is what makes that statement a measurement rather
than a belief.

**Reclamation, verified by killing a process rather than by asserting.** A scheduler with 8 running
pods was `SIGKILL`ed — no graceful shutdown, no deferred deletes. All 8 Jobs were left behind in
`Running`, which is precisely the state `ttlSecondsAfterFinished` can never clean up because its
clock only starts when a Job *finishes*. A fresh process reclaimed all 8 at startup
(`orphans_reclaimed_total = 8`). A subsequently dispatched, still-running job survived two reconcile
ticks untouched, confirming claim-before-create protects work in progress.

**The control experiment that settles it.** To separate "our scheduler is slow" from "this cluster
is slow", the same workload was run with the scheduler removed from the path entirely — a single
native `batch/v1` Job with `parallelism: 16` and `completions: 200`, so Kubernetes' own Job
controller did the dispatching:

| Dispatcher | Throughput |
|---|---:|
| Kubernetes native Job controller | 1.23 pods/s |
| This scheduler (identical conditions) | 1.32 jobs/s |

**The scheduler runs at the platform's ceiling.** It is marginally *faster* than Kubernetes' own
batch controller on the same cluster, which is within run-to-run noise — the honest reading is that
both are pinned to the same constraint, and that constraint is pod sandbox creation on the node.
There is no headroom to recover in application code, because there is no gap between our rate and
the platform's maximum rate.

This is what makes the D-072 overhead figure meaningful rather than merely small: 38 µs of scheduler
time per job, against a platform that needs ~760 ms per pod at best. Optimising the scheduler could
improve total throughput by at most 0.005%.

**Honest limits of these numbers.** Single node, 8 shared CPUs, on an M1 MacBook Air — passively
cooled, with a Supabase stack and vector databases running alongside. 1.32 jobs/s is a property of
*this hardware*, not of the scheduler. A multi-node cluster on real servers would raise the ceiling
substantially without a line of code changing, which is exactly the point: the measurements locate
the constraint outside the code under discussion.

---
