.PHONY: help build run run-k8s test test-unit test-integration test-k8s test-ui test-all fmt vet clean \
        clean-containers clean-k8s-jobs stack-up stack-down stack-reset stack-logs stack-status \
        metrics cluster-up cluster-down cluster-status loadgen load-test

GO ?= go
BIN := bin/scheduler

help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Compile the scheduler binary into bin/
	$(GO) build -o $(BIN) ./cmd/scheduler

run: build ## Run the scheduler (Ctrl-C for graceful shutdown)
	./$(BIN)

test-unit: ## Fast tests, no Docker required (~5s)
	$(GO) test -race ./internal/...

test-integration: ## End-to-end tests against real Docker (~25s). Fails if Docker is unusable.
	SCHED_REQUIRE_DOCKER=1 $(GO) test -v -timeout 20m ./test/

test-k8s: ## End-to-end tests against the kind cluster (~45s). Fails if no cluster.
	SCHED_REQUIRE_K8S=1 $(GO) test -v -timeout 30m -run 'TestK8s' ./test/

test-ui: ## Check the demo UI's timeline geometry (needs node; skipped if absent)
	@command -v node >/dev/null 2>&1 \
		&& node test/ui_check.js \
		|| echo "node not installed - skipping UI check"

test: test-unit test-integration test-ui ## Run the Docker test tiers

test-all: ## Everything including Kubernetes, with the race detector
	SCHED_REQUIRE_DOCKER=1 SCHED_REQUIRE_K8S=1 $(GO) test -race -timeout 40m ./...

cluster-up: ## Create the local kind cluster (~4 min on first run: it pulls a node image)
	kind create cluster --config deploy/kind/cluster.yaml --wait 120s
	@echo
	@kubectl --context kind-jobsched get nodes

cluster-down: ## Delete the kind cluster
	kind delete cluster --name jobsched

cluster-status: ## Show cluster and jobsched namespace state
	@kubectl --context kind-jobsched get nodes 2>/dev/null || { echo "no cluster: run 'make cluster-up'"; exit 1; }
	@echo
	@kubectl --context kind-jobsched get jobs,pods -n jobsched 2>/dev/null || true

loadgen: ## Build the load generator
	$(GO) build -o bin/loadgen ./cmd/loadgen

load-test: build loadgen ## Measure sustained throughput (needs a scheduler on :8081)
	./bin/loadgen -addr http://localhost:8081 -n $(or $(N),1000) -label "$(or $(LABEL),load-test)"

run-k8s: build ## Run the scheduler in Kubernetes mode on :8081 (needs 'make cluster-up')
	SCHED_EXECUTOR=kubernetes SCHED_K8S_CONTEXT=kind-jobsched SCHED_ADDR=:8081 ./$(BIN)

clean-k8s-jobs: ## Delete any leftover Kubernetes Jobs created by the scheduler
	kubectl --context kind-jobsched delete jobs -A -l app.kubernetes.io/managed-by=jobsched --ignore-not-found

fmt: ## Format all Go source
	gofmt -w ./cmd ./internal ./test

vet: ## Static analysis
	$(GO) vet ./...

stack-up: ## Start scheduler + Prometheus + Grafana (Grafana on http://localhost:3000)
	docker compose up --build -d
	@echo
	@echo "  scheduler   http://localhost:8080/healthz"
	@echo "  metrics     http://localhost:8080/metrics"
	@echo "  prometheus  http://localhost:9090"
	@echo "  grafana     http://localhost:3000   (opens on the jobsched dashboard, no login)"

stack-down: ## Stop the stack, keeping metric history
	docker compose down

stack-reset: ## Stop the stack and discard all metric history
	docker compose down -v

stack-logs: ## Follow scheduler logs
	docker compose logs -f scheduler

stack-status: ## Show container status and Prometheus target health
	@docker compose ps
	@echo
	@curl -s 'localhost:9090/api/v1/targets?state=active' | \
		python3 -c 'import sys,json;[print("  job=%-11s health=%s" % (t["labels"]["job"], t["health"])) for t in json.load(sys.stdin)["data"]["activeTargets"]]' \
		2>/dev/null || echo "  prometheus not reachable"

metrics: ## Print the scheduler's own metrics
	@curl -s localhost:8080/metrics | grep -E '^jobsched_' | grep -v '_bucket'

clean: ## Remove build output
	rm -rf bin/

clean-containers: ## Remove any orphaned job containers (see NOTES.md D-026)
	@ids=$$(docker ps -aq -f label=jobsched.job-id); \
	if [ -n "$$ids" ]; then docker rm -f $$ids; else echo "no orphaned job containers"; fi
