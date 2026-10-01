// Command loadgen measures the scheduler's sustained throughput.
//
// It submits N jobs and then samples the scheduler's own Prometheus counters until all N have
// reached a terminal state, reporting the rate at which jobs *complete* rather than the rate at
// which they are accepted. Submission is nearly free — the API validates, stores a record and
// returns 202 without doing any work (D-007) — so a submission-rate number would report the
// queue's write speed and would look excellent no matter how slowly the pool actually drained.
//
// Progress is read from /metrics rather than by polling /jobs, for two reasons: the job list is
// capped (D-068) so it cannot show every job in a large run, and polling a large list several
// times a second would make the measuring client a load source in its own right. A counter
// scrape is a few kilobytes regardless of N. See NOTES.md D-073.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		addr    = flag.String("addr", "http://localhost:8081", "scheduler base URL")
		total   = flag.Int("n", 1000, "number of jobs to submit")
		conc    = flag.Int("submit-concurrency", 16, "parallel submitters")
		command = flag.String("command", "true", "command each job runs")
		cpus    = flag.Float64("cpus", 0.05, "cpu limit per job")
		memory  = flag.Int("memory-mb", 32, "memory limit per job")
		label   = flag.String("label", "run", "label for this run, printed in the report")
	)
	flag.Parse()
	if err := run(*addr, *total, *conc, *command, *cpus, *memory, *label); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(addr string, total, conc int, command string, cpus float64, memMB int, label string) error {
	client := &http.Client{Timeout: 30 * time.Second}

	// Capture the configuration that produced the result, so runs are comparable (D-073).
	health, err := getJSON(client, addr+"/healthz")
	if err != nil {
		return fmt.Errorf("scheduler not reachable at %s: %w", addr, err)
	}
	base, err := scrape(client, addr)
	if err != nil {
		return err
	}
	startCompleted := base.counter("jobsched_jobs_completed_total")

	fmt.Printf("\n════ load run: %s ════\n", label)
	fmt.Printf("  target    %s\n", addr)
	fmt.Printf("  executor  %v      workers %v\n", health["executor"], health["workers"])
	fmt.Printf("  workload  %d jobs, command=%q, cpus=%g, memory=%dMi, max_attempts=1\n\n",
		total, command, cpus, memMB)

	body, _ := json.Marshal(map[string]any{
		"command": command, "cpus": cpus, "memory_mb": memMB,
		"max_attempts": 1, "timeout_seconds": 120,
	})

	// ---- submit ----
	var accepted, rejected atomic.Int64
	work := make(chan struct{})
	var wg sync.WaitGroup
	submitStart := time.Now()
	for range conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				resp, err := client.Post(addr+"/jobs", "application/json", bytes.NewReader(body))
				if err != nil {
					rejected.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusAccepted {
					accepted.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}()
	}
	for range total {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()
	submitDur := time.Since(submitStart)
	want := accepted.Load()

	fmt.Printf("  submitted %d accepted, %d rejected in %s (%.0f/s accept rate — not throughput)\n\n",
		want, rejected.Load(), submitDur.Round(time.Millisecond), float64(want)/submitDur.Seconds())
	if want == 0 {
		return fmt.Errorf("no jobs were accepted")
	}

	// ---- follow completions via the counter ----
	type sample struct {
		at   time.Time
		done float64
	}
	var samples []sample
	var peakActive, peakBusy float64
	deadline := time.Now().Add(25 * time.Minute)
	lastLog := time.Now()

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for completions")
		}
		m, err := scrape(client, addr)
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		done := m.counter("jobsched_jobs_completed_total") - startCompleted
		samples = append(samples, sample{time.Now(), done})
		peakActive = max(peakActive, m.gauge("jobsched_k8s_active_jobs"))
		peakBusy = max(peakBusy, m.gauge("jobsched_workers_busy"))

		if time.Since(lastLog) > 5*time.Second {
			fmt.Printf("  ... %.0f/%d complete   queue=%.0f running=%.0f\n",
				done, want, m.gauge("jobsched_queue_depth"), m.gauge("jobsched_workers_busy"))
			lastLog = time.Now()
		}
		if done >= float64(want) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Steady state: from the first completion to the last, so the ramp while the pool fills
	// does not drag the average down.
	var firstIdx, lastIdx int
	for i, s := range samples {
		if s.done > 0 {
			firstIdx = i
			break
		}
	}
	lastIdx = len(samples) - 1
	steady := samples[lastIdx].at.Sub(samples[firstIdx].at).Seconds()
	deltaJobs := samples[lastIdx].done - samples[firstIdx].done
	wall := samples[lastIdx].at.Sub(submitStart).Seconds()

	final, _ := scrape(client, addr)

	fmt.Printf("\n──── results: %s ────\n", label)
	fmt.Printf("  completed            %.0f jobs\n", samples[lastIdx].done)
	fmt.Printf("  wall clock           %.1fs   (first submit → last completion)\n", wall)
	fmt.Printf("  steady-state window  %.1fs\n", steady)
	if steady > 0 {
		fmt.Printf("  ►  THROUGHPUT        %.2f jobs/sec   (steady state)\n", deltaJobs/steady)
	}
	fmt.Printf("     throughput (wall) %.2f jobs/sec\n\n", samples[lastIdx].done/wall)

	fmt.Printf("  peak concurrent pods %.0f   (peak busy workers %.0f)\n", peakActive, peakBusy)
	fmt.Printf("  scheduler overhead   p50 %s   p99 %s   ← our own dispatch cost\n",
		fmtSec(final.quantile("jobsched_dispatch_overhead_seconds", 0.50)),
		fmtSec(final.quantile("jobsched_dispatch_overhead_seconds", 0.99)))
	fmt.Printf("  pod startup          p50 %s   p95 %s   ← platform cost\n",
		fmtSec(final.quantile("jobsched_k8s_pod_schedule_latency_seconds", 0.50)),
		fmtSec(final.quantile("jobsched_k8s_pod_schedule_latency_seconds", 0.95)))
	fmt.Printf("  attempt duration     p50 %s   p95 %s\n",
		fmtSec(final.quantile("jobsched_attempt_duration_seconds", 0.50)),
		fmtSec(final.quantile("jobsched_attempt_duration_seconds", 0.95)))
	fmt.Printf("  queue wait           p50 %s   p95 %s\n",
		fmtSec(final.quantile("jobsched_queue_wait_seconds", 0.50)),
		fmtSec(final.quantile("jobsched_queue_wait_seconds", 0.95)))
	fmt.Printf("  k8s api errors       %.0f\n", final.counter("jobsched_k8s_api_errors_total"))
	fmt.Printf("  orphans reclaimed    %.0f\n", final.counter("jobsched_k8s_orphans_reclaimed_total"))
	return nil
}

func fmtSec(v float64) string {
	switch {
	case v == 0:
		return "n/a"
	case v < 0.001:
		return fmt.Sprintf("%.0fµs", v*1e6)
	case v < 1:
		return fmt.Sprintf("%.1fms", v*1e3)
	default:
		return fmt.Sprintf("%.2fs", v)
	}
}

// ---- a very small Prometheus text-format reader ----

type metrics struct {
	// name -> summed value across all label sets
	sums map[string]float64
	// histogram name -> upper bound -> cumulative count
	buckets map[string]map[float64]float64
}

func (m metrics) counter(name string) float64 { return m.sums[name] }
func (m metrics) gauge(name string) float64   { return m.sums[name] }

// quantile reproduces Prometheus' histogram_quantile: find the bucket containing the target
// rank and interpolate linearly within it.
func (m metrics) quantile(name string, q float64) float64 {
	b := m.buckets[name+"_bucket"]
	if len(b) == 0 {
		return 0
	}
	bounds := make([]float64, 0, len(b))
	for ub := range b {
		bounds = append(bounds, ub)
	}
	sort.Float64s(bounds)
	total := b[bounds[len(bounds)-1]]
	if total == 0 {
		return 0
	}
	rank := q * total
	prevBound, prevCount := 0.0, 0.0
	for _, ub := range bounds {
		if b[ub] >= rank {
			if math.IsInf(ub, 1) { // +Inf bucket: no upper bound to interpolate into
				return prevBound
			}
			if b[ub] == prevCount {
				return ub
			}
			return prevBound + (ub-prevBound)*((rank-prevCount)/(b[ub]-prevCount))
		}
		prevBound, prevCount = ub, b[ub]
	}
	return bounds[len(bounds)-1]
}

func scrape(c *http.Client, addr string) (metrics, error) {
	m := metrics{sums: map[string]float64{}, buckets: map[string]map[float64]float64{}}
	resp, err := c.Get(addr + "/metrics")
	if err != nil {
		return m, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return m, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		sp := strings.LastIndex(line, " ")
		if sp < 0 {
			continue
		}
		val, err := strconv.ParseFloat(strings.TrimSpace(line[sp+1:]), 64)
		if err != nil {
			continue
		}
		series := line[:sp]
		name, labels := series, ""
		if i := strings.IndexByte(series, '{'); i >= 0 {
			name, labels = series[:i], series[i:]
		}
		m.sums[name] += val

		if strings.HasSuffix(name, "_bucket") {
			le := extractLabel(labels, "le")
			ub, err := strconv.ParseFloat(le, 64) // handles "+Inf"
			if err != nil {
				continue
			}
			if m.buckets[name] == nil {
				m.buckets[name] = map[float64]float64{}
			}
			m.buckets[name][ub] += val
		}
	}
	return m, nil
}

func extractLabel(labels, key string) string {
	i := strings.Index(labels, key+"=\"")
	if i < 0 {
		return ""
	}
	rest := labels[i+len(key)+2:]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func getJSON(c *http.Client, url string) (map[string]any, error) {
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, json.Unmarshal(b, &out)
}
