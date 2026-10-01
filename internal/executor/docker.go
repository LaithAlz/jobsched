package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// Docker runs each attempt in a fresh, single-use container via the Docker Engine API.
type Docker struct {
	cli          *client.Client
	network      string
	logTailBytes int
}

// NewDocker connects to the Docker daemon, negotiating the API version so this code works
// against daemons older and newer than the SDK was built for.
//
// DOCKER_HOST wins if set. Otherwise the socket is discovered by probing well-known paths,
// because the SDK does not read the Docker CLI's *context* configuration — see NOTES.md D-032.
func NewDocker(network string, logTailBytes int) (*Docker, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}
	if os.Getenv("DOCKER_HOST") == "" {
		if host := discoverDockerHost(); host != "" {
			opts = append(opts, client.WithHost(host))
		}
	}
	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &Docker{cli: cli, network: network, logTailBytes: logTailBytes}, nil
}

// discoverDockerHost returns the first well-known Docker socket that actually exists, or "".
func discoverDockerHost() string {
	var candidates []string
	if home, err := os.UserHomeDir(); err == nil {
		// Docker Desktop (macOS, and Linux in rootless mode) puts its socket here and does
		// not create /var/run/docker.sock.
		candidates = append(candidates, filepath.Join(home, ".docker", "run", "docker.sock"))
	}
	// The traditional location: Linux daemons, CI runners, docker-in-docker.
	candidates = append(candidates, "/var/run/docker.sock")

	for _, p := range candidates {
		if fi, err := os.Stat(p); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return "unix://" + p
		}
	}
	return ""
}

// Ping verifies the daemon is reachable. Called once at startup so a misconfigured Docker
// fails the process immediately rather than failing every job individually.
func (d *Docker) Ping(ctx context.Context) error {
	if _, err := d.cli.Ping(ctx); err != nil {
		return fmt.Errorf("docker daemon unreachable: %w", err)
	}
	return nil
}

func (d *Docker) Close() error { return d.cli.Close() }

func (d *Docker) Run(ctx context.Context, req Request) (Result, error) {
	// The attempt timeout is expressed as a context deadline. Every Docker call below takes
	// this ctx, so a timeout unblocks all of them at once rather than needing a separate
	// timer per call.
	runCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	id, err := d.create(runCtx, req)
	if err != nil {
		return Result{}, err
	}

	// Cleanup runs on every path, including timeout and shutdown. It uses a context
	// detached from runCtx (WithoutCancel keeps values, drops cancellation) because
	// removing a container after a timeout is exactly when runCtx is already dead.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
		defer cleanupCancel()
		_ = d.cli.ContainerRemove(cleanupCtx, id, container.RemoveOptions{Force: true})
	}()

	if err := d.cli.ContainerStart(runCtx, id, container.StartOptions{}); err != nil {
		return Result{}, fmt.Errorf("start container: %w", err)
	}

	exitCode, timedOut, err := d.wait(runCtx, ctx, id)
	if err != nil {
		return Result{}, err
	}

	// Logs and OOM status are read after the container has stopped but before it is
	// removed. Both use a detached context so a timed-out attempt still reports why.
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer readCancel()

	res := Result{ExitCode: exitCode, TimedOut: timedOut, RunID: id}
	res.Logs = d.logs(readCtx, id)

	// OOMKilled comes from the daemon's own record of why the kernel killed the process.
	// This is what proves the memory limit was enforced rather than merely requested:
	// exit code 137 alone only means "killed by SIGKILL".
	if info, err := d.cli.ContainerInspect(readCtx, id); err == nil {
		res.OOMKilled = info.State.OOMKilled
	}
	return res, nil
}

func (d *Docker) create(ctx context.Context, req Request) (string, error) {
	cfg := &container.Config{
		Image: req.Image,
		// Run through a shell so submitted commands can use pipes, && and redirection.
		Cmd:          []string{"/bin/sh", "-c", req.Command},
		Tty:          false, // false keeps stdout/stderr as separate multiplexed streams
		AttachStdout: true,
		AttachStderr: true,
		Labels: map[string]string{
			"jobsched.job-id":  req.JobID,
			"jobsched.attempt": fmt.Sprint(req.Attempt),
		},
	}

	hostCfg := &container.HostConfig{
		Resources: container.Resources{
			// NanoCPUs is the API equivalent of `docker run --cpus`. The daemon converts
			// it to CFS quota/period on the container's cgroup.
			NanoCPUs: int64(req.CPUs * 1e9),
			Memory:   req.MemoryMB * 1024 * 1024,
			// MemorySwap == Memory means "no swap allowance". Without this, the daemon
			// grants swap equal to the memory limit, so a process exceeding the limit
			// gets slowly swapped instead of OOM-killed and the limit stops being a hard
			// wall. See NOTES.md D-025.
			MemorySwap: req.MemoryMB * 1024 * 1024,
		},
		NetworkMode: container.NetworkMode(d.network),
		// AutoRemove is deliberately off: it makes the daemon delete the container the
		// instant it exits, which races the inspect call that reads OOMKilled and the log
		// read. Removal is done explicitly after those. See NOTES.md D-026.
		AutoRemove: false,
	}

	name := fmt.Sprintf("jobsched-%s-%d", req.JobID, req.Attempt)

	resp, err := d.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err == nil {
		return resp.ID, nil
	}

	// Pull lazily, only when creation actually reports a missing image. This costs one
	// failed create on a cold image and zero extra API calls on every subsequent job, which
	// is the right trade when almost all jobs reuse the same few images.
	if !client.IsErrNotFound(err) {
		return "", fmt.Errorf("create container: %w", err)
	}
	if err := d.pull(ctx, req.Image); err != nil {
		return "", err
	}
	resp, err = d.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("create container after pull: %w", err)
	}
	return resp.ID, nil
}

func (d *Docker) pull(ctx context.Context, ref string) error {
	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image %s: %w", ref, err)
	}
	defer rc.Close()
	// The pull only completes once its progress stream is fully consumed; discarding the
	// body is how you wait for it.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return fmt.Errorf("pull image %s: %w", ref, err)
	}
	return nil
}

// wait blocks until the container exits, the attempt times out, or the process shuts down.
//
// runCtx carries the attempt timeout; parentCtx is the outer (shutdown) context. They are
// distinguished so a timeout can be reported as a timeout rather than as a generic error.
func (d *Docker) wait(runCtx, parentCtx context.Context, id string) (exitCode int, timedOut bool, err error) {
	// ContainerWait returns two channels: one for the result, one for errors. This is a
	// common Go SDK shape — select over both, plus the context, so no path blocks forever.
	statusCh, errCh := d.cli.ContainerWait(runCtx, id, container.WaitConditionNotRunning)

	select {
	case status := <-statusCh:
		if status.Error != nil {
			return 0, false, fmt.Errorf("container wait: %s", status.Error.Message)
		}
		return int(status.StatusCode), false, nil

	case werr := <-errCh:
		// A cancelled runCtx surfaces here. Distinguish "attempt timed out" (parent still
		// alive) from "the whole scheduler is shutting down".
		if runCtx.Err() != nil && parentCtx.Err() == nil {
			d.kill(parentCtx, id)
			return 137, true, nil
		}
		if werr != nil {
			return 0, false, fmt.Errorf("container wait: %w", werr)
		}
		return 0, false, fmt.Errorf("container wait: stream closed")
	}
}

// kill stops a container that overran its timeout, so the exit code and logs can be read
// before the deferred removal.
func (d *Docker) kill(parentCtx context.Context, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), 15*time.Second)
	defer cancel()
	_ = d.cli.ContainerKill(ctx, id, "SIGKILL")
}

// logs returns the tail of the container's combined output, truncated to logTailBytes.
func (d *Docker) logs(ctx context.Context, id string) string {
	rc, err := d.cli.ContainerLogs(ctx, id, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "200", // server-side line cap, so we never stream a huge log back
	})
	if err != nil {
		return ""
	}
	defer rc.Close()

	// Without a TTY, the daemon returns stdout and stderr multiplexed in a framed stream.
	// stdcopy.StdCopy demultiplexes it; reading the stream raw would interleave 8-byte
	// binary frame headers into the output.
	var buf bytes.Buffer
	if _, err := stdcopy.StdCopy(&buf, &buf, rc); err != nil && buf.Len() == 0 {
		return ""
	}
	return tail(buf.Bytes(), d.logTailBytes)
}

// tail returns the last n bytes of b as a string, prefixed with a truncation marker if it cut.
func tail(b []byte, n int) string {
	if n <= 0 || len(b) <= n {
		return string(b)
	}
	return "...[truncated]...\n" + string(b[len(b)-n:])
}
