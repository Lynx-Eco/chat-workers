// SPDX-License-Identifier: AGPL-3.0-or-later

package hosttest

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Process is a worker binary started the way the engine starts one: a
// cleared environment, `--socket-path <path>` as the last arguments.
type Process struct {
	Cmd    *exec.Cmd
	stderr *os.File
	done   chan struct{}
	err    error
}

// StartWorker runs bin with a cleared environment (as channel_process.rs
// does) and its stderr written to stderrPath (appended).
func StartWorker(bin, socketPath, stderrPath string, extraArgs ...string) (*Process, error) {
	f, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	args := append(append([]string{}, extraArgs...), "--socket-path", socketPath)
	cmd := exec.Command(bin, args...)
	cmd.Env = []string{}
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	p := &Process{Cmd: cmd, stderr: f, done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		_ = f.Close()
		close(p.done)
	}()
	return p, nil
}

// Pid is the worker's process id.
func (p *Process) Pid() int { return p.Cmd.Process.Pid }

// Kill sends SIGKILL (kill -9) to the worker's process group (the worker is
// its leader: StartWorker sets Setpgid), so nothing the worker started
// outlives it, and waits for the worker to go.
func (p *Process) Kill() {
	pid := p.Cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		_ = p.Cmd.Process.Signal(syscall.SIGKILL)
	}
	<-p.done
}

// Done is closed when the process has exited.
func (p *Process) Done() <-chan struct{} { return p.done }

// Wait waits up to limit and returns the exit code (-1 when killed by a
// signal).
func (p *Process) Wait(limit time.Duration) (int, error) {
	select {
	case <-p.done:
	case <-time.After(limit):
		return 0, fmt.Errorf("worker still running after %v", limit)
	}
	if p.err == nil {
		return 0, nil
	}
	var ee *exec.ExitError
	if errors.As(p.err, &ee) {
		return ee.ExitCode(), nil
	}
	return 0, p.err
}

// RSSKiB reads VmRSS of the process from /proc (Linux only).
func (p *Process) RSSKiB() (int, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", p.Pid()))
	if err != nil {
		return 0, err
	}
	var rss int
	for _, line := range splitLines(string(b)) {
		if n, err := fmt.Sscanf(line, "VmRSS: %d kB", &rss); n == 1 && err == nil {
			return rss, nil
		}
	}
	return 0, errors.New("no VmRSS line")
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// BuildWorker builds the main package in pkgDir to out with `go build -tags
// tags`, using the go command and environment of the test process (pass
// store.BuildTag to build as the test binary was built).
func BuildWorker(pkgDir, out, tags, ldflags string) error {
	args := []string{"build", "-tags", tags, "-o", out}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, ".")
	cmd := exec.Command("go", args...)
	cmd.Dir = pkgDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go build in %s: %v\n%s", pkgDir, err, output)
	}
	return nil
}
