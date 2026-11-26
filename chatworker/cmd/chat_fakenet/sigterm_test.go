// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"syscall"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

// sigterm sends SIGTERM to the worker as the engine does (to its process
// group) and returns how long it took to end, failing unless it died of
// SIGTERM (the wait status the engine reports as worker_killed, not an exit
// code) within the engine's 2 s before SIGKILL.
func sigterm(t *testing.T, w *worker) time.Duration {
	t.Helper()
	began := time.Now()
	if err := syscall.Kill(-w.proc.Pid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if _, err := w.proc.Wait(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	took := time.Since(began)
	ws, ok := w.proc.Cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Fatalf("after SIGTERM: wait status %v (exited %v, code %d), want signaled by SIGTERM", w.proc.Cmd.ProcessState, ws.Exited(), ws.ExitStatus())
	}
	if took > 2*time.Second {
		t.Fatalf("ended %v after SIGTERM: past the engine's 2 s before SIGKILL", took)
	}
	return took
}

// TestSigtermDuringRefusedLogout: a sign-in reaches another account, and
// the logout of its session hangs at the network. SIGTERM then makes the
// worker end at once (not after the framework's stop or the logout), and
// the log says sigterm and refused_session_not_ended, once each.
func TestSigtermDuringRefusedLogout(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{Account: "u-other", LogoutHangMs: 60_000}})
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	failed := w.wait("failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED))
	if step(failed).GetFailed().GetCode() != "principal_mismatch" {
		t.Fatalf("failure %v", step(failed))
	}
	// The gate started the logout before the sign-in failed.
	if logs := stderrOf(t, w); !strings.Contains(logs, `"principal_mismatch"`) {
		t.Fatalf("log %q: no refusal by the gate", logs)
	}
	took := sigterm(t, w)
	t.Logf("ended %v after SIGTERM", took)
	logs := stderrOf(t, w)
	if strings.Count(logs, `"sigterm"`) != 1 || strings.Count(logs, `"refused_session_not_ended"`) != 1 {
		t.Fatalf("log %q: want sigterm and refused_session_not_ended once each", logs)
	}
}

// TestSigtermIdle: SIGTERM with nothing running logs sigterm only, and the
// worker dies of SIGTERM at once.
func TestSigtermIdle(t *testing.T) {
	w := start(t, newState(t), opts{})
	sigterm(t, w)
	logs := stderrOf(t, w)
	if strings.Count(logs, `"sigterm"`) != 1 || strings.Contains(logs, "refused_session") {
		t.Fatalf("log %q: want sigterm once and nothing of a refused session", logs)
	}
}
