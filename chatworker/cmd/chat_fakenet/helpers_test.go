// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cfn")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_fakenet")
	if err := hosttest.BuildWorker(".", binPath, store.BuildTag, "-X main.sourceCommit=0123456789abcdef0123456789abcdef01234567 -X main.version=0.1.0-test"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// worker is one run of chat_fakenet under the fake host.
type worker struct {
	t      *testing.T
	state  string
	stderr string
	ln     *hosttest.Listener
	proc   *hosttest.Process
	s      *hosttest.Session
	hello  *contract.Hello
}

type opts struct {
	script    *fakenet.Script
	principal string
	noAutoAck bool
	// key replaces testKey() as Init.store_key.
	key []byte
	// settings are added to Init.settings.
	settings map[string]string
	// hostStoredSeq is Init.host_stored_seq.
	hostStoredSeq uint64
}

// testKey is the store key of every test run unless opts.key is set.
func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(0xA0 + i)
	}
	return key
}

// newState makes a state directory (and its script) for one or more runs.
func newState(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func start(t *testing.T, state string, o opts) *worker {
	t.Helper()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{"device_name": "Lynx"}
	for k, v := range o.settings {
		settings[k] = v
	}
	if o.script != nil {
		raw, _ := json.Marshal(o.script)
		path := filepath.Join(state, "..", filepath.Base(state)+"-script.json")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		settings["script"] = path
	}
	w := &worker{t: t, state: state, stderr: filepath.Join(state, "..", filepath.Base(state)+"-stderr.log"), ln: ln}
	w.proc, err = hosttest.StartWorker(binPath, ln.Path, w.stderr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := testKey()
	if o.key != nil {
		key = o.key
	}
	w.s, w.hello, err = ln.Accept(ctx, &contract.Init{
		ContractVersion: contract.Version, Connector: "fakenet", ConnectionId: "conn-test-1",
		StateDir: state, StoreKey: key, Principal: o.principal, Settings: settings,
		HostStoredSeq: o.hostStoredSeq,
	})
	if err != nil {
		w.proc.Kill()
		t.Fatalf("accept: %v", err)
	}
	if o.noAutoAck {
		w.s.AutoAck.Store(false)
	}
	t.Cleanup(func() {
		select {
		case <-w.proc.Done():
		default:
			w.proc.Kill()
		}
		_ = ln.Close()
	})
	return w
}

func (w *worker) ctx(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func (w *worker) command(c *contract.Command) *contract.CommandReply {
	w.t.Helper()
	ctx, cancel := w.ctx(30 * time.Second)
	defer cancel()
	r, err := w.s.Command(ctx, c)
	if err != nil {
		w.t.Fatalf("command: %v", err)
	}
	return r
}

func (w *worker) wait(what string, match func(*contract.ChatEvent) bool) *contract.ChatEvent {
	w.t.Helper()
	ctx, cancel := w.ctx(20 * time.Second)
	defer cancel()
	e, err := w.s.WaitEvent(ctx, func(q *contract.QueuedEvent) bool { return match(q.GetEvent()) })
	if err != nil {
		w.t.Fatalf("waiting for %s: %v", what, err)
	}
	return e.GetEvent()
}

func (w *worker) shutdown() {
	w.t.Helper()
	ctx, cancel := w.ctx(15 * time.Second)
	defer cancel()
	if err := w.s.Shutdown(ctx, 5*time.Second); err != nil {
		w.t.Fatalf("shutdown: %v", err)
	}
	code, err := w.proc.Wait(10 * time.Second)
	if err != nil || code != 0 {
		w.t.Fatalf("worker exit %d %v", code, err)
	}
}

func signInBegin(flow string) *contract.Command {
	return &contract.Command{Kind: &contract.Command_SignInBegin{SignInBegin: &contract.SignInBegin{FlowId: flow}}}
}

func signInSubmit(step string, values map[string]string) *contract.Command {
	return &contract.Command{Kind: &contract.Command_SignInSubmit{SignInSubmit: &contract.SignInSubmit{StepId: step, Values: values}}}
}

func step(e *contract.ChatEvent) *contract.SignInStep { return e.GetSignInStep() }

func isStep(kind contract.SignInStepKind) func(*contract.ChatEvent) bool {
	return func(e *contract.ChatEvent) bool { return step(e) != nil && step(e).GetKind() == kind }
}

// signIn runs the phone flow to its end and waits for the initial sync.
func (w *worker) signIn() {
	w.t.Helper()
	if r := w.command(signInBegin(fakenet.FlowPhone)); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		w.t.Fatalf("begin: %v", r)
	}
	f := w.wait("fields step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	if r := w.command(signInSubmit(step(f).GetStepId(), map[string]string{"phone": "+15550000000"})); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		w.t.Fatalf("submit: %v", r)
	}
	w.wait("done step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	w.waitSynced()
}

func (w *worker) waitSynced() {
	w.t.Helper()
	w.wait("group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	// Let the last receipts and states arrive.
	ctx, cancel := w.ctx(3 * time.Second)
	defer cancel()
	w.s.WaitQuiet(ctx, 300*time.Millisecond)
	// The fake network stores that the login is synced after the sync's
	// last event; a worker stopped before that syncs again at its next
	// start. Wait for the stored flag, which reports nothing upward.
	st, done := openStore(w.t, w.state)
	defer done()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		err := st.DB().QueryRow(context.Background(),
			`SELECT COUNT(*) FROM user_login WHERE json_extract(metadata, '$.synced') = 1`).Scan(&n)
		if err == nil && n > 0 {
			return
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("the login's sync was not stored (%v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (w *worker) events() []*contract.ChatEvent {
	var out []*contract.ChatEvent
	for _, q := range w.s.Events() {
		out = append(out, q.GetEvent())
	}
	return out
}

func messages(evs []*contract.ChatEvent, chat, msg string) []*contract.Message {
	var out []*contract.Message
	for _, e := range evs {
		if m := e.GetMessage(); m != nil && m.GetChatId() == chat && m.GetMessageId() == msg {
			out = append(out, m)
		}
	}
	return out
}

func stderrOf(t *testing.T, w *worker) string {
	b, err := os.ReadFile(w.stderr)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

var _ = strings.Contains
