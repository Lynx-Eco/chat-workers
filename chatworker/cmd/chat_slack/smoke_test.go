// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-slack/pkg/connector"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "csl")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_slack")
	if err := hosttest.BuildWorker(".", binPath, store.BuildTag, ""); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func run(t *testing.T, settings map[string]string) (*hosttest.Session, *hosttest.Process, string) {
	t.Helper()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	state := t.TempDir()
	stderr := filepath.Join(t.TempDir(), "stderr.log")
	p, err := hosttest.StartWorker(binPath, ln.Path, stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-p.Done():
		default:
			p.Kill()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "slack_session", ConnectionId: "conn-slack",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	up := hello.GetUpstreamVersions()
	if hello.GetWorkerName() != "chat_slack" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		up["go.mau.fi/mautrix-slack"] != slackVersion || up["github.com/beeper/slackgo"] != slackgoVersion ||
		up["maunium.net/go/mautrix"] != mautrixVersion {
		t.Fatalf("hello: %v", hello)
	}
	if !hello.GetStoreEncrypted() {
		t.Fatal("Hello.store_encrypted is false: the store must be encrypted (build with -tags libsqlite3)")
	}
	return s, p, stderr
}

func waitState(t *testing.T, s *hosttest.Session, kind contract.ConnectionStateKind) *contract.ConnectionState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := s.WaitEvent(ctx, func(q *contract.QueuedEvent) bool {
		return q.GetEvent().GetConnectionState().GetState() == kind
	})
	if err != nil {
		t.Fatalf("waiting for state %v: %v", kind, err)
	}
	return e.GetEvent().GetConnectionState()
}

func command(t *testing.T, s *hosttest.Session, cmd *contract.Command) *contract.CommandReply {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := s.Command(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func begin(t *testing.T, s *hosttest.Session, flow string) *contract.CommandReply {
	t.Helper()
	return command(t, s, &contract.Command{Kind: &contract.Command_SignInBegin{SignInBegin: &contract.SignInBegin{FlowId: flow}}})
}

// waitStep waits for the first sign-in step of flow with step id stepID.
func waitStep(t *testing.T, s *hosttest.Session, flow, stepID string) *contract.SignInStep {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e, err := s.WaitEvent(ctx, func(q *contract.QueuedEvent) bool {
		st := q.GetEvent().GetSignInStep()
		return st.GetFlowId() == flow && st.GetStepId() == stepID
	})
	if err != nil {
		t.Fatalf("waiting for step %s of %s: %v", stepID, flow, err)
	}
	return e.GetEvent().GetSignInStep()
}

// TestStartsSignedOutWithoutNetwork: with no settings the worker starts on
// its encrypted store, reports signed out, lists exactly the two flows it
// offers with email first (the one a host that names none begins), refuses
// any other, the connector's "app" (a bot's sign-in) among them, begins each
// of the two to its first step as the contract's kinds (token: a WEB step
// with the auth_token and cookie_token captures; email: a FIELDS step with
// an EMAIL field, whose malformed value is refused without ending the
// sign-in), and opens no socket but the host's: no flow's first step asks
// Slack anything. WHY: catches a flow left out or added (the bot flow
// offered), the order that makes another flow the default, a first step
// that the contract cannot carry (unsupported_step), and a connector or
// flow that reaches the network before the user submits anything.
func TestStartsSignedOutWithoutNetwork(t *testing.T) {
	s, p, _ := run(t, nil)
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	var ids []string
	for _, f := range r.GetFlows() {
		ids = append(ids, f.GetId())
	}
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || !slices.Equal(ids, []string{"email", "token"}) {
		t.Fatalf("flows %v (%v), want exactly email, token", ids, r)
	}
	for _, flow := range []string{connector.LoginFlowIDApp, "", "cookies", "qr", "Email"} {
		if r := begin(t, s, flow); r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "unknown_flow" {
			t.Fatalf("sign-in with %q: %v", flow, r)
		}
	}

	if r := begin(t, s, "token"); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("begin token: %v", r)
	}
	st := waitStep(t, s, "token", connector.LoginStepIDAuthToken)
	if st.GetKind() != contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB || st.GetWeb() == nil {
		t.Fatalf("token's step: %v", st)
	}
	var captures []string
	for _, f := range st.GetWeb().GetFields() {
		if !f.GetRequired() {
			t.Fatalf("capture %s is not required", f.GetId())
		}
		captures = append(captures, f.GetId())
	}
	if !slices.Equal(captures, []string{"auth_token", "cookie_token"}) {
		t.Fatalf("token's captures %v", captures)
	}

	if r := begin(t, s, "email"); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("begin email: %v", r)
	}
	st = waitStep(t, s, "email", connector.LoginStepIDEmail)
	if st.GetKind() != contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS || len(st.GetFields()) != 1 ||
		st.GetFields()[0].GetId() != "email" || st.GetFields()[0].GetKind() != contract.FieldKind_FIELD_KIND_EMAIL {
		t.Fatalf("email's first step: %v", st)
	}
	// A value that is not an email address is refused without ending the
	// sign-in or asking Slack.
	r = command(t, s, &contract.Command{Kind: &contract.Command_SignInSubmit{SignInSubmit: &contract.SignInSubmit{
		StepId: connector.LoginStepIDEmail, Values: map[string]string{"email": "not-an-address"}}}})
	if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "invalid_field" {
		t.Fatalf("malformed email: %v", r)
	}
	if r := command(t, s, &contract.Command{Kind: &contract.Command_SignInCancel{SignInCancel: &contract.SignInCancel{}}}); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("cancel: %v", r)
	}

	time.Sleep(time.Second)
	if n, checked := nonUnixSockets(t, p.Pid()); !checked {
		t.Log("non-Unix sockets not counted: no /proc on this system")
	} else if n != 0 {
		t.Fatalf("worker opened %d non-Unix sockets before any sign-in was submitted", n)
	}
	if rss, err := p.RSSKiB(); err == nil {
		t.Logf("chat_slack idle VmRSS: %d KiB", rss)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
}

// TestStartsWithSettings: max_initial_conversations in range is taken, the
// host's common device_name is not refused (the worker does not read it),
// and the start reaches signed out.
func TestStartsWithSettings(t *testing.T) {
	s, p, _ := run(t, map[string]string{"device_name": "Lynx smoke device", "max_initial_conversations": "40", "max_initial_messages": "20"})
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
}

// TestRefusesBadSettings: a max_initial_conversations that is not a count
// from 0 to 1000 refuses the start (ERROR bad_settings) and every command
// after, and the value is not in the logs. WHY: catches a lenient parse
// that would fall back to a default, or to the connector's -1 (every
// conversation), unseen.
func TestRefusesBadSettings(t *testing.T) {
	const bad = "-1"
	s, p, stderr := run(t, map[string]string{"max_initial_conversations": bad})
	cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
	if cs.GetCode() != "bad_settings" {
		t.Fatalf("code %q", cs.GetCode())
	}
	r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "bad_settings" {
		t.Fatalf("command while unconfigured: %v", r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
	logs, _ := os.ReadFile(stderr)
	if strings.Contains(string(logs), "max_initial_conversations") {
		t.Fatalf("setting in the logs: %s", logs)
	}
}

// TestUpstreamVersionsMatchGoMod: the versions the worker reports in Hello
// are the ones go.mod pins (the fork of slack-go by its replace line).
func TestUpstreamVersionsMatchGoMod(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "replace "); ok {
			if _, to, ok := strings.Cut(rest, "=>"); ok {
				if f := strings.Fields(to); len(f) == 2 {
					pinned[f[0]] = f[1]
				}
			}
			continue
		}
		f := strings.Fields(strings.TrimPrefix(line, "require "))
		if len(f) >= 2 {
			pinned[f[0]] = f[1]
		}
	}
	for mod, want := range map[string]string{
		"maunium.net/go/mautrix":    mautrixVersion,
		"go.mau.fi/mautrix-slack":   slackVersion,
		"github.com/beeper/slackgo": slackgoVersion,
	} {
		if pinned[mod] != want {
			t.Errorf("%s: go.mod pins %q, the worker reports %q", mod, pinned[mod], want)
		}
	}
}
