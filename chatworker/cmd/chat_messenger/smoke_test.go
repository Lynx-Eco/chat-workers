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

	"go.mau.fi/mautrix-meta/pkg/connector"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/metaworker"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cms")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_messenger")
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
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "messenger", ConnectionId: "conn-messenger",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	up := hello.GetUpstreamVersions()
	if hello.GetWorkerName() != "chat_messenger" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		up["go.mau.fi/mautrix-meta"] != metaworker.MetaVersion || up["github.com/beeper/req/v3"] != metaworker.ReqVersion ||
		up["go.mau.fi/whatsmeow"] != whatsmeowVersion || up["maunium.net/go/mautrix"] != metaworker.MautrixVersion {
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

// waitStep waits for the first sign-in step of flow of kind kind.
func waitStep(t *testing.T, s *hosttest.Session, flow string, kind contract.SignInStepKind) *contract.SignInStep {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e, err := s.WaitEvent(ctx, func(q *contract.QueuedEvent) bool {
		st := q.GetEvent().GetSignInStep()
		return st.GetFlowId() == flow && st.GetKind() == kind
	})
	if err != nil {
		t.Fatalf("waiting for a %v step of %s: %v", kind, flow, err)
	}
	return e.GetEvent().GetSignInStep()
}

// checkWebStep checks a flow's cookies step as the host is given it.
func checkWebStep(t *testing.T, st *contract.SignInStep, url, domain string) {
	t.Helper()
	w := st.GetWeb()
	if st.GetStepId() != connector.LoginStepIDCookies || w == nil || w.GetUrl() != url || w.GetUserAgent() == "" || w.GetWaitForUrlPattern() == "" {
		t.Fatalf("%s's step: %v", st.GetFlowId(), st)
	}
	var captures []string
	for _, f := range w.GetFields() {
		captures = append(captures, f.GetId())
		src := f.GetSources()
		if !f.GetRequired() || len(src) != 1 || src[0].GetSource() != contract.WebCaptureSource_WEB_CAPTURE_SOURCE_COOKIE ||
			src[0].GetName() != f.GetId() || src[0].GetCookieDomain() != domain {
			t.Fatalf("%s's capture %v", st.GetFlowId(), f)
		}
	}
	if !slices.Equal(captures, []string{"xs", "c_user", "datr"}) {
		t.Fatalf("%s's captures %v", st.GetFlowId(), captures)
	}
}

// TestStartsSignedOutWithoutNetwork: with no settings the worker starts on
// its encrypted store, reports signed out, lists exactly the two cookie
// flows with facebook first (the one a host that names none begins),
// refuses any other, the connector's two password flows among them, begins
// each to its WEB step (the cookies xs, c_user and datr of its site, all
// required), fails a submission missing a cookie as
// fi.mau.meta_missing_cookies without asking Meta and without the cookie's
// value in its logs, and opens no socket but the host's. WHY: catches a
// flow left out or added (a password flow offered), the order that makes
// messenger.com the default, a first step that the contract cannot carry
// (unsupported_step), a partial capture sent to Meta, and a connector or
// flow that reaches the network before the user signed in.
func TestStartsSignedOutWithoutNetwork(t *testing.T) {
	s, p, stderr := run(t, nil)
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	var ids []string
	for _, f := range r.GetFlows() {
		ids = append(ids, f.GetId())
	}
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || !slices.Equal(ids, []string{"facebook", "messenger"}) {
		t.Fatalf("flows %v (%v), want exactly facebook, messenger", ids, r)
	}
	for _, flow := range []string{connector.FlowIDMessengerLiteIOS, connector.FlowIDMessengerLiteAndroid, "", "instagram", "Facebook", "cookies"} {
		if r := begin(t, s, flow); r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "unknown_flow" {
			t.Fatalf("sign-in with %q: %v", flow, r)
		}
	}

	if r := begin(t, s, "facebook"); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("begin facebook: %v", r)
	}
	checkWebStep(t, waitStep(t, s, "facebook", contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB), "https://www.facebook.com/", "facebook.com")
	const secret = "lynx-smoke-cookie-value"
	r = command(t, s, &contract.Command{Kind: &contract.Command_SignInSubmit{SignInSubmit: &contract.SignInSubmit{
		StepId: connector.LoginStepIDCookies, Values: map[string]string{"xs": secret}}}})
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("submit: %v", r)
	}
	if st := waitStep(t, s, "facebook", contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED); st.GetFailed().GetCode() != "fi.mau.meta_missing_cookies" {
		t.Fatalf("a partial capture: %v", st)
	}

	if r := begin(t, s, "messenger"); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("begin messenger: %v", r)
	}
	checkWebStep(t, waitStep(t, s, "messenger", contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB), "https://www.messenger.com/?no_redirect=true", "messenger.com")
	if r := command(t, s, &contract.Command{Kind: &contract.Command_SignInCancel{SignInCancel: &contract.SignInCancel{}}}); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("cancel: %v", r)
	}

	time.Sleep(time.Second)
	if n, checked := nonUnixSockets(t, p.Pid()); !checked {
		t.Log("non-Unix sockets not counted: no /proc on this system")
	} else if n != 0 {
		t.Fatalf("worker opened %d non-Unix sockets before any sign-in reached Meta", n)
	}
	if rss, err := p.RSSKiB(); err == nil {
		t.Logf("chat_messenger idle VmRSS: %d KiB", rss)
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
	if strings.Contains(string(logs), secret) {
		t.Fatalf("a cookie value in the logs: %s", logs)
	}
}

// TestStartsWithSettings: max_inbox_pages in range is taken, the host's
// common device_name is not refused (the worker does not read it), and the
// start reaches signed out.
func TestStartsWithSettings(t *testing.T) {
	s, p, _ := run(t, map[string]string{"device_name": "Lynx smoke device", "max_inbox_pages": "3", "max_initial_messages": "20"})
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

// TestRefusesBadSettings: a max_inbox_pages that is not a count from 1 to
// 20, or a key the worker does not know, refuses the start (ERROR
// bad_settings) and every command after, and neither the key nor the value
// is in the logs. WHY: catches a lenient parse that would fall back to a
// default, or to the connector's -1 (every chat of the account), unseen.
func TestRefusesBadSettings(t *testing.T) {
	for _, bad := range []map[string]string{{"max_inbox_pages": "0"}, {"max_initial_conversations": "15"}} {
		s, p, stderr := run(t, bad)
		cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
		if cs.GetCode() != "bad_settings" {
			t.Fatalf("%v: code %q", bad, cs.GetCode())
		}
		r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
		if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "bad_settings" {
			t.Fatalf("%v: command while unconfigured: %v", bad, r)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := s.Shutdown(ctx, 5*time.Second); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
			t.Fatalf("%v: exit %d %v", bad, code, err)
		}
		logs, _ := os.ReadFile(stderr)
		for k := range bad {
			if strings.Contains(string(logs), k) {
				t.Fatalf("setting in the logs: %s", logs)
			}
		}
	}
}

// TestUpstreamVersionsMatchGoMod: the versions the worker reports in Hello
// are the ones go.mod pins (the fork of imroc/req by its replace line).
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
		"maunium.net/go/mautrix":   metaworker.MautrixVersion,
		"go.mau.fi/mautrix-meta":   metaworker.MetaVersion,
		"github.com/beeper/req/v3": metaworker.ReqVersion,
		"go.mau.fi/whatsmeow":      whatsmeowVersion,
	} {
		if pinned[mod] != want {
			t.Errorf("%s: go.mod pins %q, the worker reports %q", mod, pinned[mod], want)
		}
	}
}
