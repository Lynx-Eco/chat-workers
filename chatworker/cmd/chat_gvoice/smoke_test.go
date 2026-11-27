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

	"go.mau.fi/mautrix-gvoice/pkg/connector"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cgv")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_gvoice")
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
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "gvoice", ConnectionId: "conn-gvoice",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	up := hello.GetUpstreamVersions()
	if hello.GetWorkerName() != "chat_gvoice" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		up["go.mau.fi/mautrix-gvoice"] != gvoiceVersion || up["maunium.net/go/mautrix"] != mautrixVersion || len(up) != 2 {
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

func shutdown(t *testing.T, s *hosttest.Session, p *hosttest.Process) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
}

// TestStartsSignedOutWithoutNetwork: with no settings the worker starts on
// its encrypted store, reports signed out, lists exactly the connector's one
// flow, refuses any other, begins it to its WEB step (the eight cookies the
// connector names, each a COOKIE capture of its domain, exactly the five
// Google session cookies required, the sign-up page's URL, no wait pattern
// or script), and opens no socket but the host's: the first step asks Google
// nothing. WHY: catches a flow added or renamed, a first step the contract
// cannot carry (unsupported_step), a cookie list changed upstream, and a
// connector that reaches the network before the user submits anything.
func TestStartsSignedOutWithoutNetwork(t *testing.T) {
	s, p, _ := run(t, nil)
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	var ids []string
	for _, f := range r.GetFlows() {
		ids = append(ids, f.GetId())
	}
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || !slices.Equal(ids, []string{"cookies"}) {
		t.Fatalf("flows %v (%v), want exactly cookies", ids, r)
	}
	for _, flow := range []string{"", "qr", "email", "token", "Cookies"} {
		if r := begin(t, s, flow); r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "unknown_flow" {
			t.Fatalf("sign-in with %q: %v", flow, r)
		}
	}

	if r := begin(t, s, connector.LoginFlowIDCookies); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("begin cookies: %v", r)
	}
	st := waitStep(t, s, "cookies", connector.LoginStepIDCookies)
	web := st.GetWeb()
	if st.GetKind() != contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB || web == nil {
		t.Fatalf("cookies' step: %v", st)
	}
	if web.GetUrl() != "https://voice.google.com/signup" || web.GetWaitForUrlPattern() != "" || web.GetExtractJs() != "" || web.GetUserAgent() != "" {
		t.Fatalf("web step %v", web)
	}
	domains := map[string]string{}
	var required []string
	for _, f := range web.GetFields() {
		src := f.GetSources()
		if len(src) != 1 || src[0].GetSource() != contract.WebCaptureSource_WEB_CAPTURE_SOURCE_COOKIE || src[0].GetName() != f.GetId() {
			t.Fatalf("capture %v", f)
		}
		domains[f.GetId()] = src[0].GetCookieDomain()
		if f.GetRequired() {
			required = append(required, f.GetId())
		}
	}
	slices.Sort(required)
	want := map[string]string{
		"OSID": "voice.google.com", "COMPASS": "clients6.google.com", "SID": ".google.com", "HSID": ".google.com",
		"SSID": ".google.com", "APISID": ".google.com", "SAPISID": ".google.com", "__Secure-1PSIDTS": ".google.com",
	}
	if len(domains) != len(want) || !slices.Equal(required, []string{"APISID", "HSID", "SAPISID", "SID", "SSID"}) {
		t.Fatalf("captures %v required %v", domains, required)
	}
	for id, d := range want {
		if domains[id] != d {
			t.Fatalf("capture %s domain %q, want %q", id, domains[id], d)
		}
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
		t.Logf("chat_gvoice idle VmRSS: %d KiB", rss)
	}
	shutdown(t, s, p)
}

// TestStartsWithSettings: max_initial_conversations in range and the
// backend's own history setting are taken, and the start reaches signed
// out.
func TestStartsWithSettings(t *testing.T) {
	s, p, _ := run(t, map[string]string{"max_initial_conversations": "40", "max_initial_messages": "20"})
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	shutdown(t, s, p)
}

// TestRefusesBadSettings: an unknown key, or a max_initial_conversations
// that is not a count from 0 to 1000, refuses the start (ERROR
// bad_settings) and every command after, and the key is not in the logs.
// WHY: catches a lenient parse that would fall back to a default, or to the
// connector's -1 (every thread), unseen.
func TestRefusesBadSettings(t *testing.T) {
	for _, bad := range []map[string]string{
		{"max_initial_conversations": "-1"},
		{"initial_thread_sync_count": "33333"},
	} {
		s, p, stderr := run(t, bad)
		cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
		if cs.GetCode() != "bad_settings" {
			t.Fatalf("%v: code %q", bad, cs.GetCode())
		}
		r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
		if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "bad_settings" {
			t.Fatalf("command while unconfigured: %v", r)
		}
		shutdown(t, s, p)
		logs, _ := os.ReadFile(stderr)
		for k := range bad {
			if strings.Contains(string(logs), k) {
				t.Fatalf("setting in the logs: %s", logs)
			}
		}
	}
}

// TestRefusesMissingElectron: an electron_path whose file is not there
// refuses the start (ERROR electron_not_found), and the path is not in the
// logs. WHY: catches a host that believes its texts are signed while the
// worker runs without the Electron it named.
func TestRefusesMissingElectron(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nowhere-to-be-found", "electron")
	s, p, stderr := run(t, map[string]string{"electron_path": missing})
	cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
	if cs.GetCode() != "electron_not_found" {
		t.Fatalf("code %q", cs.GetCode())
	}
	shutdown(t, s, p)
	logs, _ := os.ReadFile(stderr)
	if strings.Contains(string(logs), "nowhere-to-be-found") {
		t.Fatalf("path in the logs: %s", logs)
	}
}

// TestUpstreamVersionsMatchGoMod: the versions the worker reports in Hello
// are the ones go.mod pins.
func TestUpstreamVersionsMatchGoMod(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "replace ") {
			continue
		}
		f := strings.Fields(strings.TrimPrefix(line, "require "))
		if len(f) >= 2 {
			pinned[f[0]] = f[1]
		}
	}
	for mod, want := range map[string]string{
		"maunium.net/go/mautrix":   mautrixVersion,
		"go.mau.fi/mautrix-gvoice": gvoiceVersion,
	} {
		if pinned[mod] != want {
			t.Errorf("%s: go.mod pins %q, the worker reports %q", mod, pinned[mod], want)
		}
	}
}
