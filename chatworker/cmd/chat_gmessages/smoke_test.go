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

	"go.mau.fi/mautrix-gmessages/pkg/connector"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cgm")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_gmessages")
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
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "gmessages", ConnectionId: "conn-gm",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	up := hello.GetUpstreamVersions()
	if hello.GetWorkerName() != "chat_gmessages" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		up["go.mau.fi/mautrix-gmessages"] != gmessagesVersion || up["maunium.net/go/mautrix"] != mautrixVersion {
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
// offers with google first (the one a host that names none begins; qr,
// which Google has discontinued, second), refuses any other, begins the
// google flow to its first step as the contract's kind (a
// WEB step at Google's account chooser with the connector's cookie
// captures, the six it needs required), and opens no socket but the
// host's. The qr flow is not begun here: its first step already asks Google
// for a pairing key (QRLoginProcess.Start). WHY: catches the QR flow left
// unlisted (the connector lists only google), a flow added, the order that
// makes the QR flow the default, a first step the contract cannot carry
// (unsupported_step), a capture renamed upstream, and a connector or flow
// that reaches the network before the user submits anything.
func TestStartsSignedOutWithoutNetwork(t *testing.T) {
	s, p, _ := run(t, nil)
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	r := command(t, s, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	var ids []string
	for _, f := range r.GetFlows() {
		ids = append(ids, f.GetId())
	}
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || !slices.Equal(ids, []string{"google", "qr"}) {
		t.Fatalf("flows %v (%v), want exactly google, qr", ids, r)
	}
	for _, flow := range []string{"", "QR", "Google", "google_account", "cookies", "phone"} {
		if r := begin(t, s, flow); r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "unknown_flow" {
			t.Fatalf("sign-in with %q: %v", flow, r)
		}
	}

	if r := begin(t, s, "google"); r.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("begin google: %v", r)
	}
	st := waitStep(t, s, "google", connector.LoginStepIDGoogle)
	if st.GetKind() != contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB || st.GetWeb() == nil ||
		st.GetWeb().GetUrl() != "https://accounts.google.com/AccountChooser?continue=https://messages.google.com/web/config" {
		t.Fatalf("google's step: %v", st)
	}
	var required, optional []string
	for _, f := range st.GetWeb().GetFields() {
		if len(f.GetSources()) != 1 || f.GetSources()[0].GetSource() != contract.WebCaptureSource_WEB_CAPTURE_SOURCE_COOKIE ||
			f.GetSources()[0].GetName() != f.GetId() {
			t.Fatalf("capture %s: %v", f.GetId(), f.GetSources())
		}
		if f.GetRequired() {
			required = append(required, f.GetId())
		} else {
			optional = append(optional, f.GetId())
		}
	}
	// The connector builds the captures from a map: their order changes.
	slices.Sort(required)
	if !slices.Equal(required, []string{"APISID", "HSID", "OSID", "SAPISID", "SID", "SSID"}) ||
		!slices.Equal(optional, []string{"__Secure-1PSIDTS"}) {
		t.Fatalf("google's captures: required %v, optional %v", required, optional)
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
		t.Logf("chat_gmessages idle VmRSS: %d KiB", rss)
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

// TestStartsWithSettings: a device_name and a max_initial_conversations in
// range are taken with the backend's history settings, and the start
// reaches signed out.
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
// from 1 to 100, and a key the worker does not know, refuse the start
// (ERROR bad_settings) and every command after, and neither the key nor
// the value is in the logs. WHY: catches a lenient parse that would fall
// back to a default unseen, and a setting logged.
func TestRefusesBadSettings(t *testing.T) {
	for _, bad := range []map[string]string{
		{"max_initial_conversations": "0"},
		{"initial_chat_sync_count": "25"},
	} {
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
		for key := range bad {
			if strings.Contains(string(logs), key) {
				t.Fatalf("setting in the logs: %s", logs)
			}
		}
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
		"maunium.net/go/mautrix":      mautrixVersion,
		"go.mau.fi/mautrix-gmessages": gmessagesVersion,
	} {
		if pinned[mod] != want {
			t.Errorf("%s: go.mod pins %q, the worker reports %q", mod, pinned[mod], want)
		}
	}
}
