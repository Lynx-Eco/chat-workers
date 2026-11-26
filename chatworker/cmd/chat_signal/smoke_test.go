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

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "csig")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_signal")
	if err := hosttest.BuildWorker(".", binPath, store.BuildTag, ""); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func run(t *testing.T, settings map[string]string) (*hosttest.Session, *hosttest.Process) {
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
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "signal", ConnectionId: "conn-signal",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	up := hello.GetUpstreamVersions()
	if hello.GetWorkerName() != "chat_signal" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		up["go.mau.fi/mautrix-signal"] != signalVersion || up["maunium.net/go/mautrix"] != mautrixVersion ||
		up[libsignalModule] != libsignalgo.Version {
		t.Fatalf("hello: %v", hello)
	}
	if !hello.GetStoreEncrypted() {
		t.Fatal("Hello.store_encrypted is false: the store must be encrypted (build with -tags libsqlite3)")
	}
	return s, p
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

func stop(t *testing.T, s *hosttest.Session, p *hosttest.Process) {
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
// its encrypted store (libsignal_ffi and SQLite3 Multiple Ciphers linked
// into one binary), reports signed out, lists exactly the QR flow, refuses
// any other, opens no socket but the host's, and stops on Shutdown.
func TestStartsSignedOutWithoutNetwork(t *testing.T) {
	s, p := run(t, nil)
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := s.Command(ctx, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if err != nil || r.GetOutcome() != contract.Outcome_OUTCOME_OK || len(r.GetFlows()) == 0 {
		t.Fatalf("flows: %v %v", r, err)
	}
	var ids []string
	for _, f := range r.GetFlows() {
		ids = append(ids, f.GetId())
	}
	if !slices.Equal(ids, []string{loginFlowQR}) {
		t.Fatalf("flows %v, want exactly qr", ids)
	}
	for _, flow := range []string{"phone", "bot", "manual"} {
		r, err := s.Command(ctx, &contract.Command{Kind: &contract.Command_SignInBegin{SignInBegin: &contract.SignInBegin{FlowId: flow}}})
		if err != nil || r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "unknown_flow" {
			t.Fatalf("sign-in with %s: %v %v", flow, r, err)
		}
	}
	time.Sleep(time.Second)
	if n, checked := nonUnixSockets(t, p.Pid()); !checked {
		t.Log("non-Unix sockets not counted: no /proc on this system")
	} else if n != 0 {
		t.Fatalf("worker opened %d non-Unix sockets before any sign-in", n)
	}
	if rss, err := p.RSSKiB(); err == nil {
		t.Logf("chat_signal idle VmRSS: %d KiB", rss)
	}
	stop(t, s, p)
}

// TestStartsWithSettings: device_name is taken, and the backend's own
// settings with it.
func TestStartsWithSettings(t *testing.T) {
	s, p := run(t, map[string]string{"device_name": "Lynx smoke device", "max_initial_messages": "20"})
	waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT)
	stop(t, s, p)
}

// TestRefusesUnknownSetting: a setting the worker does not know reports
// ERROR bad_settings and refuses commands; it never reaches the network.
func TestRefusesUnknownSetting(t *testing.T) {
	s, p := run(t, map[string]string{"max_initial_conversations": "15"})
	cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
	if cs.GetCode() != "bad_settings" {
		t.Fatalf("code %q", cs.GetCode())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := s.Command(ctx, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if err != nil || r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "bad_settings" {
		t.Fatalf("command while unconfigured: %v %v", r, err)
	}
	stop(t, s, p)
}

// TestUpstreamVersionsMatchGoMod: the versions the worker reports in Hello
// are the ones go.mod pins, and the libsignal version is the one
// go/libsignal.pin names (the library build.sh links).
func TestUpstreamVersionsMatchGoMod(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "require "))
		if len(f) >= 2 {
			pinned[f[0]] = f[1]
		}
	}
	for mod, want := range map[string]string{
		"maunium.net/go/mautrix":   mautrixVersion,
		"go.mau.fi/mautrix-signal": signalVersion,
	} {
		if pinned[mod] != want {
			t.Errorf("%s: go.mod pins %q, the worker reports %q", mod, pinned[mod], want)
		}
	}
	pin, err := os.ReadFile(filepath.Join("..", "..", "..", "libsignal.pin"))
	if err != nil {
		t.Fatal(err)
	}
	var version string
	for _, line := range strings.Split(string(pin), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "version" {
			version = f[1]
		}
	}
	if version != libsignalgo.Version {
		t.Errorf("libsignal.pin names %q, the connector links %q", version, libsignalgo.Version)
	}
}
