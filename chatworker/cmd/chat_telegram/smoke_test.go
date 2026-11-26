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

	"go.mau.fi/mautrix-telegram/pkg/connector"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ctg")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_telegram")
	if err := hosttest.BuildWorker(".", binPath, store.BuildTag, ""); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

const dummyHash = "0123456789abcdef0123456789abcdef"

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
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "telegram", ConnectionId: "conn-tg",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	if hello.GetWorkerName() != "chat_telegram" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		hello.GetUpstreamVersions()["go.mau.fi/mautrix-telegram"] != telegramVersion {
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

// TestStartsSignedOutWithoutNetwork: with dummy settings the worker starts,
// reports signed out, lists its flows, and opens no socket but the host's.
func TestStartsSignedOutWithoutNetwork(t *testing.T) {
	s, p, stderr := run(t, map[string]string{"api_id": "12345", "api_hash": dummyHash})
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
	t.Logf("flows: %v", ids)
	if !slices.Equal(ids, []string{connector.LoginFlowIDPhone, connector.LoginFlowIDQR}) {
		t.Fatalf("flows %v, want exactly phone and qr", ids)
	}
	for _, flow := range []string{"bot", "manual"} {
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
		t.Logf("chat_telegram idle VmRSS: %d KiB", rss)
	}
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
	logs, _ := os.ReadFile(stderr)
	if strings.Contains(string(logs), dummyHash) || strings.Contains(string(logs), "12345") {
		t.Fatalf("settings in the logs")
	}
}

func TestRefusesMissingSettings(t *testing.T) {
	s, p, _ := run(t, map[string]string{"api_id": "12345"})
	cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
	if cs.GetCode() != "missing_settings" {
		t.Fatalf("code %q", cs.GetCode())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := s.Command(ctx, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if err != nil || r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "missing_settings" {
		t.Fatalf("command while unconfigured: %v %v", r, err)
	}
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
}
