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

	"go.mau.fi/mautrix-whatsapp/pkg/connector"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cwa")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binPath = filepath.Join(dir, "chat_whatsapp")
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
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, Connector: "whatsapp_linked", ConnectionId: "conn-wa",
		StateDir: state, StoreKey: key, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	up := hello.GetUpstreamVersions()
	if hello.GetWorkerName() != "chat_whatsapp" || hello.GetLicence() != "AGPL-3.0-or-later" ||
		up["go.mau.fi/mautrix-whatsapp"] != whatsappVersion || up["go.mau.fi/whatsmeow"] != whatsmeowVersion ||
		up["maunium.net/go/mautrix"] != mautrixVersion {
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

// TestStartsSignedOutWithoutNetwork: with no settings the worker starts on
// its encrypted store, reports signed out, lists exactly its two flows,
// refuses any other, and opens no socket but the host's.
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
	t.Logf("flows: %v", ids)
	if !slices.Equal(ids, []string{connector.LoginFlowIDQR, connector.LoginFlowIDPhone}) {
		t.Fatalf("flows %v, want exactly qr and phone", ids)
	}
	for _, flow := range []string{"bot", "manual", "cookies"} {
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
		t.Logf("chat_whatsapp idle VmRSS: %d KiB", rss)
	}
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
}

// TestStartsWithSettings: the optional settings are taken, and a number
// setting that is not a number keeps its default instead of refusing the
// start.
func TestStartsWithSettings(t *testing.T) {
	s, p := run(t, map[string]string{"device_name": "Lynx smoke device", "max_initial_conversations": "many"})
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

// TestUpstreamVersionsMatchGoMod: the versions the worker reports in Hello
// are the ones go.mod pins.
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
		"maunium.net/go/mautrix":     mautrixVersion,
		"go.mau.fi/mautrix-whatsapp": whatsappVersion,
		"go.mau.fi/whatsmeow":        whatsmeowVersion,
	} {
		if pinned[mod] != want {
			t.Errorf("%s: go.mod pins %q, the worker reports %q", mod, pinned[mod], want)
		}
	}
}

// TestSendKeyIsAWebMessageID: every key becomes an id of WhatsApp Web's
// form, the same for the same key, another for another key, and an id of
// that form is kept. WHY: catches a mapping that is not a function of the
// key (the backend finds a stored message's key by mapping each journal key
// again, sendKeyForTxn), one that lets a near-miss key through unhashed
// (lower-case hex, one digit short), and one that collapses keys.
func TestSendKeyIsAWebMessageID(t *testing.T) {
	for _, key := range []string{"", "k1", "send-0001", "3EB0", "3eb0aabbccddeeff001122", "3EB0AABBCCDDEEFF00112"} {
		got := string(whatsappSendKey(key))
		if !isWebMessageID(got) {
			t.Fatalf("key %q: %q is not of WhatsApp Web's form", key, got)
		}
		if again := string(whatsappSendKey(key)); again != got {
			t.Fatalf("key %q: %q then %q", key, got, again)
		}
	}
	if a, b := whatsappSendKey("k1"), whatsappSendKey("k2"); a == b {
		t.Fatalf("two keys, one id %q", a)
	}
	const kept = "3EB0AABBCCDDEEFF001122"
	if got := string(whatsappSendKey(kept)); got != kept {
		t.Fatalf("an id of the form is kept: %q", got)
	}
}
