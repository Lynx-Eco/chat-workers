// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// socksTrap is a SOCKS5 proxy that answers the greeting, records the
// address of each CONNECT and closes: nothing reaches Telegram. A holding
// trap instead answers the CONNECT as made and then holds the connection
// without a byte in answer: the client waits for Telegram, which never
// answers (a logout that hangs).
type socksTrap struct {
	ln    net.Listener
	hold  bool
	mu    sync.Mutex
	dests []string
}

func newSocksTrap(t *testing.T) *socksTrap {
	return startSocksTrap(t, false)
}

func startSocksTrap(t *testing.T, hold bool) *socksTrap {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &socksTrap{ln: ln, hold: hold}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *socksTrap) serve(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil || head[0] != 5 {
		return
	}
	if _, err := io.ReadFull(c, make([]byte, head[1])); err != nil {
		return
	}
	if _, err := c.Write([]byte{5, 0}); err != nil {
		return
	}
	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil {
		return
	}
	var host string
	switch req[3] {
	case 1:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(c, ip); err != nil {
			return
		}
		host = net.IP(ip).String()
	case 4:
		ip := make([]byte, 16)
		if _, err := io.ReadFull(c, ip); err != nil {
			return
		}
		host = net.IP(ip).String()
	case 3:
		n := make([]byte, 1)
		if _, err := io.ReadFull(c, n); err != nil {
			return
		}
		name := make([]byte, n[0])
		if _, err := io.ReadFull(c, name); err != nil {
			return
		}
		host = string(name)
	default:
		return
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(c, port); err != nil {
		return
	}
	s.mu.Lock()
	s.dests = append(s.dests, net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port)))))
	s.mu.Unlock()
	if s.hold {
		if _, err := c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
			return
		}
		_ = c.SetDeadline(time.Now().Add(time.Minute))
		_, _ = io.Copy(io.Discard, c)
	}
}

func (s *socksTrap) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.dests...)
}

func testConnector(t *testing.T, proxyAddr string) *connector.TelegramConnector {
	t.Helper()
	n, err := newConnector(&contract.Init{Settings: map[string]string{"api_id": "12345", "api_hash": dummyHash}})
	if err != nil {
		t.Fatal(err)
	}
	tc := n.(*connector.TelegramConnector)
	tc.Config.ProxyConfig = connector.ProxyConfig{Type: "socks5", Address: proxyAddr}
	return tc
}

func refusedLogin(method string, key []byte) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "777", Metadata: &connector.UserLoginMetadata{
		LoginMethod: method,
		Session:     connector.UserLoginSession{AuthKey: key, Datacenter: 2, ServerAddress: "149.154.167.50:443"},
	}}}
}

// TestEndRefusedLogsOutWithOwnClient: the refused login's session is used
// by a client of endRefused's own, which connects (through the connector's
// proxy setting) to the session's data centre to call auth.logOut; here the
// proxy is a trap, so the call fails within the deadline. The auth key is
// zeroed in the refused login afterwards. endRefused is given no store: it
// cannot write one. Not proven here: that auth.logOut ends the session at
// Telegram (that needs Telegram).
func TestEndRefusedLogsOutWithOwnClient(t *testing.T) {
	trap := newSocksTrap(t)
	tc := testConnector(t, trap.ln.Addr().String())
	key := bytes.Repeat([]byte{0x5a}, 256)
	login := refusedLogin(connector.LoginFlowIDPhone, key)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	began := time.Now()
	if err := endRefused(ctx, tc, login); err == nil {
		t.Fatal("logged out through a proxy that closes every connection")
	}
	if took := time.Since(began); took > 6*time.Second {
		t.Fatalf("endRefused took %v past a 3 s deadline", took)
	}
	dests := trap.seen()
	if len(dests) == 0 {
		t.Fatal("no connection was attempted")
	}
	for _, d := range dests {
		if host, port, _ := net.SplitHostPort(d); port != "443" || net.ParseIP(host) == nil {
			t.Fatalf("connected to %q, not a data centre address", d)
		}
	}
	slices.Sort(dests)
	t.Logf("%d connection attempts through the proxy, to %v", len(dests), slices.Compact(dests))
	if !bytes.Equal(login.Metadata.(*connector.UserLoginMetadata).Session.AuthKey, make([]byte, 256)) {
		t.Fatal("the refused login's auth key was not zeroed")
	}
}

// TestEndRefusedHangingStopsAtCancel: Telegram does not answer (the
// connection is made, nothing comes back), so the logout hangs. The worker's
// stop does not wait for it past its bound (backend's waitRefusedEnds, which
// then logs refused_session_not_ended and cancels the logout's context):
// cancelled, endRefused returns at once with an error (gotd's Run returns
// nil on a cancel; without errNotConfirmed this logout, never answered,
// would be logged as ended), and the refused login's key is zeroed.
func TestEndRefusedHangingStopsAtCancel(t *testing.T) {
	trap := startSocksTrap(t, true)
	tc := testConnector(t, trap.ln.Addr().String())
	login := refusedLogin(connector.LoginFlowIDPhone, bytes.Repeat([]byte{0x5a}, 256))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- endRefused(ctx, tc, login) }()
	// Wait until the client holds a connection that gets no answer.
	deadline := time.Now().Add(20 * time.Second)
	for len(trap.seen()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no connection was attempted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-result:
		t.Fatalf("the logout returned (%v) though Telegram never answered", err)
	case <-time.After(500 * time.Millisecond):
	}
	cancelled := time.Now()
	cancel()
	select {
	case err := <-result:
		if err == nil || errors.Is(err, backend.ErrNothingToEnd) {
			t.Fatalf("a cancelled logout returned %v, want an error (logged as not ended)", err)
		}
		t.Logf("returned %v after the cancel", time.Since(cancelled))
	case <-time.After(10 * time.Second):
		t.Fatal("endRefused did not return after its context was cancelled")
	}
	if !bytes.Equal(login.Metadata.(*connector.UserLoginMetadata).Session.AuthKey, make([]byte, 256)) {
		t.Fatal("the refused login's auth key was not zeroed")
	}
}

// TestEndRefusedWithoutSessionToEnd: no auth key, or a session imported with
// the manual flow (it existed before the sign-in): nothing is dialled, the
// result says there was nothing to end (not that it was ended), and the key
// is zeroed all the same.
func TestEndRefusedWithoutSessionToEnd(t *testing.T) {
	trap := newSocksTrap(t)
	tc := testConnector(t, trap.ln.Addr().String())
	for _, login := range []*bridgev2.UserLogin{
		refusedLogin(connector.LoginFlowIDQR, nil),
		refusedLogin(connector.LoginFlowIDManual, bytes.Repeat([]byte{1}, 256)),
	} {
		if err := endRefused(t.Context(), tc, login); !errors.Is(err, backend.ErrNothingToEnd) {
			t.Fatalf("got %v, want ErrNothingToEnd", err)
		}
		if k := login.Metadata.(*connector.UserLoginMetadata).Session.AuthKey; k != nil && !bytes.Equal(k, make([]byte, len(k))) {
			t.Fatal("key not zeroed")
		}
	}
	if err := endRefused(t.Context(), plainConnector{}, refusedLogin("", nil)); err == nil {
		t.Fatal("a connector that is not Telegram's was accepted")
	}
	time.Sleep(200 * time.Millisecond)
	if d := trap.seen(); len(d) != 0 {
		t.Fatalf("dialled %v", d)
	}
}

type plainConnector struct{ bridgev2.NetworkConnector }
