// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.mau.fi/mautrix-signal/pkg/connector"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/web"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/wspb"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// refusedLogin is the login the connector builds for a linked account
// before the gate sees it (login.go:220-230).
func refusedLogin(aci string) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID:       networkid.UserLoginID(aci),
		Metadata: &signalid.UserLoginMetadata{},
	}}
}

// TestDropRefusedKeepsTheOwnLink: a re-link that reached another account
// is refused at the gate after signalmeow stored the other account's
// device. The drop deletes every row of that account from every signalmeow
// table (its device, keys, pre-keys, sessions, sender keys, groups,
// recipients, the event buffer) and leaves every row of the connection's
// own device exactly as it was, including what it keeps of the other
// person. It returns the end at Signal for that device. WHY: catches the
// refused account's keys and password left on disk, a drop that also
// takes the connection's own link (or its rows naming the other person),
// and a drop that forgets the device must still leave the other phone.
func TestDropRefusedKeepsTheOwnLink(t *testing.T) {
	sc, db := testSignal(t)
	link(t, sc, mineACI, minePNI, 2, "mine-password")
	ctx := t.Context()
	// The connection's own device knows the other account as a contact.
	_, err := db.Exec(ctx, `INSERT INTO signalmeow_recipients (account_id, aci_uuid, contact_name) VALUES ($1, $2, 'Other')`, mineACI.String(), otherACI.String())
	must(t, err)
	before := dump(t, db)
	link(t, sc, otherACI, otherPNI, 3, "other-password")
	if n := countOf(dump(t, db), otherACI); n < 8 {
		t.Fatalf("the other account has %d rows before the drop", n)
	}
	end, err := dropRefused(ctx, sc, refusedLogin(otherACI.String()))
	if err != nil {
		t.Fatal(err)
	}
	if end == nil {
		t.Fatal("no end at Signal for a stored device")
	}
	after := dump(t, db)
	if n := countOf(after, otherACI); n != 0 {
		t.Fatalf("%d rows of the refused account kept: %v", n, after)
	}
	if !slices.Equal(after, before) {
		t.Fatalf("the connection's own rows changed:\nbefore %v\nafter  %v", before, after)
	}
	// The other person stays known to the connection's own device.
	if !slices.ContainsFunc(after, func(r string) bool {
		return strings.HasPrefix(r, "signalmeow_recipients:") && strings.Contains(r, "aci_uuid="+otherACI.String())
	}) {
		t.Fatal("the own device's contact row of the other person was deleted")
	}
}

// countOf is the number of dumped rows of account.
func countOf(rows []string, account interface{ String() string }) int {
	n := 0
	for _, r := range rows {
		if strings.Contains(r, " account_id="+account.String()) || strings.HasPrefix(r, "signalmeow_device: aci_uuid="+account.String()) {
			n++
		}
	}
	return n
}

// TestDropRefusedNothingStored: a refused login whose device was not
// stored (or has no password) deletes nothing and has no end; a login that
// is not a Signal login or an ACI is refused as such, so the backend keeps
// the store refused rather than take it for clean. WHY: catches an end
// made for a device that cannot sign in, and a malformed login id taken
// for "nothing to delete".
func TestDropRefusedNothingStored(t *testing.T) {
	sc, db := testSignal(t)
	link(t, sc, mineACI, minePNI, 2, "mine-password")
	before := dump(t, db)
	end, err := dropRefused(t.Context(), sc, refusedLogin(otherACI.String()))
	if err != nil || end != nil {
		t.Fatalf("nothing stored: end %v, err %v", end != nil, err)
	}
	if !slices.Equal(dump(t, db), before) {
		t.Fatal("rows changed with nothing to drop")
	}
	link(t, sc, otherACI, otherPNI, 3, "")
	end, err = dropRefused(t.Context(), sc, refusedLogin(otherACI.String()))
	if err != nil || end != nil || countOf(dump(t, db), otherACI) != 0 {
		t.Fatalf("no password: end %v, err %v", end != nil, err)
	}
	for _, id := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		if _, err := dropRefused(t.Context(), sc, refusedLogin(id)); !errors.Is(err, errNotSignal) {
			t.Errorf("login id %q: %v, want errNotSignal", id, err)
		}
	}
	if _, err := dropRefused(t.Context(), nil, refusedLogin(otherACI.String())); !errors.Is(err, errNotSignal) {
		t.Errorf("no connector: %v", err)
	}
}

// storeLogin writes the framework's rows of a stored login of aci (its user
// and its user_login row), as NewLogin leaves them.
func storeLogin(t *testing.T, sc *connector.SignalConnector, aci string) {
	t.Helper()
	ctx := t.Context()
	_, err := sc.Bridge.DB.Exec(ctx, `INSERT INTO "user" (bridge_id, mxid) VALUES ('signal', '@owner:lynx.invalid') ON CONFLICT DO NOTHING`)
	must(t, err)
	_, err = sc.Bridge.DB.Exec(ctx, `INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, metadata) VALUES ('signal', '@owner:lynx.invalid', $1, '', '{}')`, aci)
	must(t, err)
}

// TestDropOrphanDevices: at a start, a stored device that is no stored
// login's (a sign-in cut off after signalmeow stored it) is deleted with
// all its rows; the stored logins' own devices stay as they were, whatever
// the framework managed to load; no stored login deletes every device.
// WHY: catches another account's keys left in the store by a cancel that
// raced the phone's confirmation, and a sweep that takes a login's own
// device.
func TestDropOrphanDevices(t *testing.T) {
	sc, db := testSignal(t)
	link(t, sc, mineACI, minePNI, 2, "mine-password")
	storeLogin(t, sc, mineACI.String())
	before := dump(t, db)
	link(t, sc, otherACI, otherPNI, 3, "other-password")
	loaded := []*bridgev2.UserLogin{refusedLogin(mineACI.String())}
	must(t, dropOrphanDevices(t.Context(), sc, sc.Bridge, loaded))
	if after := dump(t, db); !slices.Equal(after, before) {
		t.Fatalf("after the sweep:\n%v\nwant\n%v", after, before)
	}
	_, err := db.Exec(t.Context(), `DELETE FROM user_login`)
	must(t, err)
	must(t, dropOrphanDevices(t.Context(), sc, sc.Bridge, nil))
	if n := countOf(dump(t, db), mineACI); n != 0 {
		t.Fatalf("no stored login, %d rows of a device kept", n)
	}
	if err := dropOrphanDevices(t.Context(), nil, sc.Bridge, nil); !errors.Is(err, errNotSignal) {
		t.Fatalf("no connector: %v", err)
	}
}

// TestDropOrphanDevicesKeepsALoginThatFailedToLoad: the connection's login
// is stored, but its LoadUserLogin failed at this start (a transient store
// read: the connector's DeviceByACI, the backend gate's read), so the
// framework did not load it and the hook is given no login (bridgev2
// userlogin.go:67-75 drops it with no error). The connection's own device
// and every row of it survive the sweep; the other account's orphan goes.
// The failure is real: the connector's LoadUserLogin, given a cancelled
// context, fails to read the device. WHY: catches a keep-set taken from the
// loaded logins, which would delete the connection's own device and keys
// for good on one failed read.
func TestDropOrphanDevicesKeepsALoginThatFailedToLoad(t *testing.T) {
	sc, db := testSignal(t)
	link(t, sc, mineACI, minePNI, 2, "mine-password")
	storeLogin(t, sc, mineACI.String())
	own := dump(t, db)
	link(t, sc, otherACI, otherPNI, 3, "other-password")
	failed, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sc.LoadUserLogin(failed, refusedLogin(mineACI.String())); err == nil {
		t.Fatal("the injected load did not fail")
	}
	must(t, dropOrphanDevices(t.Context(), sc, sc.Bridge, nil))
	if after := dump(t, db); !slices.Equal(after, own) {
		t.Fatalf("after the sweep with the own login not loaded:\n%v\nwant\n%v", after, own)
	}
}

// TestDropOrphanDevicesSkipsWhenLoginsUnread: when the stored logins cannot
// be read, the sweep deletes nothing and says so (the backend logs
// prepare_logins_failed). WHY: catches a sweep that takes "no rows read"
// for "no login" and deletes every device, the connection's own with it.
func TestDropOrphanDevicesSkipsWhenLoginsUnread(t *testing.T) {
	sc, db := testSignal(t)
	link(t, sc, mineACI, minePNI, 2, "mine-password")
	storeLogin(t, sc, mineACI.String())
	link(t, sc, otherACI, otherPNI, 3, "other-password")
	_, err := db.Exec(t.Context(), `ALTER TABLE user_login RENAME TO user_login_away`)
	must(t, err)
	before := dump(t, db)
	if err := dropOrphanDevices(t.Context(), sc, sc.Bridge, nil); !errors.Is(err, errLoginsUnread) {
		t.Fatalf("logins unreadable: %v, want errLoginsUnread", err)
	}
	if after := dump(t, db); !slices.Equal(after, before) {
		t.Fatalf("rows deleted with the logins unread:\n%v\nwant\n%v", after, before)
	}
}

// fakeSignal is a TLS server standing in for chat.signal.org's
// authenticated socket (/v1/websocket/), reached through web.SignalHTTPClient
// for the test's length.
type fakeSignal struct {
	t *testing.T
	// answer decides what the server does with a request on the socket
	// opened conn-th (from 1): a status to answer with, 0 to close the
	// socket, -1 to say nothing.
	answer func(conn int) int
	// refuse, when it gives not 0, answers the conn-th opening of the
	// socket with that status.
	refuse func(conn int) int

	mu       sync.Mutex
	conns    int
	auth     []string
	requests []*signalpb.WebSocketRequestMessage
}

func (f *fakeSignal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, pass, _ := r.BasicAuth()
	f.mu.Lock()
	f.auth = append(f.auth, user+":"+pass)
	f.conns++
	conn := f.conns
	f.mu.Unlock()
	if r.URL.Path != web.WebsocketPath {
		http.NotFound(w, r)
		return
	}
	if f.refuse != nil {
		if status := f.refuse(conn); status != 0 {
			w.WriteHeader(status)
			return
		}
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx := r.Context()
	for {
		msg := &signalpb.WebSocketMessage{}
		if err := wspb.Read(ctx, c, msg); err != nil {
			return
		}
		req := msg.GetRequest()
		if req == nil {
			continue
		}
		f.mu.Lock()
		f.requests = append(f.requests, proto.Clone(req).(*signalpb.WebSocketRequestMessage))
		f.mu.Unlock()
		switch status := f.answer(conn); status {
		case 0:
			_ = c.Close(websocket.StatusNormalClosure, "")
			return
		case -1:
		default:
			st := uint32(status)
			typ := signalpb.WebSocketMessage_RESPONSE
			if err := wspb.Write(ctx, c, &signalpb.WebSocketMessage{Type: &typ, Response: &signalpb.WebSocketResponseMessage{
				Id: proto.Uint64(req.GetId()), Status: &st, Message: proto.String("x"),
			}}); err != nil {
				return
			}
		}
	}
}

// serve starts f and points signalmeow's HTTP client (which its websocket
// dials with) at it, whatever the host, until the test ends.
func (f *fakeSignal) serve() {
	srv := httptest.NewUnstartedServer(f)
	srv.EnableHTTP2 = false
	srv.StartTLS()
	f.t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	old := web.SignalHTTPClient
	web.SignalHTTPClient = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // the test's own server
	}}
	f.t.Cleanup(func() { web.SignalHTTPClient = old })
}

var testCreds = deviceCreds{aci: otherACI, deviceID: 3, password: "other-password"}

// TestUnlinkDeletesTheDeviceItself: the end of a refused device signs in
// with the device's own credentials and asks Signal to delete that device,
// as the connector's logout does. Only proof that the device is gone is an
// end: a 2xx answer, or 401 when the socket opens (Signal knows no such
// device), also on the socket's reopening after one closed on the delete
// or could not be opened (503); a socket that reopens means the device is
// still there, and the delete is asked again. 403 at the opening, another
// answer, a close with no verdict before the deadline, or no answer, is
// not an end. WHY: catches an end that deletes another device id, signs in
// as another device, takes a closed socket or a 403 for proof, gives up on
// a socket that closed, or waits past the backend's deadline.
func TestUnlinkDeletesTheDeviceItself(t *testing.T) {
	always := func(v int) func(int) int { return func(int) int { return v } }
	then := func(first, rest int) func(int) int {
		return func(conn int) int {
			if conn == 1 {
				return first
			}
			return rest
		}
	}
	old := reopenPause
	reopenPause = 200 * time.Millisecond
	t.Cleanup(func() { reopenPause = old })
	for _, c := range []struct {
		name     string
		answer   func(int) int
		refuse   func(int) int
		deadline time.Duration
		want     error
		requests int
	}{
		{"deleted", always(204), nil, 3 * time.Second, nil, 1},
		{"credentials unknown: gone already", always(0), always(http.StatusUnauthorized), 3 * time.Second, nil, 0},
		{"credentials forbidden: not proof", always(0), always(http.StatusForbidden), 3 * time.Second, errNotUnlinked, 0},
		{"delete refused", always(http.StatusForbidden), nil, 3 * time.Second, errNotUnlinked, 1},
		{"server error", always(http.StatusInternalServerError), nil, 3 * time.Second, errNotUnlinked, 1},
		{"no answer", always(-1), nil, 3 * time.Second, errNotUnlinked, 1},
		{"socket closed, no verdict before the deadline", always(0), nil, 2 * time.Second, errNotUnlinked, -1},
		{"socket closed, then 401 on reopening", always(0), then(0, http.StatusUnauthorized), 5 * time.Second, nil, 1},
		{"socket closed, reopened, deleted the second time", then(0, 204), nil, 5 * time.Second, nil, 2},
		{"opening failed (503), then deleted", always(204), then(http.StatusServiceUnavailable, 0), 5 * time.Second, nil, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeSignal{t: t, refuse: c.refuse, answer: c.answer}
			f.serve()
			ctx, cancel := context.WithTimeout(t.Context(), c.deadline)
			defer cancel()
			start := time.Now()
			err := unlink(ctx, testCreds)
			if !errors.Is(err, c.want) && err != c.want {
				t.Fatalf("unlink: %v, want %v", err, c.want)
			}
			if time.Since(start) > c.deadline+2*time.Second {
				t.Fatalf("unlink took %v", time.Since(start))
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, a := range f.auth {
				if a != otherACI.String()+".3:other-password" {
					t.Fatalf("signed in as %v", f.auth)
				}
			}
			if len(f.auth) == 0 || (c.requests >= 0 && len(f.requests) != c.requests) || (c.requests < 0 && len(f.requests) < 2) {
				t.Fatalf("auth %v, requests %v, want %d", f.auth, f.requests, c.requests)
			}
			for _, req := range f.requests {
				if req.GetVerb() != http.MethodDelete || req.GetPath() != "/v1/devices/3" {
					t.Fatalf("request %s %s, want DELETE /v1/devices/3", req.GetVerb(), req.GetPath())
				}
			}
		})
	}
}

// TestDropRefusedEndUnlinks: the end dropRefused returns is the unlink of
// the device it deleted, with the credentials it read before the delete.
// WHY: catches an end that reads the device after its rows are gone (it
// would have nothing to sign in with).
func TestDropRefusedEndUnlinks(t *testing.T) {
	sc, _ := testSignal(t)
	link(t, sc, otherACI, otherPNI, 3, "other-password")
	f := &fakeSignal{t: t, answer: func(int) int { return 200 }}
	f.serve()
	end, err := dropRefused(t.Context(), sc, refusedLogin(otherACI.String()))
	if err != nil || end == nil {
		t.Fatalf("drop: end %v, err %v", end != nil, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := end(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 || f.requests[0].GetPath() != "/v1/devices/3" || f.auth[0] != otherACI.String()+".3:other-password" {
		t.Fatalf("auth %v requests %v", f.auth, f.requests)
	}
}

// TestHooksDropRefused: the backend is handed the drop and the sweep.
// WHY: catches either hook left unset (the refused account's device would
// stay in the store).
func TestHooksDropRefused(t *testing.T) {
	h := hooks()
	if h.DropRefused == nil || h.PrepareLogins == nil {
		t.Fatal("DropRefused or PrepareLogins unset")
	}
	if h.EndRefused != nil || h.EndRefusedSession {
		t.Fatal("EndRefused or EndRefusedSession set beside DropRefused")
	}
}
