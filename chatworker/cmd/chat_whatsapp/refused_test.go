// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	whatsmeowUpgrades "go.mau.fi/whatsmeow/store/sqlstore/upgrades"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
	lynxstore "lynx.invalid/ingest/chatworker/store"
)

// The accounts of these tests: mine (the connection's), the other one a
// re-link reached (refused), and a third person both know.
const (
	minePhone  = "15550001111"
	mineLID    = "100000000000001"
	otherPhone = "15550002222"
	otherLID   = "200000000000002"
	thirdPhone = "15550003333"
	thirdLID   = "300000000000003"
)

// testWA is the worker's connector on a store of its own, opened as the
// worker opens it (store.OpenDB: encrypted in the encrypted build, every
// connection with foreign_keys=ON), with the connector's two databases on
// it as its Init makes them (connector.go:110, :119-126) and whatsmeow's
// tables made (Start, connector.go:148).
func testWA(t *testing.T) (*connector.WhatsAppConnector, *dbutil.Database) {
	t.Helper()
	db, _, err := lynxstore.OpenDB(filepath.Join(t.TempDir(), "worker.db"),
		lynxstore.OpenOptions{Key: bytes.Repeat([]byte{7}, lynxstore.KeyLen), DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	n, err := newConnector(&contract.Init{})
	if err != nil {
		t.Fatal(err)
	}
	wa := n.(*connector.WhatsAppConnector)
	wa.DB = wadb.New("whatsapp", db, zerolog.Nop())
	wa.DeviceStore = sqlstore.NewWithWrappedDB(db.Child("whatsmeow_version", whatsmeowUpgrades.Table, dbutil.ZeroLogger(zerolog.Nop())), waLog.Noop)
	if err := wa.DeviceStore.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	return wa, db
}

func phoneOf(user string) types.JID { return types.NewJID(user, types.DefaultUserServer) }
func lidOf(user string) types.JID   { return types.NewJID(user, types.HiddenUserServer) }

// pair stores a companion of phone as whatsmeow's pairing does
// (pair.go:219-232): the device row (with its LID when lid is set), the
// account's LID-phone mapping and the main device's identity; then, as a
// device in use would, a session, a push name, a sender key, a message
// secret and a privacy token, all with the third person.
func pair(t *testing.T, wa *connector.WhatsAppConnector, phone string, device uint16, lid string) *store.Device {
	t.Helper()
	ctx := t.Context()
	d := wa.DeviceStore.NewDevice()
	id := types.JID{User: phone, Server: types.DefaultUserServer, Device: device}
	d.ID = &id
	if lid != "" {
		d.LID = types.JID{User: lid, Server: types.HiddenUserServer, Device: device}
	}
	d.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{1, 2, 3},
		AccountSignature:    bytes.Repeat([]byte{1}, 64),
		AccountSignatureKey: bytes.Repeat([]byte{2}, 32),
		DeviceSignature:     bytes.Repeat([]byte{3}, 64),
	}
	must(t, d.Save(ctx))
	if lid != "" {
		must(t, wa.DeviceStore.LIDMap.PutLIDMapping(ctx, lidOf(lid), phoneOf(phone)))
		must(t, d.Identities.PutIdentity(ctx, lidOf(lid).SignalAddress().String(), [32]byte{9}))
	}
	must(t, d.Sessions.PutSession(ctx, lidOf(thirdLID).SignalAddress().String(), []byte("session")))
	_, _, err := d.Contacts.PutPushName(ctx, phoneOf(thirdPhone), "Third")
	must(t, err)
	must(t, d.SenderKeys.PutSenderKey(ctx, "120363025246125486@g.us", lidOf(thirdLID).SignalAddress().String(), []byte("sk")))
	must(t, d.MsgSecrets.PutMessageSecret(ctx, phoneOf(thirdPhone), phoneOf(thirdPhone), "3EB0AA", []byte("secret")))
	must(t, d.PrivacyTokens.PutPrivacyTokens(ctx, store.PrivacyToken{User: phoneOf(thirdPhone), Token: []byte("tok"), Timestamp: time.Now()}))
	return d
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func refusedLogin(phone string, device uint16) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID:       networkid.UserLoginID(phone),
		Metadata: &waid.UserLoginMetadata{WADeviceID: device},
	}}
}

// dump is every row of every whatsmeow table, each as one line of text
// ("<table>: <col>=<value> ..."; bytes that are not printable text in
// hex), sorted.
func dump(t *testing.T, db *dbutil.Database) []string {
	t.Helper()
	ctx := t.Context()
	rows, err := db.Query(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'whatsmeow%' ORDER BY name`)
	must(t, err)
	var tables []string
	for rows.Next() {
		var name string
		must(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	must(t, rows.Close())
	if len(tables) < 10 {
		t.Fatalf("whatsmeow tables: %v", tables)
	}
	var out []string
	for _, table := range tables {
		rows, err := db.Query(ctx, `SELECT * FROM `+table)
		must(t, err)
		cols, err := rows.Columns()
		must(t, err)
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			must(t, rows.Scan(ptrs...))
			var b strings.Builder
			b.WriteString(table + ":")
			for i, v := range vals {
				if raw, isBytes := v.([]byte); isBytes {
					v = hex.EncodeToString(raw)
					if text := string(raw); utf8.ValidString(text) && strings.IndexFunc(text, func(r rune) bool { return !unicode.IsPrint(r) }) < 0 {
						v = text
					}
				}
				fmt.Fprintf(&b, " %s=%v", cols[i], v)
			}
			out = append(out, b.String())
		}
		must(t, rows.Close())
	}
	slices.Sort(out)
	return out
}

// mentions tells whether a dumped row names one of users (phone digits or
// LID users).
func mentions(row string, users ...string) bool {
	for _, u := range users {
		if strings.Contains(row, u) {
			return true
		}
	}
	return false
}

// TestDropRefusedKeepsTheOwnLink: a re-link that reached another account
// is refused at the gate after whatsmeow stored the other account's
// companion. The drop deletes every row of that account from every
// whatsmeow table (its companion, a stale companion of the same number,
// what cascades from them, its privacy tokens, its own LID-phone mapping)
// and leaves every row of the connection's own companion exactly as it
// was, including the mappings it knows. It returns the end at WhatsApp for
// that companion. WHY: catches the refused companion's keys left on disk
// (whatsmeow stores them before the gate), a table without a foreign key
// left behind (privacy tokens), the global LID map keeping the other
// account's number, and a delete wide enough to touch the connection's own
// link.
func TestDropRefusedKeepsTheOwnLink(t *testing.T) {
	wa, db := testWA(t)
	ctx := t.Context()
	mine := pair(t, wa, minePhone, 12, mineLID)
	must(t, wa.DeviceStore.LIDMap.PutLIDMapping(ctx, lidOf(thirdLID), phoneOf(thirdPhone)))
	pair(t, wa, otherPhone, 7, otherLID)
	pair(t, wa, otherPhone, 3, "")
	before := dump(t, db)
	var want []string
	others := 0
	for _, row := range before {
		if mentions(row, otherPhone, otherLID) {
			others++
		} else {
			want = append(want, row)
		}
	}
	if others < 12 || len(want) < 10 {
		t.Fatalf("setup: %d rows of the other account, %d others:\n%s", others, len(want), strings.Join(before, "\n"))
	}

	end, err := dropRefused(ctx, wa, refusedLogin(otherPhone, 7))
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if end == nil {
		t.Fatal("no end at WhatsApp for a stored companion")
	}
	after := dump(t, db)
	if !slices.Equal(after, want) {
		t.Fatalf("after the drop:\n%s\nwant:\n%s", strings.Join(after, "\n"), strings.Join(want, "\n"))
	}
	// The connection's own companion loads as it was stored.
	got, err := wa.DeviceStore.GetDevice(ctx, *mine.ID)
	if err != nil || got == nil {
		t.Fatalf("own companion: %v %v", got, err)
	}
	if *got.IdentityKey.Priv != *mine.IdentityKey.Priv || got.LID != mine.LID {
		t.Fatal("own companion changed")
	}
	if pn, err := wa.DeviceStore.LIDMap.GetPNForLID(ctx, lidOf(mineLID)); err != nil || pn.User != minePhone {
		t.Fatalf("own mapping: %v %v", pn, err)
	}
}

// TestDropRefusedKeepsAMappingTheOwnAccountKnows: when the connection's
// own companion knows the other account's person (a contact, or a Signal
// session with its LID), that person's LID-phone mapping is the own
// account's as well and stays; the refused companion's rows go all the
// same. WHY: catches the drop deleting from the global LID map a row the
// connection's own client uses.
func TestDropRefusedKeepsAMappingTheOwnAccountKnows(t *testing.T) {
	for name, know := range map[string]func(ctx context.Context, d *store.Device) error{
		"contact": func(ctx context.Context, d *store.Device) error {
			_, _, err := d.Contacts.PutPushName(ctx, phoneOf(otherPhone), "Other")
			return err
		},
		"session": func(ctx context.Context, d *store.Device) error {
			return d.Sessions.PutSession(ctx, lidOf(otherLID).SignalAddress().String(), []byte("s"))
		},
		"identity": func(ctx context.Context, d *store.Device) error {
			return d.Identities.PutIdentity(ctx, phoneOf(otherPhone).SignalAddress().String(), [32]byte{4})
		},
	} {
		t.Run(name, func(t *testing.T) {
			wa, db := testWA(t)
			ctx := t.Context()
			mine := pair(t, wa, minePhone, 12, mineLID)
			must(t, know(ctx, mine))
			pair(t, wa, otherPhone, 7, otherLID)
			if _, err := dropRefused(ctx, wa, refusedLogin(otherPhone, 7)); err != nil {
				t.Fatal(err)
			}
			var lid string
			must(t, db.QueryRow(ctx, `SELECT lid FROM whatsmeow_lid_map WHERE pn=$1`, otherPhone).Scan(&lid))
			if lid != otherLID {
				t.Fatalf("mapping %q", lid)
			}
			var n int
			must(t, db.QueryRow(ctx, `SELECT count(*) FROM whatsmeow_device WHERE jid LIKE $1`, otherPhone+"%").Scan(&n))
			if n != 0 {
				t.Fatalf("%d companions of the other account left", n)
			}
			for _, row := range dump(t, db) {
				if strings.Contains(row, "our_jid="+otherPhone) || strings.Contains(row, "jid="+otherPhone+":") {
					t.Fatalf("row of the refused companion left: %s", row)
				}
			}
		})
	}
}

// TestDropRefusedWithNothingPaired: a refused login with no companion
// stored (no device number, or none stored under it) has nothing to end at
// WhatsApp (nil end: logged as refused_session_nothing_to_end), and its
// account's LID-phone mapping is deleted all the same. WHY: catches a
// made-up end (logged as ended) and a mapping left when the device row was
// already gone.
func TestDropRefusedWithNothingPaired(t *testing.T) {
	for _, device := range []uint16{0, 9} {
		wa, db := testWA(t)
		ctx := t.Context()
		pair(t, wa, minePhone, 12, mineLID)
		must(t, wa.DeviceStore.LIDMap.PutLIDMapping(ctx, lidOf(otherLID), phoneOf(otherPhone)))
		end, err := dropRefused(ctx, wa, refusedLogin(otherPhone, device))
		if err != nil || end != nil {
			t.Fatalf("device %d: end %v, %v", device, end != nil, err)
		}
		for _, row := range dump(t, db) {
			if mentions(row, otherPhone, otherLID) {
				t.Fatalf("device %d: left %s", device, row)
			}
		}
	}
}

// TestDropRefusedFailsClosed: a connector or login that is not
// WhatsApp's, and a store that cannot be read or written, are errors (the
// backend then reports other_account_kept: the host deletes the store),
// never a silent success; a device that could not be read gives an end
// that reports that failure (logged as not ended), not one that says there
// was nothing to end. WHY: catches a drop that fails and still lets the
// worker say principal_mismatch ("nothing of the other account kept").
func TestDropRefusedFailsClosed(t *testing.T) {
	wa, db := testWA(t)
	ctx := t.Context()
	if _, err := dropRefused(ctx, plainConnector{}, refusedLogin(otherPhone, 7)); err == nil {
		t.Fatal("a connector that is not WhatsApp's was accepted")
	}
	if _, err := dropRefused(ctx, wa, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: otherPhone}}); err == nil {
		t.Fatal("a login without the connector's metadata was accepted")
	}
	pair(t, wa, otherPhone, 7, otherLID)
	_ = db.Close()
	end, err := dropRefused(ctx, wa, refusedLogin(otherPhone, 7))
	if err == nil {
		t.Fatal("a drop on a closed store succeeded")
	}
	if end == nil || end(ctx) == nil || errors.Is(end(ctx), backend.ErrNothingToEnd) {
		t.Fatal("an unread device gave no end, or one that is not a failure")
	}
}

type plainConnector struct{ bridgev2.NetworkConnector }

// socksTrap is a SOCKS5 proxy that records the address of each CONNECT and
// then closes, or (hold) answers it as made and holds the connection
// without a byte in answer: nothing reaches WhatsApp.
type socksTrap struct {
	ln    net.Listener
	hold  bool
	mu    sync.Mutex
	dests []string
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

func (s *socksTrap) url() string { return "socks5://" + s.ln.Addr().String() }

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
	case 1, 4:
		ip := make([]byte, map[byte]int{1: 4, 4: 16}[req[3]])
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
	s.dests = append(s.dests, net.JoinHostPort(host, strconv.Itoa(int(port[0])<<8|int(port[1]))))
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

// TestRefusedEndTriesWhatsAppAndFailsSafely: the end the drop returns
// connects to WhatsApp (through the connector's proxy setting, here a trap
// that closes every connection) to log the companion out, and reports the
// failure, within its deadline (logged as refused_session_not_ended).
// What the drop deleted stays deleted: the end's client writes nothing
// back. WHY: catches an end that does not try the network, one that hangs
// past its deadline, one that says ended without an answer, and a client
// that saves the companion again while it runs. Not proven here: that the
// logout removes the companion from the phone (that needs WhatsApp).
func TestRefusedEndTriesWhatsAppAndFailsSafely(t *testing.T) {
	trap := startSocksTrap(t, false)
	wa, db := testWA(t)
	wa.Config.Proxy = trap.url()
	pair(t, wa, minePhone, 12, mineLID)
	pair(t, wa, otherPhone, 7, otherLID)
	end, err := dropRefused(t.Context(), wa, refusedLogin(otherPhone, 7))
	if err != nil || end == nil {
		t.Fatalf("drop: %v (end %v)", err, end != nil)
	}
	want := dump(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()
	if err := end(ctx); err == nil || errors.Is(err, backend.ErrNothingToEnd) {
		t.Fatalf("logged out through a proxy that closes every connection: %v", err)
	}
	if took := time.Since(began); took > 8*time.Second {
		t.Fatalf("the end took %v past a 5 s deadline", took)
	}
	dests := trap.seen()
	if len(dests) == 0 {
		t.Fatal("no connection was attempted")
	}
	t.Logf("connections through the proxy: %v", dests)
	if got := dump(t, db); !slices.Equal(got, want) {
		t.Fatalf("the end changed the store:\n%s", strings.Join(got, "\n"))
	}
}

// TestUnpairHangingStopsAtCancel: WhatsApp does not answer (the connection
// is made, nothing comes back): the end returns with an error as soon as
// its context ends (the backend's deadline, or the worker's stop), and the
// in-memory copy's keys are zeroed and its stores are no-ops. WHY: catches
// a logout that outlives the stop that gave up on it, keys of the other
// account left in the copy, and a copy still writing to the store.
func TestUnpairHangingStopsAtCancel(t *testing.T) {
	trap := startSocksTrap(t, true)
	wa, _ := testWA(t)
	d := wa.DeviceStore.NewDevice()
	id := types.JID{User: otherPhone, Server: types.DefaultUserServer, Device: 7}
	d.ID = &id
	d.Account = &waAdv.ADVSignedDeviceIdentity{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- unpair(ctx, d, trap.url()) }()
	deadline := time.Now().Add(20 * time.Second)
	for len(trap.seen()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no connection was attempted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-result:
		t.Fatalf("the logout returned (%v) though WhatsApp never answered", err)
	case <-time.After(500 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("a cancelled logout returned nil (logged as ended)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unpair did not return after its context was cancelled")
	}
	var zero [32]byte
	if *d.NoiseKey.Priv != zero || *d.IdentityKey.Priv != zero || *d.SignedPreKey.Priv != zero || !bytes.Equal(d.AdvSecretKey, make([]byte, len(d.AdvSecretKey))) {
		t.Fatal("the copy's keys were not zeroed")
	}
	for name, s := range map[string]any{"sessions": d.Sessions, "lids": d.LIDs, "container": d.Container, "privacy tokens": d.PrivacyTokens} {
		if _, isNoop := s.(*store.NoopStore); !isNoop {
			t.Fatalf("the copy's %s store is %T, not a no-op", name, s)
		}
	}
}

// TestDetachedCopyWritesNothingAndHasNoPreKeys: after detach, everything a
// client could save through the copy of a refused companion (the device
// again, its deletion, a session, an identity, a sender key, a privacy
// token, a LID-phone mapping, a push name) leaves every whatsmeow table as
// it was, and the pre-key store fails every call. WHY: catches a store left
// on the database (the deleted rows written back while the unpair runs),
// which the trap-proxy tests cannot (their client never logs in), and the
// empty pre-key answer whatsmeow's post-connect upload panics on
// (prekeys.go:89).
func TestDetachedCopyWritesNothingAndHasNoPreKeys(t *testing.T) {
	wa, db := testWA(t)
	ctx := t.Context()
	pair(t, wa, minePhone, 12, mineLID)
	pair(t, wa, otherPhone, 7, otherLID)
	d, err := wa.DeviceStore.GetDevice(ctx, types.JID{User: otherPhone, Server: types.DefaultUserServer, Device: 7})
	if err != nil || d == nil {
		t.Fatalf("the paired companion was not read: %v", err)
	}
	before := dump(t, db)
	detach(d)
	must(t, d.Save(ctx))
	must(t, d.Sessions.PutSession(ctx, lidOf(thirdLID).SignalAddress().String(), []byte("changed")))
	must(t, d.Identities.PutIdentity(ctx, lidOf(thirdLID).SignalAddress().String(), [32]byte{5}))
	must(t, d.SenderKeys.PutSenderKey(ctx, "120363025246125486@g.us", lidOf(thirdLID).SignalAddress().String(), []byte("changed")))
	must(t, d.PrivacyTokens.PutPrivacyTokens(ctx, store.PrivacyToken{User: phoneOf(minePhone), Token: []byte("new"), Timestamp: time.Now()}))
	must(t, d.LIDs.PutLIDMapping(ctx, lidOf("900000000000009"), phoneOf("15550009999")))
	if _, _, err := d.Contacts.PutPushName(ctx, phoneOf(minePhone), "Changed"); err != nil {
		t.Fatal(err)
	}
	if n, err := d.PreKeys.UploadedPreKeyCount(ctx); !errors.Is(err, errNoPreKeys) {
		t.Fatalf("the uploaded pre-key count is %d, %v: whatsmeow would upload an empty list", n, err)
	}
	if keys, err := d.PreKeys.GetOrGenPreKeys(ctx, 5); !errors.Is(err, errNoPreKeys) || len(keys) != 0 {
		t.Fatalf("pre-keys %d, %v", len(keys), err)
	}
	// What Client.Logout ends with (client.go:805).
	must(t, d.Delete(ctx))
	if after := dump(t, db); !slices.Equal(before, after) {
		t.Fatalf("a detached copy changed the store:\nbefore %q\nafter  %q", before, after)
	}
}

// TestDropRefusedJudgesAMappingByItsOwnLID: the refused number has had two
// LIDs (two companion rows carry one each); the mapping row holds the one
// the connection's own companion knows, by a session. The row stays.
// WHY: catches a delete by phone number made on account of the other LID,
// which the own account does not know (the row one iteration kept, removed
// by the next).
func TestDropRefusedJudgesAMappingByItsOwnLID(t *testing.T) {
	const staleLID = "200000000000009"
	wa, db := testWA(t)
	ctx := t.Context()
	mine := pair(t, wa, minePhone, 12, mineLID)
	must(t, mine.Sessions.PutSession(ctx, lidOf(otherLID).SignalAddress().String(), []byte("s")))
	pair(t, wa, otherPhone, 8, staleLID)
	pair(t, wa, otherPhone, 7, otherLID)
	var lid string
	must(t, db.QueryRow(ctx, `SELECT lid FROM whatsmeow_lid_map WHERE pn=$1`, otherPhone).Scan(&lid))
	if lid != otherLID {
		t.Fatalf("the test's mapping is %q", lid)
	}
	for _, device := range []uint16{7, 8} {
		if _, err := dropRefused(ctx, wa, refusedLogin(otherPhone, device)); err != nil {
			t.Fatal(err)
		}
		lid = ""
		must(t, db.QueryRow(ctx, `SELECT lid FROM whatsmeow_lid_map WHERE pn=$1`, otherPhone).Scan(&lid))
		if lid != otherLID {
			t.Fatalf("after the refusal of device %d the mapping the own account knows is %q", device, lid)
		}
	}
	var n int
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM whatsmeow_device WHERE jid LIKE $1`, otherPhone+"%").Scan(&n))
	if n != 0 {
		t.Fatalf("%d companions of the other account left", n)
	}
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM whatsmeow_lid_map WHERE lid=$1`, mineLID).Scan(&n))
	if n != 1 {
		t.Fatal("the own account's mapping is gone")
	}
}

// TestDropRefusedWaitsForABusyStore: while another connection holds the
// write lock longer than SQLite's busy_timeout (5 s), the drop waits and
// then deletes, instead of failing (which ends the connection as
// other_account_kept, and the host deletes the store, own link included).
func TestDropRefusedWaitsForABusyStore(t *testing.T) {
	if testing.Short() {
		t.Skip("holds the store for 6 s")
	}
	wa, db := testWA(t)
	ctx := t.Context()
	pair(t, wa, minePhone, 12, mineLID)
	pair(t, wa, otherPhone, 7, otherLID)
	blocker, err := db.RawDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := dropRefused(ctx, wa, refusedLogin(otherPhone, 7))
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("the drop returned (%v) while the store was held", err)
	case <-time.After(6 * time.Second):
	}
	if _, err := blocker.ExecContext(ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the drop failed over a lock that passed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the drop did not finish")
	}
	var n int
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM whatsmeow_device WHERE jid LIKE $1`, otherPhone+"%").Scan(&n))
	if n != 0 {
		t.Fatalf("%d companions of the other account left", n)
	}
}

// TestHooksRefused: the worker hands the backend dropRefused and the ended
// codes, and nothing of the older refused-session hooks (DropRefused is
// used instead of them). WHY: catches the hook left unset (the backend
// would refuse the login and leave the other account's companion stored
// and linked).
func TestHooksRefused(t *testing.T) {
	h := hooks()
	if h.DropRefused == nil || reflect.ValueOf(h.DropRefused).Pointer() != reflect.ValueOf(dropRefused).Pointer() {
		t.Fatal("hooks().DropRefused is not dropRefused")
	}
	if h.EndRefused != nil || h.EndRefusedSession {
		t.Fatal("an EndRefused hook is set beside DropRefused")
	}
	if !slices.Equal(h.EndedCodes, endedCodes) {
		t.Fatal("hooks().EndedCodes is not endedCodes")
	}
}
