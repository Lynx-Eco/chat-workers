// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-signal/pkg/connector"
	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/store"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"lynx.invalid/ingest/chatworker/contract"
	lynxstore "lynx.invalid/ingest/chatworker/store"
)

// The accounts of these tests: mine (the connection's), the other one a
// re-link reached (refused), and a third person both know.
var (
	mineACI  = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	minePNI  = uuid.MustParse("11111111-2222-4222-8222-111111111111")
	otherACI = uuid.MustParse("22222222-1111-4111-8111-222222222222")
	otherPNI = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	thirdACI = uuid.MustParse("33333333-1111-4111-8111-333333333333")
)

// testSignal is the worker's connector on a store of its own, opened as
// the worker opens it (store.OpenDB: encrypted in the encrypted build,
// every connection with foreign_keys=ON), with the framework's database
// and signalmeow's container on it as the connector's Init makes them
// (connector.go:68-75), the framework's tables made, and signalmeow's
// (Start, :81-88).
func testSignal(t *testing.T) (*connector.SignalConnector, *dbutil.Database) {
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
	sc := n.(*connector.SignalConnector)
	sc.Bridge = &bridgev2.Bridge{DB: database.New("signal", database.MetaTypes{}, db)}
	// The framework's tables (user_login among them), as the backend's
	// newFramework makes them before the connector starts.
	if err := sc.Bridge.DB.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	sc.Store = store.NewStore(db, dbutil.ZeroLogger(zerolog.Nop()))
	if err := sc.Store.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	return sc, db
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// link stores a linked device of aci as signalmeow's provisioning does
// (finishProvisioning, provisioning.go: the device row, then its own
// identity keys, signed and last-resort Kyber pre-keys and its own
// recipient row with the profile key); then, as a device in use would, a
// session, an identity key, a sender key, a group, a recipient and a
// profile key of the third person.
func link(t *testing.T, sc *connector.SignalConnector, aci, pni uuid.UUID, deviceID int, password string) *store.Device {
	t.Helper()
	ctx := t.Context()
	aciKeys, err := libsignalgo.GenerateIdentityKeyPair()
	must(t, err)
	pniKeys, err := libsignalgo.GenerateIdentityKeyPair()
	must(t, err)
	must(t, sc.Store.PutDevice(ctx, &store.DeviceData{
		ACIIdentityKeyPair: aciKeys, PNIIdentityKeyPair: pniKeys,
		ACIRegistrationID: 101, PNIRegistrationID: 102,
		ACI: aci, PNI: pni, DeviceID: deviceID, Number: "+15550000000", Password: password,
	}))
	d, err := sc.Store.DeviceByACI(ctx, aci)
	must(t, err)
	_, err = d.IdentityKeyStore.SaveIdentityKey(ctx, d.ACIServiceID(), d.ACIIdentityKeyPair.GetIdentityKey())
	must(t, err)
	must(t, d.ACIPreKeyStore.StoreSignedPreKey(ctx, 1, signalmeow.GenerateSignedPreKey(1, aciKeys)))
	must(t, d.ACIPreKeyStore.StoreLastResortKyberPreKey(ctx, 1, signalmeow.GenerateKyberPreKeys(1, 1, aciKeys)[0]))
	_, err = d.RecipientStore.LoadAndUpdateRecipient(ctx, aci, pni, func(r *types.Recipient) (bool, error) {
		r.E164 = "+15550000000"
		r.Profile.Key = libsignalgo.ProfileKey(bytes.Repeat([]byte{5}, 32))
		return true, nil
	})
	must(t, err)
	// What a device in use keeps of the third person.
	thirdKeys, err := libsignalgo.GenerateIdentityKeyPair()
	must(t, err)
	third := libsignalgo.NewACIServiceID(thirdACI)
	_, err = d.IdentityKeyStore.SaveIdentityKey(ctx, third, thirdKeys.GetIdentityKey())
	must(t, err)
	_, err = d.RecipientStore.LoadAndUpdateRecipient(ctx, thirdACI, uuid.Nil, func(r *types.Recipient) (bool, error) {
		r.ContactName = "Third"
		return true, nil
	})
	must(t, err)
	must(t, d.GroupStore.StoreMasterKey(ctx, types.GroupIdentifier("group-"+aci.String()[:8]), types.SerializedGroupMasterKey("master")))
	_, err = sc.Bridge.DB.Exec(ctx, `INSERT INTO signalmeow_sessions (account_id, service_id, their_service_id, their_device_id, record) VALUES ($1, $2, $3, 1, $4)`,
		aci.String(), d.ACIServiceID().String(), third.String(), []byte("session"))
	must(t, err)
	_, err = sc.Bridge.DB.Exec(ctx, `INSERT INTO signalmeow_sender_keys (account_id, sender_uuid, sender_device_id, distribution_id, key_record) VALUES ($1, $2, 1, $3, $4)`,
		aci.String(), thirdACI.String(), uuid.NewString(), []byte("sk"))
	must(t, err)
	_, err = sc.Bridge.DB.Exec(ctx, `INSERT INTO signalmeow_event_buffer (account_id, ciphertext_hash, plaintext, server_timestamp, insert_timestamp) VALUES ($1, $2, $3, 1, 1)`,
		aci.String(), []byte("hash-"+aci.String()), []byte("plain"))
	must(t, err)
	return d
}

// dump is every row of every signalmeow table, each as one line of text
// ("<table>: <col>=<value> ..."; bytes that are not printable text in
// hex), sorted.
func dump(t *testing.T, db *dbutil.Database) []string {
	t.Helper()
	ctx := t.Context()
	rows, err := db.Query(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'signalmeow%' ORDER BY name`)
	must(t, err)
	var tables []string
	for rows.Next() {
		var name string
		must(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	must(t, rows.Close())
	if len(tables) < 15 {
		t.Fatalf("signalmeow tables: %v", tables)
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
