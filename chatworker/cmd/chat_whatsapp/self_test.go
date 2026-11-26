// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"reflect"
	"testing"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// testPhone and testLID are the test account's (phoneJID and lidJID).
const (
	testPhone = "15551234567"
	testLID   = "123456789012345"
)

// loginWith is a signed-in login as the connector loads it (LoadUserLogin,
// client.go:46-80): the login id is the phone number's digits, and its
// client has the phone JID (with the companion's device number) and the LID
// it read from the whatsmeow device.
func loginWith(client bridgev2.NetworkAPI) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: waid.MakeUserLoginID(types.NewJID(testPhone, types.DefaultUserServer))},
		Client:    client,
	}
}

func waClient(lid types.JID, device *store.Device) *connector.WhatsAppClient {
	return &connector.WhatsAppClient{
		JID:    types.JID{User: testPhone, Server: types.DefaultUserServer, Device: 12},
		LID:    lid,
		Device: device,
	}
}

// TestSelfPersonIDIsTheLID: the account's own person id is its LID's user
// id, the one the connector gives the account's messages in a group and in
// a LID chat, which the connector itself takes for the user's own
// (IsThisUser). WHY: catches the hook left unset or returning the login id
// (the phone digits): the account's own messages would carry an id that is
// not is_self, and the member list would name the account by its phone
// number.
func TestSelfPersonIDIsTheLID(t *testing.T) {
	lid := types.JID{User: testLID, Server: types.HiddenUserServer, Device: 12}
	c := waClient(lid, nil)
	got := selfPersonID(loginWith(c))
	if want := networkid.UserID(waid.LIDPrefix + testLID); got != want {
		t.Fatalf("own id %q, want %q", got, want)
	}
	if got != waid.MakeUserID(lid) {
		t.Fatalf("own id %q is not the connector's user id of the LID %q", got, waid.MakeUserID(lid))
	}
	if !c.IsThisUser(context.Background(), got) {
		t.Fatalf("the connector does not take %q for the user", got)
	}
	// The login id is the other form the connector takes for the user.
	if login := loginWith(c); !c.IsThisUser(context.Background(), networkid.UserID(login.ID)) {
		t.Fatalf("the connector does not take the login id %q for the user", login.ID)
	}
}

// TestSelfPersonIDFromTheDevice: a client whose LID field is not filled
// yet (the connector fills it lazily, client.go:237-242) is asked of the
// whatsmeow device it was loaded with. WHY: catches reading the client's
// field alone (the phone digits until the connector happened to fill it,
// then the LID: two own ids for one login).
func TestSelfPersonIDFromTheDevice(t *testing.T) {
	dev := &store.Device{LID: types.JID{User: testLID, Server: types.HiddenUserServer}}
	if got, want := selfPersonID(loginWith(waClient(types.EmptyJID, dev))), networkid.UserID(waid.LIDPrefix+testLID); got != want {
		t.Fatalf("own id %q, want %q", got, want)
	}
}

// TestSelfPersonIDFallsBack: with no LID known (a device paired before
// WhatsApp gave one, until its next connection), or no WhatsApp client, the
// hook says "" and the framework takes the login id, which is the phone
// JID's user id, the other form the connector names the account by. A JID
// of another server in the LID's place is not taken for one. WHY: catches
// a made-up id ("lid-" with nothing after it, or a phone number labelled a
// LID) instead of the framework's fallback.
func TestSelfPersonIDFallsBack(t *testing.T) {
	for name, login := range map[string]*bridgev2.UserLogin{
		"no LID":                 loginWith(waClient(types.EmptyJID, nil)),
		"device without LID":     loginWith(waClient(types.EmptyJID, &store.Device{})),
		"LID of an empty user":   loginWith(waClient(types.JID{Server: types.HiddenUserServer}, nil)),
		"phone JID in LID field": loginWith(waClient(phoneJID, nil)),
		"no client":              loginWith(nil),
		"nil WhatsApp client":    loginWith((*connector.WhatsAppClient)(nil)),
		"no login":               nil,
	} {
		if got := selfPersonID(login); got != "" {
			t.Errorf("%s: %q, want \"\"", name, got)
		}
	}
	// The framework's fallback, the login id, is the phone JID's user id.
	if id := waid.MakeUserLoginID(phoneJID); string(id) != string(waid.MakeUserID(phoneJID)) {
		t.Fatalf("login id %q is not the phone's user id %q", id, waid.MakeUserID(phoneJID))
	}
}

// TestHooksSelfPersonID: the worker hands the backend the hook. WHY:
// catches the hook left unset (the framework would take the login id).
func TestHooksSelfPersonID(t *testing.T) {
	if h := hooks().SelfPersonID; h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(selfPersonID).Pointer() {
		t.Fatal("hooks().SelfPersonID is not selfPersonID")
	}
}
