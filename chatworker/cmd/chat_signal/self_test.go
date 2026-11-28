// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-signal/pkg/connector"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// TestSelfPersonIDIsTheACI: the account's own person id is the
// connector's user id of its ACI, the id the connector gives the account's
// own messages and takes for the user's (IsThisUser), and the same string
// as the login id; a login id that is not an ACI gives "" (the framework's
// default). WHY: catches the own id taken from the phone number (the
// account's messages would carry an id that is not is_self), and an
// upstream change that makes the login id and the user id of one ACI
// differ.
func TestSelfPersonIDIsTheACI(t *testing.T) {
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: signalid.MakeUserLoginID(mineACI)}}
	got := selfPersonID(login)
	if got != signalid.MakeUserID(mineACI) || string(got) != mineACI.String() {
		t.Fatalf("own id %q, want %q", got, mineACI)
	}
	if string(got) != string(login.ID) {
		t.Fatalf("own id %q is not the login id %q", got, login.ID)
	}
	sc, _ := testSignal(t)
	device := link(t, sc, mineACI, minePNI, 2, "mine-password")
	c := &connector.SignalClient{Client: signalmeow.NewClient(device, zerolog.Nop(), nil)}
	if !c.IsThisUser(t.Context(), got) {
		t.Fatalf("the connector does not take %q for the user", got)
	}
	for _, id := range []networkid.UserLoginID{"", "+15550001111", "pni:" + networkid.UserLoginID(minePNI.String())} {
		if got := selfPersonID(&bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: id}}); got != "" {
			t.Errorf("login id %q: own id %q, want \"\"", id, got)
		}
	}
	if selfPersonID(nil) != "" || selfPersonID(&bridgev2.UserLogin{}) != "" {
		t.Fatal("no login: an own id")
	}
}
