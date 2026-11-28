// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"

	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// deterministicPrefix is the id prefix the connector gives a login with
// DeterministicIDPrefix (login.go finishLogin).
func deterministicPrefix(id networkid.UserLoginID) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:8])
}

func loginWithPrefix(id networkid.UserLoginID, prefix string) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: id, Metadata: &connector.UserLoginMetadata{IDPrefix: prefix}}}
}

// TestSelfPersonID: the account's own person id is the connector's user id
// of participant "1" under the login's id prefix, the same as the
// connector's MakeUserID makes and its ValidateUserID takes, and the same
// for the same login id after a re-pair; a login with no prefix, with a
// prefix the connector could not split, or with metadata of another kind
// gives "" (the framework's default). WHY: catches the default left in
// place (the login id, the phone's id or the account's address, which no
// event carries), an id of a form the connector would not resolve, and a
// prefix that changes with each pairing of the same phone.
func TestSelfPersonID(t *testing.T) {
	const qrID = networkid.UserLoginID("a1b2c3d4-phone-source-id")
	const googleID = networkid.UserLoginID("user@example.com/1234567890")
	for _, id := range []networkid.UserLoginID{qrID, googleID} {
		prefix := deterministicPrefix(id)
		login := loginWithPrefix(id, prefix)
		got := selfPersonID(login)
		meta := login.Metadata.(*connector.UserLoginMetadata)
		if want := (&connector.GMClient{Meta: meta}).MakeUserID("1"); got != want || got != networkid.UserID(prefix+".1") {
			t.Fatalf("%s: %q, want %q", id, got, want)
		}
		gc := &connector.GMConnector{Config: connector.Config{DeterministicIDPrefix: true}}
		if !gc.ValidateUserID(got) {
			t.Fatalf("%s: the connector does not take %q as a user id", id, got)
		}
		if again := selfPersonID(loginWithPrefix(id, deterministicPrefix(id))); again != got {
			t.Fatalf("%s: a re-pair gives %q, before %q", id, again, got)
		}
	}
	if selfPersonID(loginWithPrefix(qrID, deterministicPrefix(qrID))) == selfPersonID(loginWithPrefix(googleID, deterministicPrefix(googleID))) {
		t.Fatal("two logins share an own id")
	}
	for name, login := range map[string]*bridgev2.UserLogin{
		"nil":               nil,
		"no row":            {},
		"no metadata":       {UserLogin: &database.UserLogin{ID: qrID}},
		"other metadata":    {UserLogin: &database.UserLogin{ID: qrID, Metadata: map[string]any{"id_prefix": "1"}}},
		"nil metadata":      {UserLogin: &database.UserLogin{ID: qrID, Metadata: (*connector.UserLoginMetadata)(nil)}},
		"no prefix":         loginWithPrefix(qrID, ""),
		"prefix with a dot": loginWithPrefix(qrID, "12.34"),
	} {
		if got := selfPersonID(login); got != "" {
			t.Errorf("%s: %q, want \"\"", name, got)
		}
	}
}

// TestSelfIsTheConnectorsOwn: the connector takes participant "1" for the
// user's own, with no SIM or self participant known, which is what makes
// the own id the same from the pairing on. WHY: catches a connector that
// stopped taking "1" for the user (the own id would then name a person who
// is not the user).
func TestSelfIsTheConnectorsOwn(t *testing.T) {
	meta := &connector.UserLoginMetadata{IDPrefix: deterministicPrefix("x")}
	gc := &connector.GMClient{Meta: meta}
	if !gc.IsThisUser(t.Context(), gc.MakeUserID(selfParticipantID)) {
		t.Fatal("the connector does not take participant 1 for the user")
	}
	if gc.IsThisUser(t.Context(), gc.MakeUserID("2")) {
		t.Fatal("the connector takes another participant for the user")
	}
}

// TestHooksSelfAndScheme: the worker hands the backend its own id hook and
// the connector's network id as the identifier scheme. WHY: catches either
// left unset, and a scheme that is not the connector's network id (a
// StartChat of "gmessages:<person id>" would not have it taken off).
func TestHooksSelfAndScheme(t *testing.T) {
	h := hooks()
	if h.SelfPersonID == nil || reflect.ValueOf(h.SelfPersonID).Pointer() != reflect.ValueOf(selfPersonID).Pointer() {
		t.Fatal("hooks().SelfPersonID is not selfPersonID")
	}
	if got, want := h.IdentifierScheme, (&connector.GMConnector{}).GetName().NetworkID; got != want || got != "gmessages" {
		t.Fatalf("identifier scheme %q, the connector's network id %q", got, want)
	}
}
