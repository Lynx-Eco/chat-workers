// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"reflect"
	"slices"
	"strconv"
	"testing"

	"go.mau.fi/mautrix-signal/pkg/connector"
)

// TestHooksSendKeyFormat: the worker hands the backend the send key
// mapping, and no id it makes parses as a number, which the connector
// would take for the message's Signal timestamp (getTimestampForEvent,
// handlematrix.go:108-120). WHY: catches the hook left unset (a host key
// that is all digits would become a timestamp in 1970 or in the far
// future, and the message's id with it) and a mapping to numbers
// (backend.NumericSendKey, Telegram's).
func TestHooksSendKeyFormat(t *testing.T) {
	h := hooks().SendKeyFormat
	if h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(signalSendKey).Pointer() {
		t.Fatal("hooks().SendKeyFormat is not signalSendKey")
	}
	for _, key := range []string{"", "1", "1795000000000", "18446744073709551615", "k1", "lynx-0"} {
		id := string(h(key))
		if _, err := strconv.ParseUint(id, 10, 64); err == nil {
			t.Errorf("key %q: %q parses as a timestamp", key, id)
		}
		if !isSendKeyID(id) {
			t.Errorf("key %q: %q is not of the worker's form", key, id)
		}
	}
}

// TestSendKeyIsAFunctionOfTheKey: the same key gives the same id, two keys
// two ids, and an id of the worker's form is kept. WHY: catches a mapping
// that is not a function of the key (the backend finds a stored message's
// key by mapping each journal key again, sendKeyForTxn), one that lets a
// near-miss through unhashed (upper-case hex, one digit short), and one
// that collapses keys.
func TestSendKeyIsAFunctionOfTheKey(t *testing.T) {
	for _, key := range []string{"", "k1", "send-0001", "lynx-0123456789ABCDEF0123456789abcdef", "lynx-0123456789abcdef0123456789abcde"} {
		got := signalSendKey(key)
		if again := signalSendKey(key); again != got {
			t.Fatalf("key %q: %q then %q", key, got, again)
		}
		if string(got) == key {
			t.Fatalf("key %q not of the form was kept", key)
		}
	}
	if a, b := signalSendKey("k1"), signalSendKey("k2"); a == b {
		t.Fatalf("two keys, one id %q", a)
	}
	const kept = "lynx-0123456789abcdef0123456789abcdef"
	if got := string(signalSendKey(kept)); got != kept {
		t.Fatalf("an id of the form is kept: %q", got)
	}
}

// TestHooksIdentifierScheme: a Signal username is a "signal:" identifier,
// as the connector writes a person's (chatinfo.go:124). WHY: catches the
// scheme "tel", which would make a username look like a phone number, and
// the scheme left to the framework's default (the connector's network id,
// which a change upstream could rename).
func TestHooksIdentifierScheme(t *testing.T) {
	if got := hooks().IdentifierScheme; got != "signal" {
		t.Fatalf("identifier scheme %q, want signal", got)
	}
}

// TestHooksOfferTheQRFlowOnly: the one flow offered is the connector's QR
// flow, and the connector has it. WHY: catches a flow name that does not
// match the connector's (no sign-in would be possible) and a flow the
// connector adds later offered without a look.
func TestHooksOfferTheQRFlowOnly(t *testing.T) {
	h := hooks()
	if !slices.Equal(h.SignInFlows, []string{"qr"}) {
		t.Fatalf("flows %v, want [qr]", h.SignInFlows)
	}
	var ids []string
	for _, f := range (&connector.SignalConnector{}).GetLoginFlows() {
		ids = append(ids, f.ID)
	}
	if !slices.Contains(ids, loginFlowQR) {
		t.Fatalf("the connector's flows %v have no %q", ids, loginFlowQR)
	}
	if h.IsChannel != nil {
		t.Fatal("IsChannel set: Signal has no channels")
	}
	if h.ShowExpiresIn == nil || h.SelfPersonID == nil || len(h.EndedCodes) == 0 {
		t.Fatal("ShowExpiresIn, SelfPersonID or EndedCodes unset")
	}
}
