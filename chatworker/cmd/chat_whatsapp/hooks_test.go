// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"reflect"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow"
)

// TestHooksSendKeyFormat: the worker hands the backend the send key
// mapping, and every id it makes has the prefix the connector looks for
// before it sends an input transaction id as the message's own id
// (handlematrix.go:117). WHY: catches the hook left unset (the host's key
// would be the transaction id, the connector would send a random id, and a
// stored message could not be traced back to its key), and an id without
// the prefix.
func TestHooksSendKeyFormat(t *testing.T) {
	h := hooks().SendKeyFormat
	if h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(whatsappSendKey).Pointer() {
		t.Fatal("hooks().SendKeyFormat is not whatsappSendKey")
	}
	for _, key := range []string{"", "k1", "3EB0", "3EB0AABBCCDDEEFF001122"} {
		if id := string(h(key)); !strings.HasPrefix(id, whatsmeow.WebMessageIDPrefix) {
			t.Errorf("key %q: %q does not begin with %q", key, id, whatsmeow.WebMessageIDPrefix)
		}
	}
}

// TestHooksIdentifierScheme: a WhatsApp handle is a "whatsapp:"
// identifier, and phone numbers stay tel: (the framework writes them so
// whatever the scheme). WHY: catches the scheme "tel", which would make a
// handle the connector reported as a username look like a phone number,
// and the scheme left to the framework's default (the connector's network
// id, which a change upstream could rename).
func TestHooksIdentifierScheme(t *testing.T) {
	if got := hooks().IdentifierScheme; got != "whatsapp" {
		t.Fatalf("identifier scheme %q, want whatsapp", got)
	}
}
