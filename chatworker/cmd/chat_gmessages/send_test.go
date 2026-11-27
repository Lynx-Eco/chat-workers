// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

// TestSendKeyFormat: every key maps to a temporary id of the form the
// connector itself makes (a version 4 UUID in canonical lower-case text),
// the same key always to the same id and two keys to two ids; a key
// already of that form is kept as it is, any other (an upper-case or
// braced UUID, a UUID of another version, an empty key) is hashed. WHY:
// catches the hook mapping a key to an id the phone was not given in this
// form (the CHANGELOG for v26.08 fixed exactly that), a mapping that is not
// a function of the key (the backend could not trace a stored message back
// to its key), and a canonical id changed on the way (a key the host made
// with the native form would not match its own echo).
func TestSendKeyFormat(t *testing.T) {
	native := util.GenerateTmpID()
	if !isTmpID(native) {
		t.Fatalf("the connector's own temporary id %q is not of the form", native)
	}
	if got := string(gmessagesSendKey(native)); got != native {
		t.Fatalf("native id %q became %q", native, got)
	}
	seen := map[string]string{}
	for _, key := range []string{
		"", "k1", "k2", "send-0001",
		strings.ToUpper(native),
		"{" + native + "}",
		"urn:uuid:" + native,
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",                // version 1
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("x")).String(), // version 5
	} {
		id := string(gmessagesSendKey(key))
		if !isTmpID(id) {
			t.Errorf("key %q: %q is not a version 4 UUID in canonical form", key, id)
		}
		if id == key {
			t.Errorf("key %q was kept, not hashed", key)
		}
		if again := string(gmessagesSendKey(key)); again != id {
			t.Errorf("key %q: %q then %q", key, id, again)
		}
		if other, dup := seen[id]; dup {
			t.Errorf("keys %q and %q both give %q", other, key, id)
		}
		seen[id] = key
	}
}

// TestHooksSendKeyFormat: the worker hands the backend the send key
// mapping. WHY: catches the hook left unset (the host's key would go to
// the phone as the temporary id as it is, of whatever form).
func TestHooksSendKeyFormat(t *testing.T) {
	h := hooks().SendKeyFormat
	if h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(gmessagesSendKey).Pointer() {
		t.Fatal("hooks().SendKeyFormat is not gmessagesSendKey")
	}
}
