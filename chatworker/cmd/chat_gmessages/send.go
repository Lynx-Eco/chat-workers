// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"crypto/sha256"

	"github.com/google/uuid"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// gmessagesSendKey is the worker's Hooks.SendKeyFormat. It maps the host's
// send key to a temporary id of the form Google Messages' own app makes, a
// random (version 4) UUID in its canonical lower-case text (libgm
// util.GenerateTmpID, uuid.NewString: "Matches what the native app does";
// the connector's CHANGELOG for v26.08 fixed the form of these ids). The
// same key always gives the same id: a key already of that form is kept as
// it is, any other is hashed (SHA-256, its first 16 bytes with the version
// and variant bits of a version 4 UUID set).
//
// What the connector does with it: an input transaction id is the message's
// temporary id (HandleMatrixMessage, handlematrix.go: TmpID of the request,
// of its payload and TmpID2), which the phone gives back on the message
// once it has it (Message.TmpID; MessageEvent.GetTransactionID): that echo
// is how the framework ties the network's message to the send
// (AddPendingToSave; portal.go checkPendingMessage), and how a send that
// was answered before the phone confirmed it is resolved. bridgev2 keeps
// the transaction id on the message's row (SendTxnID), and the backend
// finds the key of a stored message by mapping each journal key again and
// comparing (sendKeyForTxn), which needs the mapping to be a function of
// the key. The message's id once the phone has it is the connector's,
// "<id prefix>.<the phone's message id>" (MakeMessageID), the id every
// message of the chat carries.
//
// The id is not what keeps a send from going out twice: the journal is (a
// key already sent, or in flight, is answered from it without the network,
// backend send.go runKeyed). The connector sends the request again by
// itself only when the phone answers it with a transient failure
// (FAILURE_2, FAILURE_3: sendRetryBackoff, 3, 8 and 20 seconds), with the
// same temporary id; what the phone does with a second request of an id it
// has is not in the source.
func gmessagesSendKey(key string) networkid.RawTransactionID {
	if isTmpID(key) {
		return networkid.RawTransactionID(key)
	}
	sum := sha256.Sum256([]byte(key))
	var u uuid.UUID
	copy(u[:], sum[:16])
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return networkid.RawTransactionID(u.String())
}

// isTmpID tells whether s is a version 4 UUID of the RFC 4122 variant in
// canonical lower-case text, the form gmessagesSendKey makes.
func isTmpID(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u.String() == s && u.Version() == 4 && u.Variant() == uuid.RFC4122
}
