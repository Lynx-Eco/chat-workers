// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"

	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// selfParticipantID is the participant id the connector always takes for
// the user's own, whatever the phone's SIMs (connector handlegmessages.go
// makeEventSender and client.go IsThisUser: participantID == "1", or one
// of the login's SelfParticipantIDs).
const selfParticipantID = "1"

// selfPersonID is the worker's Hooks.SelfPersonID: the account's own person
// id is the connector's user id of the phone's own participant "1",
// "<id prefix>.1" (GMClient.MakeUserID, id.go: every person, chat and
// message id of a login is "<its id prefix>.<the phone's id>"); "" when the
// login has no id prefix (then the framework takes the login id).
//
// Why not the login id (the framework's default): it is the paired phone's
// id (QR pairing: the phone's source id; Google account pairing:
// "<the account's address>/<a number Google gives the phone>", libgm
// FinishGaiaPairing), and no event carries it.
//
// What the phone gives the account's own events: the participant ids of
// the phone's own participants, "1" and one per SIM (a SIM's
// SIMParticipant id, Settings.SIMCards; and every participant a
// conversation marks IsMe, chatinfo.go wrapChatInfo), which the connector
// collects in the login's SelfParticipantIDs (dbmeta.go SetSIMs,
// AddSelfParticipantID). Every event from any of them is from_me: the
// connector marks it IsFromMe (makeEventSender), bridgev2 sends it as the
// user's double puppet, and the backend reports that as from_me (backend
// instance.go, intent.go). The framework keeps one own id per login, and
// "1" is the one that is the same for every phone from the pairing on: the
// SIM participants are known only once the phone sends its settings after
// the first connect (handleSettings), they differ between the two SIMs of a
// dual-SIM phone, and they change when a SIM is swapped. So with two SIMs
// (or one, when the phone names its own messages by the SIM's participant
// rather than "1"; which one the phone uses is not in the source), the
// account's own messages may carry a sender id that is not this one: they
// are from_me all the same, and that sender has no PersonInfo (the
// connector gives no profile for its own participants: GetUserInfo returns
// nothing).
//
// The id prefix is read from the login's metadata, set in the sign-in's
// finishLogin before the framework keeps the login (login.go): with
// DeterministicIDPrefix (newConnector) it is the first 8 bytes of the
// SHA-256 of the login id, in hex, so the same pairing gives the same
// prefix and the same own id again after a re-pair. No network call.
func selfPersonID(login *bridgev2.UserLogin) networkid.UserID {
	if login == nil || login.UserLogin == nil {
		return ""
	}
	meta, ok := login.Metadata.(*connector.UserLoginMetadata)
	if !ok || meta == nil || !validIDPrefix(meta.IDPrefix) {
		return ""
	}
	return (&connector.GMClient{Meta: meta}).MakeUserID(selfParticipantID)
}

// validIDPrefix tells whether p can be an id prefix of the connector's: not
// empty and without the "." its ids split at (id.go parseAnyID).
func validIDPrefix(p string) bool {
	return p != "" && !strings.Contains(p, ".")
}

// identifierScheme is the worker's Hooks.IdentifierScheme: "gmessages", the
// connector's network id written out. The connector gives a person one
// identifier, "tel:<the number as the phone gives it>" (chatinfo.go
// makeUserInfo: usually E.164 with "+", but a short code, an alphanumeric
// sender or, for an RCS participant addressed by email, an address are
// passed on in the same "tel:" form), and the account none (finishLogin
// sets only the remote name: the Google account's address, or nothing for
// a QR pairing; no RemoteProfile phone, email or username). So the scheme
// is used only by StartChat, which takes it off an identifier, as it takes
// "tel:" off, before the connector resolves it (backend commands.go
// startChat): the connector's ResolveIdentifier takes a person id of the
// login ("<prefix>.<digits>", startchat.go ValidateUserID) or a phone
// number (bridgev2.CleanNonInternationalPhoneNumber: "+" and digits).
const identifierScheme = "gmessages"
