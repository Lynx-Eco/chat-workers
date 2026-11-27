// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// selfPersonID is the worker's Hooks.SelfPersonID: the account's own person
// id is its LID's user id ("lid-<digits>", waid.MakeUserID of the
// @lid JID, waid/id.go:42-53), when the login's client knows its LID; else
// "" (the framework then takes the login id, the phone number's digits,
// which is the same as the phone JID's user id: waid.MakeUserLoginID and
// MakeUserID, id.go:64-69 and :44-45).
//
// mautrix-whatsapp v0.2611.0 names the account in both forms, as WhatsApp
// addresses a chat by phone number or by LID; it takes both for the user's
// own (IsThisUser and IsOwnJID, connector/client.go:209-217). The LID is the
// one it uses most and moves to:
//   - a message's sender is its LID whenever WhatsApp gives one beside the
//     phone JID (pickLID, events.go:82-91, GetSender), so the account's own
//     messages in a group carry its LID in either addressing mode;
//   - a message the worker sends carries the sender WhatsApp answers with,
//     the LID in a LID chat (handlematrix.go:145-160);
//   - a newsletter's and the status broadcast's own member is the LID
//     (chatinfo.go:231, :575); a direct chat names the user by the form of
//     the chat's own JID (wrapDMInfo, chatinfo.go:171-176);
//   - the connector moves direct chats from a phone JID to a LID
//     (reIDPhoneDMToLID, lidmigrate.go:83-140) and groups to LID addressing
//     (PreHandle, events.go:145-170).
//
// The framework keeps one own id per login. Where the other form still
// appears (the account's messages in a direct chat that is still addressed
// by phone number, whose sender is then the phone digits), the message is
// marked from_me all the same: the connector marks every event of the
// account's own JIDs IsFromMe (makeEventSender, id.go:31-56, IsOwnJID also
// covers the LIDs the account had before, client.go:212-232), bridgev2 then
// sends it as the user's double puppet (portal.go:2882-2887), and the
// backend reports what the double puppet sends as from_me (backend
// instance.go:269-272, intent.go:80). Only that sender id is the other
// form, and a PersonInfo of it is not is_self (the host can tie the two by
// the tel: identifier the connector gives both ghosts, userinfo.go:274-289).
//
// The LID is read from the client as the connector loaded it from the
// whatsmeow device (LoadUserLogin, client.go:77-78), with no network call.
// A device paired by this worker always has one: whatsmeow stores the LID
// the server gives at pairing (pair.go:140, :223). A device with none gets
// it at its next connection (connectionevents.go:172-185): until then the
// own id is the phone digits, and from the next framework instance on the
// LID, so the host is told a second own id once.
func selfPersonID(login *bridgev2.UserLogin) networkid.UserID {
	if login == nil {
		return ""
	}
	c, ok := login.Client.(*connector.WhatsAppClient)
	if !ok || c == nil {
		return ""
	}
	lid := c.LID
	if lid.IsEmpty() {
		lid = c.Device.GetLID()
	}
	if lid.Server != types.HiddenUserServer || lid.User == "" {
		return ""
	}
	return waid.MakeUserID(lid)
}
