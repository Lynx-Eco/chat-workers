// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// selfPersonID is the worker's Hooks.SelfPersonID: the account's own person
// id is the connector's user id of its ACI (signalid.MakeUserID, the UUID's
// text form), parsed from the login id, which is the same ACI
// (signalid.MakeUserLoginID, login.go:222-223); "" when the login id is
// not one (the framework then takes the login id itself).
//
// mautrix-signal v0.2611.0 names the account by its ACI only:
//   - the account's own messages, sent from the phone or from another
//     linked device and synced, carry its ACI as the sender (makeEventSender
//     marks them IsFromMe, as IsThisUser does, id.go:46-52 and
//     client.go:109-114);
//   - a message the worker sends is stored with the ACI as its sender
//     (doSendMessage, handlematrix.go:161-189), and its id is
//     "<ACI>|<timestamp>";
//   - a group's member list names the account by its ACI.
//
// The account's phone number is not a person id: it is the login's
// RemoteProfile.Phone (login.go:224-226) and the "tel:" identifier of its
// person. The PNI (the account's phone number identity) is the device's
// (DeviceData.PNI), never a sender the connector reports for the user.
//
// Today the two ids are the same string, so the hook changes nothing; it is
// set so that the worker states the account's own id itself, and a change
// of either form upstream fails self_test.go instead of reporting another
// own id. Read from the login alone: no store or network call.
func selfPersonID(login *bridgev2.UserLogin) networkid.UserID {
	if login == nil || login.UserLogin == nil {
		return ""
	}
	aci, err := signalid.ParseUserLoginID(login.ID)
	if err != nil {
		return ""
	}
	return signalid.MakeUserID(aci)
}
