// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"

	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// selfPersonID is the worker's Hooks.SelfPersonID: the account's own person
// id is the connector's user id of its own number, "<prefix>.<number>",
// the form of every person id of the login (chatinfo.go makeUserID: the
// login's prefix, a number the connector's own table gives each login,
// gvdb.GetLoginPrefix, and an E.164 number). The number is the login id's
// (login.go:139: the account's primary Voice number, which the connector
// also keeps as the login's remote name and profile phone, :147-150).
//
// Left to the framework's default, the account's own PersonInfo would be
// the login id, "<owner MXID>|<number>": an id of another form than every
// other person's, with the backend's internal user in it. The account's own
// messages carry no sender id of the connector's (they are IsFromMe,
// handlegvoice.go getMessageMeta: the backend reports them from_me with
// this id, as the double puppet's); should Google list the account's own
// number among a group text's members, its person id is this one.
//
// "" (the framework then takes the login id) when the login has no
// metadata of the connector's, or its prefix or number is not of the form
// the connector's own check takes (connector.go ValidateUserID: the prefix
// digits, the number digits after an optional "+"). Read from the login
// alone, with no network call.
func selfPersonID(login *bridgev2.UserLogin) networkid.UserID {
	if login == nil || login.UserLogin == nil {
		return ""
	}
	meta, ok := login.Metadata.(*connector.UserLoginMetadata)
	if !ok || meta == nil {
		return ""
	}
	number := loginNumber(login.ID)
	if !digits(meta.Prefix) || !digits(strings.TrimPrefix(number, "+")) {
		return ""
	}
	return networkid.UserID(meta.Prefix + "." + number)
}

// digits tells whether s is one or more ASCII digits.
func digits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// identifierScheme is the worker's Hooks.IdentifierScheme: "gvoice", the
// connector's network id (connector.go GetName), named. Phone numbers are
// "tel:+<digits>" whatever the scheme: a person's come from the connector
// ("tel:" of the number and of each number of the person's Google contact,
// "mailto:" of its emails, chatinfo.go wrapUserInfo), the account's from
// its RemoteProfile.Phone (login.go:149, the backend prefixes "+" when it
// is missing). The connector sets no Username, so no "gvoice:" identifier
// is reported. The framework takes "tel:" and "gvoice:" off a StartChat
// identifier (backend commands.go startChat); the connector's
// ResolveIdentifier (startchat.go) then takes a person id of the login
// ("<prefix>.<number>", the prefix checked against the login's) or a phone
// number: international ("+..."), or, for an account whose number is North
// American, ten digits, or eleven starting with 1.
const identifierScheme = "gvoice"
