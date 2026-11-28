// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-slack/pkg/slackid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// selfPersonID is the worker's Hooks.SelfPersonID: the account's own person
// id is the connector's user id of the login's team and user,
// "<team id>-<user id>" in lower case (slackid.MakeUserID), the id its
// messages, reactions and memberships carry as their sender
// (connector/id.go makeEventSender) and its ghost has (client.go connect,
// GetGhostByID). The login id, and the connection's principal, is the same
// pair in Slack's own upper case (slackid.MakeUserLoginID): left to the
// framework's default (the login id), the account's own PersonInfo would
// name an id no event carries, and is_self would be set on none of its
// messages' senders. "" when the login id is not of that form (then the
// framework takes the login id).
//
// Read from the login id alone, with no network call: one Slack login is
// one user in one workspace, and the connector derives every id of the
// account from the same pair (LoadUserLogin, client.go, parses it back from
// the login id).
func selfPersonID(login *bridgev2.UserLogin) networkid.UserID {
	if login == nil || login.UserLogin == nil {
		return ""
	}
	team, user := slackid.ParseUserLoginID(login.ID)
	if !slackID(team) || !slackID(user) {
		return ""
	}
	return slackid.MakeUserID(team, user)
}

// slackID tells whether s is a Slack id as the connector takes one: one or
// more upper-case ASCII letters and digits (startchat.go isValidSlackID).
func slackID(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range []byte(s) {
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// identifierScheme is the worker's Hooks.IdentifierScheme: "slack-internal",
// the scheme of the one identifier the connector gives a person,
// "slack-internal:<user id>" (chatinfo.go wrapUserInfo), Slack's user id in
// its own upper case, without the team. The framework uses the scheme for
// two things: it prefixes the account's RemoteProfile.Username in its
// identifiers (the connector sets none: client.go connect sets the phone,
// the email, the name and the avatar), and it takes it off a StartChat
// identifier before the connector resolves it (backend commands.go
// startChat). So the identifier the host is given for a person starts a
// chat with that person: the connector's ResolveIdentifier takes a bare
// user id in the login's team, or a person id ("<team>-<user>", the team
// checked against the login's) (startchat.go ResolveIdentifier).
//
// The account's own identifiers come from its remote profile: "mailto:" of
// its email, and "tel:+" of the phone Slack's profile holds. That phone is
// free text in Slack (the user types it), not E.164: the backend prefixes
// "+" to whatever it is (backend translate.go accountIdentifiers).
const identifierScheme = "slack-internal"
