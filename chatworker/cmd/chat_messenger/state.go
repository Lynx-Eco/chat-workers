// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-meta/pkg/connector"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2/status"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the connector's
// BAD_CREDENTIALS codes after which the session is gone at Meta. The
// backend reports them ENDED ("the network ended the session",
// channel.proto ConnectionStateKind) with the connector's code, which the
// host keeps as it is; signing in again is a new session.
//
// mautrix-meta v0.2611.0 pkg/connector (client.go connectWithRetry,
// handlemeta.go handleMetaEvent; the connector's own words for each are
// "Logged out, please relogin to continue"):
//   - meta-cookie-removed: loading the messages page answered that the
//     session is no longer valid (httpclient.ErrTokenInvalidated: logged
//     out on the site, the password changed, the session removed under
//     "Where you're logged in", or expired). The connector deletes the
//     cookies from the login.
//   - meta-redirected-to-login: the same, seen as a redirect to the login
//     page (ErrTokenInvalidatedRedirect); the cookies are deleted.
//   - meta-user-id-is-zero: the same, seen as a page with no user
//     (ErrUserIDIsZero); the cookies are deleted.
//   - meta-not-logged-in: a connect of a login with no cookies (the next
//     start after one of the three).
//   - dgw-connection-unauthorized: Meta's realtime socket closed as
//     unauthorized (a PermanentErrorEvent); reconnects stop. The cookies
//     are kept, but the connector names it a logout.
//
// Left as the backend maps them: NEEDS_USER, the user must act in
// Facebook's own app or site and then start the connection again (the
// connector keeps the cookies and asks for a restart, UserActionRestart,
// with the page to open, which the host is not given):
// fb-checkpoint-required (a checkpoint: Meta stopped the session to
// confirm it is the user's), fb-challenge-required (a security challenge),
// fb-consent-required (terms or a consent to accept; also raised while
// sending), fb-account-suspended (the account suspended or disabled), and
// meta-lserror-1357053 (an error of Meta's whose meaning is not in the
// source, which the connector takes as bad credentials and keeps the
// cookies); meta-ig-not-supported (an Instagram login on this connector)
// cannot happen, no Instagram flow being offered. RECONNECTING:
// meta-transient-disconnect, wa-transient-disconnect (the encrypted chats'
// socket), meta-connect-error and meta-lserror-<n> while the connector
// retries (ten times). ERROR: meta-connect-error and meta-lserror-<n> when
// those are spent, meta-unknown-permanent-error, meta-graphql-error,
// meta-inbox-load-failed, meta-proxy-update-fail, and the encrypted chats'
// wa-unknown-connect-error, wa-unknown-permanent-error and
// wa-cat-refresh-error (the connector reports a failure of the encrypted
// chats' connection alone as CONNECTED with that code, client.go
// FillBridgeState: the other chats still work); one whose words are
// whatsmeow's "client outdated" has the code connector_outdated
// (connectorOutdated).
var endedCodes = []status.BridgeStateErrorCode{
	connector.MetaCookieRemoved,
	connector.MetaRedirectedToLoginPage,
	connector.MetaUserIDIsZero,
	connector.MetaNotLoggedIn,
	connector.DGWConnectionUnauthorized,
}

// clientOutdated is the message whatsmeow gives its ClientOutdated event
// (types/events/events.go:120), which the connector puts in the state.
var clientOutdated = (&events.ClientOutdated{}).PermanentDisconnectDescription()

// connectorOutdated is the worker's backend.Hooks.ConnectorOutdated: the
// connector's state for WhatsApp's refusal of the client of the encrypted
// chats (they run over whatsmeow) as too old. whatsmeow answers WhatsApp's
// connect failure 405 with events.ClientOutdated (connectionevents.go:
// 139-141), a PermanentDisconnect, which mautrix-meta v0.2611.0 reports as
// UNKNOWN_ERROR wa-unknown-permanent-error with the event's own words,
// "client outdated", as the message (connector/handlewhatsapp.go:139-182);
// its FillBridgeState then reports it on top of a connected Meta state,
// CONNECTED with that code and message (client.go:769-794: the other chats
// still work). That code alone names every permanent disconnect of the
// encrypted chats (a stream replaced, a logout), so the message decides.
// The backend reports it with the code connector_outdated, its kind kept.
// The connection to Meta itself has no such state in the connector's
// source.
func connectorOutdated(st *status.BridgeState) bool {
	return st.Error == connector.WAPermanentError && st.Message == clientOutdated
}
