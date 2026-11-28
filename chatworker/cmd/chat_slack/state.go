// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"maunium.net/go/mautrix/bridgev2/status"
)

// The connector's bridge state codes for a session Slack ended
// (mautrix-slack v0.2611.0 connector/client.go init, handleConnectError,
// invalidateSession, Connect; handleslack.go InvalidAuthEvent). The
// connector exports no constants for them: a change of a string upstream is
// a change here.
const (
	// Slack answered invalid_auth or not_authed (at a connect, or as the
	// RTM's invalid_auth event): the token is no longer valid (signed out
	// of this session or of all sessions, the password changed, an admin
	// ended the sessions). The connector deletes the token from the login.
	codeInvalidAuth status.BridgeStateErrorCode = "slack-invalid-auth"
	// Slack answered token_revoked or token_expired: the session was signed
	// out (by the user, elsewhere, or by an admin). The token is deleted.
	codeTokenRevoked status.BridgeStateErrorCode = "slack-token-revoked"
	// A connect of a login with no token: the sequel of one of the two
	// above at the next start (Connect, client.go).
	codeNotLoggedIn status.BridgeStateErrorCode = "slack-not-logged-in"
	// account_inactive (the account deactivated), team_disabled (the
	// workspace gone) and user_removed_from_team: the connector disconnects
	// with the user action "restart" and KEEPS the token
	// (disconnectUntilRestart).
	codeAccountInactive status.BridgeStateErrorCode = "slack-account-inactive"
	codeTeamDisabled    status.BridgeStateErrorCode = "slack-team-disabled"
	codeUserRemoved     status.BridgeStateErrorCode = "slack-user-removed-from-team"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the BAD_CREDENTIALS
// codes after which the session is gone at Slack and in the store. The
// backend reports them ENDED ("the network ended the session",
// channel.proto ConnectionStateKind) with the connector's code, which the
// host keeps as it is; signing in again is a new session.
//
// Left as the backend maps them: codeAccountInactive, codeTeamDisabled and
// codeUserRemoved stay NEEDS_USER (the user must act; the host does not
// restart the worker after it either): the connector keeps the token and
// asks for a restart, as chat_whatsapp leaves wa-temporary-ban. An admin
// may reactivate the account, restore the workspace or add the user again,
// and a start of the worker then connects with the kept token, if Slack
// still takes it (not in the source). slack-connection-error,
// slack-rtm-disconnected and the socket-mode codes are
// TRANSIENT_DISCONNECT (RECONNECTING); slack-unknown-fetch-error,
// slack-unknown-connect-error, slack-rtm-fatal-error and slack-rtm-error
// are UNKNOWN_ERROR (ERROR).
var endedCodes = []status.BridgeStateErrorCode{
	codeInvalidAuth,
	codeTokenRevoked,
	codeNotLoggedIn,
}
