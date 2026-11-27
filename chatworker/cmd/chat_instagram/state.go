// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-meta/pkg/igconnector"
	"maunium.net/go/mautrix/bridgev2/status"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the connector's
// BAD_CREDENTIALS codes after which the session is gone at Instagram. The
// backend reports them ENDED ("the network ended the session",
// channel.proto ConnectionStateKind) with the connector's code, which the
// host keeps as it is; signing in again is a new session.
//
// mautrix-meta v0.2611.0 pkg/igconnector (client.go errorToBridgeState and
// connectWithRetry, handlemeta.go handleIGEvent; the connector's own words
// for each are "Logged out, please relogin to continue"):
//   - meta-cookie-removed: Instagram answered that the session is no longer
//     valid (httpclient.ErrTokenInvalidated, at a connect or from any
//     request after: logged out on the site or app, the password changed,
//     the session removed under "Where you're logged in", or expired). The
//     connector deletes the cookies from the login.
//   - meta-redirected-to-login: the same, seen as a redirect to the login
//     page; the cookies are deleted.
//   - meta-user-id-is-zero: the same, seen as a page with no user; the
//     cookies are deleted.
//   - meta-not-logged-in: a connect of a login with no cookies (the next
//     start after one of the three).
//   - dgw-connection-unauthorized: Instagram's realtime socket refused the
//     session as unauthorized; reconnects stop. The cookies are kept, but
//     the connector names it a logout.
//   - ig-connect-missing-user-id: the realtime socket found no messaging
//     user id for the session (instameow ErrUserIDMissing); reconnects stop.
//
// Left as the backend maps them: NEEDS_USER, the user must act in
// Instagram's own app or site and then start the connection again (the
// connector keeps the cookies, asks for a restart, UserActionRestart, with
// the page to open, which the host is not given, and checks again by
// itself, a full reconnect after a growing delay of at most
// force_refresh_interval_seconds, 20 hours, scheduleAccountStateRecheck; a
// check that connects reports CONNECTED):ig-challenge-required (a security challenge: "was
// this you?"), fb-checkpoint-required (a checkpoint: Instagram stopped the
// session to confirm it is the user's), ig-consent-required (terms or a
// consent to accept), ig-account-suspended (the account suspended),
// ig-account-restricted (Instagram limits the account: content
// unavailable; also raised while sending), and meta-lserror-1357053 (an
// error of Meta's whose meaning is not in the source, taken as bad
// credentials). ig-web-push-stale (UserActionRelogin) needs push keys the
// worker never registers (the backend has no push), and
// meta-not-instagram-account a login of another platform, which no flow
// offered makes: neither happens. RECONNECTING: ig-dgw-connection-error and
// meta-lserror-<n> while the connector retries. ERROR:
// dgw-main-stream-closed (the main stream closed more than five times in a
// row), meta-connect-error, meta-graphql-error, meta-proxy-update-fail and
// meta-lserror-<n> when the retries are spent.
var endedCodes = []status.BridgeStateErrorCode{
	igconnector.MetaCookieRemoved,
	igconnector.MetaRedirectedToLoginPage,
	igconnector.MetaUserIDIsZero,
	igconnector.MetaNotLoggedIn,
	igconnector.DGWConnectionUnauthorized,
	igconnector.NativeIGMissingUserID,
}
