// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/status"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the connector's
// BAD_CREDENTIALS codes after which Google no longer takes the login's
// cookies. The backend reports them ENDED ("the network ended the
// session", channel.proto ConnectionStateKind) with the connector's code,
// which the host keeps as it is; signing in again (fresh cookies) is a new
// session of the same principal.
//
// mautrix-gvoice v0.2611.0 (connector/client.go:35-55 names every code):
//   - gv-bad-credentials: Google answered 401 or 403 (libgv
//     ResponseError.IsAuthError): the cookies expired, or the Google
//     session was signed out (by the user, elsewhere, or by Google). Sent
//     by sendBadCredentials (client.go:179-190, with the user action
//     "relogin"), from a connect's or a reconnect's account check
//     (handleConnectError, :125-128), the realtime channel (:221-224), the
//     message list (handlegvoice.go:132-134), and the account check after a
//     send or a read Google refused so (checkSessionAfterAuthError,
//     client.go:166-177, errors.go:57-58). The connector stops its realtime
//     channel and keeps the cookies; the next start checks them again and
//     says the same.
//   - gv-not-logged-in: named, never sent by this version. Listed so that a
//     later version's "not logged in" is not taken for a user's error.
//
// The others: gv-connect-error and gv-rate-limited (an account check at
// connect that failed otherwise, retried every minute, client.go:130-163),
// gv-transient-disconnect and gv-too-many-retries (the realtime channel
// dropped, retried after 1 minute, or 10 after 5 retries, :210-247) are
// TRANSIENT_DISCONNECT, so RECONNECTING. gv-realtime-error is named and
// never sent. Nothing of the connector's is NEEDS_USER: a Google account
// with no Voice number is refused at sign-in (fi.mau.gvoice.no_number), not
// reported as a state.
var endedCodes = []status.BridgeStateErrorCode{
	connector.GVBadCredentials,
	connector.GVNotLoggedIn,
}
