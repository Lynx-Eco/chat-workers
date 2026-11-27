// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/status"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the connector's
// BAD_CREDENTIALS codes that say the pairing is gone. The backend reports
// them ENDED ("the network ended the session (logged out, banned, device
// removed)", channel.proto ConnectionStateKind) with the connector's code,
// which the host keeps as it is. The host does not restart a worker after
// ENDED (nor after NEEDS_USER); the connection waits for the user to pair
// again, which on the same phone (and, for the Google flow, the same
// account) reaches the same login id and keeps the connection's chats.
//
// mautrix-gmessages v0.2611.0 (connector/bridgestate.go codes,
// handlegmessages.go handleGMEvent and invalidateSession, client.go
// Connect):
//   - gm-unpaired: the phone revoked the pairing (RevokePairData: the user
//     removed the device under Device pairing, or signed Messages out), or
//     Google ended a Google account pairing (GaiaLoggedOut). The connector
//     clears the pairing (AuthData.ClearPairing) and saves the login.
//   - gm-unpaired-other-phone: GaiaLoggedOut while another phone is set up
//     with Google Messages on the same Google account (HasOtherGaiaPhone),
//     which can unpair this one every day. Cleared the same way.
//   - gm-unpaired-entity-not-found: Google answered "Requested entity was
//     not found" (a ping, the long poll or a connect, events
//     ErrRequestedEntityNotFound): the pairing no longer exists. Cleared.
//   - gm-not-logged-in: a connect of a login with no pairing (no session,
//     or no Tachyon token: the sequel of one of the above at the next start,
//     for a QR pairing).
//   - gm-not-logged-in-can-repair: the same for a Google account pairing
//     whose cookies are still stored. A sign-in of the google flow then
//     starts at the emoji step and pairs again with those cookies
//     (login.go StartWithOverride), but the pairing itself is gone.
//
// Left as the backend maps them (NEEDS_USER: the pairing is kept, the user
// must act):
//   - gm-logged-out-invalid-credentials, gm-logged-out-401-polling,
//     gm-logged-out-workspace-session (a Google Workspace account Google
//     signs out of the web every 14 days by default),
//     gm-logged-out-no-email-in-config, and gm-not-logged-in-can-reauth (their
//     sequel at the next start): Google no longer takes the account's
//     cookies. The connector deletes only the cookies and keeps the
//     pairing; a sign-in of the google flow with new cookies re-signs the
//     same pairing in with no emoji (doOverrideLogin). For a QR pairing
//     the first two mean Google refused the pairing's own token: the user
//     pairs by QR again (the google flow would reach another login id,
//     principal_mismatch).
//   - gm-switched-to-google-login: the phone moved Google Messages to
//     Google account pairing, which a QR pairing does not follow (until
//     the phone moves back: handleAccountChange).
//   - gm-browser-inactive, gm-browser-inactive-timeout,
//     gm-browser-inactive-inactivity: another Messages for web session took
//     the phone's active session, or Google made this one inactive (with
//     AggressiveReconnect off, newConnector: the connector does not take it
//     back by itself). A start of the worker takes it back (its connect sets
//     itself active, libgm postConnect), and so does a sign-in again.
//
// Left as transient (RECONNECTING; the connector keeps trying, and the
// connection is not parked for a phone that is briefly away):
// gm-phone-not-responding (the phone did not answer a ping: offline, asleep
// with battery optimisation, or Messages closed; the state is CONNECTED
// with that code turned TRANSIENT_DISCONNECT, bridgestate.go
// FillBridgeState, and back to CONNECTED when the phone answers),
// gm-listen-error (the long poll to Google broke), gm-ping-failed (a second
// failed ping in a row). gm-connecting is CONNECTING (connected, waiting
// for the phone's first answer). ERROR: gm-listen-fatal-error,
// gm-connection-failed. The connector defines gm-not-default-sms-app and
// gm-config-fetch-failed but sends neither: Messages not being the default
// SMS app shows only as a send the phone refuses (REJECTED, certain), and a
// failed config fetch is only logged. A low battery, mobile data and the
// phone's push throttling (data saver) are not states at all: the
// connector puts the first two in a CONNECTED state's info, which the
// backend does not report, and the last only holds back the send timeouts.
var endedCodes = []status.BridgeStateErrorCode{
	connector.GMUnpaired,
	connector.GMUnpairedOtherPhone,
	connector.GMUnpaired404,
	connector.GMNotLoggedIn,
	connector.GMNotLoggedInCanRepair,
}
