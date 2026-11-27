// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"maunium.net/go/mautrix/bridgev2/status"
)

// The connector's bridge state codes for a linked device Signal no longer
// takes (connector/errors.go:36-47, client.go). It exports no names for
// them.
const (
	// codeLoggedOut: Signal refused the device's credentials.
	codeLoggedOut status.BridgeStateErrorCode = "signal-logged-out"
	// codeNotLoggedIn: a login whose device is gone from the store.
	codeNotLoggedIn status.BridgeStateErrorCode = "signal-not-logged-in"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the connector's
// bridge state codes that say Signal ended the linked device's session. The
// connector reports each as BAD_CREDENTIALS with the user action "relogin";
// the backend reports them ENDED ("the network ended the session (logged
// out, banned, device removed)", channel.proto ConnectionStateKind), with
// the connector's code, a fixed one the host takes as it is. The host does
// not restart a worker after ENDED (nor after NEEDS_USER), and the
// connection waits for the user to link the device again.
//
// mautrix-signal v0.2611.0 and signalmeow:
//   - signal-logged-out: Signal answered the device's authenticated socket
//     with 403, or 401 (signalmeow web/signalwebsocket.go:312-315, at
//     every connect and reconnect): the device was removed under Linked
//     devices on the phone, or Signal dropped it (an account deleted or
//     re-registered, a device unused too long: Signal's own rules, not in
//     the source). The cases cannot be told apart. The connector then
//     clears the device's keys and password (bridgeStateLoop
//     client.go:192-199, ClearKeysAndDisconnect receiving.go:321-331), so
//     at the next start the same code comes from Connect (client.go:220-226,
//     a device that is no longer logged in; also from a clean shutdown of
//     a logged-out device, :209-215).
//   - signal-not-logged-in: Connect of a login with no device in the store
//     (client.go:221-222), the sequel of a device deleted
//     (LogoutRemote).
//
// Linking again is a new device (a new QR code for the phone to scan), and
// the same account's new device replaces the stored one (PutDevice's
// ON CONFLICT, store/container.go).
//
// Left as the backend maps them: signal-disconnected, signal-connection-
// error, signal-server-error, signal-rate-limited, signal-connect-failed
// and signal-idle-primary-device are TRANSIENT_DISCONNECT, so RECONNECTING
// (the last: Signal warns that the phone has not been seen for long and
// will unlink the device unless it is opened; the device still works);
// signal-connection-failed is UNKNOWN_ERROR, so ERROR.
var endedCodes = []status.BridgeStateErrorCode{
	codeLoggedOut,
	codeNotLoggedIn,
}
