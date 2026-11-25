// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/status"
)

// endedCodes are the worker's backend.Hooks.EndedCodes: the connector's
// bridge state codes that say WhatsApp ended the companion's session. The
// connector reports each as BAD_CREDENTIALS; the backend reports them ENDED
// ("the network ended the session (logged out, banned, device removed)",
// channel.proto ConnectionStateKind), with the connector's code, a fixed
// one the host takes as it is (lower case, digits and "-"). The host does
// not restart a worker after ENDED (nor after NEEDS_USER), and the
// connection waits for the user to link the device again.
//
// mautrix-whatsapp v0.2611.0 (connector/handlewhatsapp.go:43-58, 214-216,
// 609-629) and whatsmeow (types/events/events.go:193-231,
// connectionevents.go:40-47, 126-131):
//   - wa-logged-out: whatsmeow's LoggedOut with reason 401, on connect or as
//     the stream error 401 device_removed: the device was removed under
//     Linked devices on the phone, or WhatsApp dropped it (by WhatsApp's
//     own account, not in the source: a companion whose phone was not
//     online for about 14 days). The two cannot be told apart.
//   - wa-main-device-gone: reason 403 (WhatsApp Web calls it LOCKED): the
//     phone's own WhatsApp was logged out or moved away.
//   - wa-unknown-logout: reason 406 (WhatsApp Web calls it BANNED), and any
//     other logged-out reason.
//   - wa-not-logged-in: a connect of a login whose device is gone (the
//     sequel of one of the above, at the next start or reconnect:
//     connector/client.go:249-256).
//
// whatsmeow has deleted the device from the store by then
// (connectionevents.go:129, :44) and the connector cleared the login's
// device number (handlewhatsapp.go:616-620): linking again is a new pairing.
//
// Left as the backend maps them: wa-temporary-ban (reason 402, with an
// expiry; BAD_CREDENTIALS with the user action "restart",
// handlewhatsapp.go:266-272) stays NEEDS_USER: the session is not ended,
// the user waits for the ban to end and starts the connection again.
// wa-phone-offline (the phone unseen for 12 days, still connected,
// phoneping.go:32-38) is a TRANSIENT_DISCONNECT, so RECONNECTING; the
// stream and connect failures (wa-stream-replaced, wa-client-outdated,
// wa-stream-error, wa-connection-failed, wa-connect-failure-<n>) are
// UNKNOWN_ERROR, so ERROR.
var endedCodes = []status.BridgeStateErrorCode{
	connector.WALoggedOut,
	connector.WAMainDeviceGone,
	connector.WAUnknownLogout,
	connector.WANotLoggedIn,
}
