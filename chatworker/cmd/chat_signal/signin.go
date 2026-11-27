// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"time"

	"go.mau.fi/mautrix-signal/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
)

// qrRefreshInterval is how long mautrix-signal v0.2611.0 shows each QR code
// of a sign-in: its Wait replaces the code with a new one (a new
// provisioning socket, qr.Start) when the phone has not scanned it 45
// seconds after Wait began, up to maxQRRefreshes times, then fails the
// sign-in with FI.MAU.BRIDGE.LOGIN_TIMED_OUT (login.go:76-79
// qrRefreshInterval and maxQRRefreshes, Wait :182-218). Signal's server
// closes a provisioning socket after 60 seconds; the connector, as Signal
// Desktop does, opens a new one before that. It is the connector's
// unexported constant, copied: a change of it upstream is a change here.
const qrRefreshInterval = 45 * time.Second

// maxQRRefreshes is the connector's limit of new codes in one sign-in
// (login.go:78): the 20th refresh fails the sign-in instead, about 15
// minutes after it began.
const maxQRRefreshes = 20

// qrExpiresIn is the worker's Hooks.ShowExpiresIn: the seconds until the
// worker replaces a shown step (channel.proto SignInShow.expires_in_s; 0:
// it does not).
//
//   - A QR code of the connector's (step fi.mau.signal.login.qr, a
//     sgnl://linkdevice address): qrRefreshInterval. Every code lasts the
//     same: the backend calls Wait as soon as it has reported the step
//     (backend signin.go advance), and Wait's timer starts then, so the code
//     is replaced 45 seconds after it was shown, plus the moment Signal
//     takes to hand out the next one. No clock is kept: unlike WhatsApp's,
//     the connector's interval does not depend on the code's place in the
//     sign-in. The last code of a sign-in ends in a SignInFailed
//     (fi.mau.bridge.login_timed_out) at the same time instead of a new
//     code.
//   - Anything else (a step of another kind, a code that is not a
//     sgnl://linkdevice address): 0.
func qrExpiresIn(step *bridgev2.LoginStep) uint32 {
	if step == nil || step.Type != bridgev2.LoginStepTypeDisplayAndWait || step.StepID != connector.LoginStepQR {
		return 0
	}
	p := step.DisplayAndWaitParams
	if p == nil || p.Type != bridgev2.LoginDisplayTypeQR || !isLinkDeviceURL(p.Data) {
		return 0
	}
	return uint32(qrRefreshInterval / time.Second)
}

// isLinkDeviceURL tells whether s is a device link address of signalmeow's
// form: sgnl://linkdevice?uuid=...&pub_key=...&capabilities=...
// (provisioning.go startProvisioning). The address carries the provisioning
// socket's one-time id and public key: it is shown as the QR code and never
// logged.
func isLinkDeviceURL(s string) bool {
	rest, ok := strings.CutPrefix(s, "sgnl://linkdevice?")
	return ok && strings.Contains(rest, "uuid=") && strings.Contains(rest, "pub_key=")
}
