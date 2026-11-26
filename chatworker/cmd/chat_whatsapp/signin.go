// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"sync"
	"time"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
)

// qrIntervals is how long mautrix-whatsapp v0.2611.0 shows each QR code of
// a sign-in before its Wait returns the next one: the first a minute, the
// five after it 20 seconds each, then the sign-in fails with
// FI.MAU.WHATSAPP.LOGIN_TIMEOUT (pkg/connector/login.go:259 qrIntervals,
// getQRIndex :261-272, Wait :347-369). It is the connector's unexported
// variable, copied: a change of it upstream is a change here. whatsmeow's
// own QR channel uses the same numbers (qrchan.go:92-95).
var qrIntervals = []time.Duration{time.Minute, 20 * time.Second, 20 * time.Second, 20 * time.Second, 20 * time.Second, 20 * time.Second}

// qrSlack is how much later than the connector this worker may start its
// clock of a sign-in's codes. The connector's clock starts when the QR
// event comes (login.go:282 StartTime); this one when the first code is
// shown, which is the same moment but for the time it takes Start to
// return it. Without the slack, a code the connector shows at the end of
// an interval would be told the few milliseconds left of the one before.
const qrSlack = time.Second

// codeLifetimes is the worker's Hooks.ShowExpiresIn: the seconds until the
// worker replaces a shown step (channel.proto SignInShow.expires_in_s; 0:
// it does not).
//
//   - A QR code: what is left of its interval in qrIntervals, counted from
//     when the sign-in's first code was shown. The bridgev2 step carries no
//     index or time, so the clock is kept here, per sign-in: a sign-in is
//     told apart by the device keys in its codes (whatsmeow pair.go:125-130
//     makeQRData: ref,noise key,identity key,adv secret,platform; each
//     sign-in makes a new device with new keys, login.go:157 NewDevice), and
//     a new one restarts the clock. A code shown again within its interval
//     (the ADV secret rotated: same ref and keys, new secret, login.go:285-290
//     and :375-383) is told what is left of that interval.
//   - A pairing code: 0. The connector never replaces it (its Wait in the
//     phone flow has no timer, login.go:336-345), and its lifetime is not
//     known upstream: whatsmeow says "the exact expiry of pairing codes is
//     unknown", bounded by the login socket the server closes when the QR
//     codes it sent run out, about 160 seconds after connecting
//     (pair-code.go:82-84). The step then ends in a SignInFailed (the
//     connector's LOGIN_TIMEOUT on the disconnect, login.go:319-321), not in
//     a new code.
//   - Anything else (a step of another kind, a code whose payload is not of
//     whatsmeow's form): 0.
type codeLifetimes struct {
	now func() time.Time

	mu     sync.Mutex
	device string
	start  time.Time
}

func newCodeLifetimes(now func() time.Time) *codeLifetimes {
	return &codeLifetimes{now: now}
}

func (c *codeLifetimes) expiresIn(step *bridgev2.LoginStep) uint32 {
	if step == nil || step.Type != bridgev2.LoginStepTypeDisplayAndWait || step.StepID != connector.LoginStepIDQR {
		return 0
	}
	p := step.DisplayAndWaitParams
	if p == nil || p.Type != bridgev2.LoginDisplayTypeQR {
		return 0
	}
	device, ok := qrDevice(p.Data)
	if !ok {
		return 0
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if device != c.device {
		c.device, c.start = device, now
	}
	return qrLeft(now.Sub(c.start))
}

// qrDevice is the noise and identity keys of a QR code of whatsmeow's form
// (https://wa.me/settings/linked_devices#ref,noise,identity,adv,platform),
// which are the same in every code of one sign-in and new in the next one.
func qrDevice(qr string) (string, bool) {
	_, frag, ok := strings.Cut(qr, "#")
	if !ok {
		return "", false
	}
	parts := strings.Split(frag, ",")
	if len(parts) != 5 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", false
	}
	return parts[1] + "," + parts[2], true
}

// qrLeft is the whole seconds (at least 1) left of the interval a code shown
// elapsed after the sign-in's first is in, the connector's schedule
// (qrIntervals) with qrSlack; 0 past the last one.
func qrLeft(elapsed time.Duration) uint32 {
	var end time.Duration
	for _, interval := range qrIntervals {
		end += interval
		if elapsed+qrSlack < end {
			left := (end - elapsed + time.Second/2) / time.Second
			return uint32(max(left, 1))
		}
	}
	return 0
}
