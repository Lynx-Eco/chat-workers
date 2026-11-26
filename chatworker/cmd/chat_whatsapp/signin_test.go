// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
)

// fakeClock is a clock the test moves.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// qrCode is a code of whatsmeow's form (pair.go:129 makeQRData) for a
// device whose keys are named by device.
func qrCode(ref, device, adv string) string {
	return "https://wa.me/settings/linked_devices#" + ref + "," + device + "-noise," + device + "-identity," + adv + ",1"
}

// qrStep is the step the connector returns for a QR code (login.go:247
// makeQRStep).
func qrStep(code string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeDisplayAndWait,
		StepID: connector.LoginStepIDQR,
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeQR,
			Data: code,
		},
	}
}

// codeStep is the step the connector returns for a pairing code
// (login.go:236-244).
func codeStep(code string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeDisplayAndWait,
		StepID: connector.LoginStepIDCode,
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeCode,
			Data: code,
		},
	}
}

// TestQRLifetimesFollowTheConnector: each QR code of a sign-in is told the
// time the connector shows it: the first a minute, the five after it 20
// seconds, each shown a few milliseconds after the connector's own clock
// moved on (the connector starts its clock before it returns the first
// code). WHY: catches a fixed lifetime for every code (the first one would
// be told 20 s and go stale on the host 40 s early, or the others 60 s and
// be shown as live after the connector replaced them), and a schedule
// without the slack (a code shown just after a boundary would be told the
// milliseconds left of the interval before, 1 s).
func TestQRLifetimesFollowTheConnector(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1795000000, 0)}
	c := newCodeLifetimes(clock.now)
	const lag = 7 * time.Millisecond
	if got := c.expiresIn(qrStep(qrCode("ref0", "a", "adv"))); got != 60 {
		t.Fatalf("first code: %d, want 60", got)
	}
	elapsed := time.Duration(0)
	for i, interval := range qrIntervals[:len(qrIntervals)-1] {
		clock.advance(interval)
		elapsed += interval
		ref := "ref" + string(rune('1'+i))
		// The connector moved on at the boundary; this clock started lag
		// later, so it reads a little less.
		clock.advance(-lag)
		if got := c.expiresIn(qrStep(qrCode(ref, "a", "adv"))); got != 20 {
			t.Fatalf("code %d at %v: %d, want 20", i+1, elapsed, got)
		}
		clock.advance(lag)
	}
	// Past the last code the connector fails the sign-in: no code is left
	// to be told a lifetime.
	clock.advance(qrIntervals[len(qrIntervals)-1] + time.Second)
	if got := c.expiresIn(qrStep(qrCode("ref6", "a", "adv"))); got != 0 {
		t.Fatalf("after the last code: %d, want 0", got)
	}
}

// TestQRRotatedSecretKeepsItsInterval: when the server rotates the ADV
// secret, the connector shows the current code again with the new secret
// (login.go:285-290, :375-383), and it lasts only what is left of its
// interval. WHY: catches a clock restarted by any change of the payload
// (the rotated code would be told a full minute, 60, instead of 35).
func TestQRRotatedSecretKeepsItsInterval(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1795000000, 0)}
	c := newCodeLifetimes(clock.now)
	c.expiresIn(qrStep(qrCode("ref0", "a", "adv1")))
	clock.advance(25 * time.Second)
	if got := c.expiresIn(qrStep(qrCode("ref0", "a", "adv2"))); got != 35 {
		t.Fatalf("rotated first code: %d, want 35", got)
	}
	clock.advance(45 * time.Second) // 70 s: the second code, 10 s left
	if got := c.expiresIn(qrStep(qrCode("ref1", "a", "adv3"))); got != 10 {
		t.Fatalf("rotated second code: %d, want 10", got)
	}
	// Never 0 (no expiry) while a code is live: 1.1 s left is 1.
	clock.advance(8*time.Second + 900*time.Millisecond) // 78.9 s
	if got := c.expiresIn(qrStep(qrCode("ref1", "a", "adv4"))); got != 1 {
		t.Fatalf("code about to go: %d, want 1", got)
	}
	// Within the slack of the boundary a code is taken for the next one:
	// the connector replaces it within the second anyway.
	clock.advance(500 * time.Millisecond) // 79.4 s
	if got := c.expiresIn(qrStep(qrCode("ref1", "a", "adv5"))); got != 21 {
		t.Fatalf("code within the slack: %d, want 21", got)
	}
}

// TestQRNewSignInRestartsTheClock: a new sign-in makes a new device, whose
// codes carry new keys (login.go:157), and its first code lasts a minute
// again, also when the last sign-in ended before its codes ran out. WHY:
// catches one clock for the worker's life (the new sign-in's first code
// would be told what was left of the old one's schedule, here 20, or 0
// after it).
func TestQRNewSignInRestartsTheClock(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1795000000, 0)}
	c := newCodeLifetimes(clock.now)
	c.expiresIn(qrStep(qrCode("ref0", "a", "adv")))
	clock.advance(100 * time.Second)
	if got := c.expiresIn(qrStep(qrCode("ref0", "b", "adv"))); got != 60 {
		t.Fatalf("new sign-in's first code: %d, want 60", got)
	}
	clock.advance(10 * time.Minute)
	if got := c.expiresIn(qrStep(qrCode("ref0", "c", "adv"))); got != 60 {
		t.Fatalf("a later sign-in's first code: %d, want 60", got)
	}
}

// TestNoLifetimeButTheQRs: a pairing code is told 0 (the worker does not
// replace it; its lifetime is not known upstream, whatsmeow
// pair-code.go:82-84), and so is any step that is not the connector's QR
// step or whose payload is not of whatsmeow's form; none of them moves the
// QR clock. WHY: catches the QR schedule applied to a pairing code (it
// would be told 60 s, and the host would show a live code as stale), and a
// payload not parsed strictly (a lifetime made up for a code of another
// form).
func TestNoLifetimeButTheQRs(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1795000000, 0)}
	c := newCodeLifetimes(clock.now)
	c.expiresIn(qrStep(qrCode("ref0", "a", "adv")))
	clock.advance(30 * time.Second)
	qrOfCodeKind := qrStep(qrCode("ref0", "z", "adv"))
	qrOfCodeKind.DisplayAndWaitParams.Type = bridgev2.LoginDisplayTypeCode
	otherStepID := qrStep(qrCode("ref0", "z", "adv"))
	otherStepID.StepID = connector.LoginStepIDCode
	input := &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeUserInput, StepID: connector.LoginStepIDQR}
	for name, step := range map[string]*bridgev2.LoginStep{
		"pairing code":            codeStep("ABCD-EFGH"),
		"no step":                 nil,
		"no display params":       {Type: bridgev2.LoginStepTypeDisplayAndWait, StepID: connector.LoginStepIDQR},
		"QR step of another kind": qrOfCodeKind,
		"QR of another step":      otherStepID,
		"input step":              input,
		"not a URL with keys":     qrStep("2@abc,def"),
		"four parts":              qrStep("https://wa.me/settings/linked_devices#ref,z-noise,z-identity,adv"),
		"six parts":               qrStep(qrCode("ref0", "z", "adv") + ",x"),
		"no noise key":            qrStep("https://wa.me/settings/linked_devices#ref,,z-identity,adv,1"),
		"no ref":                  qrStep("https://wa.me/settings/linked_devices#,z-noise,z-identity,adv,1"),
	} {
		if got := c.expiresIn(step); got != 0 {
			t.Errorf("%s: %d, want 0", name, got)
		}
	}
	// The first sign-in's clock is where it was.
	if got := c.expiresIn(qrStep(qrCode("ref0", "a", "adv2"))); got != 30 {
		t.Fatalf("first code after other steps: %d, want 30", got)
	}
}

// TestQRDeviceIsTheKeys: the sign-in is told apart by the noise and
// identity keys, not by the ref (new in each code) or the ADV secret (new
// at a rotation). WHY: catches a device key that takes the whole payload or
// the ref, which would restart the clock at every code.
func TestQRDeviceIsTheKeys(t *testing.T) {
	a, ok := qrDevice(qrCode("ref0", "a", "adv1"))
	if !ok || !strings.Contains(a, "a-noise") || !strings.Contains(a, "a-identity") {
		t.Fatalf("device %q %v", a, ok)
	}
	for _, other := range []string{qrCode("ref1", "a", "adv1"), qrCode("ref0", "a", "adv2")} {
		if d, _ := qrDevice(other); d != a {
			t.Errorf("%q: device %q, want %q", other, d, a)
		}
	}
	if d, _ := qrDevice(qrCode("ref0", "b", "adv1")); d == a {
		t.Error("another device's keys give the same device")
	}
}

// TestHooksShowExpiresIn: the worker hands the backend a lifetime hook
// that follows the QR schedule. WHY: catches the hook left unset (the host
// told no lifetime) or set to something else.
func TestHooksShowExpiresIn(t *testing.T) {
	h := hooks().ShowExpiresIn
	if h == nil {
		t.Fatal("no ShowExpiresIn hook")
	}
	if got := h(qrStep(qrCode("ref0", "hooks-test", "adv"))); got != 60 {
		t.Errorf("first QR code: %d, want 60", got)
	}
	if got := h(codeStep("ABCD-EFGH")); got != 0 {
		t.Errorf("pairing code: %d, want 0", got)
	}
}
