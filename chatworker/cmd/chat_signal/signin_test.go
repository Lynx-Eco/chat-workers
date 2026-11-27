// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"go.mau.fi/mautrix-signal/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
)

// linkURL is an address of signalmeow's form (provisioning.go
// startProvisioning).
const linkURL = "sgnl://linkdevice?capabilities=backup5%2Cnopni2&pub_key=QUJD&uuid=abc-def"

// qrStep is the step the connector returns for a QR code (login.go:171-179).
func qrStep(data string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeDisplayAndWait,
		StepID: connector.LoginStepQR,
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeQR,
			Data: data,
		},
	}
}

// TestQRLifetime: every QR code of the connector's is told 45 seconds, the
// connector's refresh interval, the same for the first and any later one;
// anything else is told 0 (not replaced). WHY: catches the hook told the
// server's 60-second socket limit (the host would show a code 15 s after
// the connector replaced it), a lifetime given to a step that is not a
// QR code of the connector's, and a step id renamed upstream.
func TestQRLifetime(t *testing.T) {
	if qrRefreshInterval.Seconds() != 45 || maxQRRefreshes != 20 {
		t.Fatalf("schedule %v x %d", qrRefreshInterval, maxQRRefreshes)
	}
	for i := range 3 {
		if got := qrExpiresIn(qrStep(linkURL)); got != 45 {
			t.Fatalf("code %d: %d, want 45", i, got)
		}
	}
	code := qrStep(linkURL)
	code.DisplayAndWaitParams.Type = bridgev2.LoginDisplayTypeCode
	other := qrStep(linkURL)
	other.StepID = "fi.mau.signal.login.other"
	notWait := qrStep(linkURL)
	notWait.Type = bridgev2.LoginStepTypeUserInput
	noParams := qrStep(linkURL)
	noParams.DisplayAndWaitParams = nil
	for name, step := range map[string]*bridgev2.LoginStep{
		"nil":                nil,
		"a code, not a QR":   code,
		"another step id":    other,
		"not display & wait": notWait,
		"no params":          noParams,
		"not a link address": qrStep("https://signal.org/"),
		"no key":             qrStep("sgnl://linkdevice?uuid=abc"),
		"complete":           {Type: bridgev2.LoginStepTypeComplete, StepID: connector.LoginStepComplete},
	} {
		if got := qrExpiresIn(step); got != 0 {
			t.Errorf("%s: %d, want 0", name, got)
		}
	}
	if qrStep(linkURL).StepID != "fi.mau.signal.login.qr" {
		t.Fatal("the connector's QR step id changed")
	}
}
