// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"regexp"
	"slices"
	"testing"

	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
)

// hostFixedCode is the form of a code the host keeps as it is
// (connected_workers/session.rs fixed_code; anything else becomes
// "invalid").
var hostFixedCode = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// needsUserCodes are the connector's BAD_CREDENTIALS codes the worker
// leaves NEEDS_USER (state.go).
var needsUserCodes = []status.BridgeStateErrorCode{
	connector.GMLoggedOutInvalidCreds,
	connector.GMLoggedOut401,
	connector.GMLoggedOutWorkspaceSession,
	connector.GMLoggedOutNoEmailInConfig,
	connector.GMNotLoggedInCanReauth,
	connector.GMSwitchedToGoogleLogin,
	connector.GMBrowserInactive,
	connector.GMBrowserInactiveTimeout,
	connector.GMBrowserInactiveInactivity,
}

// TestGMessagesStates: each state the connector reports for a pairing that
// is gone (revoked on the phone, ended by Google, not found, none stored)
// is ENDED with the connector's own code; a Google web sign-in Google no
// longer takes, a phone that moved to Google account pairing and a session
// made inactive stay NEEDS_USER; a phone that does not answer, a broken
// long poll and failed pings are RECONNECTING; fatal and connect errors
// ERROR. Each code is one the host keeps, and each BAD_CREDENTIALS code is
// one the connector names (its human errors map, bridgestate.go init). WHY:
// catches an unpaired connection reported NEEDS_USER (the contract's ENDED
// is for a session the network ended), a cookie expiry reported ENDED (its
// pairing is kept), a phone briefly away parked (the host would stop
// restarting it), a code renamed upstream, and a code the host would turn
// into "invalid".
func TestGMessagesStates(t *testing.T) {
	h := hooks()
	const (
		ended      = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
		needsUser  = contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER
		reconnect  = contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING
		connecting = contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTING
		failed     = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR
	)
	type row struct {
		event status.BridgeStateEvent
		code  status.BridgeStateErrorCode
		want  contract.ConnectionStateKind
	}
	rows := []row{
		{status.StateBadCredentials, connector.GMUnpaired, ended},
		{status.StateBadCredentials, connector.GMUnpairedOtherPhone, ended},
		{status.StateBadCredentials, connector.GMUnpaired404, ended},
		{status.StateBadCredentials, connector.GMNotLoggedIn, ended},
		{status.StateBadCredentials, connector.GMNotLoggedInCanRepair, ended},
		{status.StateTransientDisconnect, connector.GMPhoneNotResponding, reconnect},
		{status.StateTransientDisconnect, connector.GMListenError, reconnect},
		{status.StateTransientDisconnect, connector.GMPingFailed, reconnect},
		{status.StateConnecting, connector.GMConnecting, connecting},
		{status.StateUnknownError, connector.GMFatalError, failed},
		{status.StateUnknownError, connector.GMConnectionFailed, failed},
	}
	for _, code := range needsUserCodes {
		rows = append(rows, row{status.StateBadCredentials, code, needsUser})
	}
	for _, c := range rows {
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.event, Error: c.code, RemoteID: "user@example.com/1"}, identifierScheme)
		if cs.GetState() != c.want || cs.GetCode() != string(c.code) {
			t.Errorf("%s %s: %v %q, want %v %q", c.event, c.code, cs.GetState(), cs.GetCode(), c.want, c.code)
		}
		if !hostFixedCode.MatchString(cs.GetCode()) {
			t.Errorf("code %q is not one the host keeps", cs.GetCode())
		}
	}
	for _, code := range append(slices.Clone(endedCodes), needsUserCodes...) {
		if _, ok := status.BridgeStateHumanErrors[code]; !ok {
			t.Errorf("%s is not a code of the connector", code)
		}
	}
	if !slices.Equal(h.EndedCodes, endedCodes) {
		t.Fatalf("hooks' ended codes %v", h.EndedCodes)
	}
}

// TestPhoneAwayIsTransient: the connector's own state for a connection
// whose phone does not answer is TRANSIENT_DISCONNECT
// gm-phone-not-responding (FillBridgeState turns the CONNECTED it sends on
// the phone's silence into that), which the backend reports RECONNECTING;
// a phone that moved to Google account pairing is BAD_CREDENTIALS
// gm-switched-to-google-login, NEEDS_USER. WHY: catches the phone's
// silence reported as a state that parks the connection, which a phone
// asleep for a minute would do.
func TestPhoneAwayIsTransient(t *testing.T) {
	h := hooks()
	away := &connector.GMClient{Main: &connector.GMConnector{}, Meta: &connector.UserLoginMetadata{}, PhoneResponding: false}
	st := away.FillBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	if st.StateEvent != status.StateTransientDisconnect || st.Error != connector.GMPhoneNotResponding {
		t.Fatalf("phone away: %s %s", st.StateEvent, st.Error)
	}
	if cs := h.ReportedState(&st, identifierScheme); cs.GetState() != contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING {
		t.Fatalf("phone away reported %v", cs.GetState())
	}
	switched := &connector.GMClient{Main: &connector.GMConnector{}, Meta: &connector.UserLoginMetadata{}, PhoneResponding: true, SwitchedToGoogleLogin: true}
	st = switched.FillBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	if st.StateEvent != status.StateBadCredentials || st.Error != connector.GMSwitchedToGoogleLogin {
		t.Fatalf("switched: %s %s", st.StateEvent, st.Error)
	}
	if cs := h.ReportedState(&st, identifierScheme); cs.GetState() != contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER {
		t.Fatalf("switched reported %v", cs.GetState())
	}
}
