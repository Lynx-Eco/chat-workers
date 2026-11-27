// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"regexp"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
)

// hostFixedCode is the form of a code the host keeps as it is
// (connected_workers/session.rs fixed_code; anything else becomes
// "invalid").
var hostFixedCode = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// TestSignalStates: each state the connector reports for a linked device
// Signal no longer takes is ENDED with the connector's own code (the
// device removed on the phone or dropped by Signal, and a login whose
// device is gone); the idle-phone warning and every disconnect are
// RECONNECTING, a connection that failed for good ERROR. Each code is one
// the host keeps, and each is one the connector registers a message for
// (errors.go:36-46), so a code renamed upstream fails here. The states
// are those the connector sends (client.go bridgeStateLoop and Connect,
// errors.go connectionErrorState, sendLoggedOutState, FillBridgeState).
// WHY: catches an unlinked device reported NEEDS_USER (the contract's ENDED
// is for a session the network ended), a failure that is not final
// reported as one (the host would stop restarting), and a code the host
// would turn into "invalid".
func TestSignalStates(t *testing.T) {
	h := hooks()
	const (
		ended     = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
		reconnect = contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING
		failed    = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR
	)
	for _, c := range []struct {
		event status.BridgeStateEvent
		code  status.BridgeStateErrorCode
		want  contract.ConnectionStateKind
	}{
		{status.StateBadCredentials, codeLoggedOut, ended},
		{status.StateBadCredentials, codeNotLoggedIn, ended},
		{status.StateTransientDisconnect, "signal-disconnected", reconnect},
		{status.StateTransientDisconnect, "signal-connection-error", reconnect},
		{status.StateTransientDisconnect, "signal-server-error", reconnect},
		{status.StateTransientDisconnect, "signal-rate-limited", reconnect},
		{status.StateTransientDisconnect, "signal-connect-failed", reconnect},
		{status.StateTransientDisconnect, "signal-idle-primary-device", reconnect},
		{status.StateUnknownError, "signal-connection-failed", failed},
	} {
		if _, known := status.BridgeStateHumanErrors[c.code]; !known {
			t.Errorf("%s is not a code the connector registers", c.code)
		}
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.event, Error: c.code, RemoteID: networkid.UserLoginID(mineACI.String())}, identifierScheme)
		if cs.GetState() != c.want || cs.GetCode() != string(c.code) {
			t.Errorf("%s %s: %v %q, want %v %q", c.event, c.code, cs.GetState(), cs.GetCode(), c.want, c.code)
		}
		if !hostFixedCode.MatchString(cs.GetCode()) {
			t.Errorf("code %q is not one the host keeps", cs.GetCode())
		}
	}
}
