// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"regexp"
	"testing"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
)

// hostFixedCode is the form of a code the host keeps as it is
// (connected_workers/session.rs fixed_code; anything else becomes
// "invalid").
var hostFixedCode = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// TestWhatsAppStates: each state the connector reports for a companion
// WhatsApp unlinked is ENDED with the connector's own code (the device
// removed on the phone or expired, the phone logged out, banned, and a
// login whose device is gone); a temporary ban stays NEEDS_USER, the phone
// offline warning RECONNECTING, and stream and connect failures ERROR. Each
// code is one the host keeps. The states are those the connector sends
// (handlewhatsapp.go:214-272, 609-629, client.go:249-256, phoneping.go:
// 32-38). WHY: catches an unlinked companion reported NEEDS_USER (the
// contract's ENDED is for a session the network ended), a temporary ban
// reported ENDED, a failure that is not final reported as one (the host
// would stop restarting), and a code the host would turn into "invalid".
func TestWhatsAppStates(t *testing.T) {
	h := hooks()
	const (
		ended     = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
		needsUser = contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER
		reconnect = contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING
		failed    = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR
	)
	for _, c := range []struct {
		event status.BridgeStateEvent
		code  status.BridgeStateErrorCode
		want  contract.ConnectionStateKind
	}{
		{status.StateBadCredentials, connector.WALoggedOut, ended},
		{status.StateBadCredentials, connector.WAMainDeviceGone, ended},
		{status.StateBadCredentials, connector.WAUnknownLogout, ended},
		{status.StateBadCredentials, connector.WANotLoggedIn, ended},
		{status.StateBadCredentials, connector.WATemporaryBan, needsUser},
		{status.StateTransientDisconnect, connector.WAPhoneOffline, reconnect},
		{status.StateTransientDisconnect, connector.WADisconnected, reconnect},
		{status.StateTransientDisconnect, connector.WAKeepaliveTimeout, reconnect},
		{status.StateUnknownError, connector.WAStreamReplaced, failed},
		{status.StateUnknownError, connector.WAClientOutdated, failed},
		{status.StateUnknownError, connector.WAStreamError, failed},
		{status.StateUnknownError, connector.WAConnectionFailed, failed},
		{status.StateUnknownError, "wa-connect-failure-503", failed},
	} {
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.event, Error: c.code, RemoteID: testPhone}, identifierScheme)
		if cs.GetState() != c.want || cs.GetCode() != string(c.code) {
			t.Errorf("%s %s: %v %q, want %v %q", c.event, c.code, cs.GetState(), cs.GetCode(), c.want, c.code)
		}
		if !hostFixedCode.MatchString(cs.GetCode()) {
			t.Errorf("code %q is not one the host keeps", cs.GetCode())
		}
	}
}
