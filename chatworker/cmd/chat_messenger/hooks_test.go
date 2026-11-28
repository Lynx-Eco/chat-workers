// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"reflect"
	"regexp"
	"slices"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/metaworker"
)

// hostFixedCode is the form of a code the host keeps as it is
// (connected_workers/session.rs fixed_code; anything else becomes
// "invalid").
var hostFixedCode = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// needsUserCodes are the connector's BAD_CREDENTIALS codes the backend
// leaves NEEDS_USER (state.go).
var needsUserCodes = []status.BridgeStateErrorCode{
	connector.FBCheckpointRequired,
	connector.FBChallengeRequired,
	connector.FBConsentRequired,
	connector.FBAccountSuspended,
}

// TestHooks: the hooks the worker gives the backend are the shared Meta
// ones with this connector's flows, codes and scheme; the flows offered are
// the connector's two cookie flows, in its order (facebook first, the
// default of a host that names none), and none of its password flows. WHY:
// catches a hook left unset or set to another function, a password flow
// offered (the user's password typed into the worker), a flow id that is
// not the connector's (every begin of it would be unknown_flow), the order
// that makes messenger.com the default, and the default identifier scheme
// moving with the connector.
func TestHooks(t *testing.T) {
	h := hooks()
	same := func(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }
	if !same(h.SelfPersonID, metaworker.SelfPersonID) || !same(h.SendKeyFormat, metaworker.SendKey) ||
		!same(h.EndRefused, metaworker.EndRefused) || !same(h.RateLimitWait, metaworker.RateLimitWait) {
		t.Error("a shared hook is not the shared one")
	}
	if h.DropRefused != nil || h.EndRefusedSession || h.IsChannel != nil || h.ShowExpiresIn != nil || h.PrepareLogins != nil {
		t.Error("a hook set that the worker does not use")
	}
	if h.IdentifierScheme != "facebook" {
		t.Errorf("identifier scheme %q", h.IdentifierScheme)
	}
	if !slices.Equal(h.SignInFlows, []string{"facebook", "messenger"}) {
		t.Errorf("flows %v", h.SignInFlows)
	}
	// In the connector's order, which the backend keeps (offeredFlows).
	var order []string
	for _, f := range (&connector.MetaConnector{}).GetLoginFlows() {
		if slices.Contains(h.SignInFlows, f.ID) {
			order = append(order, f.ID)
		}
	}
	if !slices.Equal(order, h.SignInFlows) {
		t.Errorf("the connector lists %v of the flows offered, want %v", order, h.SignInFlows)
	}
	for _, f := range []string{connector.FlowIDMessengerLiteIOS, connector.FlowIDMessengerLiteAndroid} {
		if slices.Contains(h.SignInFlows, f) {
			t.Errorf("password flow %s offered", f)
		}
	}
	if !slices.Equal(h.EndedCodes, endedCodes) {
		t.Errorf("ended codes %v", h.EndedCodes)
	}
}

// TestMessengerStates: each state the connector reports for a session Meta
// ended is ENDED with the connector's own code (cookies no longer valid,
// seen three ways; a login with no cookies; the realtime socket
// unauthorized); a checkpoint, a challenge, a consent and a suspension stay
// NEEDS_USER (the user clears them in Facebook's app or site); connection
// drops RECONNECTING; unknown failures ERROR. Each code is one the host
// keeps, and one the connector names (its human errors, handlemeta.go
// init): a code renamed upstream drops out of that map. WHY: catches an
// ended session that waits as NEEDS_USER as if it could come back, a
// checkpoint reported ENDED (the user would sign in again for nothing), a
// failure that is not final reported as one, and a code the host would turn
// into "invalid".
func TestMessengerStates(t *testing.T) {
	h := hooks()
	const (
		ended     = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
		needsUser = contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER
		reconnect = contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING
		failed    = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR
		connected = contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	)
	for _, c := range []struct {
		event status.BridgeStateEvent
		code  status.BridgeStateErrorCode
		want  contract.ConnectionStateKind
	}{
		{status.StateBadCredentials, "meta-cookie-removed", ended},
		{status.StateBadCredentials, "meta-redirected-to-login", ended},
		{status.StateBadCredentials, "meta-user-id-is-zero", ended},
		{status.StateBadCredentials, "meta-not-logged-in", ended},
		{status.StateBadCredentials, "dgw-connection-unauthorized", ended},
		{status.StateBadCredentials, "fb-checkpoint-required", needsUser},
		{status.StateBadCredentials, "fb-challenge-required", needsUser},
		{status.StateBadCredentials, "fb-consent-required", needsUser},
		{status.StateBadCredentials, "fb-account-suspended", needsUser},
		{status.StateBadCredentials, "meta-lserror-1357053", needsUser},
		{status.StateTransientDisconnect, connector.MetaTransientDisconnect, reconnect},
		{status.StateTransientDisconnect, connector.WADisconnected, reconnect},
		{status.StateTransientDisconnect, connector.MetaConnectError, reconnect},
		{status.StateTransientDisconnect, "meta-lserror-1357004", reconnect},
		{status.StateUnknownError, connector.MetaConnectError, failed},
		{status.StateUnknownError, connector.MetaPermanentError, failed},
		{status.StateUnknownError, connector.MetaGraphQLError, failed},
		{status.StateUnknownError, connector.MetaInboxLoadFailed, failed},
		{status.StateUnknownError, connector.MetaProxyUpdateFail, failed},
		{status.StateUnknownError, connector.WAConnectError, failed},
		{status.StateConnected, connector.WAPermanentError, connected},
	} {
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.event, Error: c.code, RemoteID: "100004567890123"}, h.IdentifierScheme)
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
		if slices.Contains(needsUserCodes, code) && slices.Contains(endedCodes, code) {
			t.Errorf("%s is both ended and needs-user", code)
		}
	}
}

// TestMessengerClientOutdated: the connector's state for WhatsApp's refusal
// of the encrypted chats' client (wa-unknown-permanent-error with
// whatsmeow's "client outdated", connector/handlewhatsapp.go:139-182), as
// reported alone and on top of a connected Meta state (client.go
// FillBridgeState), has the code connector_outdated and its kind kept; the
// same code with another permanent disconnect's words, or none, keeps the
// connector's code. WHY: catches the hook left unset, one that takes every
// permanent disconnect of the encrypted chats for an outdated client, and a
// partly working connection reported as failed.
func TestMessengerClientOutdated(t *testing.T) {
	h := hooks()
	if clientOutdated != "client outdated" {
		t.Fatalf("whatsmeow's words are %q", clientOutdated)
	}
	for _, c := range []struct {
		st       status.BridgeState
		want     contract.ConnectionStateKind
		wantCode string
	}{
		{status.BridgeState{StateEvent: status.StateUnknownError, Error: connector.WAPermanentError, Message: clientOutdated}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, "connector_outdated"},
		{status.BridgeState{StateEvent: status.StateConnected, Error: connector.WAPermanentError, Message: clientOutdated}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED, "connector_outdated"},
		{status.BridgeState{StateEvent: status.StateConnected, Error: connector.WAPermanentError, Message: "stream replaced"}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED, string(connector.WAPermanentError)},
		{status.BridgeState{StateEvent: status.StateConnected, Error: connector.WAPermanentError}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED, string(connector.WAPermanentError)},
		{status.BridgeState{StateEvent: status.StateUnknownError, Error: connector.MetaPermanentError, Message: clientOutdated}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, string(connector.MetaPermanentError)},
	} {
		c.st.RemoteID = "100004567890123"
		cs := h.ReportedState(&c.st, h.IdentifierScheme)
		if cs.GetState() != c.want || cs.GetCode() != c.wantCode {
			t.Errorf("%s %s %q: %v %q, want %v %q", c.st.StateEvent, c.st.Error, c.st.Message, cs.GetState(), cs.GetCode(), c.want, c.wantCode)
		}
	}
}
