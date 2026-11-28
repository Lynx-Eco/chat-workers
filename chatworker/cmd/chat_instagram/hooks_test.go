// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"reflect"
	"regexp"
	"slices"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/igconnector"
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
	igconnector.IGChallengeRequired,
	igconnector.FBCheckpointRequired,
	igconnector.IGConsentRequired,
	igconnector.IGAccountSuspended,
	igconnector.IGAccountRestricted,
}

// TestHooks: the hooks the worker gives the backend are the shared Meta
// ones with this connector's flow, codes and scheme; the one flow offered
// is the connector's cookie flow, none of its password flows. WHY: catches
// a hook left unset or set to another function, a password flow offered
// (the user's password typed into the worker), a flow id that is not the
// connector's (every begin would be unknown_flow), and an identifier scheme
// other than the one the connector's own identifiers carry.
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
	if h.IdentifierScheme != "instagram" {
		t.Errorf("identifier scheme %q", h.IdentifierScheme)
	}
	if !slices.Equal(h.SignInFlows, []string{"instagram"}) {
		t.Errorf("flows %v", h.SignInFlows)
	}
	var all []string
	for _, f := range (&igconnector.IGConnector{}).GetLoginFlows() {
		all = append(all, f.ID)
	}
	if !slices.Equal(all, []string{"android", "instagram", "instagram-password"}) {
		t.Fatalf("the connector's flows are now %v: decide whether a new one fits a user's own account", all)
	}
	for _, f := range []string{igconnector.FlowIDAndroidNative, igconnector.FlowIDWebNative} {
		if slices.Contains(h.SignInFlows, f) {
			t.Errorf("password flow %s offered", f)
		}
	}
	if !slices.Equal(h.EndedCodes, endedCodes) {
		t.Errorf("ended codes %v", h.EndedCodes)
	}
}

// TestInstagramStates: each state the connector reports for a session
// Instagram ended is ENDED with the connector's own code (cookies no
// longer valid, seen three ways; a login with no cookies; the realtime
// socket unauthorized or without a user id); a challenge, a checkpoint, a
// consent, a suspension and a restriction stay NEEDS_USER (the user clears
// them in Instagram's app or site); connection drops RECONNECTING; unknown
// failures ERROR. Each code is one the host keeps, and one the connector
// names (its human errors, handlemeta.go init): a code renamed upstream
// drops out of that map. WHY: catches an ended session that waits as
// NEEDS_USER as if it could come back, a challenge reported ENDED (the user
// would sign in again for nothing, and the connector's own recheck could
// not bring it back), a failure that is not final reported as one, and a
// code the host would turn into "invalid".
func TestInstagramStates(t *testing.T) {
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
		{status.StateBadCredentials, "meta-cookie-removed", ended},
		{status.StateBadCredentials, "meta-redirected-to-login", ended},
		{status.StateBadCredentials, "meta-user-id-is-zero", ended},
		{status.StateBadCredentials, "meta-not-logged-in", ended},
		{status.StateBadCredentials, "dgw-connection-unauthorized", ended},
		{status.StateBadCredentials, "ig-connect-missing-user-id", ended},
		{status.StateBadCredentials, "ig-challenge-required", needsUser},
		{status.StateBadCredentials, "fb-checkpoint-required", needsUser},
		{status.StateBadCredentials, "ig-consent-required", needsUser},
		{status.StateBadCredentials, "ig-account-suspended", needsUser},
		{status.StateBadCredentials, "ig-account-restricted", needsUser},
		{status.StateBadCredentials, "ig-web-push-stale", needsUser},
		{status.StateBadCredentials, "meta-lserror-1357053", needsUser},
		{status.StateTransientDisconnect, igconnector.DGWConnectionError, reconnect},
		{status.StateTransientDisconnect, "meta-lserror-1357004", reconnect},
		{status.StateUnknownError, igconnector.DGWMainStreamClosed, failed},
		{status.StateUnknownError, igconnector.MetaConnectError, failed},
		{status.StateUnknownError, igconnector.MetaGraphQLError, failed},
		{status.StateUnknownError, igconnector.MetaProxyUpdateFail, failed},
	} {
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.event, Error: c.code, RemoteID: "17841400000000001"}, h.IdentifierScheme)
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

// TestAccountIdentifiers: the account's own identifier is
// "instagram:<username>", the form the connector gives every person's
// username, from the remote profile the connector fills at each connect.
// WHY: catches a scheme that makes the account's identifier unlike every
// other person's.
func TestAccountIdentifiers(t *testing.T) {
	h := hooks()
	st := &status.BridgeState{StateEvent: status.StateConnected, RemoteID: "17841400000000001"}
	st.RemoteProfile.Username = "ada.lovelace"
	cs := h.ReportedState(st, h.IdentifierScheme)
	if !slices.Equal(cs.GetAccountIdentifiers(), []string{"instagram:ada.lovelace"}) {
		t.Fatalf("identifiers %v", cs.GetAccountIdentifiers())
	}
}
