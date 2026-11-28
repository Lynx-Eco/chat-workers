// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"go.mau.fi/mautrix-gvoice/pkg/libgv"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// The accounts of these tests: mine (the connection's) and the other one a
// re-link reached (refused), as the connector names their logins
// ("<owner MXID>|<number>", login.go:139).
const (
	mineNumber  = "+15550001111"
	otherNumber = "+15550002222"
)

func loginIDOf(number string) networkid.UserLoginID {
	return networkid.UserLoginID(string(backend.OwnerMXID) + "|" + number)
}

func loginOf(id networkid.UserLoginID, prefix string) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID:       id,
		Metadata: &connector.UserLoginMetadata{Prefix: prefix, Cookies: map[string]string{"SID": "s"}},
	}}
}

// TestSelfPersonID: the account's own person id is "<prefix>.<number>", the
// connector's user id of its own number (the form the connector's own check
// takes); a login with no metadata, no prefix, no number or one not of the
// form gives "" (the framework's default). WHY: catches the default left in
// place (the login id, with the backend's owner MXID in it, a form no other
// person id has) and a prefix and number swapped.
func TestSelfPersonID(t *testing.T) {
	for _, c := range []struct {
		id     networkid.UserLoginID
		prefix string
		want   networkid.UserID
	}{
		{loginIDOf(mineNumber), "1", "1.+15550001111"},
		{loginIDOf("15550001111"), "12", "12.15550001111"},
		{loginIDOf(mineNumber), "", ""},
		{loginIDOf(mineNumber), "x1", ""},
		{loginIDOf(""), "1", ""},
		{loginIDOf("+"), "1", ""},
		{loginIDOf("+1555 000"), "1", ""},
		{"+15550001111", "1", ""},
		{"", "1", ""},
	} {
		got := selfPersonID(loginOf(c.id, c.prefix))
		if got != c.want {
			t.Errorf("%q prefix %q: %q, want %q", c.id, c.prefix, got, c.want)
		}
		if got != "" && !(&connector.GVConnector{}).ValidateUserID(got) {
			t.Errorf("%q: the connector does not take %q as a user id", c.id, got)
		}
	}
	if selfPersonID(nil) != "" || selfPersonID(&bridgev2.UserLogin{}) != "" {
		t.Fatal("no login, an id")
	}
	noMeta := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: loginIDOf(mineNumber)}}
	if selfPersonID(noMeta) != "" {
		t.Fatal("no metadata, an id")
	}
}

// TestHooksReportedState: cookies Google no longer takes are ENDED with the
// connector's code; its transient codes are RECONNECTING; the realtime
// channel's connected state is CONNECTED; a BAD_CREDENTIALS code that is not
// the connector's stays NEEDS_USER. Every code is one the connector names
// (client.go init). WHY: catches a code renamed upstream (the host keeps
// codes as they are), expired cookies waiting as NEEDS_USER as if they could
// come back, and a reconnect reported as an end.
func TestHooksReportedState(t *testing.T) {
	h := hooks()
	for _, c := range []struct {
		ev       status.BridgeStateEvent
		code     status.BridgeStateErrorCode
		want     contract.ConnectionStateKind
		wantCode string
	}{
		{status.StateBadCredentials, connector.GVBadCredentials, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, "gv-bad-credentials"},
		{status.StateBadCredentials, connector.GVNotLoggedIn, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, "gv-not-logged-in"},
		{status.StateTransientDisconnect, connector.GVConnectError, contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING, "gv-connect-error"},
		{status.StateTransientDisconnect, connector.GVRateLimited, contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING, "gv-rate-limited"},
		{status.StateTransientDisconnect, connector.GVDisconnected, contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING, "gv-transient-disconnect"},
		{status.StateTransientDisconnect, connector.GVTooManyRetries, contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING, "gv-too-many-retries"},
		{status.StateUnknownError, connector.GVRealtimeError, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, "gv-realtime-error"},
		{status.StateConnected, "", contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED, "connected"},
		{status.StateConnecting, "", contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTING, "connecting"},
		{status.StateBadCredentials, "gv-something-else", contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER, "gv-something-else"},
	} {
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.ev, Error: c.code}, h.IdentifierScheme)
		if cs.GetState() != c.want || cs.GetCode() != c.wantCode {
			t.Errorf("%s %s: %v %q, want %v %q", c.ev, c.code, cs.GetState(), cs.GetCode(), c.want, c.wantCode)
		}
	}
	for _, code := range []status.BridgeStateErrorCode{
		connector.GVNotLoggedIn, connector.GVBadCredentials, connector.GVConnectError, connector.GVDisconnected,
		connector.GVRealtimeError, connector.GVTooManyRetries, connector.GVRateLimited,
	} {
		if _, ok := status.BridgeStateHumanErrors[code]; !ok {
			t.Errorf("%s is not a code of the connector", code)
		}
	}
	var ended []string
	for _, c := range endedCodes {
		ended = append(ended, string(c))
	}
	if !slices.Equal(ended, []string{"gv-bad-credentials", "gv-not-logged-in"}) {
		t.Errorf("ended codes %v", ended)
	}
}

// TestHooks: the hooks the worker gives the backend. WHY: catches a hook
// left unset or set to another function, a flow added or renamed, and a
// send key mapping (nothing of the key reaches Google Voice: a mapping
// would only make the stored transaction id differ from the key).
func TestHooks(t *testing.T) {
	h := hooks()
	same := func(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }
	if h.SelfPersonID == nil || !same(h.SelfPersonID, selfPersonID) {
		t.Error("SelfPersonID")
	}
	if h.DropRefused == nil || !same(h.DropRefused, dropRefused) {
		t.Error("DropRefused")
	}
	if h.PrepareLogins == nil || !same(h.PrepareLogins, dropOrphanPrefixes) {
		t.Error("PrepareLogins")
	}
	if h.RateLimitWait == nil || !same(h.RateLimitWait, rateLimitWait) {
		t.Error("RateLimitWait")
	}
	if h.SendKeyFormat != nil || h.EndRefused != nil || h.EndRefusedSession || h.IsChannel != nil || h.ShowExpiresIn != nil ||
		h.WatchRateLimit != nil || h.OriginalMessageID != nil {
		t.Error("a hook set that the worker does not use")
	}
	if h.IdentifierScheme != "gvoice" {
		t.Errorf("identifier scheme %q", h.IdentifierScheme)
	}
	if !slices.Equal(h.SignInFlows, []string{"cookies"}) || connector.LoginFlowIDCookies != "cookies" {
		t.Errorf("flows %v", h.SignInFlows)
	}
	if !slices.Equal(h.EndedCodes, endedCodes) {
		t.Errorf("ended codes %v", h.EndedCodes)
	}
}

// TestRateLimitWait: Google's 429 is the rate limit, with its Retry-After in
// seconds, also wrapped; a 429 with no or a date Retry-After has no wait; a
// huge one is bounded; any other status or error is not the rate limit.
// WHY: catches a history fetch Google throttled answered as a plain failure
// (the host would retry at once), and an overflowing wait.
func TestRateLimitWait(t *testing.T) {
	resp := func(code int, retryAfter string) error {
		h := http.Header{}
		if retryAfter != "" {
			h.Set("Retry-After", retryAfter)
		}
		return &libgv.ResponseError{Resp: &http.Response{StatusCode: code, Header: h}}
	}
	for _, c := range []struct {
		err  error
		ok   bool
		wait time.Duration
	}{
		{resp(429, "30"), true, 30 * time.Second},
		{fmt.Errorf("failed to fetch messages: %w", resp(429, " 7 ")), true, 7 * time.Second},
		{resp(429, ""), true, 0},
		{resp(429, "Wed, 21 Oct 2026 07:28:00 GMT"), true, 0},
		{resp(429, "-5"), true, 0},
		{resp(429, "99999999999999999999"), true, maxRetryAfter},
		{resp(429, "999999"), true, maxRetryAfter},
		{&libgv.ResponseError{Resp: &http.Response{StatusCode: 429}}, true, 0},
		{resp(403, "30"), false, 0},
		{resp(503, "30"), false, 0},
		{&libgv.ResponseError{}, false, 0},
		{errors.New("429"), false, 0},
		{nil, false, 0},
	} {
		wait, ok := rateLimitWait(c.err)
		if ok != c.ok || wait != c.wait {
			t.Errorf("%v: %v %v, want %v %v", c.err, wait, ok, c.wait, c.ok)
		}
	}
}
