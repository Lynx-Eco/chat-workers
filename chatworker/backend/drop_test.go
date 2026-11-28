// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
)

// connectionStates are the ConnectionStates in the outbox, oldest first.
func connectionStates(t *testing.T, st *store.Store) []*contract.ConnectionState {
	t.Helper()
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []*contract.ConnectionState
	for _, p := range pending {
		var ev contract.ChatEvent
		if err := contract.Unmarshal(p.Event, &ev); err != nil {
			t.Fatal(err)
		}
		if cs := ev.GetConnectionState(); cs != nil {
			out = append(out, cs)
		}
	}
	return out
}

// waitLog waits until the log has want.
func waitLog(t *testing.T, out *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("log %q lacks %s", out.String(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// gatedWithAccount is a backend whose connection's account is u-mine, its
// gate, and its log.
func gatedWithAccount(t *testing.T) (*Backend, *store.Store, bridgev2.NetworkConnector, *recordingNet, *lockedBuffer) {
	t.Helper()
	b, st := testBackend(t)
	if _, err := st.DB().Exec(t.Context(), `CREATE TABLE user_login (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	b.init.Principal = "u-mine"
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	inner := &recordingNet{}
	return b, st, b.gate(inner, nil), inner, out
}

// TestGateDropRefusedHook: a new login of another account is refused
// (principal_mismatch) after the worker's DropRefused hook ran, before the
// gate returned, once, with the real connector, that login and a bounded
// context; the end it returned then runs in the background and its result
// is logged. EndRefused, also set, is not used. A login of the
// connection's account is loaded and the hook is not called. WHY: catches
// a drop run in the background (a stop could cut it, and the host would be
// told principal_mismatch, "nothing kept", while the other account's rows
// are still in the store) and the hook called for the connection's own
// login.
func TestGateDropRefusedHook(t *testing.T) {
	b, st, g, inner, out := gatedWithAccount(t)
	var drops int
	endRan := make(chan struct{})
	b.hooks.EndRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error {
		t.Error("EndRefused called beside DropRefused")
		return nil
	}
	other := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}}
	b.hooks.DropRefused = func(ctx context.Context, n bridgev2.NetworkConnector, l *bridgev2.UserLogin) (func(context.Context) error, error) {
		drops++
		if _, has := ctx.Deadline(); !has || n != inner || l != other {
			t.Errorf("drop called with %v %v (deadline %v)", n, l, has)
		}
		return func(ctx context.Context) error {
			defer close(endRan)
			if _, has := ctx.Deadline(); !has {
				t.Error("end without a deadline")
			}
			return nil
		}, nil
	}
	err := g.LoadUserLogin(t.Context(), other)
	if drops != 1 {
		t.Fatalf("drop ran %d times before the gate returned", drops)
	}
	if !errors.Is(err, ErrOtherAccount) || errCode(err, "fallback") != "principal_mismatch" {
		t.Fatalf("other account: %v", err)
	}
	select {
	case <-endRan:
	case <-time.After(5 * time.Second):
		t.Fatal("the end the drop returned did not run")
	}
	waitLog(t, out, `"refused_session_ended"`)
	if err := g.LoadUserLogin(t.Context(), &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-mine"}}); err != nil {
		t.Fatalf("own account: %v", err)
	}
	if drops != 1 || inner.loads.Load() != 1 {
		t.Fatalf("drops %d, loads %d", drops, inner.loads.Load())
	}
	if b.refused.Load() || len(connectionStates(t, st)) != 0 {
		t.Fatal("a successful drop refused the store or reported a state")
	}
}

// TestGateDropRefusedFails: when the drop could not delete what the
// connector stored of the other account, the login is refused as
// other_account_kept (never principal_mismatch), the connection is ENDED
// other_account_kept (the host deletes the store), and the worker begins no
// sign-in on that store. The end the drop returned runs all the same. WHY:
// catches a failed drop reported as principal_mismatch, which tells the
// host that nothing of the other account is kept.
func TestGateDropRefusedFails(t *testing.T) {
	b, st, g, _, out := gatedWithAccount(t)
	endRan := make(chan struct{})
	b.hooks.DropRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) (func(context.Context) error, error) {
		return func(context.Context) error {
			close(endRan)
			return errors.New("no network")
		}, errors.New("disk I/O error")
	}
	err := g.LoadUserLogin(t.Context(), &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}})
	if !errors.Is(err, ErrOtherAccountKept) || errors.Is(err, ErrOtherAccount) || errCode(err, "fallback") != "other_account_kept" {
		t.Fatalf("got %v (code %q)", err, errCode(err, "fallback"))
	}
	if !b.refused.Load() {
		t.Fatal("the worker would begin a sign-in on that store")
	}
	if r := b.signInBegin(t.Context(), "c1", &contract.SignInBegin{FlowId: "qr"}); r.GetCode() != "other_account_kept" {
		t.Fatalf("sign-in begin answered %v", r)
	}
	states := connectionStates(t, st)
	if len(states) != 1 || states[0].GetState() != contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED || states[0].GetCode() != "other_account_kept" {
		t.Fatalf("states %v", states)
	}
	select {
	case <-endRan:
	case <-time.After(5 * time.Second):
		t.Fatal("the end did not run")
	}
	waitLog(t, out, `"refused_login_not_dropped"`)
	waitLog(t, out, `"refused_session_not_ended"`)
}

// TestGateDropRefusedFailsWithNoEnd: a drop that failed and returned no end
// (the refused device's credentials could not even be read) is logged as
// refused_session_not_ended, never refused_session_nothing_to_end, beside
// refused_login_not_dropped. WHY: catches a log that says there was nothing
// to end when nobody could tell, hiding a device left linked.
func TestGateDropRefusedFailsWithNoEnd(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	b.hooks.DropRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) (func(context.Context) error, error) {
		return nil, errors.New("disk I/O error")
	}
	if err := b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}}); !errors.Is(err, ErrOtherAccountKept) {
		t.Fatalf("got %v", err)
	}
	logs := out.String()
	if !strings.Contains(logs, `"refused_session_not_ended"`) || strings.Contains(logs, "nothing_to_end") || !strings.Contains(logs, `"refused_login_not_dropped"`) {
		t.Fatalf("log %q", logs)
	}
}

// TestDropRefusedWhileStopping: a refused login the gate sees after the
// worker's stop began is still dropped (what was stored of the other
// account must not outlive the refusal), and its end at the network is not
// started (logged as refused_session_not_ended at once); a drop with no
// end is logged as refused_session_nothing_to_end. WHY: catches the drop
// tied to the background end, which a stop skips.
func TestDropRefusedWhileStopping(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	dropped := 0
	var endStarted bool
	b.hooks.DropRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) (func(context.Context) error, error) {
		dropped++
		return func(context.Context) error { endStarted = true; return nil }, nil
	}
	b.waitRefusedEnds(time.Now().Add(time.Second))
	if err := b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}}); err != nil {
		t.Fatal(err)
	}
	if dropped != 1 {
		t.Fatal("the drop did not run while the worker stops")
	}
	if n := strings.Count(out.String(), `"refused_session_not_ended"`); n != 1 {
		t.Fatalf("log %q: want refused_session_not_ended once", out.String())
	}
	b.waitRefusedEnds(time.Now().Add(time.Second))
	if endStarted {
		t.Fatal("an end was started after the stop began")
	}

	b2, _ := testBackend(t)
	out2 := &lockedBuffer{}
	b2.log = logx.New(out2, "c")
	b2.hooks.DropRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) (func(context.Context) error, error) {
		return nil, nil
	}
	if err := b2.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out2.String(), `"refused_session_nothing_to_end"`) || strings.Contains(out2.String(), `"refused_session_ended"`) {
		t.Fatalf("log %q", out2.String())
	}
}

// TestReportedStateEndedCodes: a BAD_CREDENTIALS state is NEEDS_USER
// unless its code is one of the worker's EndedCodes, then ENDED; the code
// is the connector's either way, and no other state is changed by a code.
// With no EndedCodes (chat_telegram, chat_fakenet) every BAD_CREDENTIALS is
// NEEDS_USER as before. WHY: catches the ended codes applied to another
// state (a stream error would stop the worker for good) or to every
// worker.
func TestReportedStateEndedCodes(t *testing.T) {
	const gone = status.BridgeStateErrorCode("x-device-removed")
	h := Hooks{EndedCodes: []status.BridgeStateErrorCode{gone}}
	for _, c := range []struct {
		hooks Hooks
		st    status.BridgeState
		want  contract.ConnectionStateKind
	}{
		{h, status.BridgeState{StateEvent: status.StateBadCredentials, Error: gone}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED},
		{h, status.BridgeState{StateEvent: status.StateBadCredentials, Error: "x-temporary-ban"}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER},
		{h, status.BridgeState{StateEvent: status.StateUnknownError, Error: gone}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR},
		{h, status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: gone}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING},
		{Hooks{}, status.BridgeState{StateEvent: status.StateBadCredentials, Error: gone}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER},
	} {
		cs := c.hooks.ReportedState(&c.st, "x")
		if cs.GetState() != c.want || cs.GetCode() != string(c.st.Error) {
			t.Errorf("%s %s with %v: %v %q, want %v", c.st.StateEvent, c.st.Error, c.hooks.EndedCodes, cs.GetState(), cs.GetCode(), c.want)
		}
	}
	if h.ReportedState(&status.BridgeState{StateEvent: status.StateRunning}, "x") != nil {
		t.Fatal("RUNNING reported")
	}
}

// TestReportedStateConnectorOutdated: a state the worker's
// ConnectorOutdated says is the network's refusal of an outdated client is
// reported with the code connector_outdated, its kind as the connector gave
// it; every other state keeps the connector's code, and with no
// ConnectorOutdated no state is changed. WHY: catches the refusal reported
// under each connector's own code (a host cannot tell it from other errors
// across networks), a kind changed with the code (a partly working
// connection reported as failed), and the code given to every state.
func TestReportedStateConnectorOutdated(t *testing.T) {
	const outdated = status.BridgeStateErrorCode("x-client-outdated")
	h := Hooks{
		EndedCodes:        []status.BridgeStateErrorCode{"x-device-removed"},
		ConnectorOutdated: func(st *status.BridgeState) bool { return st.Error == outdated },
	}
	if ConnectorOutdatedCode != "connector_outdated" {
		t.Fatalf("the code is %q", ConnectorOutdatedCode)
	}
	for _, c := range []struct {
		hooks     Hooks
		st        status.BridgeState
		wantState contract.ConnectionStateKind
		wantCode  string
	}{
		{h, status.BridgeState{StateEvent: status.StateUnknownError, Error: outdated}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, ConnectorOutdatedCode},
		{h, status.BridgeState{StateEvent: status.StateConnected, Error: outdated}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED, ConnectorOutdatedCode},
		{h, status.BridgeState{StateEvent: status.StateUnknownError, Error: "x-stream-error"}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, "x-stream-error"},
		{h, status.BridgeState{StateEvent: status.StateBadCredentials, Error: "x-device-removed"}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, "x-device-removed"},
		{Hooks{}, status.BridgeState{StateEvent: status.StateUnknownError, Error: outdated}, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, string(outdated)},
	} {
		cs := c.hooks.ReportedState(&c.st, "x")
		if cs.GetState() != c.wantState || cs.GetCode() != c.wantCode {
			t.Errorf("%s %s: %v %q, want %v %q", c.st.StateEvent, c.st.Error, cs.GetState(), cs.GetCode(), c.wantState, c.wantCode)
		}
	}
	if h.ReportedState(&status.BridgeState{StateEvent: status.StateRunning, Error: outdated}, "x") != nil {
		t.Fatal("RUNNING reported")
	}
}

// TestPrepareLoginsEnds: at a start, the ends the worker's PrepareLoginsEnds
// returns (chat_signal: the unlinks of the devices its sweep deleted) run in
// the background with the real connector behind the gate, each logged by
// its outcome, also when the hook returned an error beside them (logged
// prepare_logins_failed); a nil end is skipped. WHY: catches devices the
// sweep deleted left linked at the network with no line in the log.
func TestPrepareLoginsEnds(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	inner := &recordingNet{}
	var gotNet bridgev2.NetworkConnector
	ran := make(chan string, 2)
	b.hooks.PrepareLoginsEnds = func(_ context.Context, n bridgev2.NetworkConnector, _ *bridgev2.Bridge, _ []*bridgev2.UserLogin) ([]func(context.Context) error, error) {
		gotNet = n
		return []func(context.Context) error{
			func(context.Context) error { ran <- "ended"; return nil },
			nil,
			func(context.Context) error { ran <- "failed"; return errors.New("no network") },
		}, errors.New("partly swept")
	}
	b.prepareLogins(t.Context(), &bridgev2.Bridge{Network: b.gate(inner, nil)}, nil)
	if gotNet != bridgev2.NetworkConnector(inner) {
		t.Fatalf("given %v, want the real connector", gotNet)
	}
	for range 2 {
		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Fatal("an end did not run")
		}
	}
	waitLog(t, out, `"prepare_logins_failed"`)
	waitLog(t, out, `"refused_session_ended"`)
	waitLog(t, out, `"refused_session_not_ended"`)
	b.waitRefusedEnds(time.Now().Add(5 * time.Second))
}
