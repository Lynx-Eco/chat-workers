// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"slices"
	"testing"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

func TestSignInQRRotates(t *testing.T) {
	w := start(t, newState(t), opts{})
	// chat_fakenet sets no Hooks.SignInFlows: every flow of the connector
	// is offered.
	r := w.command(&contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	var ids []string
	for _, f := range r.GetFlows() {
		ids = append(ids, f.GetId())
	}
	if !slices.Equal(ids, []string{fakenet.FlowPhone, fakenet.FlowQR, fakenet.FlowCookies, fakenet.FlowFail}) {
		t.Fatalf("flows %v", ids)
	}
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	w.wait("done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	var shows []*contract.SignInShow
	for _, e := range w.events() {
		if s := step(e); s != nil && s.GetKind() == contract.SignInStepKind_SIGN_IN_STEP_KIND_SHOW_AND_WAIT {
			shows = append(shows, s.GetShow())
		}
	}
	if len(shows) != 3 {
		t.Fatalf("%d QR steps, want 3", len(shows))
	}
	for i, s := range shows {
		if s.GetKind() != contract.ShowKind_SHOW_KIND_QR || s.GetPayload() != fakenet.QRPayload(i+1) || s.GetExpiresInS() != 20 {
			t.Fatalf("QR %d: %v", i, s)
		}
	}
	done := w.wait("done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	if step(done).GetDone().GetPrincipal() != string(fakenet.Me) || step(done).GetDone().GetAccountName() != "Fake User" {
		t.Fatalf("done: %v", done)
	}
	w.waitSynced()
	w.shutdown()
	assertCleanLogs(t, w)
}

func TestSignInCookiesFailAndCancel(t *testing.T) {
	w := start(t, newState(t), opts{})
	expect(t, w.command(signInBegin(fakenet.FlowFail)), contract.Outcome_OUTCOME_OK, "")
	f := w.wait("code field", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	if step(f).GetFields()[0].GetKind() != contract.FieldKind_FIELD_KIND_CODE {
		t.Fatalf("field kind %v", step(f))
	}
	expect(t, w.command(signInSubmit("wrong-step", map[string]string{"code": "1"})), contract.Outcome_OUTCOME_REJECTED, "no_such_step")
	expect(t, w.command(signInSubmit(step(f).GetStepId(), map[string]string{"code": "1"})), contract.Outcome_OUTCOME_OK, "")
	failed := w.wait("failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED))
	if step(failed).GetFailed().GetCode() != "fi.fakenet.bad_code" {
		t.Fatalf("failure code %v", step(failed))
	}

	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
	f = w.wait("phone field", func(e *contract.ChatEvent) bool {
		return step(e).GetKind() == contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS && step(e).GetFlowId() == fakenet.FlowPhone
	})
	expect(t, w.command(signInSubmit(step(f).GetStepId(), map[string]string{"phone": "not a phone"})), contract.Outcome_OUTCOME_REJECTED, "invalid_field")
	expect(t, w.command(&contract.Command{Kind: &contract.Command_SignInCancel{SignInCancel: &contract.SignInCancel{}}}), contract.Outcome_OUTCOME_OK, "")
	w.wait("cancelled", func(e *contract.ChatEvent) bool { return step(e).GetFailed().GetCode() == "cancelled" })

	expect(t, w.command(signInBegin(fakenet.FlowCookies)), contract.Outcome_OUTCOME_OK, "")
	web := w.wait("web step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB))
	wb := step(web).GetWeb()
	if wb.GetUrl() != "https://fakenet.invalid/login" || len(wb.GetFields()) != 1 || wb.GetFields()[0].GetSources()[0].GetSource() != contract.WebCaptureSource_WEB_CAPTURE_SOURCE_COOKIE ||
		wb.GetWaitForUrlPattern() == "" {
		t.Fatalf("web: %v", wb)
	}
	expect(t, w.command(signInSubmit(step(web).GetStepId(), map[string]string{"session": "s3cret"})), contract.Outcome_OUTCOME_OK, "")
	w.wait("done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	w.waitSynced()
	w.shutdown()
}

// TestPrincipalMismatchAtSignIn: a sign-in that turns out to be another
// account is logged out again, and nothing of that account is reported.
func TestPrincipalMismatchAtSignIn(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{Account: "u-other"}})
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	failed := w.wait("failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED))
	if step(failed).GetFailed().GetCode() != "principal_mismatch" {
		t.Fatalf("failure %v", step(failed))
	}
	ctx, cancel := w.ctx(5e9)
	w.s.WaitQuiet(ctx, 800e6)
	cancel()
	for _, e := range w.events() {
		switch {
		case e.GetSignInStep() != nil:
		case e.GetConnectionState() != nil && e.GetConnectionState().GetPrincipal() == "":
		default:
			t.Fatalf("event of the other account reported: %v", e)
		}
	}
	recs, _ := fakenet.ReadNetworkLog(state)
	logged := false
	for _, r := range recs {
		logged = logged || (r.Kind == "logout" && r.Target == "u-other")
	}
	if !logged {
		t.Fatalf("the other account was not logged out at the network")
	}
	w.shutdown()
}

// TestPrincipalMismatchAtStart: a start whose store holds a login of another
// account than Init's principal does not run that login and does not keep
// the store: it is wiped (the login logged out at the network: it is not the
// connection's own), and the connection is reported ENDED
// other_account_wiped. Commands are answered as by a worker with no login.
// Before the fix the worker reported ERROR principal_mismatch and kept the
// store as it was, refusing every sign-in on it.
func TestPrincipalMismatchAtStart(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	w2 := start(t, state, opts{principal: "u-somebody-else"})
	ended := w2.wait("ended", isEnded).GetConnectionState()
	if ended.GetCode() != "other_account_wiped" || ended.GetPrincipal() != "" {
		t.Fatalf("ended: %v", ended)
	}
	ctx, cancel := w2.ctx(3e9)
	w2.s.WaitQuiet(ctx, 500e6)
	cancel()
	for _, e := range w2.events() {
		if cs := e.GetConnectionState(); cs != nil && cs.GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED {
			t.Fatalf("refused login was started")
		}
		if cs := e.GetConnectionState(); cs != nil && cs.GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR {
			t.Fatalf("an error was reported: %v", cs)
		}
	}
	expect(t, w2.command(sendCmd(fakenet.DMAlice, "k", "x", 0)), contract.Outcome_OUTCOME_REJECTED, "not_connected")
	w2.shutdown()
	assertWiped(t, state)
	if !networkLoggedOut(t, state, string(fakenet.Me)) {
		t.Fatal("the stored login of another account was not logged out at the network")
	}
}
