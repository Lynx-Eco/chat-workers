// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"slices"
	"testing"

	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

// TestSignInFlowsAllowList: with Hooks.SignInFlows set, ListSignInFlows
// lists only those of the connector's flows (here the fake network's phone
// and qr, of its four), and SignInBegin of any other flow, one the
// connector has included, is refused as unknown_flow before anything
// starts. Without it every flow is offered.
func TestSignInFlowsAllowList(t *testing.T) {
	b, _ := testBackend(t)
	conn, err := fakenet.New(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	b.brp.Store(&bridgev2.Bridge{Network: conn})
	list := func() []string {
		r := b.Command(t.Context(), &contract.Command{CommandId: "l", Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
		var ids []string
		for _, f := range r.GetFlows() {
			ids = append(ids, f.GetId())
		}
		return ids
	}
	if ids := list(); !slices.Equal(ids, []string{fakenet.FlowPhone, fakenet.FlowQR, fakenet.FlowCookies, fakenet.FlowFail}) {
		t.Fatalf("flows without an allow-list: %v", ids)
	}
	b.hooks.SignInFlows = []string{fakenet.FlowQR, fakenet.FlowPhone}
	if ids := list(); !slices.Equal(ids, []string{fakenet.FlowPhone, fakenet.FlowQR}) {
		t.Fatalf("flows with the allow-list: %v", ids)
	}
	for _, flow := range []string{fakenet.FlowCookies, fakenet.FlowFail, "nonexistent"} {
		r := b.Command(t.Context(), &contract.Command{CommandId: "b", Kind: &contract.Command_SignInBegin{SignInBegin: &contract.SignInBegin{FlowId: flow}}})
		if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "unknown_flow" {
			t.Fatalf("begin %s: %v", flow, r)
		}
	}
	if b.signIn != nil {
		t.Fatal("a refused flow started a sign-in")
	}
}
