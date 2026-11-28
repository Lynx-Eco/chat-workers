// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

// chatLevel tells whether an event is about chats or people: everything but
// connection states, sign-in steps and send results.
func chatLevel(e *contract.ChatEvent) bool {
	return e.GetMessage() != nil || e.GetEdit() != nil || e.GetReaction() != nil || e.GetMessageDeleted() != nil ||
		e.GetReadUpTo() != nil || e.GetDelivered() != nil || e.GetChatInfo() != nil || e.GetChatRemoved() != nil ||
		e.GetHistoryBatch() != nil || e.GetChatAbilities() != nil || e.GetPersonInfo() != nil
}

// assertSelfFirst: of the events with a seq above after, the first about
// chats or people is the account's own PersonInfo (is_self, person_id
// account), and no other account is announced as self.
func assertSelfFirst(t *testing.T, w *worker, after uint64, account string) *contract.PersonInfo {
	t.Helper()
	var self *contract.PersonInfo
	for _, q := range w.s.Events() {
		e := q.GetEvent()
		if q.GetSeq() <= after || !chatLevel(e) {
			continue
		}
		p := e.GetPersonInfo()
		if self == nil {
			if !p.GetIsSelf() || p.GetPersonId() != account {
				t.Fatalf("the first event about chats or people after seq %d is not the account's own PersonInfo: %v", after, e)
			}
			self = p
			continue
		}
		if p.GetIsSelf() && p.GetPersonId() != account {
			t.Fatalf("another account announced as self: %v", e)
		}
	}
	if self == nil {
		t.Fatalf("no event about chats or people after seq %d", after)
	}
	return self
}

// TestSelfPersonFirst: after a sign-in, the first event about chats or
// people is the account's own PersonInfo, with the account's name, and its
// person_id is the id the account has as a member of the group and as the
// sender of a message from it. After a restart on the stored login it comes
// again first, before the echo of a send.
func TestSelfPersonFirst(t *testing.T) {
	state := newState(t)
	// The network reports no profile of the account itself
	// (NoSelfInfo): its PersonInfo is the worker's own.
	script := &fakenet.Script{NoSelfInfo: true}
	w := start(t, state, opts{script: script})
	w.signIn()
	self := assertSelfFirst(t, w, 0, string(fakenet.Me))
	// The name and identifiers come from the login as stored, also when a
	// chat event of the login comes before the sign-in's end.
	if self.GetName() != "Fake User" || len(self.GetIdentifiers()) == 0 {
		t.Errorf("own PersonInfo: %v, want the account's name and identifiers", self)
	}
	evs := w.events()
	member := false
	for _, e := range evs {
		for _, m := range e.GetChatInfo().GetMembers() {
			member = member || (e.GetChatInfo().GetChatId() == fakenet.Group && m.GetPersonId() == self.GetPersonId() &&
				m.GetRole() == contract.MemberRole_MEMBER_ROLE_OWNER)
		}
	}
	if !member {
		t.Errorf("the group's info has no member %q (the account)", self.GetPersonId())
	}
	m6 := messages(evs, fakenet.DMAlice, "m6")
	if len(m6) != 1 || !m6[0].GetFromMe() || m6[0].GetSenderId() != self.GetPersonId() {
		t.Errorf("message from the account: %v, want sender %q", m6, self.GetPersonId())
	}
	w.shutdown()

	w = start(t, state, opts{script: script})
	w.wait("own person", func(e *contract.ChatEvent) bool { return e.GetPersonInfo().GetIsSelf() })
	expect(t, w.command(sendCmd(fakenet.DMAlice, "self-1", "after restart", 0)), contract.Outcome_OUTCOME_OK, "")
	echo := w.wait("echo", func(e *contract.ChatEvent) bool { return e.GetMessage().GetSendKey() == "self-1" }).GetMessage()
	self = assertSelfFirst(t, w, 0, string(fakenet.Me))
	if !echo.GetFromMe() || echo.GetSenderId() != self.GetPersonId() {
		t.Errorf("echo %v, want from %q", echo, self.GetPersonId())
	}
	w.shutdown()
}

// TestSelfNamedBeforeSignInEnd: when the login's chats come before its
// sign-in completes (CompleteDelayMs: the connector connects and syncs
// first, as mautrix-telegram does), the account's own PersonInfo that goes
// before them still has the account's name and identifiers, from the login
// as the framework stored it.
func TestSelfNamedBeforeSignInEnd(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{NoSelfInfo: true, CompleteDelayMs: 1500}})
	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
	f := w.wait("fields step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	expect(t, w.command(signInSubmit(step(f).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
	w.wait("a chat before the sign-in's end", func(e *contract.ChatEvent) bool { return e.GetChatInfo() != nil })
	for _, e := range w.events() {
		if e.GetSignInStep().GetKind() == contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE {
			t.Fatal("the sign-in completed before the first chat: the case is not exercised")
		}
	}
	self := assertSelfFirst(t, w, 0, string(fakenet.Me))
	if self.GetName() != "Fake User" || len(self.GetIdentifiers()) == 0 {
		t.Errorf("own PersonInfo: %v, want the account's name and identifiers", self)
	}
	w.wait("done step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	w.shutdown()
}

// TestSelfFirstWithoutAnnouncement: the sign-in never completes while the
// login's whole sync is reported (CompleteDelayMs far beyond the test: the
// connector connects and syncs the new login first, as mautrix-telegram
// does), so neither announcement of the account's own PersonInfo (a stored
// login at a start, a login after NewLogin returned) runs: the only gate is
// the one every chat event passes (emitChat, ensureSelf). The own
// PersonInfo is still stored once, before every event about the login's
// chats and people, of every kind the sync has.
func TestSelfFirstWithoutAnnouncement(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{NoSelfInfo: true, CompleteDelayMs: 600000}})
	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
	f := w.wait("fields step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	expect(t, w.command(signInSubmit(step(f).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
	w.wait("group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	w.quiet(300e6)
	kinds := map[string]bool{}
	selves := 0
	for _, e := range w.events() {
		if e.GetSignInStep().GetKind() == contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE {
			t.Fatal("the sign-in completed: the case is not exercised")
		}
		if e.GetPersonInfo().GetIsSelf() {
			selves++
		}
		switch {
		case e.GetChatInfo() != nil:
			kinds["chat"] = true
		case e.GetMessage() != nil:
			kinds["message"] = true
		case e.GetEdit() != nil:
			kinds["edit"] = true
		case e.GetReaction() != nil:
			kinds["reaction"] = true
		case e.GetReadUpTo() != nil:
			kinds["read"] = true
		case e.GetPersonInfo() != nil && !e.GetPersonInfo().GetIsSelf():
			kinds["person"] = true
		}
	}
	if len(kinds) != 6 {
		t.Fatalf("the sync reported only %v before the sign-in's end", kinds)
	}
	if selves != 1 {
		t.Fatalf("%d own PersonInfo events, want 1", selves)
	}
	assertSelfFirst(t, w, 0, string(fakenet.Me))
	w.proc.Kill()
}

// TestSelfPersonNotForRefused: a sign-in of another account than the
// connection's announces nothing of it; the connection's own sign-in that
// follows announces its account first.
func TestSelfPersonNotForRefused(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{principal: otherAccount, script: &fakenet.Script{QRAccount: otherAccount, NoSelfInfo: true}})
	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
	f := w.wait("fields step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	expect(t, w.command(signInSubmit(step(f).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
	failed := w.wait("failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED))
	if step(failed).GetFailed().GetCode() != "principal_mismatch" {
		t.Fatalf("failure %v", step(failed))
	}
	w.quiet(800e6)
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	for _, e := range w.events() {
		if e.GetPersonInfo() != nil {
			t.Fatalf("the refused sign-in reported a person: %v", e)
		}
	}
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	w.waitAfter(last, "done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	w.waitAfter(last, "group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	assertSelfFirst(t, w, last, otherAccount)
	w.shutdown()
}

// TestSelfPersonBypassingTheGate: a connector that loads its new login
// itself passes no gate; the account's own PersonInfo still comes before the
// first event about its chats (emitChat), and before people met in them.
func TestSelfPersonBypassingTheGate(t *testing.T) {
	w := start(t, newState(t), opts{script: &fakenet.Script{BypassLoadGate: true, NoSelfInfo: true}})
	w.signIn()
	assertSelfFirst(t, w, 0, string(fakenet.Me))
	w.shutdown()
}
