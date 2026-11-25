// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
	"lynx.invalid/ingest/chatworker/store"
)

const otherAccount = "u-other"

// storeSnapshot is every row of every table of the store, as text, sorted.
func storeSnapshot(t *testing.T, state string) map[string][]string {
	t.Helper()
	st, done := openStore(t, state)
	defer done()
	ctx := context.Background()
	out := map[string][]string{}
	for name := range storeRows(t, state) {
		rows, err := st.DB().Query(ctx, `SELECT * FROM "`+name+`"`)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		var list []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			line := ""
			for i, v := range vals {
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				line += fmt.Sprintf("%s=%v;", cols[i], v)
			}
			list = append(list, line)
		}
		_ = rows.Close()
		sort.Strings(list)
		out[name] = list
	}
	return out
}

func outboxKind(t *testing.T, st *store.Store, seq uint64) *contract.ChatEvent {
	t.Helper()
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pending {
		if p.Seq == seq {
			var ev contract.ChatEvent
			if err := contract.Unmarshal(p.Event, &ev); err != nil {
				t.Fatal(err)
			}
			return &ev
		}
	}
	return nil
}

// assertSameStore: the store after equals the store before but for the
// allow-list:
//   - lynx_outbox: rows acknowledged since are gone; new rows are sign-in
//     steps (the refused sign-in's steps and its failure);
//   - lynx_kv: the counters signin_event and state_event;
//   - sqlite_sequence: lynx_outbox's last seq.
func assertSameStore(t *testing.T, state string, before, after map[string][]string) {
	t.Helper()
	st, done := openStore(t, state)
	defer done()
	for name := range union(before, after) {
		b, a := set(before[name]), set(after[name])
		for row := range union(b, a) {
			if b[row] && a[row] {
				continue
			}
			switch name {
			case "lynx_outbox":
				if !a[row] {
					continue // acknowledged
				}
				var seq uint64
				if _, err := fmt.Sscanf(row, "seq=%d;", &seq); err != nil {
					t.Errorf("outbox row %q", row)
					continue
				}
				if ev := outboxKind(t, st, seq); ev == nil || ev.GetSignInStep() == nil {
					t.Errorf("outbox has a new event that is not a sign-in step: %v", ev)
				}
				continue
			case "lynx_kv":
				if hasPrefix(row, "name=signin_event;") || hasPrefix(row, "name=state_event;") {
					continue
				}
			case "sqlite_sequence":
				if hasPrefix(row, "name=lynx_outbox;") {
					continue
				}
			}
			if a[row] {
				t.Errorf("%s: row added: %s", name, row)
			} else {
				t.Errorf("%s: row removed: %s", name, row)
			}
		}
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func set(rows []string) map[string]bool {
	m := map[string]bool{}
	for _, r := range rows {
		m[r] = true
	}
	return m
}

func union[V any](a, b map[string]V) map[string]bool {
	m := map[string]bool{}
	for k := range a {
		m[k] = true
	}
	for k := range b {
		m[k] = true
	}
	return m
}

func (w *worker) quiet(d time.Duration) {
	ctx, cancel := w.ctx(10 * time.Second)
	defer cancel()
	w.s.WaitQuiet(ctx, d)
}

// refusedQR runs a QR sign-in that reaches the other account and checks it
// fails as principal_mismatch.
func (w *worker) refusedQR() {
	w.t.Helper()
	after := uint64(0)
	if evs := w.s.Events(); len(evs) > 0 {
		after = evs[len(evs)-1].GetSeq()
	}
	expect(w.t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	f := w.waitAfter(after, "failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED)).GetEvent()
	if step(f).GetFailed().GetCode() != "principal_mismatch" {
		w.t.Fatalf("failure %v", step(f))
	}
}

func networkLoggedOut(t *testing.T, state, account string) bool {
	recs, _ := fakenet.ReadNetworkLog(state)
	for _, r := range recs {
		if r.Kind == "logout" && r.Target == account {
			return true
		}
	}
	return false
}

// TestRefusedSignInWritesNothing: a sign-in that reaches another account
// than the connection's is refused when the framework loads the new login,
// before it is stored: the whole store is the same before and after (but the
// allow-list of assertSameStore), live and after a shutdown, the session at
// the network is ended, and the same worker then signs in as the right
// account. Both with the connection's own login stored and with only a
// principal in Init.
func TestRefusedSignInWritesNothing(t *testing.T) {
	for _, own := range []bool{true, false} {
		for _, stop := range []bool{false, true} {
			t.Run(fmt.Sprintf("own_login=%v/shutdown=%v", own, stop), func(t *testing.T) {
				state := newState(t)
				if own {
					w := start(t, state, opts{})
					w.signIn()
					w.shutdown()
				}
				w := start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{QRAccount: otherAccount}})
				if own {
					w.wait("connected", func(e *contract.ChatEvent) bool {
						return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
					})
				}
				w.quiet(time.Second)
				before := storeSnapshot(t, state)
				w.refusedQR()
				w.quiet(1500 * time.Millisecond)
				if stop {
					w.shutdown()
				}
				assertSameStore(t, state, before, storeSnapshot(t, state))
				if !networkLoggedOut(t, state, otherAccount) {
					t.Errorf("the other account's session was not ended at the network")
				}
				for _, e := range w.events() {
					if p := e.GetConnectionState().GetPrincipal(); p == otherAccount {
						t.Fatalf("a state of the other account was reported: %v", e)
					}
				}
				if stop {
					return
				}
				// The right account, in the same worker.
				last := w.s.Events()[len(w.s.Events())-1].GetSeq()
				expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
				f := w.waitAfter(last, "fields", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
				expect(t, w.command(signInSubmit(step(f.GetEvent()).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
				d := w.waitAfter(last, "done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE)).GetEvent()
				if step(d).GetDone().GetPrincipal() != string(fakenet.Me) {
					t.Fatalf("done: %v", step(d))
				}
				w.shutdown()
			})
		}
	}
}

// TestRefusedSignInBypassingTheGate: a connector that loads its new login
// itself is not stopped by the gate; when the sign-in turns out to be
// another account, everything goes as at logout (the outbox too), the
// sign-in fails as principal_mismatch and the connection ENDS
// (other_account_wiped). Nothing of the other account stays, and the same
// worker then signs in as the right account.
func TestRefusedSignInBypassingTheGate(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	w = start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{QRAccount: otherAccount, BypassLoadGate: true}})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.quiet(time.Second)
	w.refusedQR()
	ended := w.waitAfter(0, "ended", isEnded).GetEvent().GetConnectionState()
	if ended.GetCode() != "other_account_wiped" {
		t.Fatalf("ended: %v", ended)
	}
	w.quiet(1500 * time.Millisecond)
	rows := storeRows(t, state)
	for name, n := range rows {
		if n != 0 && !keptTables[name] {
			t.Errorf("table %s has %d rows after the refusal", name, n)
		}
	}
	st, done := openStore(t, state)
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pending {
		var ev contract.ChatEvent
		_ = contract.Unmarshal(p.Event, &ev)
		if ev.GetSignInStep() == nil && ev.GetConnectionState() == nil {
			t.Errorf("outbox keeps %q", ev.GetEventId())
		}
	}
	done()
	if !networkLoggedOut(t, state, otherAccount) {
		t.Errorf("the other account's session was not ended at the network")
	}
	// The wipe deletes the connection's own login too, but never ends its
	// session at the network: only the other account's is.
	if networkLoggedOut(t, state, string(fakenet.Me)) {
		t.Errorf("the connection's own session was ended at the network")
	}
	for _, e := range w.events() {
		if e.GetConnectionState().GetPrincipal() == otherAccount {
			t.Fatalf("a state of the other account was reported: %v", e)
		}
	}
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
	f := w.waitAfter(last, "fields", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	expect(t, w.command(signInSubmit(step(f.GetEvent()).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
	d := w.waitAfter(last, "done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE)).GetEvent()
	if step(d).GetDone().GetPrincipal() != string(fakenet.Me) {
		t.Fatalf("done: %v", step(d))
	}
	w.waitAfter(last, "group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	w.shutdown()
	st, done = openStore(t, state)
	defer done()
	var ids []string
	q, _ := st.DB().Query(context.Background(), `SELECT DISTINCT room_receiver FROM message UNION SELECT id FROM user_login UNION SELECT receiver FROM portal UNION SELECT receiver FROM lynx_rooms`)
	for q.Next() {
		var id string
		_ = q.Scan(&id)
		ids = append(ids, id)
	}
	_ = q.Close()
	for _, id := range ids {
		if id != string(fakenet.Me) {
			t.Fatalf("rows of %q after the second sign-in: %v", id, ids)
		}
	}
}

// TestRefusedSignInBypassingTheGateWipeFails: as
// TestRefusedSignInBypassingTheGate, but the wipe of everything fails (a
// trigger refuses the delete of the store's nonce, so the wipe's transaction
// is rolled back): rows of the other account may then still be in the store,
// so the worker must not say principal_mismatch (the host's sign that it kept
// nothing of that account): the sign-in fails as other_account_kept, the
// connection ENDS with the same code, and the log says wipe_failed.
func TestRefusedSignInBypassingTheGateWipeFails(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	st, done := openStore(t, state)
	if _, err := st.DB().Exec(context.Background(), `CREATE TRIGGER test_fail_wipe BEFORE DELETE ON lynx_store_nonce
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	done()
	w = start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{QRAccount: otherAccount, BypassLoadGate: true}})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.quiet(time.Second)
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	f := w.waitAfter(last, "failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED)).GetEvent()
	if code := step(f).GetFailed().GetCode(); code != "other_account_kept" {
		t.Fatalf("a refusal whose wipe failed: code %q, want other_account_kept", code)
	}
	ended := w.waitAfter(last, "ended", isEnded).GetEvent().GetConnectionState()
	if ended.GetCode() != "other_account_kept" {
		t.Fatalf("ended: %v", ended)
	}
	for _, e := range w.events() {
		if step(e).GetFailed().GetCode() == "principal_mismatch" || e.GetConnectionState().GetCode() == "other_account_wiped" {
			t.Fatalf("the worker said it kept nothing of the other account: %v", e)
		}
	}
	w.shutdown()
	logs := stderrOf(t, w)
	if !strings.Contains(logs, `"wipe_failed"`) {
		t.Fatalf("log lacks wipe_failed:\n%s", logs)
	}
	if strings.Contains(logs, otherAccount) {
		t.Fatalf("the other account was logged:\n%s", logs)
	}
}

// TestRefusedSignInBypassingTheGateNoPrincipal: as
// TestRefusedSignInBypassingTheGate with no principal in Init, the
// connection's account known from its stored login. Nothing of the other
// account reaches the host, neither while its login exists (its states, its
// sync) nor from the outbox after the wipe: after the sign-in began only
// sign-in steps and states that are not the other account's are delivered,
// and what the outbox keeps is the failure and the ENDED state. The other
// account's login syncs before its sign-in completes (CompleteDelayMs).
func TestRefusedSignInBypassingTheGateNoPrincipal(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	w = start(t, state, opts{noAutoAck: true, script: &fakenet.Script{QRAccount: otherAccount, BypassLoadGate: true, CompleteDelayMs: 1500}})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.quiet(time.Second)
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	w.refusedQR()
	ended := w.waitAfter(last, "ended", isEnded).GetEvent().GetConnectionState()
	if ended.GetCode() != "other_account_wiped" {
		t.Fatalf("ended: %v", ended)
	}
	w.quiet(1500 * time.Millisecond)
	for _, q := range w.s.Events() {
		e := q.GetEvent()
		if q.GetSeq() <= last {
			continue
		}
		if (e.GetSignInStep() == nil && e.GetConnectionState() == nil) || e.GetConnectionState().GetPrincipal() == otherAccount {
			t.Errorf("delivered after the other account's sign-in began: %v", e)
		}
	}
	w.shutdown()
	st, done := openStore(t, state)
	defer done()
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pending {
		var ev contract.ChatEvent
		_ = contract.Unmarshal(p.Event, &ev)
		failed := ev.GetSignInStep().GetKind() == contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED
		if !failed && !isEnded(&ev) {
			t.Errorf("outbox keeps %v", &ev)
		}
	}
}

// TestRefusedSignInFailingAfterTheLogin: a connector that loads its new
// logins itself (past the gate) stores the other account's login, and then
// its login process fails (here with a plain error: wait_failed), or the
// host cancels the sign-in while the process holds its end
// (CompleteDelayMs), or the process completes without naming its login
// (no_login). Each goes through the rule of a login of another account
// refused after it was stored: everything is wiped, the sign-in fails as
// principal_mismatch (not wait_failed, cancelled or no_login, which would
// tell the host nothing of the login left) and the connection ENDS
// other_account_wiped; nothing of the other account stays, and only its
// session is ended at the network. Before the fix the sign-in failed with
// its own code and the login stayed in the store (no_login: before the last
// fix of it; the connection's own session ended too: before the last fix of
// the wipe).
func TestRefusedSignInFailingAfterTheLogin(t *testing.T) {
	for _, how := range []string{"fail", "cancel", "no_login_id"} {
		t.Run(how, func(t *testing.T) {
			state := newState(t)
			w := start(t, state, opts{})
			w.signIn()
			w.shutdown()
			script := &fakenet.Script{QRAccount: otherAccount, BypassLoadGate: true}
			switch how {
			case "fail":
				script.FailAfterLogin = true
			case "cancel":
				script.CompleteDelayMs = 60_000
			case "no_login_id":
				script.CompleteNoLoginID = true
			}
			w = start(t, state, opts{principal: string(fakenet.Me), script: script})
			w.wait("connected", func(e *contract.ChatEvent) bool {
				return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
			})
			w.quiet(time.Second)
			last := w.s.Events()[len(w.s.Events())-1].GetSeq()
			expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
			if how == "cancel" {
				waitStoredLogin(t, state, otherAccount)
				expect(t, w.command(&contract.Command{Kind: &contract.Command_SignInCancel{SignInCancel: &contract.SignInCancel{}}}),
					contract.Outcome_OUTCOME_OK, "")
			}
			f := w.waitAfter(last, "failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED)).GetEvent()
			if code := step(f).GetFailed().GetCode(); code != "principal_mismatch" {
				t.Fatalf("a sign-in that stored the other account and failed: code %q, want principal_mismatch", code)
			}
			ended := w.waitAfter(last, "ended", isEnded).GetEvent().GetConnectionState()
			if ended.GetCode() != "other_account_wiped" {
				t.Fatalf("ended: %v", ended)
			}
			w.quiet(1500 * time.Millisecond)
			w.shutdown()
			assertNothingOfTheOther(t, w, state)
		})
	}
}

// assertNothingOfTheOther: after a wipe because of the other account, the
// store keeps no row (but the kept tables'), no state of the other account
// was reported, the log does not name it, its session was ended at the
// network and the connection's own was not.
func assertNothingOfTheOther(t *testing.T, w *worker, state string) {
	t.Helper()
	for name, n := range storeRows(t, state) {
		if n != 0 && !keptTables[name] {
			t.Errorf("table %s has %d rows after the refusal", name, n)
		}
	}
	for _, e := range w.events() {
		if e.GetConnectionState().GetPrincipal() == otherAccount {
			t.Fatalf("a state of the other account was reported: %v", e)
		}
	}
	if strings.Contains(stderrOf(t, w), otherAccount) {
		t.Fatal("the other account was logged")
	}
	if !networkLoggedOut(t, state, otherAccount) {
		t.Error("the other account's session was not ended at the network")
	}
	if networkLoggedOut(t, state, string(fakenet.Me)) {
		t.Error("the connection's own session was ended at the network")
	}
}

// TestLoginStoredAfterTheCancel: the host cancels a sign-in while its login
// process waits (a QR shown), and the process, past the gate, stores the
// other account's login only after the cancel was answered and failed as
// cancelled (StoreAfterCancelGate: the test lets it go on then). The wait
// that returns after its sign-in ended finds that login in the store and
// ends it by the same rule: everything wiped, a second failure
// principal_mismatch and the connection ENDED other_account_wiped. Without
// that check (the branch in advance's wait) the login stays in the store
// and nothing is reported.
func TestLoginStoredAfterTheCancel(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	gate := filepath.Join(t.TempDir(), "store-after-cancel")
	if err := syscall.Mkfifo(gate, 0o600); err != nil {
		t.Fatal(err)
	}
	w = start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{
		QRAccount: otherAccount, BypassLoadGate: true, QRRotateMs: 60_000, StoreAfterCancelGate: gate,
	}})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.quiet(time.Second)
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	w.waitAfter(last, "the QR", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_SHOW_AND_WAIT))
	expect(t, w.command(&contract.Command{Kind: &contract.Command_SignInCancel{SignInCancel: &contract.SignInCancel{}}}),
		contract.Outcome_OUTCOME_OK, "")
	cancelled := w.waitAfter(last, "cancelled", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED))
	if code := step(cancelled.GetEvent()).GetFailed().GetCode(); code != "cancelled" {
		t.Fatalf("the cancel's failure: %q", code)
	}
	// Nothing was stored when the cancel was answered.
	st, done := openStore(t, state)
	ids, err := st.StoredLoginIDs(context.Background())
	done()
	if err != nil || contains(ids, otherAccount) {
		t.Fatalf("logins at the cancel: %v %v", ids, err)
	}
	// Now the login process stores the login (it waits on the FIFO for a
	// writer: the open below returns once it reads).
	opened := make(chan error, 1)
	go func() { opened <- os.WriteFile(gate, []byte("go"), 0o600) }()
	select {
	case err := <-opened:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the login process did not wait for the gate")
	}
	f := w.waitAfter(cancelled.GetSeq(), "the second failure", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED)).GetEvent()
	if code := step(f).GetFailed().GetCode(); code != "principal_mismatch" {
		t.Fatalf("the failure after the login stored past the cancel: %q", code)
	}
	ended := w.waitAfter(cancelled.GetSeq(), "ended", isEnded).GetEvent().GetConnectionState()
	if ended.GetCode() != "other_account_wiped" {
		t.Fatalf("ended: %v", ended)
	}
	w.quiet(1500 * time.Millisecond)
	w.shutdown()
	assertNothingOfTheOther(t, w, state)
}

// TestSignInReadErrorWipesNothing: a sign-in of the connection's own account
// fails (a mistyped code: the fail flow), and the store cannot be read for
// its logins at that moment (the test hides the table). A read error is no
// evidence of another account: nothing is wiped and nobody is logged out at
// the network (the connection's own login would be the one), but the store
// cannot be taken for clean either: the sign-in fails as other_account_kept
// and the connection ENDS with the same code, so that the host deletes it;
// a new sign-in is refused meanwhile. Before the fix the read error counted
// as another account: everything was wiped and the connection's own session
// ended at the network.
func TestSignInReadErrorWipesNothing(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	w = start(t, state, opts{principal: string(fakenet.Me)})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.quiet(time.Second)
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	expect(t, w.command(signInBegin(fakenet.FlowFail)), contract.Outcome_OUTCOME_OK, "")
	fields := w.waitAfter(last, "the code field", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS)).GetEvent()
	hideLogins(t, state, true)
	expect(t, w.command(signInSubmit(step(fields).GetStepId(), map[string]string{"code": "000000"})), contract.Outcome_OUTCOME_OK, "")
	f := w.waitAfter(last, "failed", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED)).GetEvent()
	if code := step(f).GetFailed().GetCode(); code != "other_account_kept" {
		t.Fatalf("a failed sign-in whose logins could not be read: code %q, want other_account_kept", code)
	}
	ended := w.waitAfter(last, "ended", isEnded).GetEvent().GetConnectionState()
	if ended.GetCode() != "other_account_kept" {
		t.Fatalf("ended: %v", ended)
	}
	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_REJECTED, "other_account_kept")
	w.shutdown()
	hideLogins(t, state, false)
	recs, _ := fakenet.ReadNetworkLog(state)
	for _, r := range recs {
		if r.Kind == "logout" {
			t.Fatalf("a session was ended at the network: %v", r)
		}
	}
	st, done := openStore(t, state)
	ids, err := st.StoredLoginIDs(context.Background())
	done()
	if err != nil || !contains(ids, string(fakenet.Me)) {
		t.Fatalf("the connection's own login was not kept: %v %v", ids, err)
	}
	logs := stderrOf(t, w)
	if !strings.Contains(logs, `"login_read_failed"`) || strings.Contains(logs, `"principal_mismatch_after_login"`) {
		t.Fatalf("log:\n%s", logs)
	}
}

// hideLogins renames the framework's login table away (hide) or back, from
// a connection of the test's own: every read of the stored logins fails
// while it is away.
func hideLogins(t *testing.T, state string, hide bool) {
	t.Helper()
	st, done := openStore(t, state)
	defer done()
	q := `ALTER TABLE user_login RENAME TO test_hidden_user_login`
	if !hide {
		q = `ALTER TABLE test_hidden_user_login RENAME TO user_login`
	}
	if _, err := st.DB().Exec(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}

// seedOtherLogin leaves in the store, beside the connection's own login, a
// login of the other account: a re-link past the gate that stored it, cut
// off (the worker killed) before it ended.
func seedOtherLogin(t *testing.T, state string) {
	t.Helper()
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	w = start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{
		QRAccount: otherAccount, BypassLoadGate: true, CompleteDelayMs: 60_000,
	}})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	waitStoredLogin(t, state, otherAccount)
	w.proc.Kill()
	st, done := openStore(t, state)
	ids, err := st.StoredLoginIDs(context.Background())
	done()
	if err != nil || !contains(ids, otherAccount) || !contains(ids, string(fakenet.Me)) {
		t.Fatalf("seeded logins: %v %v", ids, err)
	}
}

// TestOtherAccountAtStart: a start whose store holds a login of another
// account (a re-link past the gate cut off before its wipe: cancelled,
// stopped or crashed) never keeps that store. The worker wipes it (only the
// other account's session ended at the network) and reports the connection
// ENDED other_account_wiped; a sign-in then works on the emptied store.
// When the wipe fails the report is other_account_kept (the host deletes the
// store) and no sign-in is begun on it. Before the fix the worker reported
// ERROR principal_mismatch, kept the store and refused every sign-in.
func TestOtherAccountAtStart(t *testing.T) {
	for _, failWipe := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail_wipe=%v", failWipe), func(t *testing.T) {
			state := newState(t)
			seedOtherLogin(t, state)
			if failWipe {
				st, done := openStore(t, state)
				if _, err := st.DB().Exec(context.Background(), `CREATE TRIGGER test_fail_wipe BEFORE DELETE ON lynx_store_nonce
					BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
					t.Fatal(err)
				}
				done()
			}
			w := start(t, state, opts{principal: string(fakenet.Me)})
			ended := w.wait("ended", isEnded).GetConnectionState()
			want := "other_account_wiped"
			if failWipe {
				want = "other_account_kept"
			}
			if ended.GetCode() != want || ended.GetPrincipal() != "" {
				t.Fatalf("ended: %v, want %s", ended, want)
			}
			w.quiet(time.Second)
			for _, e := range w.events() {
				if cs := e.GetConnectionState(); cs != nil && cs.GetState() != contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED {
					t.Fatalf("a state other than the ENDED was reported: %v", cs)
				}
			}
			logs := stderrOf(t, w)
			if !strings.Contains(logs, `"principal_mismatch_at_start"`) || strings.Contains(logs, otherAccount) {
				t.Fatalf("log:\n%s", logs)
			}
			if !networkLoggedOut(t, state, otherAccount) {
				t.Error("the other account's session was not ended at the network")
			}
			if networkLoggedOut(t, state, string(fakenet.Me)) {
				t.Error("the connection's own session was ended at the network")
			}
			if failWipe {
				if !strings.Contains(logs, `"wipe_failed"`) {
					t.Fatalf("log lacks wipe_failed:\n%s", logs)
				}
				expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_REJECTED, "other_account_kept")
				w.shutdown()
				return
			}
			w.shutdown()
			for name, n := range storeRows(t, state) {
				if n != 0 && !keptTables[name] {
					t.Errorf("table %s has %d rows after the start", name, n)
				}
			}
			w = start(t, state, opts{principal: string(fakenet.Me)})
			w.signIn()
			w.shutdown()
			st, done := openStore(t, state)
			ids, err := st.StoredLoginIDs(context.Background())
			done()
			if err != nil || len(ids) != 1 || ids[0] != string(fakenet.Me) {
				t.Fatalf("logins after the new sign-in: %v %v", ids, err)
			}
		})
	}
}

// TestShutdownWaitsForTheWipe: the host cancels a re-link past the gate that
// stored the other account's login, and stops the worker while the wipe
// that follows still runs (the other account's network logout takes 3 s:
// LogoutHangMs). The stop waits for the wipe within its grace: the failure
// and the ENDED are delivered before the ShutdownAck, and the store keeps
// nothing. Before the fix the stop went on at once and the wipe was cut off
// with the process: the other account's login stayed in the store.
func TestShutdownWaitsForTheWipe(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	w = start(t, state, opts{principal: string(fakenet.Me), script: &fakenet.Script{
		QRAccount: otherAccount, BypassLoadGate: true, CompleteDelayMs: 60_000, LogoutHangMs: 3000,
	}})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.quiet(time.Second)
	last := w.s.Events()[len(w.s.Events())-1].GetSeq()
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	waitStoredLogin(t, state, otherAccount)
	// The cancel is answered after the wipe; the stop comes before.
	go func() {
		ctx, cancel := w.ctx(30 * time.Second)
		defer cancel()
		_, _ = w.s.Command(ctx, &contract.Command{Kind: &contract.Command_SignInCancel{SignInCancel: &contract.SignInCancel{}}})
	}()
	waitLogged(t, w, `"principal_mismatch_after_login"`)
	ctx, cancel := w.ctx(20 * time.Second)
	defer cancel()
	if err := w.s.Shutdown(ctx, 10*time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if code, err := w.proc.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("worker exit %d %v", code, err)
	}
	var failure, ended string
	for _, q := range w.s.Events() {
		if q.GetSeq() <= last {
			continue
		}
		if f := step(q.GetEvent()).GetFailed(); f != nil {
			failure = f.GetCode()
		}
		if isEnded(q.GetEvent()) {
			ended = q.GetEvent().GetConnectionState().GetCode()
		}
	}
	if failure != "principal_mismatch" || ended != "other_account_wiped" {
		t.Fatalf("delivered before the stop: failure %q, ended %q", failure, ended)
	}
	if strings.Contains(stderrOf(t, w), `"shutdown_wipe_incomplete"`) {
		t.Fatal("the stop did not wait for the wipe")
	}
	assertNothingOfTheOther(t, w, state)
}

// waitLogged waits until the worker's log has line.
func waitLogged(t *testing.T, w *worker, line string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(stderrOf(t, w), line) {
		if time.Now().After(deadline) {
			t.Fatalf("the log has no %s", line)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitStoredLogin waits until the store holds a login of account.
func waitStoredLogin(t *testing.T, state, account string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		st, done := openStore(t, state)
		ids, err := st.StoredLoginIDs(context.Background())
		done()
		if err == nil && contains(ids, account) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no login of the account was stored (%v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
