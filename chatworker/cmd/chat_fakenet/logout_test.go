// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
	"lynx.invalid/ingest/chatworker/store"
)

// keptTables are the only tables of the store that may keep rows after a
// logout. Every other table, the framework's and ours, present and future,
// must be empty. What each may hold is checked row by row in assertWiped.
var keptTables = map[string]bool{
	// dbutil's schema versions (integers only, see assertVersionTable): the
	// framework's tables', and the connector's child database's. A wipe that
	// emptied them would make the next start create the tables again.
	"version":            true,
	fakenet.VersionTable: true,
	// The name of the program that owns the file.
	"database_owner": true,
	// The framework's own flags (which upgrades of its data it has made),
	// written again when the new framework instance starts. Only the keys
	// in frameworkFlags.
	"kv_store": true,
	// The worker's named counters (fixed names, integers): they keep event
	// ids and seqs from repeating after a wipe.
	"lynx_kv": true,
	// SQLite's own: the last seq of lynx_outbox, for the same reason.
	"sqlite_sequence": true,
	// The store's nonce (12 random bytes in hex), made again by the wipe:
	// the ids of the events the worker generates carry it.
	"lynx_store_nonce": true,
	// The upward queue: only the ENDED state of the logout itself, until the
	// host acknowledges it.
	"lynx_outbox": true,
}

var keptCounters = map[string]bool{"boot": true, "state_event": true, "signin_event": true}

var frameworkFlags = map[string]bool{"split_portals_enabled": true, "bridge_info_version": true, "unknown_rooms_cleaned_up": true}

// openStore opens a worker's store beside the worker (WAL lets it read
// while the worker runs). DevPlain opens the plain store of a worker run
// with dev_plain_store in the plainstore build; the encrypted build ignores
// it.
func openStore(t *testing.T, state string) (*store.Store, func()) {
	t.Helper()
	db, _, err := store.OpenDB(filepath.Join(state, "worker.db"), store.OpenOptions{Key: testKey(), DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return st, func() { _ = db.Close() }
}

// storeRows counts the rows of every table in the store.
func storeRows(t *testing.T, state string) map[string]int {
	t.Helper()
	st, done := openStore(t, state)
	defer done()
	ctx := context.Background()
	rows, err := st.DB().Query(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	_ = rows.Close()
	out := map[string]int{}
	for _, n := range names {
		var c int
		if err := st.DB().QueryRow(ctx, `SELECT COUNT(*) FROM "`+n+`"`).Scan(&c); err != nil {
			t.Fatal(err)
		}
		out[n] = c
	}
	return out
}

func inventory(rows map[string]int) string {
	var names []string
	for n := range rows {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		fmt.Fprintf(&b, "%s=%d ", n, rows[n])
	}
	return b.String()
}

// assertWiped: every table of the store is empty but keptTables, and those
// hold nothing of an account or of a person.
func assertWiped(t *testing.T, state string) {
	t.Helper()
	rows := storeRows(t, state)
	for name, n := range rows {
		if n != 0 && !keptTables[name] {
			t.Errorf("table %s has %d rows after logout", name, n)
		}
	}
	st, done := openStore(t, state)
	defer done()
	ctx := context.Background()
	q, err := st.DB().Query(ctx, `SELECT name FROM lynx_kv`)
	if err != nil {
		t.Fatal(err)
	}
	for q.Next() {
		var name string
		_ = q.Scan(&name)
		if !keptCounters[name] {
			t.Errorf("lynx_kv keeps %q after logout", name)
		}
	}
	_ = q.Close()
	q, err = st.DB().Query(ctx, `SELECT key FROM kv_store`)
	if err != nil {
		t.Fatal(err)
	}
	for q.Next() {
		var key string
		_ = q.Scan(&key)
		if !frameworkFlags[key] {
			t.Errorf("kv_store keeps %q after logout", key)
		}
	}
	_ = q.Close()
	var nonces int
	var nonce string
	if err := st.DB().QueryRow(ctx, `SELECT COUNT(*), MAX(nonce) FROM lynx_store_nonce`).Scan(&nonces, &nonce); err != nil ||
		nonces != 1 || !regexp.MustCompile(`^[0-9a-f]{24}$`).MatchString(nonce) {
		t.Errorf("lynx_store_nonce: %d rows, %d chars (%v)", nonces, len(nonce), err)
	}
	assertVersionTable(t, st, "version", -1)
	assertVersionTable(t, st, fakenet.VersionTable, fakenet.SchemaVersion)
	var owner string
	if err := st.DB().QueryRow(ctx, `SELECT group_concat(owner) FROM database_owner`).Scan(&owner); err != nil || owner != "lynx-chatworker" {
		t.Errorf("database_owner %q (%v)", owner, err)
	}
	q, err = st.DB().Query(ctx, `SELECT name FROM sqlite_sequence`)
	if err != nil {
		t.Fatal(err)
	}
	for q.Next() {
		var name string
		_ = q.Scan(&name)
		if name != "lynx_outbox" {
			t.Errorf("sqlite_sequence keeps %q after logout", name)
		}
	}
	_ = q.Close()
	pending, err := st.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pending {
		var ev contract.ChatEvent
		if err := contract.Unmarshal(p.Event, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.GetConnectionState().GetState() != contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED {
			t.Errorf("outbox keeps %q after logout", ev.GetEventId())
		}
	}
	// The media directory may exist again (the new framework instance makes
	// it), empty.
	for _, dir := range []string{"media", "transfer", "tmp"} {
		_ = filepath.WalkDir(filepath.Join(state, dir), func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				t.Errorf("%s survived logout", path)
			}
			return nil
		})
	}
}

// assertVersionTable: a dbutil version table has its two integer columns and
// one row of integers (version, when want >= 0), and nothing else.
func assertVersionTable(t *testing.T, st *store.Store, table string, want int) {
	t.Helper()
	ctx := context.Background()
	q, err := st.DB().Query(ctx, `SELECT name FROM pragma_table_info($1) ORDER BY name`, table)
	if err != nil {
		t.Fatal(err)
	}
	var cols []string
	for q.Next() {
		var c string
		_ = q.Scan(&c)
		cols = append(cols, c)
	}
	_ = q.Close()
	if strings.Join(cols, ",") != "compat,version" {
		t.Errorf("%s has columns %v", table, cols)
		return
	}
	var n, ints, version int
	err = st.DB().QueryRow(ctx, `SELECT COUNT(*), COALESCE(SUM(typeof(version) = 'integer' AND typeof(compat) IN ('integer', 'null')), 0),
		COALESCE(MAX(version), 0) FROM "`+table+`"`).Scan(&n, &ints, &version)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || ints != 1 || (want >= 0 && version != want) {
		t.Errorf("%s after logout: %d rows, %d of integers, version %d (want one row, version %d)", table, n, ints, version, want)
	}
}

// contactsData is what the fake network tells about its people and chats:
// none of it may be left in the store's files after a logout.
var contactsData = []string{
	"Alice Example", "Bob Example", "Carol Example", "u-alice", "u-bob", "u-carol", "+1555000000",
	"SECRETTEXT", "replying to bob", "dm-alice", "grp-1", "Renamed club", "Fake User",
	fakenet.SessionToken,
}

// assertFilesClean reads the store's files byte by byte for what the fake
// network told (deleted rows can survive in free pages and in the
// write-ahead log).
//
// The store is encrypted, so the scan of the raw files proves only that none
// of it is there in plaintext, wiped or not. What proves the wipe is the
// second scan, of the decrypted file (decryptedStore): every page of it,
// free pages included, read byte by byte the same way.
func assertFilesClean(t *testing.T, state string) {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(state, "worker.db*"))
	if len(files) == 0 {
		t.Fatalf("no store files")
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range contactsData {
			if bytes.Contains(raw, []byte(s)) {
				t.Errorf("%s still contains %q", filepath.Base(f), s)
			}
		}
	}
	if store.PlainBuild {
		// The development build's files are plaintext: the scan above
		// was the check.
		return
	}
	plain := decryptedStore(t, state)
	for _, s := range contactsData {
		if bytes.Contains(plain, []byte(s)) {
			t.Errorf("the decrypted store still contains %q", s)
		}
	}
}

// decryptedStore copies the store's file and write-ahead log (the worker
// has stopped), opens the copy with the key, checkpoints the log into it and
// decrypts every page of it in place (PRAGMA rekey to no key: sqlite3mc
// rewrites each page, free pages included), and returns the plaintext file.
// The checkpoint keeps the newest image of each page of the log: Wipe
// truncated the log, so every frame in it now was written after the wipe.
// It fails the test unless the result is a plain SQLite file holding the
// worker's schema, so that a scan of it is a scan of the content.
func decryptedStore(t *testing.T, state string) []byte {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "worker.db")
	for _, side := range []string{"", "-wal"} {
		b, err := os.ReadFile(filepath.Join(state, "worker.db") + side)
		if os.IsNotExist(err) && side != "" {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+side, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	db, _, err := store.OpenDB(path, store.OpenOptions{Key: testKey()}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn, err := db.RawDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var busy, frames, copied int
	if err := conn.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &copied); err != nil || busy != 0 {
		t.Fatalf("checkpoint of the copy: busy %d, %v", busy, err)
	}
	// sqlite3mc rekeys only outside WAL mode.
	for _, q := range []string{`PRAGMA journal_mode = DELETE`, `PRAGMA rekey = ''`} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_ = conn.Close()
	_ = db.Close()
	plain, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(plain, []byte("SQLite format 3\x00")) || !bytes.Contains(plain, []byte("CREATE TABLE lynx_outbox")) {
		t.Fatalf("the copy of the store did not decrypt (%d bytes)", len(plain))
	}
	t.Logf("decrypted store: %d bytes, %d pages from the log", len(plain), frames)
	return plain
}

func logoutCmd() *contract.Command {
	return &contract.Command{Kind: &contract.Command_Logout{Logout: &contract.Logout{}}}
}

func isEnded(e *contract.ChatEvent) bool {
	return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
}

// waitAfter is wait for an event with a seq above after.
func (w *worker) waitAfter(after uint64, what string, match func(*contract.ChatEvent) bool) *contract.QueuedEvent {
	w.t.Helper()
	ctx, cancel := w.ctx(20 * time.Second)
	defer cancel()
	q, err := w.s.WaitEvent(ctx, func(q *contract.QueuedEvent) bool { return q.GetSeq() > after && match(q.GetEvent()) })
	if err != nil {
		w.t.Fatalf("waiting for %s: %v", what, err)
	}
	return q
}

// logout runs Logout, checks its ENDED state and that nothing is reported
// after it, and returns that state's seq.
func (w *worker) logout(principal string) uint64 {
	w.t.Helper()
	expect(w.t, w.command(logoutCmd()), contract.Outcome_OUTCOME_OK, "")
	ended := w.waitAfter(0, "ended", isEnded)
	if p := ended.GetEvent().GetConnectionState().GetPrincipal(); p != principal || ended.GetEvent().GetConnectionState().GetCode() != "logged_out" {
		w.t.Fatalf("ended: %v", ended.GetEvent())
	}
	ctx, cancel := w.ctx(3 * time.Second)
	w.s.WaitQuiet(ctx, 500*time.Millisecond)
	cancel()
	for _, q := range w.s.Events() {
		if q.GetSeq() > ended.GetSeq() {
			w.t.Fatalf("event after ENDED: %v", q.GetEvent())
		}
	}
	return ended.GetSeq()
}

// TestLogoutWipes: after Logout nothing of the account or of the people it
// talked to is left in the store: every table, the framework's and ours, is
// checked, and the files are read byte by byte, encrypted as they are and
// decrypted (assertFilesClean).
func TestLogoutWipes(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	// A send (journal row), a reaction of ours that stays, a fetched avatar.
	expect(t, w.command(sendCmd(fakenet.DMAlice, "lw-send", "bye", 0)), contract.Outcome_OUTCOME_OK, "")
	expect(t, w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.Group, "m4", ""), SendKey: "lw-react", Emoji: "🎉"}}}),
		contract.Outcome_OUTCOME_OK, "")
	var avatar string
	for _, e := range w.events() {
		if p := e.GetPersonInfo(); p.GetPersonId() == string(fakenet.Alice) {
			avatar = p.GetAvatarRef()
		}
	}
	expect(t, w.command(&contract.Command{Kind: &contract.Command_FetchAttachment{FetchAttachment: &contract.FetchAttachment{Handle: avatar, TargetName: "avatar.png"}}}),
		contract.Outcome_OUTCOME_OK, "")

	before := storeRows(t, state)
	t.Logf("store before logout: %s", inventory(before))
	for _, table := range []string{"user", "user_login", "user_portal", "portal", "ghost", "message", "reaction", "kv_store",
		"lynx_send_journal", "lynx_rooms", "lynx_members", "lynx_state", "lynx_people", "lynx_spool", "lynx_kv",
		fakenet.SessionTable} {
		if before[table] == 0 {
			t.Fatalf("the session left no rows in %s: the test does not cover it", table)
		}
	}

	// The scan of the decrypted store (assertFilesClean) finds what is
	// there: before the logout it holds the people and the session. The
	// worker is stopped for that, so that the copy is of a store at rest,
	// and started again.
	w.shutdown()
	if !store.PlainBuild {
		plain := decryptedStore(t, state)
		for _, s := range []string{"Alice Example", "u-alice", fakenet.SessionToken} {
			if !bytes.Contains(plain, []byte(s)) {
				t.Fatalf("the decrypted store does not hold %q before the logout: a scan of it after proves nothing", s)
			}
		}
	}
	w = start(t, state, opts{})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})

	w.logout(string(fakenet.Me))
	assertWiped(t, state)
	w.shutdown()
	assertWiped(t, state)
	assertFilesClean(t, state)
}

// TestSignInAfterLogout: the same worker signs in again after a logout, and
// the chats and people of the new sign-in are reported in full. The
// connector's own table (a dbutil child database, as real connectors keep
// their sessions) is emptied by the logout, its version kept, and written
// again by the new sign-in.
func TestSignInAfterLogout(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	if n := storeRows(t, state)[fakenet.SessionTable]; n != 1 {
		t.Fatalf("%d sessions stored by the sign-in", n)
	}
	ended := w.logout(string(fakenet.Me))
	if rows := storeRows(t, state); rows[fakenet.SessionTable] != 0 || rows[fakenet.VersionTable] != 1 {
		t.Fatalf("store after logout: %s", inventory(rows))
	}

	expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
	f := w.waitAfter(ended, "fields step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
	expect(t, w.command(signInSubmit(step(f.GetEvent()).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
	w.waitAfter(ended, "done step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	w.waitAfter(ended, "group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	m1 := w.waitAfter(ended, "alice's message", func(e *contract.ChatEvent) bool { return e.GetMessage().GetMessageId() == "m1" }).GetEvent().GetMessage()
	if m1.GetSenderId() != string(fakenet.Alice) || m1.GetChatId() != fakenet.DMAlice {
		t.Fatalf("m1 after sign-in again: %v", m1)
	}
	p := w.waitAfter(ended, "alice", func(e *contract.ChatEvent) bool { return e.GetPersonInfo().GetPersonId() == string(fakenet.Alice) }).GetEvent().GetPersonInfo()
	if p.GetName() != "Alice Example" || p.GetAvatarRef() == "" {
		t.Fatalf("alice after sign-in again: %v", p)
	}
	expect(t, w.command(sendCmd(fakenet.DMAlice, "again-1", "hello again", 0)), contract.Outcome_OUTCOME_OK, "")
	w.shutdown()
	rows := storeRows(t, state)
	if rows["user_login"] != 1 || rows["message"] == 0 || rows["ghost"] == 0 || rows[fakenet.SessionTable] != 1 {
		t.Fatalf("store after sign-in again: %s", inventory(rows))
	}
}

// TestLogoutWhileHostNotReading: the host stops reading while the network
// sends (the worker's writer and pump are held, an event in hand), and a
// Logout runs meanwhile. When the host reads again, the logout's ENDED
// arrives, last: the reset of the queue by the wipe does not lose it.
func TestLogoutWhileHostNotReading(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	script := &fakenet.Script{}
	// About 440 kB of events: more than the socket and the endpoint's queue
	// hold, so the pump waits on the host with an event in hand.
	script.Ticker.Count, script.Ticker.IntervalMs, script.Ticker.DelayMs, script.Ticker.PadBytes = 400, 1, 300, 1000
	w2 := start(t, state, opts{script: script})
	w2.s.PauseReading(10 * time.Second)
	time.Sleep(3 * time.Second)
	// The reply comes once the host reads again; the logout itself runs
	// while it does not.
	w2.logout(string(fakenet.Me))
	w2.shutdown()
}

// TestSignInAfterAnotherAccountAtStart: a stored login of another account
// than the connection's is wiped at start (and logged out at the network),
// and the connection then signs in on the emptied store; the store keeps its
// login only. Before the fix the start refused to run with that login and
// every sign-in was rejected principal_mismatch until a Logout.
func TestSignInAfterAnotherAccountAtStart(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()

	other := "u-other"
	w2 := start(t, state, opts{principal: other, script: &fakenet.Script{Account: other, AccountName: "Other User"}})
	ended := w2.wait("ended", isEnded).GetConnectionState()
	if ended.GetCode() != "other_account_wiped" {
		t.Fatalf("ended: %v", ended)
	}
	recs, _ := fakenet.ReadNetworkLog(state)
	loggedOut := false
	for _, r := range recs {
		loggedOut = loggedOut || (r.Kind == "logout" && r.Target == string(fakenet.Me))
	}
	if !loggedOut {
		t.Fatalf("the stored login was not logged out at the network")
	}

	w2.signIn()
	done := w2.wait("done", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
	if step(done).GetDone().GetPrincipal() != other {
		t.Fatalf("done: %v", step(done))
	}
	w2.shutdown()
	st, closeDB := openStore(t, state)
	defer closeDB()
	var ids []string
	q, err := st.DB().Query(context.Background(), `SELECT id FROM user_login`)
	if err != nil {
		t.Fatal(err)
	}
	for q.Next() {
		var id string
		_ = q.Scan(&id)
		ids = append(ids, id)
	}
	_ = q.Close()
	if len(ids) != 1 || ids[0] != other {
		t.Fatalf("logins after sign-in: %v", ids)
	}
}

// TestLogoutWhenRestartFails: when no framework instance can be started on
// the emptied store, the logout itself is still done (the session ended, the
// store wiped, ENDED reported) and its reply says the worker cannot go on
// (OK, restart_failed); the worker then exits with ExitRestartFailed and a
// fixed log code, so that the engine starts it again from a clean process,
// which signs in.
func TestLogoutWhenRestartFails(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{FailRestart: true}})
	w.signIn()
	expect(t, w.command(logoutCmd()), contract.Outcome_OUTCOME_OK, "restart_failed")
	ended := w.waitAfter(0, "ended", isEnded).GetEvent().GetConnectionState()
	if ended.GetPrincipal() != string(fakenet.Me) || ended.GetCode() != "logged_out" {
		t.Fatalf("ended: %v", ended)
	}
	code, err := w.proc.Wait(15 * time.Second)
	if err != nil || code != backend.ExitRestartFailed {
		t.Fatalf("worker exit %d %v, want %d", code, err, backend.ExitRestartFailed)
	}
	if !strings.Contains(stderrOf(t, w), `"code":"exit_restart_failed"`) {
		t.Errorf("no exit_restart_failed line in the log")
	}
	assertWiped(t, state)
	assertFilesClean(t, state)

	w = start(t, state, opts{})
	w.signIn()
	w.shutdown()
}

// TestStoppedInstanceWritesNothing: account A signs in and receives, logs
// out (its framework instance stopped, the store wiped, a new instance
// started), then the stopped instance writes late through the backend's view
// and the intents it holds (fakenet's LateWrites: a room created with a name
// and a member, room state and a member in A's room, a person's name, an
// upload, a bridge state of A), and account B signs in. Nothing of the late
// writes is in the store or in the events after the logout's ENDED, nothing
// of A is reported after it, B's own PersonInfo is the first event about
// chats or people after it, and the refusal is logged once, by its code.
func TestStoppedInstanceWritesNothing(t *testing.T) {
	state := newState(t)
	const accountB = "u-b"
	w := start(t, state, opts{script: &fakenet.Script{LateWrites: true, QRAccount: accountB, NoSelfInfo: true}})
	w.signIn()
	ended := w.logout(string(fakenet.Me))
	// B's sign-in begins with the stopped instance's late writes.
	expect(t, w.command(signInBegin(fakenet.FlowQR)), contract.Outcome_OUTCOME_OK, "")
	done := w.waitAfter(ended, "done step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE)).GetEvent()
	if p := step(done).GetDone().GetPrincipal(); p != accountB {
		t.Fatalf("signed in as %q, want %q", p, accountB)
	}
	w.waitAfter(ended, "B's group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	w.quiet(time.Second)
	// B's own PersonInfo comes first after the logout, and A is not
	// announced again (assertSelfFirst).
	assertSelfFirst(t, w, ended, accountB)
	for _, q := range w.s.Events() {
		if q.GetSeq() <= ended {
			continue
		}
		e := q.GetEvent()
		if strings.Contains(fmt.Sprint(e), fakenet.LateMark) {
			t.Errorf("a late write of the stopped instance was reported: %v", e)
		}
		if e.GetConnectionState().GetPrincipal() == string(fakenet.Me) {
			t.Errorf("a state of the logged-out account was reported after its ENDED: %v", e)
		}
	}
	w.shutdown()
	for table, rows := range storeSnapshot(t, state) {
		for _, row := range rows {
			if strings.Contains(row, fakenet.LateMark) || strings.Contains(row, string(fakenet.LateGhost)) ||
				(table == "lynx_rooms" && strings.Contains(row, "receiver="+string(fakenet.Me)+";")) {
				t.Errorf("%s keeps a row of the stopped instance: %s", table, row)
			}
		}
	}
	if n := strings.Count(stderrOf(t, w), `"code":"stale_write_refused"`); n != 1 {
		t.Errorf("%d log lines stale_write_refused, want 1 (the stopped instance's writes, logged once)", n)
	}
}
