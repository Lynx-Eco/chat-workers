// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/store"
)

// plainMarkers must never be found in the store's files: the SQLite header,
// a message text, a contact name, an identifier, the account and a table
// name. Each is searched in about 2 MB of ciphertext, so each is at least
// minMarker bytes long: a 4-byte one (fakenet.Me, "u-me") is found there by
// chance about once in 2000 runs. The account is therefore atRestAccount,
// not fakenet.Me. TestPlainStoreHoldsMarkers shows that a plain store of the
// same run holds every one of them, so the scan can see them.
var plainMarkers = []string{"SQLite format 3", "SECRETTEXT", fakenet.AliceText, "Alice Example", "fakenet:alice", atRestAccount, "user_login"}

// atRestAccount is the account the at-rest runs sign in as.
const atRestAccount = "u-at-rest-account-0001"

// minMarker is the shortest marker searched in ciphertext: a chance match
// of 8 given bytes in 2 MB is about 1 in 10^13.
const minMarker = 8

// atRestRun starts the worker on state with the at-rest account, signs in
// (the sync: messages, people, chats) and sends once, acknowledging nothing,
// so that the outbox keeps every event. On a plain store (plain) the wait
// for the stored sync flag, which opens the store encrypted, is left out.
func atRestRun(t *testing.T, state string, settings map[string]string, plain bool) *worker {
	t.Helper()
	w := start(t, state, opts{noAutoAck: true, script: &fakenet.Script{Account: atRestAccount}, settings: settings})
	if plain {
		expect(t, w.command(signInBegin(fakenet.FlowPhone)), contract.Outcome_OUTCOME_OK, "")
		f := w.wait("fields step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS))
		expect(t, w.command(signInSubmit(step(f).GetStepId(), map[string]string{"phone": "+15550000000"})), contract.Outcome_OUTCOME_OK, "")
		w.wait("done step", isStep(contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE))
		w.wait("group rename", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" })
	} else {
		w.signIn()
	}
	expect(t, w.command(sendCmd(fakenet.DMAlice, "k-at-rest", "at rest", 0)), contract.Outcome_OUTCOME_OK, "")
	return w
}

// markersIn tells which markers the files hold.
func markersIn(t *testing.T, markers []string, paths ...string) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range markers {
			found[m] = found[m] || bytes.Contains(b, []byte(m))
		}
	}
	return found
}

// plainBin builds chat_fakenet with the plain store once, for the tests
// that need a plain file (plainstore, with dev_plain_store set).
func plainBin(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds a second worker")
	}
	if store.PlainBuild {
		t.Skip("this is the plain build")
	}
	plainOnce.Do(func() {
		plainPath = filepath.Join(filepath.Dir(binPath), "chat_fakenet_plain")
		plainErr = hosttest.BuildWorker(".", plainPath, "plainstore", "")
	})
	if plainErr != nil {
		t.Fatal(plainErr)
	}
	return plainPath
}

var (
	plainOnce sync.Once
	plainPath string
	plainErr  error
)

// TestPlainStoreHoldsMarkers is the control of TestStoreEncryptedAtRest: the
// same run on a plain store (the plain build with dev_plain_store) holds
// every marker that scan looks for in the encrypted one, while running, so
// not finding them there is the cipher's doing, not a scan that cannot see
// them. Every marker is at least minMarker bytes.
func TestPlainStoreHoldsMarkers(t *testing.T) {
	for _, m := range plainMarkers {
		if len(m) < minMarker {
			t.Fatalf("marker %q is shorter than %d bytes: ciphertext matches it by chance", m, minMarker)
		}
	}
	bin := plainBin(t)
	saved := binPath
	binPath = bin
	defer func() { binPath = saved }()
	state := newState(t)
	w := atRestRun(t, state, map[string]string{"dev_plain_store": "1"}, true)
	if w.hello.GetStoreEncrypted() {
		t.Fatal("the plain control store said encrypted")
	}
	found := markersIn(t, plainMarkers, storeFiles(state)...)
	w.shutdown()
	for _, m := range plainMarkers {
		if !found[m] {
			t.Errorf("the plain store does not hold %q: the at-rest scan for it proves nothing", m)
		}
	}
}

func storeFiles(state string) []string {
	db := filepath.Join(state, "worker.db")
	return []string{db, db + "-wal", db + "-shm"}
}

// scanFiles fails the test for each marker found in an existing file and
// returns the total size scanned.
func scanFiles(t *testing.T, markers []string, paths ...string) int64 {
	t.Helper()
	var total int64
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		total += int64(len(b))
		for _, m := range markers {
			if bytes.Contains(b, []byte(m)) {
				t.Errorf("%s holds plaintext %q", filepath.Base(p), m)
			}
		}
	}
	return total
}

// fileSums is the size and digest of each path ("absent" when missing).
func fileSums(t *testing.T, paths ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			out[filepath.Base(p)] = "absent"
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		out[filepath.Base(p)] = fmt.Sprintf("%d:%x", len(b), sum[:8])
	}
	return out
}

func sameFiles(t *testing.T, before map[string]string, paths ...string) {
	t.Helper()
	after := fileSums(t, paths...)
	for name, sum := range before {
		if after[name] != sum {
			t.Errorf("%s changed: %s -> %s", name, sum, after[name])
		}
	}
}

func isState(code string) func(*contract.ChatEvent) bool {
	return func(e *contract.ChatEvent) bool {
		cs := e.GetConnectionState()
		return cs.GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR && cs.GetCode() == code
	}
}

// refused checks a start refused with code: Hello says not encrypted, one
// ConnectionState ERROR arrives, and the worker exits ExitStoreRefused.
func refused(t *testing.T, w *worker, code string) {
	t.Helper()
	if w.hello.GetStoreEncrypted() {
		t.Fatalf("refused start said store_encrypted")
	}
	w.wait(code, isState(code))
	if c, err := w.proc.Wait(10 * time.Second); err != nil || c != backend.ExitStoreRefused {
		t.Fatalf("exit %d %v, want %d", c, err, backend.ExitStoreRefused)
	}
	if s := stderrOf(t, w); !strings.Contains(s, `"code":"`+code+`"`) {
		t.Fatalf("stderr has no %s: %s", code, s)
	}
}

func TestStoreEncryptedAtRest(t *testing.T) {
	state := newState(t)
	w := atRestRun(t, state, nil, false)
	if !w.hello.GetStoreEncrypted() {
		t.Fatalf("hello: store not encrypted")
	}
	// While running: the WAL holds the sync.
	if fi, err := os.Stat(storeFiles(state)[1]); err != nil || fi.Size() == 0 {
		t.Fatalf("no WAL while running: %v", err)
	}
	n := scanFiles(t, plainMarkers, storeFiles(state)...)
	t.Logf("running: scanned %d bytes of worker.db, -wal, -shm", n)
	before := w.s.Events()
	maxSeq := before[len(before)-1].GetSeq()
	w.shutdown()
	n = scanFiles(t, plainMarkers, storeFiles(state)...)
	t.Logf("after shutdown: scanned %d bytes", n)

	// Plain SQLite (the same library with no key) refuses the file.
	raw, err := sql.Open("sqlite3", "file:"+storeFiles(state)[0]+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var tables int
	err = raw.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&tables)
	_ = raw.Close()
	var se sqlite3.Error
	if !errors.As(err, &se) || se.Code != sqlite3.ErrNotADB {
		t.Fatalf("plain open: %d tables, %v", tables, err)
	}

	// Same key: everything resumes (outbox, journal, login).
	w2 := start(t, state, opts{})
	if !w2.hello.GetStoreEncrypted() || w2.hello.GetLastUnackedSeq() != maxSeq {
		t.Fatalf("restart hello: %v (want last_unacked_seq %d)", w2.hello, maxSeq)
	}
	w2.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	r := w2.command(sendCmd(fakenet.DMAlice, "k-at-rest", "at rest", 0))
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || !r.GetDeduplicated() {
		t.Fatalf("journal lost: %v", r)
	}
	w2.shutdown()

	// Another key: refused, nothing on disk changed.
	sums := fileSums(t, storeFiles(state)...)
	t.Logf("before the wrong key: %v", sums)
	other := bytes.Repeat([]byte{0x5A}, 32)
	w3 := start(t, state, opts{key: other})
	refused(t, w3, store.CodeKeyMismatch)
	sameFiles(t, sums, storeFiles(state)...)

	// And the right key still works after the refusal.
	w4 := start(t, state, opts{})
	w4.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w4.shutdown()

	// No log of any run holds the key.
	logs := stderrOf(t, w4)
	for _, bad := range []string{hex.EncodeToString(testKey()), strings.ToUpper(hex.EncodeToString(testKey())), hex.EncodeToString(other), "key=", "raw:hex", "a0a1a2a3"} {
		if strings.Contains(logs, bad) {
			t.Fatalf("log holds %q", bad)
		}
	}
}

func TestStoreKeyInvalid(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{key: bytes.Repeat([]byte{1}, 31)})
	refused(t, w, store.CodeKeyInvalid)
	if entries, _ := os.ReadDir(state); len(entries) != 0 {
		t.Fatalf("a refused key left %d entries in the state directory", len(entries))
	}
}

// TestStoreUnreachableIsTransient: a store that cannot be reached now (here:
// a state directory the worker may not write) is no verdict on the store.
// The worker sends no Hello and exits 1, a failed start for the host (started
// again, parked as crash_loop after 5 in 600 s); it does not
// answer "not encrypted" and exit ExitStoreRefused, which the host would
// take as final. Once the directory is writable the same connection starts.
func TestStoreUnreachableIsTransient(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes a read-only directory")
	}
	state := newState(t)
	if err := os.Chmod(state, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(state, 0o700) })
	expectNoHello(t, state, "store_open_failed")
	if entries, _ := os.ReadDir(state); len(entries) != 0 {
		t.Fatalf("an unreachable store left %d entries", len(entries))
	}

	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	w := start(t, state, opts{})
	if !w.hello.GetStoreEncrypted() {
		t.Fatal("the store did not open once it was reachable")
	}
	w.shutdown()
}

// expectNoHello starts the worker on state and expects it to answer no
// Hello, exit 1 and log code: a store it cannot use now, which the host
// takes for a failed start.
func expectNoHello(t *testing.T, state, code string) {
	t.Helper()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	stderr := filepath.Join(state, "..", filepath.Base(state)+"-stderr.log")
	proc, err := hosttest.StartWorker(binPath, ln.Path, stderr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, hello, err := ln.Accept(ctx, &contract.Init{
		ContractVersion: contract.Version, Connector: "fakenet", ConnectionId: "conn-test-1",
		StateDir: state, StoreKey: testKey(), Settings: map[string]string{"device_name": "Lynx"},
	})
	if err == nil {
		proc.Kill()
		t.Fatalf("the worker answered Hello (store_encrypted %v) for a store it cannot use now", hello.GetStoreEncrypted())
	}
	if c, err := proc.Wait(10 * time.Second); err != nil || c != 1 {
		t.Fatalf("exit %d %v, want 1", c, err)
	}
	raw, _ := os.ReadFile(stderr)
	if !strings.Contains(string(raw), `"code":"`+code+`"`) {
		t.Fatalf("stderr has no %s: %s", code, raw)
	}
}

// TestStoreSetupFailedIsTransient: the file opens and passes the cipher
// checks, but the worker's own tables cannot be set up on it (here a view
// stands where a table of ours goes): no Hello, exit 1, a fixed code in the
// log.
func TestStoreSetupFailedIsTransient(t *testing.T) {
	state := newState(t)
	db, _, err := store.OpenDB(filepath.Join(state, "worker.db"), store.OpenOptions{Key: testKey(), DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), `CREATE VIEW lynx_send_journal AS SELECT 1 AS id`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	expectNoHello(t, state, "store_setup_failed")
}

func TestStorePlainFileRefused(t *testing.T) {
	state := newState(t)
	db := filepath.Join(state, "worker.db")
	if err := os.WriteFile(db, append([]byte("SQLite format 3\x00"), make([]byte, 4080)...), 0o600); err != nil {
		t.Fatal(err)
	}
	sums := fileSums(t, storeFiles(state)...)
	w := start(t, state, opts{})
	refused(t, w, store.CodeNotEncrypted)
	sameFiles(t, sums, storeFiles(state)...)
}

// TestPlainBuildRefuses builds chat_fakenet with -tags plainstore (the
// driver's own plain SQLite) and checks that it refuses to start as an
// encrypted worker, creating nothing, unless told dev_plain_store.
func TestPlainBuildRefuses(t *testing.T) {
	bin := plainBin(t)
	saved := binPath
	binPath = bin
	defer func() { binPath = saved }()
	state := newState(t)
	w := start(t, state, opts{})
	refused(t, w, store.CodeNotEncrypted)
	if entries, _ := os.ReadDir(state); len(entries) != 0 {
		t.Fatalf("the plain build left %d entries in the state directory", len(entries))
	}
	// The development switch: plain file, Hello says so.
	w = start(t, state, opts{settings: map[string]string{"dev_plain_store": "1"}})
	if w.hello.GetStoreEncrypted() {
		t.Fatalf("plain store said encrypted")
	}
	w.wait("signed out", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT
	})
	w.shutdown()
	head := make([]byte, 16)
	f, err := os.Open(filepath.Join(state, "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Read(head)
	_ = f.Close()
	if string(head) != "SQLite format 3\x00" {
		t.Fatalf("dev plain store is not a plain file")
	}
}

// TestDevPlainIgnoredWhenEncrypted: the encrypted build encrypts even when
// the development switch is set.
func TestDevPlainIgnoredWhenEncrypted(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{settings: map[string]string{"dev_plain_store": "1"}})
	if !w.hello.GetStoreEncrypted() {
		t.Fatalf("encrypted build opened a plain store")
	}
	w.shutdown()
	scanFiles(t, []string{"SQLite format 3"}, storeFiles(state)...)
}

// TestSpoolAndTransferOnDisk: a spooled avatar is encrypted in media/ (no
// PNG bytes, not named by its digest), FetchAttachment decrypts it with the
// right digest into a 0600 transfer file, and a refused transfer leaves no
// file, partial or final.
func TestSpoolAndTransferOnDisk(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	var avatar string
	for _, e := range w.events() {
		if p := e.GetPersonInfo(); p.GetPersonId() == string(fakenet.Alice) && p.GetAvatarRef() != "" {
			avatar = p.GetAvatarRef()
		}
	}
	sha, ok := strings.CutPrefix(avatar, "spool:")
	if !ok || len(sha) != 64 {
		t.Fatalf("avatar handle %q", avatar)
	}
	media := filepath.Join(state, "media")
	entries, err := os.ReadDir(media)
	if err != nil || len(entries) == 0 {
		t.Fatalf("media: %d entries, %v", len(entries), err)
	}
	var paths []string
	for _, e := range entries {
		if e.Name() == sha {
			t.Fatalf("a spool file is named by its digest")
		}
		paths = append(paths, filepath.Join(media, e.Name()))
	}
	pngMarkers := []string{"\x89PNG", "IHDR", "IDAT"}
	scanFiles(t, pngMarkers, paths...)

	fetch := func(h, name string, max uint64) *contract.CommandReply {
		return w.command(&contract.Command{Kind: &contract.Command_FetchAttachment{FetchAttachment: &contract.FetchAttachment{Handle: h, TargetName: name, MaxBytes: max}}})
	}
	r := fetch(avatar, "avatar.png", 0)
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	if r.GetSha256Hex() != sha {
		t.Fatalf("fetched digest %s, handle %s", r.GetSha256Hex(), sha)
	}
	out := filepath.Join(state, "transfer", "avatar.png")
	fi, err := os.Stat(out)
	if err != nil || fi.Mode().Perm() != 0o600 || fi.Size() != int64(r.GetBytesWritten()) {
		t.Fatalf("transfer file: %v %v", fi, err)
	}
	if b, _ := os.ReadFile(out); !bytes.HasPrefix(b, []byte("\x89PNG")) {
		t.Fatalf("transfer file is not the PNG")
	}
	expect(t, fetch(avatar, "too-small.png", 10), contract.Outcome_OUTCOME_REJECTED, "too_large")
	var image string
	for _, m := range messages(w.events(), fakenet.DMBob, "m2") {
		if a := m.GetContent().GetAttachments(); len(a) == 1 {
			image = a[0].GetHandle()
		}
	}
	expect(t, fetch(image, "img-small.bin", 1000), contract.Outcome_OUTCOME_REJECTED, "too_large")
	left, _ := os.ReadDir(filepath.Join(state, "transfer"))
	for _, e := range left {
		if e.Name() != "avatar.png" {
			t.Fatalf("a refused transfer left %s", e.Name())
		}
	}
	w.shutdown()
	scanFiles(t, pngMarkers, paths...)
}
