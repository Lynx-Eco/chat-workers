// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build libsqlite3

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func code(err error) string {
	var oe *OpenError
	if errors.As(err, &oe) {
		return oe.Code
	}
	if err == nil {
		return ""
	}
	return "not an OpenError: " + err.Error()
}

func fileSum(t *testing.T, p string) [32]byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(b)
}

func TestLibraryIsSqlite3mc(t *testing.T) {
	v, err := LibraryVersion()
	if err != nil || !strings.Contains(v, "2.7.1") {
		t.Fatalf("sqlite3mc_version() = %q, %v", v, err)
	}
	t.Logf("linked: %s", v)
}

func TestOpenEncrypted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "w.db")
	key := bytes.Repeat([]byte{0x42}, KeyLen)

	if _, _, err := OpenDB(path, OpenOptions{Key: key[:31]}, zerolog.Nop()); code(err) != CodeKeyInvalid {
		t.Fatalf("31-byte key: %v", err)
	}
	if _, _, err := OpenDB(path, OpenOptions{Key: append(bytes.Clone(key), 0)}, zerolog.Nop()); code(err) != CodeKeyInvalid {
		t.Fatalf("33-byte key: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an invalid key created the file: %v", err)
	}

	db, enc, err := OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil || !enc {
		t.Fatalf("open: %v %v", enc, err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('PLAINTEXT-MARKER')"); err != nil {
		t.Fatal(err)
	}
	var mode string
	var fk bool
	if err := db.QueryRow(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode %q %v", mode, err)
	}
	if err := db.QueryRow(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil || !fk {
		t.Fatalf("foreign_keys %v %v", fk, err)
	}
	_ = db.Close()
	raw, _ := os.ReadFile(path)
	if bytes.HasPrefix(raw, []byte(plainHeader)) || bytes.Contains(raw, []byte("PLAINTEXT-MARKER")) || bytes.Contains(raw, []byte("CREATE TABLE")) {
		t.Fatalf("plaintext in the file")
	}

	sum := fileSum(t, path)
	other := bytes.Repeat([]byte{0x43}, KeyLen)
	if _, _, err := OpenDB(path, OpenOptions{Key: other}, zerolog.Nop()); code(err) != CodeKeyMismatch {
		t.Fatalf("wrong key: %v", err)
	}
	if fileSum(t, path) != sum {
		t.Fatalf("a wrong key changed the file")
	}
	for _, side := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(path + side); err == nil {
			t.Fatalf("a wrong key left %s", side)
		}
	}

	db, enc, err = OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil || !enc {
		t.Fatalf("reopen: %v", err)
	}
	var v string
	if err := db.QueryRow(ctx, "SELECT v FROM t").Scan(&v); err != nil || v != "PLAINTEXT-MARKER" {
		t.Fatalf("read back %q %v", v, err)
	}
	_ = db.Close()
}

func TestOpenRefusesPlainAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, KeyLen)
	plain := filepath.Join(dir, "plain.db")
	if err := os.WriteFile(plain, append([]byte(plainHeader), make([]byte, 4080)...), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := fileSum(t, plain)
	if _, _, err := OpenDB(plain, OpenOptions{Key: key}, zerolog.Nop()); code(err) != CodeNotEncrypted {
		t.Fatalf("plain file: %v", err)
	}
	if fileSum(t, plain) != sum {
		t.Fatalf("plain file changed")
	}
	// Random bytes are told apart from a wrong key only as far as the
	// cipher allows: page 1 fails its tag either way.
	junk := filepath.Join(dir, "junk.db")
	if err := os.WriteFile(junk, bytes.Repeat([]byte{0x99}, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenDB(junk, OpenOptions{Key: key}, zerolog.Nop()); code(err) != CodeKeyMismatch {
		t.Fatalf("junk file: %v", err)
	}
}

func TestOpenErrorTextIsTheCode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.db")
	key := bytes.Repeat([]byte{0x42}, KeyLen)
	db, _, err := OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	_, _, err = OpenDB(path, OpenOptions{Key: bytes.Repeat([]byte{0x43}, KeyLen)}, zerolog.Nop())
	if err == nil || err.Error() != CodeKeyMismatch {
		t.Fatalf("error text %q", err)
	}
	if strings.Contains(err.Error(), "4343") || strings.Contains(err.Error(), "key=") {
		t.Fatalf("key material in the error")
	}
}

func TestURIPathEscaping(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a?b#c%d")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "w.db")
	db, enc, err := OpenDB(path, OpenOptions{Key: bytes.Repeat([]byte{1}, KeyLen)}, zerolog.Nop())
	if err != nil || !enc {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (v)"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("not created at its path: %v", err)
	}
}

// TestWrongKeyLeavesHotWAL: after an unclean stop (-wal and -shm still
// there), a wrong key changes none of the three files, and the right key
// then recovers what only the WAL held.
func TestWrongKeyLeavesHotWAL(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	key := bytes.Repeat([]byte{0x42}, KeyLen)
	db, _, err := OpenDB(filepath.Join(src, "w.db"), OpenOptions{Key: key}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('in the wal')"); err != nil {
		t.Fatal(err)
	}
	// Copy the files while the connection is open: the state a kill leaves.
	dst := t.TempDir()
	path := filepath.Join(dst, "w.db")
	for _, side := range []string{"", "-wal", "-shm"} {
		b, err := os.ReadFile(filepath.Join(src, "w.db") + side)
		if err != nil {
			t.Fatalf("%s: %v", side, err)
		}
		if err := os.WriteFile(path+side, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()
	sums := map[string][32]byte{}
	for _, side := range []string{"", "-wal", "-shm"} {
		sums[side] = fileSum(t, path+side)
	}
	if _, _, err := OpenDB(path, OpenOptions{Key: bytes.Repeat([]byte{0x43}, KeyLen)}, zerolog.Nop()); code(err) != CodeKeyMismatch {
		t.Fatalf("wrong key: %v", err)
	}
	for side, sum := range sums {
		if fileSum(t, path+side) != sum {
			t.Fatalf("a wrong key changed w.db%s", side)
		}
	}
	db, _, err = OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v string
	if err := db.QueryRow(ctx, "SELECT v FROM t").Scan(&v); err != nil || v != "in the wal" {
		t.Fatalf("recovered %q %v", v, err)
	}
}

// TestEveryConnectionKeyed: every connection the store uses is keyed and
// checked, the pool's (all five at once), withFullSync's (the journal's
// in_flight row) and Wipe's checkpoint connection: after a wipe the file
// still opens with the key only, and its log is truncated.
func TestEveryConnectionKeyed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "w.db")
	key := bytes.Repeat([]byte{0x42}, KeyLen)
	db, enc, err := OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil || !enc {
		t.Fatalf("open: %v %v", enc, err)
	}
	s, err := Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	salt := func(conn *sql.Conn) string {
		var v sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT sqlite3mc_codec_data('cipher_salt')`).Scan(&v); err != nil {
			t.Errorf("codec data: %v", err)
		}
		return v.String
	}
	var conns []*sql.Conn
	for range 5 {
		conn, err := db.RawDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
	}
	first := salt(conns[0])
	for i, conn := range conns {
		if got := salt(conn); len(got) != 32 || got != first {
			t.Errorf("connection %d: salt %q, want the file's", i, got)
		}
		_ = conn.Close()
	}
	err = withFullSync(ctx, db, func(conn *sql.Conn) error {
		if got := salt(conn); got != first {
			t.Errorf("withFullSync's connection: salt %q", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.JournalBegin(ctx, JournalRow{Key: "k", ChatID: "PLAINTEXT-MARKER", Kind: "send", Boot: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path + "-wal"); err != nil || fi.Size() != 0 {
		t.Fatalf("log after the wipe: %v %v", fi, err)
	}
	_ = db.Close()
	raw, _ := os.ReadFile(path)
	if bytes.HasPrefix(raw, []byte(plainHeader)) || bytes.Contains(raw, []byte("lynx_send_journal")) {
		t.Fatalf("plaintext in the file after the wipe")
	}
	if _, _, err := OpenDB(path, OpenOptions{Key: bytes.Repeat([]byte{0x43}, KeyLen)}, zerolog.Nop()); code(err) != CodeKeyMismatch {
		t.Fatalf("wrong key after the wipe: %v", err)
	}
	db, _, err = OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM lynx_send_journal`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("journal after the wipe: %d rows, %v", n, err)
	}
}

// TestCipherCheckFailureIsNotAVerdict: a cipher check whose query fails
// (out of memory, I/O) says nothing about the store: the open fails as
// store_open_failed (transient, retried), never as store_not_encrypted (a
// verdict). Only SQLite's "no such function" (a library that is not
// sqlite3mc) or an answer that says so is store_not_encrypted. Both for a
// new file and for an existing one (the read-only probe first).
func TestCipherCheckFailureIsNotAVerdict(t *testing.T) {
	key := bytes.Repeat([]byte{0x24}, KeyLen)
	existing := filepath.Join(t.TempDir(), "w.db")
	db, _, err := OpenDB(existing, OpenOptions{Key: key}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	before := fileSum(t, existing)
	t.Cleanup(func() { cipherQuery = queryText })
	for _, q := range []string{"SELECT sqlite3mc_version()", "PRAGMA cipher", "SELECT sqlite3mc_codec_data('cipher_salt')"} {
		for _, c := range []struct {
			name string
			fail func(conn *sqlite3.SQLiteConn) (string, bool, error)
			want string
		}{
			{"nomem", func(*sqlite3.SQLiteConn) (string, bool, error) {
				return "", false, sqlite3.Error{Code: sqlite3.ErrNomem}
			}, CodeOpenFailed},
			{"ioerr", func(*sqlite3.SQLiteConn) (string, bool, error) {
				return "", false, sqlite3.Error{Code: sqlite3.ErrIoErr}
			}, CodeOpenFailed},
			{"no such function", func(conn *sqlite3.SQLiteConn) (string, bool, error) {
				return queryText(conn, "SELECT lynx_no_such_function()")
			}, CodeNotEncrypted},
			{"no answer", func(*sqlite3.SQLiteConn) (string, bool, error) { return "", false, nil }, CodeNotEncrypted},
		} {
			cipherQuery = func(conn *sqlite3.SQLiteConn, query string) (string, bool, error) {
				if query == q {
					return c.fail(conn)
				}
				return queryText(conn, query)
			}
			for which, path := range map[string]string{"new": filepath.Join(t.TempDir(), "w.db"), "existing": existing} {
				db, _, err := OpenDB(path, OpenOptions{Key: key}, zerolog.Nop())
				if err == nil {
					_ = db.Close()
				}
				if code(err) != c.want {
					t.Errorf("%s fails with %s, %s file: %v, want %s", q, c.name, which, err, c.want)
				}
			}
			cipherQuery = queryText
		}
	}
	if fileSum(t, existing) != before {
		t.Fatal("a failed open changed the existing file")
	}
}

// TestPoolCloseZeroesKey: the pool's copy of the key, which every new
// connection is opened with, is zeroed when the pool is closed.
func TestPoolCloseZeroesKey(t *testing.T) {
	c := &connector{path: filepath.Join(t.TempDir(), "w.db"), key: bytes.Repeat([]byte{0x42}, KeyLen)}
	db, err := openPool(c, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(c.key, make([]byte, KeyLen)) {
		t.Fatal("the pool's key was not zeroed when it was closed")
	}
	// A late connection is refused, not opened with the zeroed key.
	if conn, err := c.Connect(context.Background()); !errors.Is(err, errPoolClosed) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("a connection after the close: %v, want errPoolClosed", err)
	}
}
