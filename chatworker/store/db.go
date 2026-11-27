// SPDX-License-Identifier: AGPL-3.0-or-later

package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
)

// KeyLen is the length of Init.store_key: a raw 256-bit key, used as is
// (no KDF), so a key of any other length is refused.
const KeyLen = 32

// Cipher is the SQLite3 Multiple Ciphers cipher scheme of the store:
// ChaCha20 with a Poly1305 tag on every page (sqlite3mc's default).
const Cipher = "chacha20"

// Fixed codes of a refused open, reported as ConnectionState ERROR.
const (
	// CodeKeyInvalid: Init.store_key is not exactly KeyLen bytes. Nothing
	// on disk was touched.
	CodeKeyInvalid = "store_key_invalid"
	// CodeNotEncrypted: the linked library does not encrypt, the cipher in
	// effect is not Cipher, or the file on disk is a plain SQLite file.
	// Nothing was created; an existing file was not touched.
	CodeNotEncrypted = "store_not_encrypted"
	// CodeKeyMismatch: an existing file does not decrypt with this key
	// (SQLITE_NOTADB on page 1). The file is left as it is.
	CodeKeyMismatch = "store_key_mismatch"
	// CodeCorrupt: the file decrypted but SQLite found it malformed.
	CodeCorrupt = "store_corrupt"
	// CodeOpenFailed: any other failure (I/O, permissions).
	CodeOpenFailed = "store_open_failed"
)

// OpenError is a refused open. Its text is the fixed code only: it never
// carries the driver's message, and never the DSN (which holds the key).
type OpenError struct {
	Code string
	// SQLite is the primary SQLite result code behind it, 0 if none.
	SQLite int
}

func (e *OpenError) Error() string { return e.Code }

// OpenOptions are the inputs of OpenDB.
type OpenOptions struct {
	// Key is Init.store_key. OpenDB keeps its own copy (needed to open
	// further pooled connections); the caller may zero its slice after.
	Key []byte
	// DevPlain opens a plain file. It is honoured only by a binary built
	// with -tags plainstore (PlainBuild) and ignored by the encrypted
	// build, which always encrypts.
	DevPlain bool
}

const plainHeader = "SQLite format 3\x00"

// OpenDB opens (creating if needed) the worker's single SQLite file,
// encrypted with SQLite3 Multiple Ciphers (ChaCha20-Poly1305, raw key). It
// reports whether the store is encrypted. Before the file is opened it
// checks that the linked library is sqlite3mc; on every new connection, and
// before any pragma that writes, it checks that the cipher in effect is
// Cipher and that the codec encrypts the main database. Every error is an
// *OpenError.
func OpenDB(path string, o OpenOptions, log zerolog.Logger) (*dbutil.Database, bool, error) {
	if len(o.Key) != KeyLen {
		return nil, false, &OpenError{Code: CodeKeyInvalid}
	}
	if o.DevPlain && PlainBuild {
		db, err := openPool(&connector{path: path}, log)
		if err != nil {
			return nil, false, classify(err, true)
		}
		return db, false, nil
	}
	if _, err := LibraryVersion(); err != nil {
		return nil, false, err
	}
	side := []string{path, path + "-wal", path + "-shm", path + "-journal"}
	existed := make([]bool, len(side))
	for i, p := range side {
		_, err := os.Lstat(p)
		existed[i] = err == nil
	}
	nonEmpty, err := inspect(path)
	if err != nil {
		return nil, false, err
	}
	c := &connector{path: path, key: bytes.Clone(o.Key)}
	if nonEmpty {
		// An existing file is first probed read-only and immutable (no
		// locks, no -wal or -shm opened), so that a wrong key touches no
		// byte of it, nor of a WAL left by an unclean stop.
		if err := c.probe(); err != nil {
			clear(c.key)
			return nil, false, classify(err, true)
		}
	}
	db, err := openPool(c, log)
	if err != nil {
		clear(c.key)
		// Remove only what this attempt created: a file that was there
		// before is never deleted or rewritten.
		for i, p := range side {
			if !existed[i] {
				_ = os.Remove(p)
			}
		}
		return nil, false, classify(err, nonEmpty)
	}
	return db, true, nil
}

// inspect reports whether path is a non-empty file, and refuses a plain
// SQLite file (its first 16 bytes are the SQLite header; an sqlite3mc file
// starts with its random KDF salt).
func inspect(path string) (bool, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, &OpenError{Code: CodeOpenFailed}
	}
	defer f.Close()
	head := make([]byte, len(plainHeader))
	n, err := io.ReadFull(f, head)
	if n == 0 && (errors.Is(err, io.EOF)) {
		return false, nil
	}
	if string(head[:n]) == plainHeader {
		return true, &OpenError{Code: CodeNotEncrypted}
	}
	return true, nil
}

func classify(err error, existed bool) error {
	var oe *OpenError
	if errors.As(err, &oe) {
		return oe
	}
	var se sqlite3.Error
	if errors.As(err, &se) {
		switch {
		case se.Code == sqlite3.ErrNotADB && existed:
			return &OpenError{Code: CodeKeyMismatch, SQLite: int(se.Code)}
		case se.Code == sqlite3.ErrNotADB || se.Code == sqlite3.ErrCorrupt:
			return &OpenError{Code: CodeCorrupt, SQLite: int(se.Code)}
		}
		return &OpenError{Code: CodeOpenFailed, SQLite: int(se.Code)}
	}
	return &OpenError{Code: CodeOpenFailed}
}

// openPool makes the pool and opens its first connection, so that every
// check has run before OpenDB returns. Every connection the store or the
// framework ever uses comes from this pool (dbutil child databases share it;
// Wipe's checkpoint and withFullSync take theirs from it), so every one is
// opened by connector.Connect, keyed and checked.
//
// All five connections stay open when idle: each new encrypted connection
// runs the cipher checks and starts with a cold page cache that is decrypted
// again (with one kept, the slow-host test stored its ticks 1-2.5 s later).
func openPool(c *connector, log zerolog.Logger) (*dbutil.Database, error) {
	raw := sql.OpenDB(c)
	raw.SetMaxOpenConns(5)
	raw.SetMaxIdleConns(5)
	conn, err := raw.Conn(context.Background())
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	_ = conn.Close()
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		_ = raw.Close()
		return nil, &OpenError{Code: CodeOpenFailed}
	}
	db.Owner = "lynx-chatworker"
	db.Log = dbutil.ZeroLogger(log)
	return db, nil
}

// LibraryVersion asks the linked SQLite for sqlite3mc_version() on an
// in-memory database (nothing on disk). Plain SQLite has no such function.
func LibraryVersion() (string, error) {
	var d sqlite3.SQLiteDriver
	conn, err := d.Open("file::memory:")
	if err != nil {
		return "", &OpenError{Code: CodeOpenFailed}
	}
	defer conn.Close()
	v, ok, err := cipherQuery(conn.(*sqlite3.SQLiteConn), "SELECT sqlite3mc_version()")
	if err := cipherAnswer(err, ok && v != ""); err != nil {
		return "", classify(err, false)
	}
	return v, nil
}

// cipherAnswer judges one answer of a cipher check (sqlite3mc_version(),
// PRAGMA cipher, the codec's salt). Only an answer that says the store is
// not encrypted is that verdict (CodeNotEncrypted, final): good false, or
// SQLite reporting that the function does not exist, as a library that is
// not sqlite3mc does. Any other failure of the query (out of memory, I/O,
// a busy or locked file) says nothing about the store and is returned as
// it is, for classify (CodeOpenFailed, transient; SQLITE_NOTADB on an
// existing file is a wrong key).
func cipherAnswer(err error, good bool) error {
	switch {
	case err != nil && noSuchFunction(err):
		return &OpenError{Code: CodeNotEncrypted}
	case err != nil:
		return err
	case !good:
		return &OpenError{Code: CodeNotEncrypted}
	}
	return nil
}

// noSuchFunction tells whether err is SQLite's answer to a call of a
// function the library does not have (SQLITE_ERROR, "no such function").
func noSuchFunction(err error) bool {
	var se sqlite3.Error
	return errors.As(err, &se) && se.Code == sqlite3.ErrError && strings.Contains(se.Error(), "no such function")
}

// connector opens each pooled connection with the key as a URI parameter of
// sqlite3_open_v2, the only place where mattn/go-sqlite3 lets sqlite3mc key
// a connection before its own pragmas read the schema. The DSN is built in a
// byte buffer per connection and zeroed when the open returns; it is never
// logged or put in an error, and the only Go strings over it are the one
// handed to the driver and the substrings its query parsing makes of it,
// which share the buffer and are zeroed with it.
//
// What is zeroed, and what cannot be. Zeroed: Init's key once Run has its
// copy, the frame buffer Init was read into (wire.ReadFrame), Run's copy
// once the store is open and the spool keys derived, each DSN buffer, and
// connector.key when the open fails or the pool is closed. Kept while the
// worker runs, because
// it is needed: connector.key (every new pooled connection is keyed with
// it) and the spool's derived keys. Not zeroed, because Go code cannot
// reach them: the C string mattn/go-sqlite3 copies the DSN into
// (C.CString, released with free, not cleared); SQLite's own copy of the
// URI with its parameters, kept by the pager for the connection's life and
// freed uncleared (sqlite3mc reads the key from it and does not erase it);
// the cipher's key material in each connection (it encrypts every page).
// Those are C heap memory; they last until it is reused, as do the Go
// heap's own leftovers (the GC frees without clearing). A guarantee against
// a reader of the process's memory is therefore not given; what is given is
// that the key reaches no file, log, error or reply.
type connector struct {
	path string
	// mu is held while the key is read into a DSN and while Close zeroes
	// it; closed is set by Close.
	mu     sync.Mutex
	key    []byte // nil: the development plain store
	closed bool
	drv    sqlite3.SQLiteDriver
}

// errPoolClosed: a connection asked of a pool after it was closed (its key
// is zeroed); never opened with the zeroed key.
var errPoolClosed = errors.New("store: the pool is closed")

func (c *connector) Driver() driver.Driver { return &c.drv }

// Close zeroes the key: database/sql calls it when the pool is closed, after
// which no connection is opened again.
func (c *connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.key)
	c.closed = true
	return nil
}

// The pragmas of every connection, run after the cipher checks (the first
// of them is the first write to a new file): those of the framework's
// sqlite3-fk-wal driver; deleted rows overwritten (secure_delete, for Wipe);
// and temporary tables in memory, never in plaintext temporary files.
//
// synchronous=NORMAL: in WAL mode a commit reaches the disk at the next
// checkpoint. Writes that must survive a power loss go through withFullSync.
var connPragmas = []string{
	"PRAGMA foreign_keys = ON",
	"PRAGMA journal_mode = WAL",
	"PRAGMA synchronous = NORMAL",
	"PRAGMA busy_timeout = 5000",
	"PRAGMA secure_delete = ON",
	"PRAGMA temp_store = MEMORY",
}

// probe opens the file immutable and read-only and decrypts page 1 only
// (PRAGMA user_version): the database file alone may be behind its WAL, so
// nothing beyond page 1 is read.
func (c *connector) probe() error {
	dc, err := c.open("&mode=ro&immutable=1")
	if err != nil {
		return err
	}
	conn := dc.(*sqlite3.SQLiteConn)
	defer conn.Close()
	v, ok, err := cipherQuery(conn, "SELECT sqlite3mc_version()")
	if err := cipherAnswer(err, ok && v != ""); err != nil {
		return err
	}
	v, ok, err = cipherQuery(conn, "PRAGMA cipher")
	if err := cipherAnswer(err, ok && v == Cipher); err != nil {
		return err
	}
	_, _, err = queryText(conn, "PRAGMA user_version")
	return err
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	dc, err := c.open("")
	if err != nil {
		return nil, err
	}
	conn := dc.(*sqlite3.SQLiteConn)
	if c.key != nil {
		if err := verifyCipher(conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	for _, p := range connPragmas {
		if _, err := conn.Exec(p, nil); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (c *connector) open(extra string) (driver.Conn, error) {
	const opts = "?_txlock=immediate&_busy_timeout=5000"
	const keyOpts = "&cipher=" + Cipher + "&key=raw:hex:"
	buf := make([]byte, 0, len("file:")+3*len(c.path)+len(opts)+len(extra)+len(keyOpts)+2*KeyLen)
	buf = append(buf, "file:"...)
	buf = appendURIPath(buf, c.path)
	buf = append(buf, opts...)
	buf = append(buf, extra...)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errPoolClosed
	}
	if c.key != nil {
		buf = append(buf, keyOpts...)
		n := len(buf)
		buf = buf[:n+hex.EncodedLen(len(c.key))]
		hex.Encode(buf[n:], c.key)
	}
	c.mu.Unlock()
	// The driver copies the DSN into C memory for the open and keeps no Go
	// reference to it, so the buffer can be zeroed as soon as it returns.
	conn, err := c.drv.Open(unsafe.String(unsafe.SliceData(buf), len(buf)))
	clear(buf[:cap(buf)])
	runtime.KeepAlive(buf)
	return conn, err
}

// appendURIPath escapes what would end or change the path of a file: URI.
func appendURIPath(buf []byte, p string) []byte {
	const hexd = "0123456789ABCDEF"
	for i := 0; i < len(p); i++ {
		switch ch := p[i]; ch {
		case '%', '?', '#':
			buf = append(buf, '%', hexd[ch>>4], hexd[ch&15])
		default:
			buf = append(buf, ch)
		}
	}
	return buf
}

// verifyCipher is the fail-closed check on an opened connection: the
// library is sqlite3mc, the cipher in effect is Cipher, page 1 decrypts (a
// wrong key fails here with SQLITE_NOTADB), and the codec encrypts the main
// database with a write cipher (it has a KDF salt).
func verifyCipher(conn *sqlite3.SQLiteConn) error {
	v, ok, err := cipherQuery(conn, "SELECT sqlite3mc_version()")
	if err := cipherAnswer(err, ok && v != ""); err != nil {
		return err
	}
	v, ok, err = cipherQuery(conn, "PRAGMA cipher")
	if err := cipherAnswer(err, ok && v == Cipher); err != nil {
		return err
	}
	if _, _, err := queryText(conn, "SELECT count(*) FROM sqlite_master"); err != nil {
		return err
	}
	salt, ok, err := cipherQuery(conn, "SELECT sqlite3mc_codec_data('cipher_salt')")
	return cipherAnswer(err, ok && len(salt) == 32)
}

// cipherQuery runs the queries of the cipher checks (queryText; tests make
// them fail).
var cipherQuery = queryText

// queryText runs a one-value query; ok is false for NULL or no row.
func queryText(conn *sqlite3.SQLiteConn, q string) (string, bool, error) {
	rows, err := conn.Query(q, nil)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	if len(dest) == 0 {
		return "", false, nil
	}
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return "", false, nil
		}
		return "", false, err
	}
	switch v := dest[0].(type) {
	case nil:
		return "", false, nil
	case string:
		return v, true, nil
	case []byte:
		return string(v), true, nil
	case int64:
		return "", true, nil
	default:
		return "", true, nil
	}
}

// withFullSync runs fn on one connection with synchronous=FULL, so that a
// commit made in fn is on disk when it returns (the WAL is synced, with every
// earlier commit in it). Every other write keeps NORMAL: syncing each of the
// many writes of a sync with the network would hold the network up. The
// connection is one of the pool's: keyed like every other.
func withFullSync(ctx context.Context, db *dbutil.Database, fn func(conn *sql.Conn) error) error {
	conn, err := db.RawDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA synchronous = FULL`); err != nil {
		return err
	}
	ferr := fn(conn)
	// Back to the pool's setting; a failure only leaves the connection
	// slower.
	_, rerr := conn.ExecContext(context.Background(), `PRAGMA synchronous = NORMAL`)
	return errors.Join(ferr, rerr)
}
