// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"go.mau.fi/mautrix-gvoice/pkg/connector/gvdb"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	lynxstore "lynx.invalid/ingest/chatworker/store"
)

// testGV is the worker's connector on a store of its own, opened as the
// worker opens it (store.OpenDB: encrypted in the encrypted build), with the
// framework's tables (user_login among them) made as the backend's
// newFramework makes them, and the connector's database on it as its Init
// makes it (connector.go Init: gvdb.New on the framework's database) with
// its table made (Start).
func testGV(t *testing.T) (*gvNetwork, *dbutil.Database) {
	t.Helper()
	keepPath(t)
	db, _, err := lynxstore.OpenDB(filepath.Join(t.TempDir(), "worker.db"),
		lynxstore.OpenOptions{Key: bytes.Repeat([]byte{7}, lynxstore.KeyLen), DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	nc, err := newConnector(initWith(nil))
	if err != nil {
		t.Fatal(err)
	}
	n := nc.(*gvNetwork)
	fw := database.New("gvoice", database.MetaTypes{}, db)
	if err := fw.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	n.DB = gvdb.New(db, zerolog.Nop())
	if err := n.DB.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	return n, db
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// prefixOf takes the login's prefix as the connector's SubmitCookies does
// (login.go:140), storing its row.
func prefixOf(t *testing.T, n *gvNetwork, id networkid.UserLoginID) string {
	t.Helper()
	p, err := n.DB.GetLoginPrefix(t.Context(), id)
	must(t, err)
	return p
}

// storeLogin writes the framework's rows of a stored login (its user and
// its user_login row), as NewLogin leaves them.
func storeLogin(t *testing.T, db *dbutil.Database, id networkid.UserLoginID) {
	t.Helper()
	ctx := t.Context()
	_, err := db.Exec(ctx, `INSERT INTO "user" (bridge_id, mxid) VALUES ('gvoice', '@owner:lynx.invalid') ON CONFLICT DO NOTHING`)
	must(t, err)
	_, err = db.Exec(ctx, `INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, metadata) VALUES ('gvoice', '@owner:lynx.invalid', $1, '', '{}')`, string(id))
	must(t, err)
}

// prefixRows is every stored prefix row, "<prefix> <login id>", sorted.
func prefixRows(t *testing.T, db *dbutil.Database) []string {
	t.Helper()
	rows, err := db.Query(t.Context(), `SELECT prefix, login_id FROM gvoice_login_prefix`)
	must(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var prefix int64
		var id string
		must(t, rows.Scan(&prefix, &id))
		out = append(out, strconv.FormatInt(prefix, 10)+" "+id)
	}
	must(t, rows.Err())
	slices.Sort(out)
	return out
}

// TestDropRefusedDeletesTheOtherAccountsPrefix: the refused login's prefix
// row (the other account's number, stored by the connector before NewLogin)
// is deleted at once; the connection's own row stays with its prefix, which
// a later prefix of the same login gives again; there is no end at Google.
// WHY: catches the other account's number left in the store after a
// refusal, a drop that takes the connection's own prefix (its person ids
// would change), and a sign-out of the user's Google web session.
func TestDropRefusedDeletesTheOtherAccountsPrefix(t *testing.T) {
	n, db := testGV(t)
	mine, other := loginIDOf(mineNumber), loginIDOf(otherNumber)
	minePrefix := prefixOf(t, n, mine)
	storeLogin(t, db, mine)
	before := prefixRows(t, db)
	prefixOf(t, n, other)
	if len(prefixRows(t, db)) != 2 {
		t.Fatalf("rows %v", prefixRows(t, db))
	}
	end, err := dropRefused(t.Context(), n, loginOf(other, "2"))
	if err != nil || end != nil {
		t.Fatalf("drop: end %v, %v", end != nil, err)
	}
	if after := prefixRows(t, db); !slices.Equal(after, before) {
		t.Fatalf("after the drop %v, want %v", after, before)
	}
	if again := prefixOf(t, n, mine); again != minePrefix {
		t.Fatalf("own prefix %q, then %q", minePrefix, again)
	}
	// A refused login whose row is already gone: nothing to do, no error.
	if end, err := dropRefused(t.Context(), n, loginOf(other, "2")); err != nil || end != nil {
		t.Fatalf("second drop: %v %v", end != nil, err)
	}
}

// TestDropRefusedErrors: a connector that is not the worker's, one with no
// database, or no login is an error (the backend fails the sign-in as
// other_account_kept: the row may be there), never a silent success.
func TestDropRefusedErrors(t *testing.T) {
	n, _ := testGV(t)
	other := loginOf(loginIDOf(otherNumber), "2")
	for name, c := range map[string]struct {
		inner bridgev2.NetworkConnector
		login *bridgev2.UserLogin
	}{
		"the bare connector": {n.GVConnector, other},
		"no connector":       {nil, other},
		"a nil network":      {(*gvNetwork)(nil), other},
		"no database":        {newGVNetwork(&connector.GVConnector{}), other},
		"no login":           {n, nil},
		"an empty login":     {n, &bridgev2.UserLogin{}},
	} {
		if end, err := dropRefused(t.Context(), c.inner, c.login); !errors.Is(err, errNotGVoice) || end != nil {
			t.Errorf("%s: %v %v", name, end != nil, err)
		}
	}
}

// TestDropRefusedFailsOnAStoreError: a delete the store refuses is the
// drop's error (other_account_kept), not taken for done.
func TestDropRefusedFailsOnAStoreError(t *testing.T) {
	n, db := testGV(t)
	prefixOf(t, n, loginIDOf(otherNumber))
	_, err := db.Exec(t.Context(), `ALTER TABLE gvoice_login_prefix RENAME TO gvoice_login_prefix_away`)
	must(t, err)
	if _, err := dropRefused(t.Context(), n, loginOf(loginIDOf(otherNumber), "1")); err == nil {
		t.Fatal("a failed delete counted as done")
	}
}

// TestDropOrphanPrefixes: at a start, a prefix row that is no stored
// login's (a sign-in cut off between the connector's prefix and NewLogin)
// is deleted; the stored logins' own rows stay, whatever the framework
// loaded; with no stored login every row goes. WHY: catches another
// account's number left in the store by a cut-off sign-in, and a sweep that
// takes a login's own prefix (its person ids would change at the next
// sign-in).
func TestDropOrphanPrefixes(t *testing.T) {
	n, db := testGV(t)
	mine := loginIDOf(mineNumber)
	prefixOf(t, n, mine)
	storeLogin(t, db, mine)
	own := prefixRows(t, db)
	prefixOf(t, n, loginIDOf(otherNumber))
	prefixOf(t, n, loginIDOf(""))
	must(t, dropOrphanPrefixes(t.Context(), n, nil, nil))
	if after := prefixRows(t, db); !slices.Equal(after, own) {
		t.Fatalf("after the sweep %v, want %v", after, own)
	}
	if err := dropOrphanPrefixes(t.Context(), n.GVConnector, nil, nil); !errors.Is(err, errNotGVoice) {
		t.Fatalf("the bare connector: %v", err)
	}
	_, err := db.Exec(t.Context(), `DELETE FROM user_login`)
	must(t, err)
	must(t, dropOrphanPrefixes(t.Context(), n, nil, nil))
	if after := prefixRows(t, db); len(after) != 0 {
		t.Fatalf("no stored login, rows kept: %v", after)
	}
}

// TestDropOrphanPrefixesSkipsWhenLoginsUnread: when the stored logins
// cannot be read, the sweep deletes nothing and says so (the backend logs
// prepare_logins_failed). WHY: catches a sweep that takes "no rows read"
// for "no login" and deletes the connection's own prefix.
func TestDropOrphanPrefixesSkipsWhenLoginsUnread(t *testing.T) {
	n, db := testGV(t)
	mine := loginIDOf(mineNumber)
	prefixOf(t, n, mine)
	storeLogin(t, db, mine)
	prefixOf(t, n, loginIDOf(otherNumber))
	_, err := db.Exec(t.Context(), `ALTER TABLE user_login RENAME TO user_login_away`)
	must(t, err)
	before := prefixRows(t, db)
	if err := dropOrphanPrefixes(t.Context(), n, nil, nil); err == nil {
		t.Fatal("logins unreadable, the sweep said done")
	}
	if after := prefixRows(t, db); !slices.Equal(after, before) {
		t.Fatalf("rows deleted with the logins unread: %v, want %v", after, before)
	}
}
