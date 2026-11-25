// SPDX-License-Identifier: AGPL-3.0-or-later

package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
)

// testOpts opens with a fixed key: encrypted in the libsqlite3 build, plain
// in the plainstore development build.
func testOpts() OpenOptions {
	return OpenOptions{Key: bytes.Repeat([]byte{7}, KeyLen), DevPlain: true}
}

func openTest(t *testing.T, dir string) *Store {
	t.Helper()
	db, _, err := OpenDB(filepath.Join(dir, "w.db"), testOpts(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := Open(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOutboxOrderAckAndReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, dir)
	var handed []uint64
	for i := 0; i < 5; i++ {
		seq, err := s.AppendEvent(ctx, []byte{byte(i)}, func(seq uint64) { handed = append(handed, seq) })
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq %d, want %d", seq, i+1)
		}
	}
	if len(handed) != 5 || handed[4] != 5 {
		t.Fatalf("handed %v", handed)
	}
	if n, err := s.Ack(ctx, 3); err != nil || n != 3 {
		t.Fatalf("ack: %d %v", n, err)
	}
	_ = s.db.Close()
	s = openTest(t, dir)
	pending, err := s.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].Seq != 4 || !bytes.Equal(pending[1].Event, []byte{4}) {
		t.Fatalf("pending after reopen: %+v", pending)
	}
	// Seqs keep rising after acks and a wipe.
	if err := s.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	seq, err := s.AppendEvent(ctx, []byte{9}, nil)
	if err != nil || seq != 6 {
		t.Fatalf("seq after wipe %d %v", seq, err)
	}
}

func TestJournal(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	ok, err := s.JournalBegin(ctx, JournalRow{Key: "k1", ChatID: "c", Kind: "send", Boot: 1, CommandID: "cmd-1"})
	if err != nil || !ok {
		t.Fatalf("begin: %v %v", ok, err)
	}
	ok, err = s.JournalBegin(ctx, JournalRow{Key: "k1", ChatID: "c", Kind: "send", Boot: 2})
	if err != nil || ok {
		t.Fatalf("second begin must not insert: %v %v", ok, err)
	}
	r, err := s.JournalGet(ctx, "k1")
	if err != nil || r.State != InFlight || r.Boot != 1 || r.CommandID != "cmd-1" {
		t.Fatalf("row %+v %v", r, err)
	}
	if err := s.JournalFinish(ctx, "k1", Sent, "remote-1", ""); err != nil {
		t.Fatal(err)
	}
	r, _ = s.JournalGet(ctx, "k1")
	if r.State != Sent || r.RemoteID != "remote-1" {
		t.Fatalf("row %+v", r)
	}
	if r, _ := s.JournalGet(ctx, "nope"); r != nil {
		t.Fatalf("unknown key found")
	}
}

func TestRoomsMembersGenerations(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	if err := s.RememberRoom(ctx, "!r", "chat", "login"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RoomCreated(ctx, "!r", true, false); err != nil {
		t.Fatal(err)
	}
	r, err := s.Room(ctx, "!r")
	if err != nil || r == nil || !r.Direct || r.CreatedGen != 1 || r.ChatID != "chat" {
		t.Fatalf("room %+v %v", r, err)
	}
	gen, changed, err := s.ChatInfoChanged(ctx, "!r", "h1")
	if err != nil || !changed || gen != 1 {
		t.Fatalf("first: %d %v %v", gen, changed, err)
	}
	if _, changed, _ := s.ChatInfoChanged(ctx, "!r", "h1"); changed {
		t.Fatalf("same hash reported as changed")
	}
	gen, changed, _ = s.ChatInfoChanged(ctx, "!r", "h2")
	if !changed || gen != 2 {
		t.Fatalf("second: %d %v", gen, changed)
	}
	gen, changed, _ = s.ChatInfoChanged(ctx, "!r", "h1")
	if !changed || gen != 3 {
		t.Fatalf("back to the first value must be a new generation: %d %v", gen, changed)
	}
	if ch, err := s.SetMember(ctx, "!r", Member{UserID: "@a", Membership: "join"}); err != nil || !ch {
		t.Fatalf("member: %v %v", ch, err)
	}
	if ch, _ := s.SetMember(ctx, "!r", Member{UserID: "@a", Membership: "join"}); ch {
		t.Fatalf("unchanged member reported as changed")
	}
	if ch, _ := s.SetMember(ctx, "!r", Member{UserID: "@a", Membership: "leave"}); !ch {
		t.Fatalf("membership change not reported")
	}
	ms, _ := s.Members(ctx, "!r")
	if len(ms) != 1 || ms[0].Membership != "leave" {
		t.Fatalf("members %+v", ms)
	}
	if err := s.RoomDeleted(ctx, "!r"); err != nil {
		t.Fatal(err)
	}
	if ms, _ := s.Members(ctx, "!r"); len(ms) != 0 {
		t.Fatalf("members survive deletion")
	}
	if n, _ := s.Next(ctx, "boot"); n != 1 {
		t.Fatalf("counter %d", n)
	}
	if n, _ := s.Next(ctx, "boot"); n != 2 {
		t.Fatalf("counter %d", n)
	}
}

// TestDurableAndOverwriting: every connection of the pool overwrites deleted
// rows (secure_delete) and commits with synchronous=NORMAL (1); a write
// through withFullSync (the journal's in_flight row) runs with FULL (2), and
// its connection goes back to the pool at NORMAL.
func TestDurableAndOverwriting(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	check := func(want int) {
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var syncMode, secure int
				if err := s.db.QueryRow(ctx, `PRAGMA synchronous`).Scan(&syncMode); err != nil || syncMode != want {
					t.Errorf("synchronous %d (%v), want %d", syncMode, err, want)
				}
				if err := s.db.QueryRow(ctx, `PRAGMA secure_delete`).Scan(&secure); err != nil || secure != 1 {
					t.Errorf("secure_delete %d (%v), want 1", secure, err)
				}
			}()
		}
		wg.Wait()
	}
	check(1)
	err := withFullSync(ctx, s.db, func(conn *sql.Conn) error {
		var mode int
		if err := conn.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&mode); err != nil || mode != 2 {
			t.Errorf("synchronous inside withFullSync %d (%v), want 2", mode, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.JournalBegin(ctx, JournalRow{Key: "k", ChatID: "c", Kind: "send", Boot: 1}); err != nil {
		t.Fatal(err)
	}
	check(1)
}

// TestJournalFingerprint: the fingerprint is kept, and a store made before
// it was kept gets the column.
func TestJournalFingerprint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, _, err := OpenDB(filepath.Join(dir, "w.db"), testOpts(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE lynx_send_journal (
		send_key TEXT PRIMARY KEY, chat_id TEXT NOT NULL, kind TEXT NOT NULL,
		state TEXT NOT NULL CHECK (state IN ('in_flight', 'sent', 'rejected')),
		remote_id TEXT NOT NULL DEFAULT '', code TEXT NOT NULL DEFAULT '', boot INTEGER NOT NULL,
		command_id TEXT NOT NULL DEFAULT '');
		INSERT INTO lynx_send_journal (send_key, chat_id, kind, state, boot) VALUES ('old', 'c', 'send', 'sent', 1)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	s := openTest(t, dir)
	if r, err := s.JournalGet(ctx, "old"); err != nil || r == nil || r.Fingerprint != "" {
		t.Fatalf("old row %+v %v", r, err)
	}
	if _, err := s.JournalBegin(ctx, JournalRow{Key: "new", ChatID: "c", Kind: "send", Boot: 2, Fingerprint: "f1"}); err != nil {
		t.Fatal(err)
	}
	if r, err := s.JournalGet(ctx, "new"); err != nil || r.Fingerprint != "f1" {
		t.Fatalf("new row %+v %v", r, err)
	}
}

// TestRoomFollowsPortal: a store made before rooms followed a re-IDed
// portal gets the column, its rooms standing for their chat ids; a room
// moved to another portal id keeps its chat id, is found by that chat id
// (never by the new one) and is moved back when its portal gets its first
// id again.
func TestRoomFollowsPortal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, _, err := OpenDB(filepath.Join(dir, "w.db"), testOpts(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE lynx_rooms (
		room_id TEXT PRIMARY KEY, chat_id TEXT NOT NULL, receiver TEXT NOT NULL,
		created INTEGER NOT NULL DEFAULT 0, is_direct INTEGER NOT NULL DEFAULT 0, is_space INTEGER NOT NULL DEFAULT 0,
		deleted INTEGER NOT NULL DEFAULT 0, created_gen INTEGER NOT NULL DEFAULT 0,
		info_hash TEXT NOT NULL DEFAULT '', info_gen INTEGER NOT NULL DEFAULT 0,
		abilities_hash TEXT NOT NULL DEFAULT '', abilities_gen INTEGER NOT NULL DEFAULT 0);
		INSERT INTO lynx_rooms (room_id, chat_id, receiver, created) VALUES ('!old', 'chat:1', 'login', 1)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	s := openTest(t, dir)
	r, err := s.Room(ctx, "!old")
	if err != nil || r == nil || r.ChatID != "chat:1" || r.PortalID != "chat:1" || r.ReIDed() || r.Superseded {
		t.Fatalf("old room %+v %v", r, err)
	}
	if err := s.RoomPortalMoved(ctx, "!old", "channel:2"); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Room(ctx, "!old"); err != nil || r.ChatID != "chat:1" || r.PortalID != "channel:2" || !r.ReIDed() {
		t.Fatalf("moved room %+v %v", r, err)
	}
	if r, err := s.RoomOfChat(ctx, "chat:1", "login"); err != nil || r == nil || r.RoomID != "!old" || r.PortalID != "channel:2" {
		t.Fatalf("room of the chat id %+v %v", r, err)
	}
	for _, c := range [][2]string{{"channel:2", "login"}, {"chat:1", "other"}} {
		if r, err := s.RoomOfChat(ctx, c[0], c[1]); err != nil || r != nil {
			t.Fatalf("room of %v: %+v %v, want none", c, r, err)
		}
	}
	if err := s.RoomPortalMoved(ctx, "!old", "chat:1"); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Room(ctx, "!old"); r.PortalID != "chat:1" || r.ReIDed() {
		t.Fatalf("room moved back %+v", r)
	}
	if n := countPortalIDs(t, s); n != 0 {
		t.Fatalf("%d rows keep a portal id equal to their chat id", n)
	}
}

// TestSupersededRoom: a superseded room (made under a key whose room was
// re-IDed away) is stored with the key it was made for and no chat id: no
// lookup by chat id finds it, the live room's id and "" included; it is
// remembered once, forgotten by a wipe like every room, and the column is
// there again after the wipe (TestRoomFollowsPortal: added to an old
// store on open).
func TestSupersededRoom(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	if err := s.RememberRoom(ctx, "!live", "chat:1", "login"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RoomCreated(ctx, "!live", false, false); err != nil {
		t.Fatal(err)
	}
	if err := s.RoomPortalMoved(ctx, "!live", "channel:1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.RememberSupersededRoom(ctx, "!old", "chat:1", "login"); err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.Room(ctx, "!old")
	if err != nil || r == nil || !r.Superseded || r.ChatID != "" || r.PortalID != "chat:1" || r.Receiver != "login" {
		t.Fatalf("superseded room %+v %v", r, err)
	}
	if _, err := s.RoomCreated(ctx, "!old", false, false); err != nil {
		t.Fatal(err)
	}
	if live, err := s.RoomOfChat(ctx, "chat:1", "login"); err != nil || live == nil || live.RoomID != "!live" || live.Superseded {
		t.Fatalf("room of the live chat id: %+v %v", live, err)
	}
	if r, err := s.RoomOfChat(ctx, "", "login"); err != nil || r != nil {
		t.Fatalf("room of no chat id: %+v %v", r, err)
	}
	if err := s.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Room(ctx, "!old"); r != nil {
		t.Fatalf("a wipe kept the superseded room %+v", r)
	}
	if err := s.RememberSupersededRoom(ctx, "!old", "chat:1", "login"); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Room(ctx, "!old"); r == nil || !r.Superseded {
		t.Fatalf("after the wipe: %+v", r)
	}
}

// TestSupersededRoomLive: a superseded room becomes a live room of a chat
// id only when no live room of its login has that id; a deleted room of
// the id does not stop it, and its generations go on past that room's, so
// none of its event ids repeats one of the deleted room's. A deleted or a
// live room is not made live again; another login's room of the id does
// not count.
func TestSupersededRoomLive(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	live := func(room, chat string) bool {
		t.Helper()
		ok, err := s.SupersededRoomLive(ctx, room, chat)
		must(err)
		return ok
	}
	// The live room of channel:1, three times created, reported twice.
	must(s.RememberRoom(ctx, "!prev", "channel:1", "login"))
	for range 3 {
		created(ctx, s, t, "!prev")
	}
	_, _, err := s.ChatInfoChanged(ctx, "!prev", "a")
	must(err)
	_, _, err = s.ChatInfoChanged(ctx, "!prev", "b")
	must(err)
	_, _, err = s.AbilitiesChanged(ctx, "!prev", "a")
	must(err)
	must(s.RememberSupersededRoom(ctx, "!sup", "chat:1", "login"))
	created(ctx, s, t, "!sup")
	// Another login's live room of the id does not count.
	must(s.RememberRoom(ctx, "!other", "channel:1", "login2"))
	created(ctx, s, t, "!other")
	if live("!sup", "channel:1") {
		t.Fatal("made live while a live room has the chat id")
	}
	if r, _ := s.Room(ctx, "!sup"); r == nil || !r.Superseded || r.ChatID != "" || r.PortalID != "chat:1" {
		t.Fatalf("the room kept superseded: %+v", r)
	}
	if live("!sup", "") {
		t.Fatal("made live under no chat id")
	}
	must(s.RoomDeleted(ctx, "!prev"))
	if !live("!sup", "channel:1") {
		t.Fatal("not made live once the room of the chat id is deleted")
	}
	r, _ := s.Room(ctx, "!sup")
	if r == nil || r.Superseded || r.ChatID != "channel:1" || r.PortalID != "channel:1" || r.ReIDed() || !r.Created || r.Deleted || r.CreatedGen != 4 {
		t.Fatalf("the live room %+v, want chat channel:1, generation 4", r)
	}
	if got, _ := s.RoomOfChat(ctx, "channel:1", "login"); got == nil || got.RoomID != "!sup" {
		t.Fatalf("the chat id names %+v, want the room made live", got)
	}
	if gen, changed, _ := s.ChatInfoChanged(ctx, "!sup", "a"); !changed || gen != 3 {
		t.Errorf("its first ChatInfo: generation %d changed %v, want 3 (after the deleted room's 2)", gen, changed)
	}
	if gen, changed, _ := s.AbilitiesChanged(ctx, "!sup", "a"); !changed || gen != 2 {
		t.Errorf("its first abilities: generation %d changed %v, want 2", gen, changed)
	}
	if live("!sup", "channel:1") {
		t.Error("a live room made live again")
	}
	must(s.RememberSupersededRoom(ctx, "!gone", "chat:2", "login"))
	must(s.RoomDeleted(ctx, "!gone"))
	if live("!gone", "channel:2") {
		t.Error("a deleted superseded room made live")
	}
}

// created is RoomCreated of a room that is not made superseded.
func created(ctx context.Context, s *Store, t *testing.T, room string) {
	t.Helper()
	superseded, err := s.RoomCreated(ctx, room, false, false)
	if err != nil || superseded {
		t.Fatalf("RoomCreated(%s): superseded %v, %v", room, superseded, err)
	}
}

// TestNeverCreatedRoomIsNoChat: a row remembered for a room the framework
// never created (GenerateDeterministicRoomID, then the creation cancelled:
// bridgev2's ReIDPortal onto a key whose portal row has no room, which
// deletes that row and calls no DeleteRoom; created = 0, deleted = 0 for
// good) is no chat: the chat id names the live room, never that row,
// whatever the room ids' order ("!r..." < "!s..."); a superseded room
// moved onto its key is made live; the row stays as it was. A room of the
// id created later, while the room made live is live, is made a
// superseded room (one live room a chat id); created once that one is
// removed, it is the live room again, its generations past the removed
// one's. RoomsOfReceiver lists no never-created row.
func TestNeverCreatedRoomIsNoChat(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// The chat id names nothing while its only row was never created.
	must(s.RememberRoom(ctx, "!r-never", "channel:1", "login"))
	if r, err := s.RoomOfChat(ctx, "channel:1", "login"); err != nil || r != nil {
		t.Fatalf("a never created row named: %+v %v", r, err)
	}
	// The superseded room whose portal moved onto channel:1, reported
	// before under no chat id.
	must(s.RememberSupersededRoom(ctx, "!s-moved", "chat:1", "login"))
	created(ctx, s, t, "!s-moved")
	_, _, err := s.ChatInfoChanged(ctx, "!s-moved", "a")
	must(err)
	made, err := s.SupersededRoomLive(ctx, "!s-moved", "channel:1")
	if err != nil || !made {
		t.Fatalf("made live while only a never created row has the id: %v %v", made, err)
	}
	if r, err := s.RoomOfChat(ctx, "channel:1", "login"); err != nil || r == nil || r.RoomID != "!s-moved" {
		t.Fatalf("channel:1 names %+v %v, want the room made live", r, err)
	}
	never, _ := s.Room(ctx, "!r-never")
	if never == nil || never.Created || never.Deleted || never.Superseded || never.ChatID != "channel:1" || never.CreatedGen != 0 {
		t.Fatalf("the never created row %+v, want it as it was", never)
	}
	if ids, err := s.RoomsOfReceiver(ctx, "login"); err != nil || fmt.Sprint(ids) != "[!s-moved]" {
		t.Fatalf("the login's rooms %v %v, want the created one alone", ids, err)
	}
	// A creation of the remembered id while the room made live is live:
	// a superseded room, which no chat id names.
	superseded, err := s.RoomCreated(ctx, "!r-never", false, false)
	if err != nil || !superseded {
		t.Fatalf("created beside the live room: superseded %v %v", superseded, err)
	}
	r, _ := s.Room(ctx, "!r-never")
	if r == nil || !r.Superseded || r.ChatID != "" || r.PortalID != "channel:1" || !r.Created {
		t.Fatalf("the room created beside the live one %+v", r)
	}
	if r, _ := s.RoomOfChat(ctx, "channel:1", "login"); r == nil || r.RoomID != "!s-moved" {
		t.Fatalf("channel:1 names %+v, want the room made live", r)
	}
	// The live room removed: a room of the id created then is the live
	// room, its generations past the removed one's.
	must(s.RememberRoom(ctx, "!r-later", "channel:1", "login"))
	must(s.RoomDeleted(ctx, "!s-moved"))
	if r, _ := s.RoomOfChat(ctx, "channel:1", "login"); r != nil {
		t.Fatalf("channel:1 names %+v after its room was removed", r)
	}
	moved, _ := s.Room(ctx, "!s-moved")
	created(ctx, s, t, "!r-later")
	later, _ := s.Room(ctx, "!r-later")
	if later == nil || later.CreatedGen != moved.CreatedGen+1 {
		t.Fatalf("the later room %+v, want generation %d", later, moved.CreatedGen+1)
	}
	if gen, changed, _ := s.ChatInfoChanged(ctx, "!r-later", "a"); !changed || gen != 2 {
		t.Errorf("its first ChatInfo: generation %d, want 2 (after the removed room's 1)", gen)
	}
	if r, _ := s.RoomOfChat(ctx, "channel:1", "login"); r == nil || r.RoomID != "!r-later" {
		t.Fatalf("channel:1 names %+v, want the room created later", r)
	}
}

// TestRoomOfChatOneLiveRow: whatever rows a login has with a chat id (a
// never created one, removed ones, superseded ones, another login's live
// one, at room ids before and after the live one's), the chat id names
// its one live room; created again while another is live, a room is
// made superseded, not a second live room of the id.
func TestRoomOfChatOneLiveRow(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Room ids around the live one's: "!a" < "!m" < "!z".
	must(s.RememberRoom(ctx, "!a-never", "chat:1", "login"))
	must(s.RememberRoom(ctx, "!b-removed", "chat:1", "login"))
	created(ctx, s, t, "!b-removed")
	must(s.RoomDeleted(ctx, "!b-removed"))
	must(s.RememberRoom(ctx, "!m-live", "chat:1", "login"))
	created(ctx, s, t, "!m-live")
	must(s.RememberRoom(ctx, "!y-other", "chat:1", "login2"))
	created(ctx, s, t, "!y-other")
	must(s.RememberRoom(ctx, "!z-never", "chat:1", "login"))
	must(s.RememberSupersededRoom(ctx, "!0-sup", "chat:1", "login"))
	created(ctx, s, t, "!0-sup")
	for _, login := range []string{"login", "login2"} {
		want := map[string]string{"login": "!m-live", "login2": "!y-other"}[login]
		if r, err := s.RoomOfChat(ctx, "chat:1", login); err != nil || r == nil || r.RoomID != want {
			t.Fatalf("chat:1 of %s names %+v %v, want %s", login, r, err, want)
		}
	}
	// A second creation under the id while one is live.
	for _, room := range []string{"!a-never", "!b-removed"} {
		if superseded, err := s.RoomCreated(ctx, room, false, false); err != nil || !superseded {
			t.Fatalf("%s created beside the live room: superseded %v %v", room, superseded, err)
		}
	}
	var live int
	must(s.db.QueryRow(ctx, `SELECT COUNT(*) FROM lynx_rooms WHERE chat_id = 'chat:1' AND receiver = 'login'
		AND created = 1 AND deleted = 0 AND superseded = 0`).Scan(&live))
	if live != 1 {
		t.Fatalf("%d live rows of chat:1, want 1", live)
	}
	if r, _ := s.RoomOfChat(ctx, "chat:1", "login"); r == nil || r.RoomID != "!m-live" {
		t.Fatalf("chat:1 names %+v, want !m-live", r)
	}
	// The live room created again (a chat removed and made again) stays
	// the live room: another row is not a live one.
	created(ctx, s, t, "!m-live")
}

func countPortalIDs(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(context.Background(), `SELECT COUNT(*) FROM lynx_rooms WHERE portal_id <> ''`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestWritesWaitForABusyStore: while another connection holds the write
// lock longer than SQLite's busy_timeout (5 s), the writes that carry an
// upward event or a profile wait for it instead of failing (an event lost
// that way was never reported).
func TestWritesWaitForABusyStore(t *testing.T) {
	if testing.Short() {
		t.Skip("holds the store for 6 s")
	}
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	if err := s.PutProfile(ctx, "@g", "g", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	blocker, err := s.db.RawDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	type result struct {
		what string
		err  error
	}
	results := make(chan result, 3)
	go func() {
		_, err := s.AppendEvent(ctx, []byte{1}, nil)
		results <- result{"AppendEvent", err}
	}()
	go func() { results <- result{"PutProfile", s.PutProfile(ctx, "@g", "g", []byte(`{"displayname":"x"}`))} }()
	go func() {
		_, _, err := s.PersonInfoChanged(ctx, "@g", "h1")
		results <- result{"PersonInfoChanged", err}
	}()
	time.Sleep(6 * time.Second)
	if _, err := blocker.ExecContext(ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Errorf("%s failed while the store was busy: %v", r.what, r.err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("a write did not finish")
		}
	}
	if p, _ := s.Pending(ctx); len(p) != 1 {
		t.Fatalf("%d events stored", len(p))
	}
}

// childDB is a connector's dbutil child database: its own version table and
// one table of account data, created by its first upgrade (no IF NOT EXISTS,
// as real connectors write it).
func childDB(s *Store, versionTable, dataTable string) *dbutil.Database {
	table := dbutil.BuildUpgradeTable().WithRaw(0, 1, 0, "create", dbutil.TxnModeOn, func(ctx context.Context, db *dbutil.Database) error {
		_, err := db.Exec(ctx, `CREATE TABLE `+dataTable+` (login_id TEXT PRIMARY KEY, token TEXT NOT NULL)`)
		return err
	}).Finish()
	return s.db.Child(versionTable, table, nil)
}

// TestWipeKeepsVersionTables: Wipe keeps every dbutil version table, the
// framework's and every child database's whatever its name, so that the
// next start does not run the child's first upgrade again on its existing
// tables; it empties the child's data table; and a version table keeps
// nothing but integers. A table that only looks like one by its name is
// emptied.
func TestWipeKeepsVersionTables(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	if err := s.db.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	children := []*dbutil.Database{childDB(s, "conn_version", "conn_session"), childDB(s, "conn2_schema", "conn2_session")}
	for i, c := range children {
		if err := c.Upgrade(ctx); err != nil {
			t.Fatal(err)
		}
		data := []string{"conn_session", "conn2_session"}[i]
		if _, err := s.db.Exec(ctx, `INSERT INTO `+data+` (login_id, token) VALUES ('me', 'SECRET')`); err != nil {
			t.Fatal(err)
		}
	}
	// A version-shaped table with text in it, and a *_version table that is
	// not dbutil's. Tables of the shape that hold what dbutil never writes:
	// several rows, a number no schema version reaches, a negative compat;
	// and one whose columns are not declared INTEGER.
	if _, err := s.db.Exec(ctx, `CREATE TABLE odd (version INTEGER, compat INTEGER);
		INSERT INTO odd (version, compat) VALUES ('SECRET', 1), (1, 1);
		CREATE TABLE notes_version (version INTEGER, note TEXT);
		INSERT INTO notes_version VALUES (1, 'SECRET');
		CREATE TABLE pins (version INTEGER, compat INTEGER);
		INSERT INTO pins VALUES (1234, 5678), (4321, 8765);
		CREATE TABLE phone (version INTEGER);
		INSERT INTO phone VALUES (15550000000);
		CREATE TABLE neg (version INTEGER, compat INTEGER);
		INSERT INTO neg VALUES (3, -42);
		CREATE TABLE untyped (version, compat);
		INSERT INTO untyped VALUES (7, 7)`); err != nil {
		t.Fatal(err)
	}
	if err := s.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	// The restart: every child upgrades again, finds its version and runs
	// nothing.
	for _, c := range children {
		if err := c.Upgrade(ctx); err != nil {
			t.Fatalf("child upgrade after Wipe: %v", err)
		}
	}
	count := func(q string) int {
		var n int
		if err := s.db.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for table, want := range map[string]int{
		"conn_version": 1, "conn2_schema": 1, "conn_session": 0, "conn2_session": 0,
		"odd": 1, "notes_version": 0, "pins": 0, "phone": 0, "neg": 0, "untyped": 0,
	} {
		if n := count(`SELECT COUNT(*) FROM "` + table + `"`); n != want {
			t.Errorf("%s: %d rows after Wipe, want %d", table, n, want)
		}
	}
	if n := count(`SELECT COUNT(*) FROM odd WHERE typeof(version) <> 'integer'`); n != 0 {
		t.Errorf("a version table keeps a value that is not an integer")
	}
}

// TestWipeCheckpointBusy: while another connection reads (it holds frames of
// the write-ahead log), the checkpoint after the wipe cannot truncate the
// log, and Wipe fails rather than report success with the deleted content
// still in the log. A reader that goes away while Wipe tries again does not
// fail it, and the log is then empty.
func TestWipeCheckpointBusy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, dir)
	read := func() *sql.Conn {
		if err := s.PutProfile(ctx, "@g", "g", []byte(`{"displayname":"SECRET"}`)); err != nil {
			t.Fatal(err)
		}
		c, err := s.db.RawDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if _, err := c.ExecContext(ctx, `BEGIN`); err != nil {
			t.Fatal(err)
		}
		if err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM lynx_people`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return c
	}
	end := func(c *sql.Conn) {
		_, _ = c.ExecContext(ctx, `COMMIT`)
		_ = c.Close()
	}
	c := read()
	err := s.Wipe(ctx)
	end(c)
	if !errors.Is(err, ErrCheckpointBusy) {
		t.Fatalf("Wipe with a reader holding the log: %v, want ErrCheckpointBusy", err)
	}
	c = read()
	go func() {
		time.Sleep(300 * time.Millisecond)
		end(c)
	}()
	if err := s.Wipe(ctx); err != nil {
		t.Fatalf("Wipe after the reader went: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "w.db-wal")); err != nil || fi.Size() != 0 {
		t.Fatalf("log after Wipe: %v %v", fi, err)
	}
}

// TestSinceAndWipe: Since runs nothing for an epoch a wipe ended, and a Wipe
// waits for the Since running, so its writes are wiped.
func TestSinceAndWipe(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	epoch := s.Epoch()
	inside, release := make(chan struct{}), make(chan struct{})
	sinceDone := make(chan error, 1)
	go func() {
		sinceDone <- s.Since(epoch, func() error {
			close(inside)
			<-release
			_, err := s.AppendEvent(ctx, []byte("before"), nil)
			return err
		})
	}()
	<-inside
	wiped := make(chan error, 1)
	go func() { wiped <- s.Wipe(ctx) }()
	select {
	case err := <-wiped:
		t.Fatalf("Wipe did not wait for the running Since: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-sinceDone; err != nil {
		t.Fatal(err)
	}
	if err := <-wiped; err != nil {
		t.Fatal(err)
	}
	if err := s.Since(epoch, func() error { _, err := s.AppendEvent(ctx, []byte("after"), nil); return err }); !errors.Is(err, ErrWiped) {
		t.Fatalf("Since after the wipe: %v", err)
	}
	if p, _ := s.Pending(ctx); len(p) != 0 {
		t.Fatalf("%d events left", len(p))
	}
	if s.Epoch() != epoch+1 {
		t.Fatalf("epoch %d", s.Epoch())
	}
}

// TestAppendEventSinceWipe: an append for an epoch a wipe ended writes
// nothing and calls nothing; one for the current epoch is written. It takes
// no wipe lock, so it may run inside Since.
func TestAppendEventSinceWipe(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	epoch := s.Epoch()
	if err := s.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	called := false
	if seq, err := s.AppendEventSince(ctx, epoch, []byte("old"), func(uint64) { called = true }); !errors.Is(err, ErrWiped) || seq != 0 || called {
		t.Fatalf("append for the old epoch: seq %d, %v, then called %v", seq, err, called)
	}
	var seq uint64
	err := s.Since(s.Epoch(), func() error {
		var err error
		seq, err = s.AppendEventSince(ctx, s.Epoch(), []byte("new"), nil)
		return err
	})
	if err != nil || seq == 0 {
		t.Fatalf("append for the current epoch: seq %d, %v", seq, err)
	}
	if p, _ := s.Pending(ctx); len(p) != 1 || string(p[0].Event) != "new" {
		t.Fatalf("outbox %v", p)
	}
}

func TestSeqFloor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, dir)
	// Empty outbox, no sqlite_sequence row yet.
	if err := s.RaiseSeqFloor(ctx, 41); err != nil {
		t.Fatal(err)
	}
	seq, err := s.AppendEvent(ctx, []byte{1}, nil)
	if err != nil || seq != 42 {
		t.Fatalf("first seq above the floor: %d %v", seq, err)
	}
	// A lower floor changes nothing.
	if err := s.RaiseSeqFloor(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if seq, err := s.AppendEvent(ctx, []byte{2}, nil); err != nil || seq != 43 {
		t.Fatalf("seq after a lower floor: %d %v", seq, err)
	}
	// A floor above queued events: they keep their seq, new ones go above.
	if err := s.RaiseSeqFloor(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if seq, err := s.AppendEvent(ctx, []byte{3}, nil); err != nil || seq != 101 {
		t.Fatalf("seq after raising the floor: %d %v", seq, err)
	}
	pending, err := s.Pending(ctx)
	if err != nil || len(pending) != 3 || pending[0].Seq != 42 || pending[1].Seq != 43 || pending[2].Seq != 101 {
		t.Fatalf("pending %+v %v", pending, err)
	}
	// Durable: after acks and a reopen, numbering goes on above 101.
	if _, err := s.Ack(ctx, 101); err != nil {
		t.Fatal(err)
	}
	_ = s.db.Close()
	s = openTest(t, dir)
	if seq, err := s.AppendEvent(ctx, []byte{4}, nil); err != nil || seq != 102 {
		t.Fatalf("seq after reopen: %d %v", seq, err)
	}
	// After a wipe, too.
	if err := s.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	if seq, err := s.AppendEvent(ctx, []byte{5}, nil); err != nil || seq != 103 {
		t.Fatalf("seq after wipe: %d %v", seq, err)
	}
	// A zero floor is a no-op on a fresh store.
	fresh := openTest(t, t.TempDir())
	if err := fresh.RaiseSeqFloor(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if seq, err := fresh.AppendEvent(ctx, []byte{6}, nil); err != nil || seq != 1 {
		t.Fatalf("seq with no floor: %d %v", seq, err)
	}
}

// TestNonce: a store gets a nonce when it is made, keeps it across reopens,
// gets a new one at every wipe (kept across reopens too), and another store
// file gets another one.
func TestNonce(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, dir)
	first := s.Nonce()
	if len(first) != 2*nonceBytes {
		t.Fatalf("nonce of %d chars", len(first))
	}
	if again := openTest(t, dir).Nonce(); again != first {
		t.Fatal("a reopen changed the nonce")
	}
	if other := openTest(t, t.TempDir()).Nonce(); other == first {
		t.Fatal("two store files have one nonce")
	}
	seen := map[string]bool{first: true}
	for i := 0; i < 3; i++ {
		if err := s.Wipe(ctx); err != nil {
			t.Fatal(err)
		}
		n := s.Nonce()
		if seen[n] {
			t.Fatalf("wipe %d kept or repeated the nonce", i)
		}
		seen[n] = true
		if again := openTest(t, dir).Nonce(); again != n {
			t.Fatalf("wipe %d: the reopened store has another nonce", i)
		}
	}
}
