// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
)

// logBackend is testBackend with its log kept and a media spool.
func logBackend(t *testing.T) (*Backend, *store.Store, *lockedBuffer) {
	t.Helper()
	dir := t.TempDir()
	db, _, err := store.OpenDB(filepath.Join(dir, "worker.db"),
		store.OpenOptions{Key: bytes.Repeat([]byte{7}, store.KeyLen), DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	st, err := store.Open(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	out := &lockedBuffer{}
	log := logx.New(out, "c")
	b := New(&contract.Init{StateDir: dir}, st, NewEmitter(st, log, nil), log, Hooks{}, 1)
	if err := b.SetSpoolKey(bytes.Repeat([]byte{7}, store.KeyLen)); err != nil {
		t.Fatal(err)
	}
	return b, st, out
}

func countRows(t *testing.T, st *store.Store, q string) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func outboxLen(t *testing.T, st *store.Store) int {
	t.Helper()
	p, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(p)
}

// spoolFiles counts the spool's files (not the ones being written).
func spoolFiles(t *testing.T, b *Backend) int {
	t.Helper()
	entries, err := os.ReadDir(b.mediaDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			n++
		}
	}
	return n
}

const staleCode = `"code":"stale_write_refused"`

// TestStaleInstanceWritesRefused: a framework instance stopped by a logout
// whose goroutine outlives the stop writes, through the view and the intents
// it was given, after the wipe: a room announced, a room created, room
// state, a member (joined, and as member state), a person, a room deleted,
// uploaded bytes. Each write is refused with store.ErrWiped, leaves its
// table as the wipe left it (and nothing in the outbox or the spool), and is
// logged once per instance by its code; the same write through the instance
// made after the wipe is stored.
func TestStaleInstanceWritesRefused(t *testing.T) {
	key := networkid.PortalKey{ID: "chat-1", Receiver: "acct-b"}
	ghost := networkid.UserID("u-old")
	nameState := func(name string) *event.Event {
		return &event.Event{Type: event.StateRoomName, StateKey: new(string), Content: event.Content{Parsed: &event.RoomNameEventContent{Name: name}}}
	}
	cases := []struct {
		name string
		// room: the new instance announces the chat's room first
		// (announced), and creates it too (created).
		room string
		// rows counts what the write leaves in the store.
		rows string
		// write writes as fw. noErr: the call returns no error
		// (GenerateDeterministicRoomID).
		noErr bool
		write func(ctx context.Context, fw *instance, room id.RoomID) error
	}{
		{"remember room", "", `SELECT COUNT(*) FROM lynx_rooms`, true,
			func(ctx context.Context, fw *instance, _ id.RoomID) error {
				fw.GenerateDeterministicRoomID(key)
				return nil
			}},
		{"create room", "announced",
			`SELECT (SELECT COUNT(*) FROM lynx_rooms WHERE created = 1) + (SELECT COUNT(*) FROM lynx_state) + (SELECT COUNT(*) FROM lynx_members)`, false,
			func(ctx context.Context, fw *instance, room id.RoomID) error {
				_, err := fw.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{
					BeeperLocalRoomID: room, InitialState: []*event.Event{nameState("Old chat")},
					BeeperInitialMembers: []id.UserID{formatGhost(ghost)},
				})
				return err
			}},
		{"room state", "created", `SELECT COUNT(*) FROM lynx_state`, false,
			func(ctx context.Context, fw *instance, room id.RoomID) error {
				s := nameState("Old chat")
				_, err := fw.BotIntent().SendState(ctx, room, s.Type, "", &s.Content, time.Now())
				return err
			}},
		{"member joined", "created", `SELECT COUNT(*) FROM lynx_members`, false,
			func(ctx context.Context, fw *instance, room id.RoomID) error {
				return fw.GhostIntent(ghost).EnsureJoined(ctx, room)
			}},
		{"member state", "created", `SELECT COUNT(*) FROM lynx_members`, false,
			func(ctx context.Context, fw *instance, room id.RoomID) error {
				_, err := fw.BotIntent().SendState(ctx, room, event.StateMember, string(formatGhost(ghost)),
					&event.Content{Parsed: &event.MemberEventContent{Membership: event.MembershipJoin, Displayname: "Old Name"}}, time.Now())
				return err
			}},
		{"person", "", `SELECT COUNT(*) FROM lynx_people`, false,
			func(ctx context.Context, fw *instance, _ id.RoomID) error {
				return fw.GhostIntent(ghost).SetDisplayName(ctx, "Old Name")
			}},
		{"room deleted", "created", `SELECT COUNT(*) FROM lynx_rooms WHERE deleted = 1`, false,
			func(ctx context.Context, fw *instance, room id.RoomID) error {
				return fw.BotIntent().DeleteRoom(ctx, room, false)
			}},
		{"upload", "", `SELECT COUNT(*) FROM lynx_spool`, false,
			func(ctx context.Context, fw *instance, room id.RoomID) error {
				_, _, err := fw.GhostIntent(ghost).UploadMedia(ctx, room, []byte("old avatar bytes"), "old.png", "image/png")
				return err
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			b, st, logs := logBackend(t)
			old := b.newInstance()
			if err := st.Wipe(ctx); err != nil {
				t.Fatal(err)
			}
			fresh := b.newInstance()
			room := roomIDFor(key)
			if c.room != "" {
				fresh.GenerateDeterministicRoomID(key)
			}
			if c.room == "created" {
				if _, err := fresh.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{BeeperLocalRoomID: room}); err != nil {
					t.Fatal(err)
				}
			}
			before, events, files := countRows(t, st, c.rows), outboxLen(t, st), spoolFiles(t, b)
			if before != 0 {
				t.Fatalf("%d rows before the write: the case does not test it", before)
			}

			for i := range 2 {
				err := c.write(ctx, old, room)
				if !c.noErr && !errors.Is(err, store.ErrWiped) {
					t.Fatalf("write %d of the stopped instance: %v, want store.ErrWiped", i, err)
				}
			}
			if n := countRows(t, st, c.rows); n != 0 {
				t.Fatalf("the stopped instance's write left %d rows after the wipe", n)
			}
			if n := outboxLen(t, st); n != events {
				t.Fatalf("the stopped instance's write reported %d events after the wipe", n-events)
			}
			if n := spoolFiles(t, b); n != files {
				t.Fatalf("the stopped instance's upload left %d files in the spool", n-files)
			}
			if n := old.stale.Load(); n != 2 {
				t.Fatalf("%d refused writes counted, want 2", n)
			}
			if n := strings.Count(logs.String(), staleCode); n != 1 {
				t.Fatalf("%d log lines %s for two refused writes of one instance, want 1:\n%s", n, staleCode, logs)
			}

			if err := c.write(ctx, fresh, room); err != nil {
				t.Fatalf("the same write through the new instance: %v", err)
			}
			if n := countRows(t, st, c.rows); n == 0 {
				t.Fatal("the same write through the new instance stored nothing")
			}
			if n := fresh.stale.Load(); n != 0 {
				t.Fatalf("the new instance counted %d refused writes", n)
			}
		})
	}
}

// TestWipeWaitsForWriteInFlight: a write of an instance that is running when
// a wipe begins (a logout while a framework goroutine stores a profile) is
// waited for by the wipe, as an event is, and deleted by it: no row of the
// old epoch is left after the wipe, and what the instance does next (the
// person's report) is refused.
func TestWipeWaitsForWriteInFlight(t *testing.T) {
	ctx := context.Background()
	b, st, _ := logBackend(t)
	fw := b.newInstance()
	inside, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	b.writeHook = func() {
		once.Do(func() {
			close(inside)
			<-release
		})
	}
	wrote := make(chan error, 1)
	go func() { wrote <- fw.GhostIntent("u-old").SetDisplayName(ctx, "Old Name") }()
	select {
	case <-inside:
	case <-time.After(30 * time.Second):
		t.Fatal("the write did not begin")
	}
	wiped := make(chan error, 1)
	go func() { wiped <- st.Wipe(ctx) }()
	waitWipeBlocked(t, wiped)
	close(release)
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("the write begun before the wipe: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the write did not end")
	}
	select {
	case err := <-wiped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the wipe did not end after the write")
	}
	if n := countRows(t, st, `SELECT COUNT(*) FROM lynx_people`); n != 0 {
		t.Fatalf("%d people of the old epoch left after the wipe", n)
	}
	if n := outboxLen(t, st); n != 0 {
		t.Fatalf("%d events of the old epoch left after the wipe", n)
	}
	if n := fw.stale.Load(); n != 1 {
		t.Fatalf("%d refused writes after the wipe (the person's report), want 1", n)
	}
}

// waitWipeBlocked returns once a goroutine is in store.Wipe waiting for the
// wipe lock (the write in flight holds it for reading), and fails if the
// wipe ends first: then it did not wait for the write.
func waitWipeBlocked(t *testing.T, wiped <-chan error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "store.(*Store).Wipe(") && strings.Contains(g, "sync.(*RWMutex).Lock(") {
				return
			}
		}
		select {
		case err := <-wiped:
			t.Fatalf("the wipe ended (%v) while a write of the instance was in flight: it did not wait for it", err)
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no wipe waiting for the write in flight")
		}
	}
}

// TestInstanceIsTheFrameworksView: the framework is handed the instance, not
// the backend, so every intent it gets carries the instance's epoch
// (newFramework; checked here on NewBridge as newFramework calls it).
func TestInstanceIsTheFrameworksView(t *testing.T) {
	b, _, _ := logBackend(t)
	fw := b.newInstance()
	var mc bridgev2.MatrixConnector = fw
	if _, ok := mc.(interface{ BotIntent() bridgev2.MatrixAPI }); !ok {
		t.Fatal("the instance has no BotIntent: NewBridge would panic")
	}
	if _, ok := mc.(bridgev2.MatrixConnectorWithArbitraryRoomState); !ok {
		t.Fatal("the instance lost the backend's room state reads")
	}
	for _, in := range []bridgev2.MatrixAPI{fw.BotIntent(), fw.GhostIntent("u-x")} {
		if in.(*Intent).fw != fw {
			t.Fatal("an intent handed out by the instance does not carry it")
		}
	}
	dp, _, err := fw.NewUserIntent(context.Background(), OwnerMXID, "")
	if err != nil || dp.(*Intent).fw != fw {
		t.Fatalf("double puppet: %v", err)
	}
	if b.instanceOf(&bridgev2.Bridge{Matrix: fw}) != fw {
		t.Fatal("instanceOf does not find the instance of a bridge")
	}
}
