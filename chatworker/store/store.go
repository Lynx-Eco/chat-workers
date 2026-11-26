// SPDX-License-Identifier: AGPL-3.0-or-later

// Package store holds the worker's own tables, kept in the same SQLite
// database (and the same *dbutil.Database) as the bridge framework's tables:
//
//   - lynx_outbox: the upward queue. Every event is written here before its
//     frame is queued, and deleted when the host acknowledges it.
//   - lynx_send_journal: one row per send key, written in_flight before the
//     network call.
//   - lynx_rooms, lynx_members, lynx_state, lynx_people: what the framework
//     asks a homeserver about later (room <-> chat, members, power levels,
//     room state, ghost profiles), and what was last reported upward.
//   - lynx_spool: bytes the connector pushed (uploads), kept as encrypted
//     files in the media spool.
//   - lynx_kv: durable counters (boot count, event generations).
//   - lynx_store_nonce: the store's nonce (Nonce), part of the id of every
//     event the worker generates.
//
// Nothing here is held only in memory.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/util/dbutil"
)

const schema = `
CREATE TABLE IF NOT EXISTS lynx_outbox (
	seq   INTEGER PRIMARY KEY AUTOINCREMENT,
	event BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS lynx_kv (
	name  TEXT PRIMARY KEY,
	value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS lynx_send_journal (
	send_key   TEXT PRIMARY KEY,
	chat_id    TEXT NOT NULL,
	kind       TEXT NOT NULL,
	state      TEXT NOT NULL CHECK (state IN ('in_flight', 'sent', 'rejected')),
	remote_id  TEXT NOT NULL DEFAULT '',
	code       TEXT NOT NULL DEFAULT '',
	boot       INTEGER NOT NULL,
	command_id TEXT NOT NULL DEFAULT '',
	-- what the command asked for (backend: commandFingerprint), so a key
	-- used again for something else is not taken for a repeat
	fingerprint TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS lynx_rooms (
	room_id        TEXT PRIMARY KEY,
	chat_id        TEXT NOT NULL,
	receiver       TEXT NOT NULL,
	created        INTEGER NOT NULL DEFAULT 0,
	is_direct      INTEGER NOT NULL DEFAULT 0,
	is_space       INTEGER NOT NULL DEFAULT 0,
	deleted        INTEGER NOT NULL DEFAULT 0,
	created_gen    INTEGER NOT NULL DEFAULT 0,
	info_hash      TEXT NOT NULL DEFAULT '',
	info_gen       INTEGER NOT NULL DEFAULT 0,
	abilities_hash TEXT NOT NULL DEFAULT '',
	abilities_gen  INTEGER NOT NULL DEFAULT 0,
	-- the portal id the room stands for now, when the framework re-IDed
	-- its portal after chat_id was reported ('': chat_id itself)
	portal_id      TEXT NOT NULL DEFAULT '',
	-- 1: a room made under a key whose room was re-IDed away from it
	-- (backend reid.go): nothing of it is reported, its chat_id is ''
	superseded     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS lynx_rooms_chat ON lynx_rooms (chat_id, receiver);
CREATE TABLE IF NOT EXISTS lynx_members (
	room_id     TEXT NOT NULL,
	user_id     TEXT NOT NULL,
	membership  TEXT NOT NULL,
	displayname TEXT NOT NULL DEFAULT '',
	avatar_url  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (room_id, user_id)
);
CREATE TABLE IF NOT EXISTS lynx_state (
	room_id   TEXT NOT NULL,
	type      TEXT NOT NULL,
	state_key TEXT NOT NULL,
	content   BLOB NOT NULL,
	PRIMARY KEY (room_id, type, state_key)
);
CREATE TABLE IF NOT EXISTS lynx_people (
	user_id   TEXT PRIMARY KEY,
	person_id TEXT NOT NULL,
	profile   BLOB NOT NULL,
	info_hash TEXT NOT NULL DEFAULT '',
	info_gen  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS lynx_store_nonce (
	id    INTEGER PRIMARY KEY CHECK (id = 1),
	nonce TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lynx_spool (
	sha256    TEXT PRIMARY KEY,
	mime_type TEXT NOT NULL,
	file_name TEXT NOT NULL,
	size      INTEGER NOT NULL
);
`

// Store is the worker's own tables.
type Store struct {
	db *dbutil.Database
	// outboxMu makes "assign seq, then hand over" one step for callers that
	// need the hand-over order to equal seq order.
	outboxMu sync.Mutex
	// floor is the host's stored mark (RaiseSeqFloor), under outboxMu.
	floor uint64
	// wipeMu is held by Wipe for writing and by Since for reading; epoch
	// counts the wipes. Wipe changes epoch while it holds wipeMu and
	// outboxMu, so it does not change while either is held; it is atomic so
	// that Epoch takes no lock (it is called inside Since too).
	wipeMu sync.RWMutex
	epoch  atomic.Uint64
	// nonce is the store's nonce (Nonce), as stored.
	nonce atomic.Pointer[string]
}

// ErrWiped: the store was wiped after the operation began (Since).
var ErrWiped = errors.New("store: wiped since the operation began")

// Epoch counts the wipes made in this process. An operation that must write
// nothing after a wipe (a send begun before a logout) reads it when it
// begins and writes through Since (or AppendEventSince).
func (s *Store) Epoch() uint64 {
	return s.epoch.Load()
}

// Since runs fn unless the store was wiped after epoch, and then returns
// ErrWiped. A Wipe waits for the fn running: fn's writes are made before
// the wipe (and deleted by it) or not at all. fn must not call Since or
// Wipe. Sends write through it, and so does every write the backend makes
// for a framework instance (the rooms, members, room state, people and spool
// rows), with the epoch of when the instance was made.
func (s *Store) Since(epoch uint64, fn func() error) error {
	s.wipeMu.RLock()
	defer s.wipeMu.RUnlock()
	if s.epoch.Load() != epoch {
		return ErrWiped
	}
	return fn()
}

// Open creates the tables if needed.
func Open(ctx context.Context, db *dbutil.Database) (*Store, error) {
	if _, err := db.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("create worker tables: %w", err)
	}
	// A store made before the journal had fingerprints.
	if has, err := db.ColumnExists(ctx, "lynx_send_journal", "fingerprint"); err != nil {
		return nil, err
	} else if !has {
		if _, err := db.Exec(ctx, `ALTER TABLE lynx_send_journal ADD COLUMN fingerprint TEXT NOT NULL DEFAULT ''`); err != nil {
			return nil, fmt.Errorf("add journal fingerprint: %w", err)
		}
	}
	// A store made before rooms followed a re-IDed portal.
	if has, err := db.ColumnExists(ctx, "lynx_rooms", "portal_id"); err != nil {
		return nil, err
	} else if !has {
		if _, err := db.Exec(ctx, `ALTER TABLE lynx_rooms ADD COLUMN portal_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return nil, fmt.Errorf("add room portal id: %w", err)
		}
	}
	// A store made before superseded rooms.
	if has, err := db.ColumnExists(ctx, "lynx_rooms", "superseded"); err != nil {
		return nil, err
	} else if !has {
		if _, err := db.Exec(ctx, `ALTER TABLE lynx_rooms ADD COLUMN superseded INTEGER NOT NULL DEFAULT 0`); err != nil {
			return nil, fmt.Errorf("add room superseded: %w", err)
		}
	}
	s := &Store{db: db}
	if err := s.loadNonce(ctx); err != nil {
		return nil, fmt.Errorf("store nonce: %w", err)
	}
	return s, nil
}

// nonceBytes is the size of the store's nonce, before hex.
const nonceBytes = 12

// Nonce is the store's nonce: 12 random bytes in hex, made when the store is
// made and made again by every Wipe. It is part of the id of every event the
// worker generates (its own PersonInfo, connection states, chat and person
// info by generation, abilities, removals, sign-in steps, edit echoes),
// whose other parts are counters and generations of this store: those start
// again in a new store file, and the generations in a wiped one, while the
// host lands each event id once for the life of the connection. With the
// nonce such an id never names an event of an earlier store, and it stays
// the same for a replay within the store (the nonce is kept until a wipe,
// which also empties the queue). Ids of the network's events (messages,
// edits, reactions, deletions, reads, deliveries, history pages) carry no
// nonce: the same network event is the same event after a store loss too.
func (s *Store) Nonce() string {
	return *s.nonce.Load()
}

func newNonce() (string, error) {
	b := make([]byte, nonceBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// loadNonce reads the store's nonce, making it when there is none (a new
// store, or one made before nonces).
func (s *Store) loadNonce(ctx context.Context) error {
	fresh, err := newNonce()
	if err != nil {
		return err
	}
	var nonce string
	err = retryBusy(ctx, func() error {
		if _, err := s.db.Exec(ctx, `INSERT INTO lynx_store_nonce (id, nonce) VALUES (1, $1) ON CONFLICT (id) DO NOTHING`, fresh); err != nil {
			return err
		}
		return s.db.QueryRow(ctx, `SELECT nonce FROM lynx_store_nonce WHERE id = 1`).Scan(&nonce)
	})
	if err != nil {
		return err
	}
	s.nonce.Store(&nonce)
	return nil
}

// DB is the shared database.
func (s *Store) DB() *dbutil.Database { return s.db }

// exec is Exec, retried while another writer holds the file (retryBusy).
func (s *Store) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := retryBusy(ctx, func() error {
		var err error
		res, err = s.db.Exec(ctx, q, args...)
		return err
	})
	return res, err
}

// StoredLoginIDs lists the framework's stored logins (read straight from
// the file, not the framework's cache).
func (s *Store) StoredLoginIDs(ctx context.Context) ([]string, error) {
	var ids []string
	err := retryBusy(ctx, func() error {
		ids = nil
		rows, err := s.db.Query(ctx, `SELECT id FROM user_login ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	})
	return ids, err
}

// ---- outbox ----

// QueuedEvent is one row of the upward queue.
type QueuedEvent struct {
	Seq   uint64
	Event []byte
}

// AppendEvent stores one encoded ChatEvent and returns its seq. While it
// runs, then(seq) is called under the outbox lock, so that a caller handing
// events on in then() hands them on in seq order.
func (s *Store) AppendEvent(ctx context.Context, event []byte, then func(seq uint64)) (uint64, error) {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	return s.appendEvent(ctx, event, then)
}

// AppendEventSince is AppendEvent unless the store was wiped after epoch
// (Epoch): then it writes nothing, calls nothing and returns ErrWiped. The
// epoch is compared under the outbox lock, which Wipe holds while it changes
// the epoch and empties the outbox, so an event is either written before the
// wipe (and deleted by it) or not at all: an emitter that began before a
// wipe and waited on the lock while it ran writes nothing after it. Unlike
// Since it takes no wipe lock, so it may be called inside Since.
func (s *Store) AppendEventSince(ctx context.Context, epoch uint64, event []byte, then func(seq uint64)) (uint64, error) {
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	if s.epoch.Load() != epoch {
		return 0, ErrWiped
	}
	return s.appendEvent(ctx, event, then)
}

// appendEvent is AppendEvent under the outbox lock.
func (s *Store) appendEvent(ctx context.Context, event []byte, then func(seq uint64)) (uint64, error) {
	var seq int64
	err := retryBusy(ctx, func() error {
		return s.db.QueryRow(ctx, `INSERT INTO lynx_outbox (event) VALUES ($1) RETURNING seq`, event).Scan(&seq)
	})
	if err != nil {
		return 0, err
	}
	if uint64(seq) <= s.floor {
		// Never reached while the floor is in sqlite_sequence; refused rather
		// than handed to a host that would drop it as a repeat.
		_, _ = s.exec(ctx, `DELETE FROM lynx_outbox WHERE seq = $1`, seq)
		return 0, fmt.Errorf("seq %d at or below the host's stored mark %d", seq, s.floor)
	}
	if then != nil {
		then(uint64(seq))
	}
	return uint64(seq), nil
}

// RaiseSeqFloor makes every seq given from now on greater than floor: the
// host's Init.host_stored_seq, the highest seq of this connection it has
// stored. The host takes every seq at or below it for a repeat, so a worker
// whose queue and numbering were lost or reset while its connection lived on
// numbers on above it. The floor is kept in the outbox's AUTOINCREMENT
// counter (sqlite_sequence, which Wipe keeps), so it is durable and only
// ever rises; queued events keep their seq. A floor of 0 changes nothing.
func (s *Store) RaiseSeqFloor(ctx context.Context, floor uint64) error {
	if floor == 0 {
		return nil
	}
	if floor > 1<<62 {
		return fmt.Errorf("seq floor %d out of range", floor)
	}
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	if floor > s.floor {
		s.floor = floor
	}
	return retryBusy(ctx, func() error {
		return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
			// sqlite_sequence has its row for the table only after the first
			// insert; it has no unique constraint, so insert it only if
			// missing.
			if _, err := s.db.Exec(ctx, `INSERT INTO sqlite_sequence (name, seq)
				SELECT 'lynx_outbox', 0 WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'lynx_outbox')`); err != nil {
				return err
			}
			_, err := s.db.Exec(ctx, `UPDATE sqlite_sequence SET seq = $1 WHERE name = 'lynx_outbox' AND seq < $1`, int64(floor))
			return err
		})
	})
}

// Pending is every queued event, oldest first.
func (s *Store) Pending(ctx context.Context) ([]QueuedEvent, error) {
	rows, err := s.db.Query(ctx, `SELECT seq, event FROM lynx_outbox ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueuedEvent
	for rows.Next() {
		var q QueuedEvent
		var seq int64
		if err := rows.Scan(&seq, &q.Event); err != nil {
			return nil, err
		}
		q.Seq = uint64(seq)
		out = append(out, q)
	}
	return out, rows.Err()
}

// Ack deletes every event with seq <= upTo.
func (s *Store) Ack(ctx context.Context, upTo uint64) (int64, error) {
	res, err := s.exec(ctx, `DELETE FROM lynx_outbox WHERE seq <= $1`, int64(upTo))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ---- counters ----

// Next increments the named counter and returns its new value (1 first).
func (s *Store) Next(ctx context.Context, name string) (uint64, error) {
	var v int64
	err := retryBusy(ctx, func() error {
		return s.db.QueryRow(ctx, `INSERT INTO lynx_kv (name, value) VALUES ($1, 1)
			ON CONFLICT (name) DO UPDATE SET value = lynx_kv.value + 1 RETURNING value`, name).Scan(&v)
	})
	return uint64(v), err
}

// ---- send journal ----

// SendState is the state of a journal row.
type SendState string

const (
	InFlight SendState = "in_flight"
	Sent     SendState = "sent"
	Rejected SendState = "rejected"
)

// JournalRow is one send key.
type JournalRow struct {
	Key       string
	ChatID    string
	Kind      string
	State     SendState
	RemoteID  string
	Code      string
	Boot      uint64
	CommandID string
	// Fingerprint is what the command asked for; empty in rows written
	// before it was kept.
	Fingerprint string
}

// JournalGet returns the row for key, nil when there is none.
func (s *Store) JournalGet(ctx context.Context, key string) (*JournalRow, error) {
	var r JournalRow
	var boot int64
	err := s.db.QueryRow(ctx, `SELECT send_key, chat_id, kind, state, remote_id, code, boot, command_id, fingerprint
		FROM lynx_send_journal WHERE send_key = $1`, key).
		Scan(&r.Key, &r.ChatID, &r.Kind, &r.State, &r.RemoteID, &r.Code, &boot, &r.CommandID, &r.Fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.Boot = uint64(boot)
	return &r, nil
}

// JournalBegin writes the row in_flight. It returns false (and writes
// nothing) when the key already has a row.
//
// The row is written with synchronous=FULL: it must be on disk before the
// network call, or a power loss could take it back after the message went
// out and the key would be sent again.
func (s *Store) JournalBegin(ctx context.Context, r JournalRow) (bool, error) {
	var n int64
	err := retryBusy(ctx, func() error { return s.journalBegin(ctx, r, &n) })
	return n == 1, err
}

func (s *Store) journalBegin(ctx context.Context, r JournalRow, n *int64) error {
	return withFullSync(ctx, s.db, func(conn *sql.Conn) error {
		res, err := conn.ExecContext(ctx, `INSERT INTO lynx_send_journal (send_key, chat_id, kind, state, boot, command_id, fingerprint)
			VALUES ($1, $2, $3, 'in_flight', $4, $5, $6) ON CONFLICT (send_key) DO NOTHING`,
			r.Key, r.ChatID, r.Kind, int64(r.Boot), r.CommandID, r.Fingerprint)
		if err != nil {
			return err
		}
		*n, err = res.RowsAffected()
		return err
	})
}

// JournalFinish records the final state of a key.
func (s *Store) JournalFinish(ctx context.Context, key string, state SendState, remoteID, code string) error {
	_, err := s.exec(ctx, `UPDATE lynx_send_journal SET state = $2, remote_id = $3, code = $4 WHERE send_key = $1`,
		key, string(state), remoteID, code)
	return err
}

// ---- rooms ----

// Room is one fake room and what was last reported about it.
type Room struct {
	RoomID string
	// ChatID is the chat's id in the contract: the id of the portal the
	// room was made for, for as long as the store lives.
	ChatID string
	// PortalID is the id of the portal the room stands for now: ChatID,
	// unless the framework gave the portal a new id since (RoomPortalMoved).
	PortalID   string
	Receiver   string
	Created    bool
	Direct     bool
	Space      bool
	Deleted    bool
	CreatedGen uint64
	// Superseded: the room was made for a portal under a key whose room
	// the framework had re-IDed away (RememberSupersededRoom). Nothing of
	// it is reported; its ChatID is "" and no chat id names it, until it
	// becomes a live room (SupersededRoomLive). PortalID is then the id of
	// the key it was superseded under.
	Superseded bool
}

// ReIDed tells whether the room's portal has another id than the one the
// room was first reported under.
func (r *Room) ReIDed() bool { return r.PortalID != r.ChatID }

// RememberRoom records which chat a room id stands for. The framework asks
// for the room id (GenerateDeterministicRoomID) before it creates the room,
// and the creation request does not name the chat. A row of the id that is
// no live room (removed, or never created) is about to stand for the portal
// chatID: what it kept of a portal it stood for before (its portal_id, the
// key it followed until that portal moved or went) is refreshed, so no
// lookup reads that key, and a row that was a superseded room with no chat
// id gets the chat id again (RoomCreated decides anew). A row of another
// chat id keeps it (and its generations, which that chat's next room goes
// on from) and stands for chatID's portal.
func (s *Store) RememberRoom(ctx context.Context, roomID, chatID, receiver string) error {
	_, err := s.exec(ctx, `INSERT INTO lynx_rooms (room_id, chat_id, receiver) VALUES ($1, $2, $3)
		ON CONFLICT (room_id) DO UPDATE SET
			superseded = CASE WHEN lynx_rooms.chat_id = '' THEN 0 ELSE lynx_rooms.superseded END,
			portal_id = CASE WHEN lynx_rooms.chat_id IN ('', excluded.chat_id) THEN '' ELSE excluded.chat_id END,
			chat_id = CASE WHEN lynx_rooms.chat_id = '' THEN excluded.chat_id ELSE lynx_rooms.chat_id END
		WHERE lynx_rooms.created = 0 OR lynx_rooms.deleted = 1`, roomID, chatID, receiver)
	return err
}

// RememberSupersededRoom records a superseded room (Room.Superseded) of the
// portal portalID: its chat id is empty, which no lookup by chat id finds
// (RoomOfChat), so it never shares one with the live room of that key. A
// row of the id that is no live room and has no chat id is refreshed to
// stand for portalID (RememberRoom). A row that has a chat id is not taken
// (backend.supersededRoom asks RoomLivedAsChat and gives another id).
func (s *Store) RememberSupersededRoom(ctx context.Context, roomID, portalID, receiver string) error {
	_, err := s.exec(ctx, `INSERT INTO lynx_rooms (room_id, chat_id, portal_id, receiver, superseded) VALUES ($1, '', $2, $3, 1)
		ON CONFLICT (room_id) DO UPDATE SET portal_id = excluded.portal_id, superseded = 1
		WHERE lynx_rooms.chat_id = '' AND (lynx_rooms.created = 0 OR lynx_rooms.deleted = 1)`, roomID, portalID, receiver)
	return err
}

// RoomLivedAsChat tells whether the room id has a row with a chat id: a
// room that is or was a chat of the host (a superseded room made live and
// removed since keeps its chat id and generations).
func (s *Store) RoomLivedAsChat(ctx context.Context, roomID string) (bool, error) {
	var n int
	err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM lynx_rooms WHERE room_id = $1 AND chat_id <> ''`, roomID).Scan(&n)
	return n > 0, err
}

// liveRow is the condition, on the alias o, of a row that is a live room:
// created, not deleted, not superseded. Only such a row is a chat: a row
// remembered for a room the framework never created
// (GenerateDeterministicRoomID, then a creation that was cancelled or
// failed: bridgev2's ReIDPortal cancels the creation in flight of a
// re-ID's target and deletes its portal row, which has no room, and calls
// no DeleteRoom) keeps created = 0 and deleted = 0 for good, and is none.
// At most one row of a login with a chat id is live (RoomCreated and
// SupersededRoomLive, each one statement or transaction, keep it so).
const liveRow = `o.created = 1 AND o.deleted = 0 AND o.superseded = 0`

// SupersededRoomLive makes a superseded room that is created and not
// deleted a live room of the chat id chatID (backend reid.go: the
// framework moved its portal onto a key that had no portal), and tells
// whether it did. It does not when another room of its login that is live
// (liveRow) has chatID: a chat id names one live room. A deleted room of
// chatID (its row stays after RoomDeleted, with its chat id and
// generations; its removal was reported) does not stop it, nor a row of
// chatID that was never created (it stays as it is, and is no chat): the
// room's generations then go on from the highest of the rows of chatID,
// so none of its event ids is one of theirs. What was last reported of
// the room is cleared: its next ChatInfo and abilities are reported.
func (s *Store) SupersededRoomLive(ctx context.Context, roomID, chatID string) (bool, error) {
	const others = `FROM lynx_rooms o WHERE o.chat_id = $2 AND o.receiver = lynx_rooms.receiver AND o.room_id <> $1`
	res, err := s.exec(ctx, `UPDATE lynx_rooms SET superseded = 0, chat_id = $2, portal_id = '',
		created_gen = created_gen + COALESCE((SELECT MAX(o.created_gen) `+others+`), 0),
		info_gen = info_gen + COALESCE((SELECT MAX(o.info_gen) `+others+`), 0),
		abilities_gen = abilities_gen + COALESCE((SELECT MAX(o.abilities_gen) `+others+`), 0),
		info_hash = '', abilities_hash = ''
		WHERE room_id = $1 AND superseded = 1 AND created = 1 AND deleted = 0 AND $2 <> ''
		AND NOT EXISTS (SELECT 1 `+others+` AND `+liveRow+`)`, roomID, chatID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// RoomPortalMoved records that the room now stands for the portal of id
// portalID (the framework re-IDed it); its chat id stays as it is.
func (s *Store) RoomPortalMoved(ctx context.Context, roomID, portalID string) error {
	_, err := s.exec(ctx, `UPDATE lynx_rooms SET portal_id = CASE WHEN chat_id = $2 THEN '' ELSE $2 END
		WHERE room_id = $1`, roomID, portalID)
	return err
}

const roomColumns = `room_id, chat_id, portal_id, receiver, created, is_direct, is_space, deleted, created_gen, superseded`

func scanRoom(row interface{ Scan(...any) error }) (*Room, error) {
	var r Room
	var gen int64
	err := row.Scan(&r.RoomID, &r.ChatID, &r.PortalID, &r.Receiver, &r.Created, &r.Direct, &r.Space, &r.Deleted, &gen, &r.Superseded)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if r.PortalID == "" {
		r.PortalID = r.ChatID
	}
	r.CreatedGen = uint64(gen)
	return &r, nil
}

// Room returns the room, nil when unknown.
func (s *Store) Room(ctx context.Context, roomID string) (*Room, error) {
	return scanRoom(s.db.QueryRow(ctx, `SELECT `+roomColumns+` FROM lynx_rooms WHERE room_id = $1`, roomID))
}

// LiveRooms lists every live room (liveRow), of every login.
func (s *Store) LiveRooms(ctx context.Context) ([]*Room, error) {
	rows, err := s.db.Query(ctx, `SELECT `+roomColumns+` FROM lynx_rooms o WHERE `+liveRow+` ORDER BY o.room_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Room
	for rows.Next() {
		r, err := scanRoom(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RoomOfChat returns the live room (liveRow) of a chat id of the contract
// (its ChatID) and login, nil when there is none. No order decides between
// rows: at most one row of a login with a chat id is live. A removed room,
// a row of a room never created and a superseded room (its chat id is
// empty) are never one.
func (s *Store) RoomOfChat(ctx context.Context, chatID, receiver string) (*Room, error) {
	if chatID == "" {
		return nil, nil
	}
	return scanRoom(s.db.QueryRow(ctx, `SELECT `+roomColumns+` FROM lynx_rooms o WHERE o.chat_id = $1 AND o.receiver = $2
		AND `+liveRow, chatID, receiver))
}

// RoomCreated marks the room created (again) and starts a new generation of
// it, so a chat removed and created again gets new event ids; its
// generations go on past every other row of its chat id and login (a room
// of that id made live from a superseded one, SupersededRoomLive, and
// removed since), so none of its event ids is one of theirs. A room whose
// chat id another live room of its login has (liveRow; that one's portal
// was re-IDed away from the key, or it is a superseded room made live
// under it) is made a superseded room instead (Room.Superseded, its chat
// id empty, PortalID the key it was made for): a chat id names one live
// room. It tells whether it did that. One transaction: the check and the
// write see the same rows.
func (s *Store) RoomCreated(ctx context.Context, roomID string, direct, space bool) (superseded bool, err error) {
	err = retryBusy(ctx, func() error {
		var err error
		superseded, err = s.roomCreatedTxn(ctx, roomID, direct, space)
		return err
	})
	return superseded, err
}

func (s *Store) roomCreatedTxn(ctx context.Context, roomID string, direct, space bool) (bool, error) {
	const others = `FROM lynx_rooms o WHERE o.chat_id = lynx_rooms.chat_id AND lynx_rooms.chat_id <> ''
		AND o.receiver = lynx_rooms.receiver AND o.room_id <> lynx_rooms.room_id`
	var n int64
	err := s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		res, err := s.db.Exec(ctx, `UPDATE lynx_rooms SET superseded = 1, chat_id = '',
			portal_id = CASE WHEN portal_id = '' THEN chat_id ELSE portal_id END
			WHERE room_id = $1 AND superseded = 0 AND EXISTS (SELECT 1 `+others+` AND `+liveRow+`)`, roomID)
		if err != nil {
			return err
		}
		if n, err = res.RowsAffected(); err != nil {
			return err
		}
		_, err = s.db.Exec(ctx, `UPDATE lynx_rooms SET created = 1, is_direct = $2, is_space = $3, deleted = 0,
			created_gen = MAX(created_gen, COALESCE((SELECT MAX(o.created_gen) `+others+`), 0)) + 1,
			info_gen = MAX(info_gen, COALESCE((SELECT MAX(o.info_gen) `+others+`), 0)),
			abilities_gen = MAX(abilities_gen, COALESCE((SELECT MAX(o.abilities_gen) `+others+`), 0)),
			info_hash = '', abilities_hash = '' WHERE room_id = $1`, roomID, direct, space)
		return err
	})
	return n > 0, err
}

// RoomDeleted marks the room deleted and drops its state and members.
func (s *Store) RoomDeleted(ctx context.Context, roomID string) error {
	return retryBusy(ctx, func() error { return s.roomDeletedTxn(ctx, roomID) })
}

func (s *Store) roomDeletedTxn(ctx context.Context, roomID string) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if _, err := s.db.Exec(ctx, `UPDATE lynx_rooms SET deleted = 1 WHERE room_id = $1`, roomID); err != nil {
			return err
		}
		if _, err := s.db.Exec(ctx, `DELETE FROM lynx_state WHERE room_id = $1`, roomID); err != nil {
			return err
		}
		_, err := s.db.Exec(ctx, `DELETE FROM lynx_members WHERE room_id = $1`, roomID)
		return err
	})
}

// bump compares hash with what was last reported in column hashCol and, when
// it differs, stores it and increments genCol. It returns the generation and
// whether anything changed.
func (s *Store) bump(ctx context.Context, table, keyCol, key, hashCol, genCol, hash string) (uint64, bool, error) {
	var gen int64
	q := fmt.Sprintf(`UPDATE %s SET %s = $2, %s = %s + 1 WHERE %s = $1 AND %s <> $2 RETURNING %s`,
		table, hashCol, genCol, genCol, keyCol, hashCol, genCol)
	err := retryBusy(ctx, func() error { return s.db.QueryRow(ctx, q, key, hash).Scan(&gen) })
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(gen), true, nil
}

// ChatInfoChanged records the hash of a chat-info report.
func (s *Store) ChatInfoChanged(ctx context.Context, roomID, hash string) (uint64, bool, error) {
	return s.bump(ctx, "lynx_rooms", "room_id", roomID, "info_hash", "info_gen", hash)
}

// AbilitiesChanged records the hash of a chat-abilities report.
func (s *Store) AbilitiesChanged(ctx context.Context, roomID, hash string) (uint64, bool, error) {
	return s.bump(ctx, "lynx_rooms", "room_id", roomID, "abilities_hash", "abilities_gen", hash)
}

// RoomsOfReceiver lists the room ids of one login's chats: its rooms that
// were created (a row of a room never created is none), removed and
// superseded ones included.
func (s *Store) RoomsOfReceiver(ctx context.Context, receiver string) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT room_id FROM lynx_rooms WHERE receiver = $1 AND created = 1`, receiver)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---- state ----

// PutState stores one state event's content (JSON).
func (s *Store) PutState(ctx context.Context, roomID, evtType, stateKey string, content []byte) error {
	_, err := s.exec(ctx, `INSERT INTO lynx_state (room_id, type, state_key, content) VALUES ($1, $2, $3, $4)
		ON CONFLICT (room_id, type, state_key) DO UPDATE SET content = excluded.content`,
		roomID, evtType, stateKey, content)
	return err
}

// GetState returns one state event's content, nil when absent.
func (s *Store) GetState(ctx context.Context, roomID, evtType, stateKey string) ([]byte, error) {
	var content []byte
	err := s.db.QueryRow(ctx, `SELECT content FROM lynx_state WHERE room_id = $1 AND type = $2 AND state_key = $3`,
		roomID, evtType, stateKey).Scan(&content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return content, err
}

// ---- members ----

// Member is one room member.
type Member struct {
	UserID      string
	Membership  string
	Displayname string
	AvatarURL   string
}

// SetMember stores a membership and tells whether it changed.
func (s *Store) SetMember(ctx context.Context, roomID string, m Member) (bool, error) {
	res, err := s.exec(ctx, `INSERT INTO lynx_members (room_id, user_id, membership, displayname, avatar_url)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (room_id, user_id) DO UPDATE SET membership = excluded.membership,
			displayname = excluded.displayname, avatar_url = excluded.avatar_url
		WHERE lynx_members.membership <> excluded.membership OR lynx_members.displayname <> excluded.displayname
			OR lynx_members.avatar_url <> excluded.avatar_url`,
		roomID, m.UserID, m.Membership, m.Displayname, m.AvatarURL)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// Members lists a room's members, ordered by user id.
func (s *Store) Members(ctx context.Context, roomID string) ([]Member, error) {
	rows, err := s.db.Query(ctx, `SELECT user_id, membership, displayname, avatar_url FROM lynx_members
		WHERE room_id = $1 ORDER BY user_id`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Member
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.UserID, &m.Membership, &m.Displayname, &m.AvatarURL); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ---- people ----

// PutProfile stores a ghost's profile (JSON) under its user id.
func (s *Store) PutProfile(ctx context.Context, userID, personID string, profile []byte) error {
	_, err := s.exec(ctx, `INSERT INTO lynx_people (user_id, person_id, profile) VALUES ($1, $2, $3)
		ON CONFLICT (user_id) DO UPDATE SET profile = excluded.profile`, userID, personID, profile)
	return err
}

// GetProfile returns a stored profile, nil when absent.
func (s *Store) GetProfile(ctx context.Context, userID string) ([]byte, error) {
	var p []byte
	err := s.db.QueryRow(ctx, `SELECT profile FROM lynx_people WHERE user_id = $1`, userID).Scan(&p)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

// PersonInfoChanged records the hash of a person-info report.
func (s *Store) PersonInfoChanged(ctx context.Context, userID, hash string) (uint64, bool, error) {
	return s.bump(ctx, "lynx_people", "user_id", userID, "info_hash", "info_gen", hash)
}

// PersonUnsent is the info hash of a person whose stored profile is not
// what was last reported of them: it came without an event of a chat that
// is sent, before they were reported, or its report was refused. It equals
// no content hash, so the next report of them is a change.
const PersonUnsent = "unsent"

// PersonInfoUnsent marks the person's stored profile as not reported
// (PersonUnsent), unless what was last reported has the hash it would
// have (hash).
func (s *Store) PersonInfoUnsent(ctx context.Context, userID, hash string) error {
	_, err := s.exec(ctx, `UPDATE lynx_people SET info_hash = $3 WHERE user_id = $1 AND info_hash <> $2`,
		userID, hash, PersonUnsent)
	return err
}

// PersonInfoRefused undoes the record of a person-info report of
// generation gen whose event was not stored: the person is marked as not
// reported (PersonUnsent).
func (s *Store) PersonInfoRefused(ctx context.Context, userID string, gen uint64) error {
	_, err := s.exec(ctx, `UPDATE lynx_people SET info_hash = $3 WHERE user_id = $1 AND info_gen = $2`,
		userID, int64(gen), PersonUnsent)
	return err
}

// ChatInfoRefused and AbilitiesRefused undo the record of a report of
// generation gen whose event was not stored: the next report is a change.
func (s *Store) ChatInfoRefused(ctx context.Context, roomID string, gen uint64) error {
	_, err := s.exec(ctx, `UPDATE lynx_rooms SET info_hash = '' WHERE room_id = $1 AND info_gen = $2`, roomID, int64(gen))
	return err
}

func (s *Store) AbilitiesRefused(ctx context.Context, roomID string, gen uint64) error {
	_, err := s.exec(ctx, `UPDATE lynx_rooms SET abilities_hash = '' WHERE room_id = $1 AND abilities_gen = $2`, roomID, int64(gen))
	return err
}

// Person returns a person's stored profile and the hash of what was last
// reported of them; nil when the person is not stored.
func (s *Store) Person(ctx context.Context, userID string) (profile []byte, infoHash string, err error) {
	err = s.db.QueryRow(ctx, `SELECT profile, info_hash FROM lynx_people WHERE user_id = $1`, userID).Scan(&profile, &infoHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", nil
	}
	return profile, infoHash, err
}

// ---- spool ----

// SpoolEntry describes bytes kept in the media directory.
type SpoolEntry struct {
	SHA256   string
	MimeType string
	FileName string
	Size     int64
}

// PutSpool records a spooled file.
func (s *Store) PutSpool(ctx context.Context, e SpoolEntry) error {
	_, err := s.exec(ctx, `INSERT INTO lynx_spool (sha256, mime_type, file_name, size) VALUES ($1, $2, $3, $4)
		ON CONFLICT (sha256) DO NOTHING`, e.SHA256, e.MimeType, e.FileName, e.Size)
	return err
}

// GetSpool returns a spooled file's entry, nil when unknown.
func (s *Store) GetSpool(ctx context.Context, sha string) (*SpoolEntry, error) {
	var e SpoolEntry
	err := s.db.QueryRow(ctx, `SELECT sha256, mime_type, file_name, size FROM lynx_spool WHERE sha256 = $1`, sha).
		Scan(&e.SHA256, &e.MimeType, &e.FileName, &e.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// ListSpool returns the digest of every spooled file.
func (s *Store) ListSpool(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT sha256 FROM lynx_spool`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sha string
		if err := rows.Scan(&sha); err != nil {
			return nil, err
		}
		out = append(out, sha)
	}
	return out, rows.Err()
}

// DeleteSpool forgets a spooled file (evicted).
func (s *Store) DeleteSpool(ctx context.Context, sha string) error {
	_, err := s.db.Exec(ctx, `DELETE FROM lynx_spool WHERE sha256 = $1`, sha)
	return err
}

// ---- wipe ----

// ErrCheckpointBusy: Wipe deleted the rows but could not truncate the
// write-ahead log, which still holds the deleted content.
var ErrCheckpointBusy = errors.New("store: the write-ahead log could not be truncated (a reader holds it)")

// KeptOnWipe are the only tables Wipe leaves as they are, beside the schema
// version tables (isVersionTable). None holds anything of an account or of a
// person:
//   - database_owner: the name of the program that owns the file (dbutil);
//   - lynx_kv: the worker's named counters (fixed names, integers), which keep
//     event ids and seqs from repeating after a wipe;
//   - sqlite_sequence: SQLite's last rowid of lynx_outbox, for the same
//     reason.
var KeptOnWipe = map[string]bool{"database_owner": true, "lynx_kv": true, "sqlite_sequence": true}

// isVersionTable tells whether a table has the shape of a dbutil schema
// version table: the columns (version, compat), or (version) as made before
// dbutil added compat, each declared INTEGER as dbutil declares them. The
// framework's ("version") and every child database's (a connector's own
// tables: mautrix-telegram's "telegram_version") are kept by Wipe: emptied,
// the next start would run the child's first upgrade again on tables that
// exist, and fail.
//
// They are found by their shape, not by their name, because nothing records
// which child databases exist (dbutil.Database.Child keeps no list and the
// framework does not know its connector's) and dbutil lets a child name its
// version table anything: a rule on the name could miss one silently. What
// such a table may keep is narrow (keepVersionRows): one row of small
// non-negative integers, as dbutil writes it (it deletes the table's rows
// before it inserts the one row of a new version). A table of that shape
// holding anything else is not dbutil's and is emptied like any other.
func isVersionTable(cols []column) bool {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		if !strings.EqualFold(c.typ, "INTEGER") {
			return false
		}
		names = append(names, c.name)
	}
	slices.Sort(names)
	return slices.Equal(names, []string{"compat", "version"}) || slices.Equal(names, []string{"version"})
}

// maxSchemaVersion bounds the numbers a version table keeps: a schema
// version counts upgrades, and no schema has had thousands. A larger number
// (a timestamp, a phone number, an id) is not a version.
const maxSchemaVersion = 1 << 16

// keepVersionRows deletes from a version-shaped table every row but one of
// small non-negative integers, and every row when more than one is left.
//
// The kept row's rowid is not bounded: a table of this shape that is not
// dbutil's could carry a large number in it (dbutil's own row has rowid 1,
// as it empties the table before each insert). Left as it is: it is one
// integer in a table of a shape no account data is written in by the
// framework or the connector, and renumbering it would rewrite dbutil's
// table for a case not seen.
func (s *Store) keepVersionRows(ctx context.Context, table string, cols []column) error {
	q := `DELETE FROM ` + quoteIdent(table) + ` WHERE NOT (typeof(version) = 'integer' AND version BETWEEN 0 AND $1)`
	if len(cols) == 2 {
		q += ` OR NOT (compat IS NULL OR (typeof(compat) = 'integer' AND compat BETWEEN 0 AND $1))`
	}
	if _, err := s.db.Exec(ctx, q, maxSchemaVersion); err != nil {
		return err
	}
	var n int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM `+quoteIdent(table)).Scan(&n); err != nil {
		return err
	}
	if n > 1 {
		_, err := s.db.Exec(ctx, `DELETE FROM `+quoteIdent(table))
		return err
	}
	return nil
}

// Wipe deletes every row of every table of the file, the framework's, the
// connector's and the worker's, but KeptOnWipe and the schema version tables
// (isVersionTable), which keep at most one row of small integers
// (keepVersionRows): the tables are read from the
// schema, so a table a later framework version adds is wiped too. Rows are
// overwritten as they are deleted (secure_delete, see OpenDB) and the
// write-ahead log is then truncated, so the deleted content does not stay in
// the file either.
//
// The framework keeps what it read in memory: the caller stops its instance
// before and starts a new one after.
//
// keep names more tables to leave as they are.
//
// Wipe starts a new epoch (Epoch, Since, AppendEventSince): what an
// operation begun before it would write after it is refused.
func (s *Store) Wipe(ctx context.Context, keep ...string) error {
	s.wipeMu.Lock()
	defer s.wipeMu.Unlock()
	s.outboxMu.Lock()
	defer s.outboxMu.Unlock()
	s.epoch.Add(1)
	nonce, err := newNonce()
	if err != nil {
		return err
	}
	err = retryBusy(ctx, func() error { return s.wipeTxn(ctx, keep, nonce) })
	if err != nil {
		return err
	}
	s.nonce.Store(&nonce)
	return s.truncateWAL(ctx)
}

// checkpointLimit bounds how long Wipe tries to truncate the write-ahead log
// while another connection reads it.
const checkpointLimit = 3 * time.Second

// truncateWAL copies the write-ahead log into the file and truncates it
// (wal_checkpoint(TRUNCATE)). The checkpoint reports in its busy column when
// it could not finish because a connection was reading the log: then the
// log still holds frames with the deleted rows. It is tried again, waiting
// little each time, until checkpointLimit; then Wipe fails with
// ErrCheckpointBusy. There is nothing else to fall back on: no SQLite
// operation removes frames a reader still holds (that reader's snapshot is
// made of them), and a wipe that reported success with them left would
// hide them.
func (s *Store) truncateWAL(ctx context.Context) error {
	conn, err := s.db.RawDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var prev int
	if err := conn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&prev); err != nil {
		return err
	}
	// Short waits on this connection, so that a reader that goes is seen
	// soon; the pool's wait is put back after.
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 100`); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), fmt.Sprintf(`PRAGMA busy_timeout = %d`, prev)) }()
	deadline := time.Now().Add(checkpointLimit)
	for {
		var busy, frames, copied int
		err := conn.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &copied)
		if err == nil && busy == 0 {
			return nil
		}
		if err != nil && !isBusy(err) {
			return err
		}
		if time.Now().After(deadline) {
			return errors.Join(ErrCheckpointBusy, err)
		}
		select {
		case <-ctx.Done():
			return errors.Join(ErrCheckpointBusy, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// wipeTxn empties the tables and, in the same transaction, gives the store
// a new nonce (lynx_store_nonce is emptied with the rest).
func (s *Store) wipeTxn(ctx context.Context, keep []string, nonce string) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		// Parents and children are deleted in one go; the keys are checked
		// at commit, when every table is empty.
		if _, err := s.db.Exec(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
			return err
		}
		rows, err := s.db.Query(ctx, `SELECT name FROM sqlite_master WHERE type = 'table'`)
		if err != nil {
			return err
		}
		var names []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				return err
			}
			if !KeptOnWipe[name] && !slices.Contains(keep, name) {
				names = append(names, name)
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		var tables []string
		for _, name := range names {
			cols, err := s.columns(ctx, name)
			if err != nil {
				return err
			}
			if !isVersionTable(cols) {
				tables = append(tables, name)
				continue
			}
			if err := s.keepVersionRows(ctx, name, cols); err != nil {
				return err
			}
		}
		for _, table := range tables {
			if _, err := s.db.Exec(ctx, `DELETE FROM `+quoteIdent(table)); err != nil {
				return err
			}
		}
		_, err = s.db.Exec(ctx, `INSERT INTO lynx_store_nonce (id, nonce) VALUES (1, $1)
			ON CONFLICT (id) DO UPDATE SET nonce = excluded.nonce`, nonce)
		return err
	})
}

func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// column is a table's column: its name and declared type.
type column struct{ name, typ string }

// columns are a table's columns.
func (s *Store) columns(ctx context.Context, table string) ([]column, error) {
	rows, err := s.db.Query(ctx, `SELECT name, type FROM pragma_table_info($1)`, table)
	if err != nil {
		return nil, err
	}
	var cols []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.typ); err != nil {
			_ = rows.Close()
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, errors.Join(rows.Err(), rows.Close())
}
