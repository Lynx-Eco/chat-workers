// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// memKV is the framework's kv_store in memory: a missing key answers
// sql.ErrNoRows, as KVQuery.GetWithError does.
type memKV struct {
	values  map[database.Key]string
	sets    int
	failGet error
}

func (m *memKV) GetWithError(_ context.Context, key database.Key) (string, error) {
	if m.failGet != nil {
		return "", m.failGet
	}
	v, ok := m.values[key]
	if !ok {
		return "", sql.ErrNoRows
	}
	return v, nil
}

func (m *memKV) SetWithError(_ context.Context, key database.Key, value string) error {
	if m.values == nil {
		m.values = map[database.Key]string{}
	}
	m.values[key] = value
	m.sets++
	return nil
}

// completedSync is a login's metadata after a dialog sync: complete, with
// the fields a cut-off sync would hold set too, so that a reset of any of
// them shows.
func completedSync() *connector.UserLoginMetadata {
	return &connector.UserLoginMetadata{
		LoginMethod:          connector.LoginFlowIDPhone,
		DialogSyncComplete:   true,
		DialogSyncCursor:     networkid.PortalID("1234567"),
		DialogSyncCount:      15,
		DialogSyncOffsetID:   99,
		DialogSyncOffsetDate: 1700000000,
		PinnedDialogs:        []networkid.PortalID{"42"},
	}
}

// login is one stored login whose saves are counted.
func login(meta *connector.UserLoginMetadata, saves *int, err error) syncedLogin {
	return syncedLogin{meta: meta, save: func(context.Context) error {
		*saves++
		return err
	}}
}

// TestSyncLimitsGrewResetsTheDialogSync: a start whose create or update limit
// is greater than the limits last applied resets every login's dialog sync
// (the five fields the connector's own restart clears, nothing else of the
// metadata) and saves it before the new limits are recorded, so that the
// connector's connect-time sync runs again with them. Mutants: no reset;
// the record written first (a failed save would lose the reset for good);
// only the create limit compared.
func TestSyncLimitsGrewResetsTheDialogSync(t *testing.T) {
	for _, c := range []struct {
		name           string
		last           string
		update, create int
		want           string
	}{
		{"create grew", "100 15", 1000, 1000, "1000 1000"},
		{"update grew", "100 15", 300, 15, "300 15"},
		{"create grew, update same", "200 15", 200, 200, "200 200"},
		{"no limit now", "100 15", -1, 15, "-1 15"},
	} {
		t.Run(c.name, func(t *testing.T) {
			kv := &memKV{values: map[database.Key]string{kvSyncLimits: c.last}}
			meta := completedSync()
			saves := 0
			if err := applySyncLimits(context.Background(), kv, c.update, c.create, []syncedLogin{login(meta, &saves, nil)}); err != nil {
				t.Fatal(err)
			}
			want := completedSync()
			want.DialogSyncComplete = false
			want.DialogSyncCursor = ""
			want.DialogSyncCount = 0
			want.DialogSyncOffsetID = 0
			want.DialogSyncOffsetDate = 0
			if !reflect.DeepEqual(meta, want) {
				t.Errorf("metadata %+v, want %+v", meta, want)
			}
			if saves != 1 {
				t.Errorf("saved %d times, want 1", saves)
			}
			if got := kv.values[kvSyncLimits]; got != c.want {
				t.Errorf("recorded %q, want %q", got, c.want)
			}
		})
	}

	// A save that fails keeps the old record: the next start resets again.
	kv := &memKV{values: map[database.Key]string{kvSyncLimits: "100 15"}}
	saves := 0
	err := applySyncLimits(context.Background(), kv, 1000, 1000, []syncedLogin{login(completedSync(), &saves, errors.New("disk"))})
	if err == nil {
		t.Fatal("a failed save was not reported")
	}
	if got := kv.values[kvSyncLimits]; got != "100 15" || kv.sets != 0 {
		t.Errorf("recorded %q (%d sets) after a failed save, want the old record", got, kv.sets)
	}
}

// TestSyncLimitsSameOrShrunkLeaveTheDialogSync: limits equal to those last
// applied, or smaller (either of them, or both), change no login and save
// none: the rooms made stay and a cut-off sync resumes as before. A smaller
// limit is recorded; the same one is not written again. Mutant: a reset on
// any change.
func TestSyncLimitsSameOrShrunkLeaveTheDialogSync(t *testing.T) {
	for _, c := range []struct {
		name           string
		last           string
		update, create int
		want           string
		sets           int
	}{
		{"same", "1000 1000", 1000, 1000, "1000 1000", 0},
		{"both shrank", "1000 1000", 100, 15, "100 15", 1},
		{"create shrank", "1000 1000", 1000, 15, "1000 15", 1},
		{"update shrank", "300 15", 100, 15, "100 15", 1},
		{"was no limit", "-1 15", 1000, 15, "1000 15", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			kv := &memKV{values: map[database.Key]string{kvSyncLimits: c.last}}
			meta := completedSync()
			saves := 0
			if err := applySyncLimits(context.Background(), kv, c.update, c.create, []syncedLogin{login(meta, &saves, nil)}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(meta, completedSync()) {
				t.Errorf("metadata changed: %+v", meta)
			}
			if saves != 0 {
				t.Errorf("saved %d times, want 0", saves)
			}
			if got := kv.values[kvSyncLimits]; got != c.want || kv.sets != c.sets {
				t.Errorf("recorded %q in %d sets, want %q in %d", got, kv.sets, c.want, c.sets)
			}
		})
	}
}

// TestSyncLimitsFirstStartRecordsOnly: with no record (the first start of
// this build, a store a wipe emptied, a connection not signed in yet) the
// limits are recorded and no login is touched: the sign-in's sync already
// ran with the limits of its start. A record this worker did not write is
// taken as none. A kv_store that cannot be read records nothing and resets
// nothing. Mutant: a missing record read as limits of 0 (every login reset
// at the first start).
func TestSyncLimitsFirstStartRecordsOnly(t *testing.T) {
	for _, last := range []string{"", "garbage", "100", "100 15 3"} {
		kv := &memKV{}
		if last != "" {
			kv.values = map[database.Key]string{kvSyncLimits: last}
		}
		meta := completedSync()
		saves := 0
		if err := applySyncLimits(context.Background(), kv, 1000, 1000, []syncedLogin{login(meta, &saves, nil)}); err != nil {
			t.Fatalf("%q: %v", last, err)
		}
		if !reflect.DeepEqual(meta, completedSync()) || saves != 0 {
			t.Errorf("%q: metadata %+v, %d saves; want untouched", last, meta, saves)
		}
		if got := kv.values[kvSyncLimits]; got != "1000 1000" {
			t.Errorf("%q: recorded %q", last, got)
		}
	}

	// No login yet (before the first sign-in): recorded all the same.
	kv := &memKV{}
	if err := applySyncLimits(context.Background(), kv, 100, 15, nil); err != nil {
		t.Fatal(err)
	}
	if got := kv.values[kvSyncLimits]; got != "100 15" {
		t.Errorf("recorded %q", got)
	}

	failing := &memKV{failGet: errors.New("busy")}
	meta := completedSync()
	saves := 0
	if err := applySyncLimits(context.Background(), failing, 1000, 1000, []syncedLogin{login(meta, &saves, nil)}); err == nil {
		t.Fatal("a failed read was not reported")
	}
	if failing.sets != 0 || saves != 0 || !reflect.DeepEqual(meta, completedSync()) {
		t.Errorf("after a failed read: %d sets, %d saves, metadata %+v", failing.sets, saves, meta)
	}
}

// TestPrepareLoginsHook: the worker's hooks run resyncOnGrownLimits at its
// start, and it refuses anything but the Telegram connector without
// touching a login.
func TestPrepareLoginsHook(t *testing.T) {
	if h := hooks().PrepareLogins; h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(resyncOnGrownLimits).Pointer() {
		t.Fatal("hooks().PrepareLogins is not resyncOnGrownLimits")
	}
	if err := resyncOnGrownLimits(context.Background(), nil, nil, nil); !errors.Is(err, errNotTelegram) {
		t.Fatalf("err %v, want errNotTelegram", err)
	}
}
