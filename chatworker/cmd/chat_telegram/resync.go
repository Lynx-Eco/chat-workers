// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// kvSyncLimits is the key, in the framework's kv_store, of the dialog-sync
// limits this worker last applied: "<update> <create>", the connector's
// Sync.UpdateLimit and Sync.CreateLimit as readSyncLimits set them. The
// kv_store is the framework's own table in the worker's store; a wipe (a
// logout, another account's login refused) empties it with the account, so
// a new sign-in starts with no record.
const kvSyncLimits database.Key = "lynx_telegram_sync_limits"

// syncLimitsKV is what applySyncLimits needs of the framework's kv_store
// (*database.KVQuery).
type syncLimitsKV interface {
	GetWithError(ctx context.Context, key database.Key) (string, error)
	SetWithError(ctx context.Context, key database.Key, value string) error
}

// syncedLogin is one stored login's connector metadata and how to save it.
type syncedLogin struct {
	meta *connector.UserLoginMetadata
	save func(ctx context.Context) error
}

// resyncOnGrownLimits is the worker's Hooks.PrepareLogins: at its start,
// before any login connects, the connector's dialog sync is made to run
// again when the sync limits grew (applySyncLimits).
//
// The connector makes rooms for the dialogs its dialog sync looks at, at a
// sign-in, and after that only resumes a sync that was cut off: once one is
// complete (UserLoginMetadata.DialogSyncComplete), every later connect skips
// it (chatsync.go syncChats, called with restart false at connect,
// client.go). Only its bridge command restarts it, and the worker runs no
// bridge command. So a connection whose host raised sync_create_limit or
// sync_update_limit after its sign-in (its user chose to import every chat)
// would never get the rooms. The client the connector made for each stored
// login holds that login's own metadata (client.go NewTelegramClient), so
// the reset here is what its connect-time sync reads.
func resyncOnGrownLimits(ctx context.Context, inner bridgev2.NetworkConnector, br *bridgev2.Bridge, logins []*bridgev2.UserLogin) error {
	tc, isTC := inner.(*connector.TelegramConnector)
	if !isTC || br == nil || br.DB == nil || br.DB.KV == nil {
		return errNotTelegram
	}
	synced := make([]syncedLogin, 0, len(logins))
	for _, l := range logins {
		meta, isMeta := l.Metadata.(*connector.UserLoginMetadata)
		if !isMeta {
			continue
		}
		synced = append(synced, syncedLogin{meta: meta, save: l.Save})
	}
	return applySyncLimits(ctx, br.DB.KV, tc.Config.Sync.UpdateLimit, tc.Config.Sync.CreateLimit, synced)
}

// applySyncLimits compares the limits of this start (update, create) with
// those last applied (kvSyncLimits). When either grew, each login's dialog
// sync is reset (resetDialogSync) and saved, so that the connector's
// connect-time sync runs again from the newest dialog with the new limits.
// A limit that shrank or stayed changes no login: the rooms made stay, and a
// sync cut off resumes as before. With no record (the first start of this
// build, or of a store a wipe emptied) nothing is reset: the sign-in's sync
// ran with the limits of its own start. The limits of this start are then
// recorded, after every reset is saved: a save that fails leaves the old
// record, and the next start tries again.
func applySyncLimits(ctx context.Context, kv syncLimitsKV, update, create int, logins []syncedLogin) error {
	stored, err := kv.GetWithError(ctx, kvSyncLimits)
	if errors.Is(err, sql.ErrNoRows) {
		stored, err = "", nil
	}
	if err != nil {
		return err
	}
	current := formatSyncLimits(update, create)
	if stored == current {
		return nil
	}
	if lastUpdate, lastCreate, known := parseSyncLimits(stored); known &&
		(limitGrew(update, lastUpdate) || limitGrew(create, lastCreate)) {
		for _, l := range logins {
			resetDialogSync(l.meta)
			if err := l.save(ctx); err != nil {
				return err
			}
		}
	}
	return kv.SetWithError(ctx, kvSyncLimits, current)
}

// resetDialogSync clears what the connector keeps of a login's dialog sync,
// the same five fields its syncChats clears when told to restart: the next
// sync begins at the newest dialog and counts from 0.
func resetDialogSync(meta *connector.UserLoginMetadata) {
	meta.DialogSyncComplete = false
	meta.DialogSyncCursor = networkid.PortalID("")
	meta.DialogSyncCount = 0
	meta.DialogSyncOffsetID = 0
	meta.DialogSyncOffsetDate = 0
}

func formatSyncLimits(update, create int) string {
	return fmt.Sprintf("%d %d", update, create)
}

// parseSyncLimits reads a kvSyncLimits value; known is false for none or one
// this worker did not write.
func parseSyncLimits(v string) (update, create int, known bool) {
	if v == "" {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(v, "%d %d", &update, &create); err != nil || formatSyncLimits(update, create) != v {
		return 0, 0, false
	}
	return update, create, true
}

// limitGrew tells whether the connector's limit now covers more dialogs than
// last: a negative limit is the connector's "no limit".
func limitGrew(now, last int) bool {
	if last < 0 {
		return false
	}
	return now < 0 || now > last
}
