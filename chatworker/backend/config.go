// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
)

// ServerName is the made-up server part of every Matrix-shaped id the
// framework sees. `.invalid` never resolves.
const ServerName = "lynx.invalid"

// OwnerMXID is the one local user every login belongs to.
const OwnerMXID = id.UserID("@owner:" + ServerName)

// BotMXID is the framework's bot identity. It is required by the framework
// and never acts on anything.
const BotMXID = id.UserID("@lynxbot:" + ServerName)

// CommandPrefix is set to something nobody types: the framework routes any
// message body that starts with the prefix to its command processor
// (bridgev2/queue.go). It begins with invisible separators.
const CommandPrefix = "⁣⁣lynx-internal-command"

// History settings (Init.settings), each a count of messages per chat.
const (
	// SettingInitialMessages is how many of a chat's newest messages the
	// worker imports when it first makes the chat's room: at sign-in, for
	// each chat the connector makes in its initial sync, and later for a
	// chat made when a message comes in it. 0 turns it off.
	SettingInitialMessages = "max_initial_messages"
	// SettingCatchupMessages is how many messages the worker imports, at
	// most, for a chat it already has when the connector says it missed
	// some (a resync whose newest message is newer than the newest stored:
	// mautrix-telegram after a channel or supergroup gap too long for its
	// update stream, mautrix-whatsapp for a history sync of a chat it
	// has). 0 turns it off.
	SettingCatchupMessages = "max_catchup_messages"
	// DefaultInitialMessages: a new connection's chats open with their
	// recent conversation, not empty (a Telegram sign-in landed every chat
	// with no message before this was on).
	DefaultInitialMessages = 50
	// DefaultCatchupMessages: what a worker that was down missed in a gap
	// the network does not replay, bounded.
	DefaultCatchupMessages = 100
	// MaxHistorySetting bounds both settings: one chat's import is one
	// framework batch (split into HistoryBatch events of maxBatchMessages),
	// and every chat made at sign-in asks the network for that many.
	MaxHistorySetting = 500
)

// History is what the worker imports by itself, with no FetchHistory.
type History struct {
	Initial int
	Catchup int
}

// ReadHistory reads the history settings from Init: each absent or empty
// is its default; a value that is not a decimal count from 0 to
// MaxHistorySetting refuses the start (bad_settings), so a typo does not
// turn history off or flood a network unseen.
func ReadHistory(init *contract.Init) (History, error) {
	h := History{Initial: DefaultInitialMessages, Catchup: DefaultCatchupMessages}
	for _, s := range []struct {
		key string
		to  *int
	}{{SettingInitialMessages, &h.Initial}, {SettingCatchupMessages, &h.Catchup}} {
		v := strings.TrimSpace(init.GetSettings()[s.key])
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > MaxHistorySetting || strings.TrimLeft(v, "0123456789") != "" {
			return History{}, &SettingsError{Code: "bad_settings"}
		}
		*s.to = n
	}
	return h, nil
}

// BridgeConfig is the framework configuration the backend fixes in code
// (spec: "Settings the backend fixes"), with the history settings h.
// Nothing here is read from a file.
func BridgeConfig(h History) *bridgeconfig.BridgeConfig {
	return &bridgeconfig.BridgeConfig{
		CommandPrefix: CommandPrefix,
		// Synchronous handling: QueueMatrixEvent returns the handling result,
		// and a remote event is fully handled when QueueRemoteEvent returns.
		PortalEventBuffer: 0,
		AsyncEvents:       false,
		// Outgoing messages get the deterministic event id
		// (GenerateDeterministicEventID) once the network accepted them.
		OutgoingMessageReID: true,
		// A repeated transaction id is answered from the database.
		DeduplicateMatrixMessages: true,
		// Every chat belongs to exactly one login.
		SplitPortals:          true,
		PrivateChatPortalMeta: true,
		BridgeStatusNotices:   "none",
		// Without this `tel:` identifiers are stripped from ghost profiles.
		PhoneNumbersInProfile: true,
		// Lynx decides what to erase; the backend erases on Logout itself.
		CleanupOnLogout: bridgeconfig.CleanupOnLogouts{Enabled: false},
		Permissions: bridgeconfig.PermissionConfig{
			string(OwnerMXID): &bridgeconfig.PermissionLevelAdmin,
		},
		Backfill: bridgeconfig.BackfillConfig{
			Enabled: true,
			// A room the framework makes for a chat the connector can
			// backfill gets the chat's newest h.Initial messages, fetched
			// in the room's creation, before any later event of the chat
			// (bridgev2 portal.go createMatrixRoom, doForwardBackfill): the
			// host sends no command for it. They reach the host as
			// HistoryBatch events with no command id (batchSend). A chat
			// resync that says messages were missed fetches up to
			// h.Catchup newer than the newest stored. Older history comes
			// only on FetchHistory: the backward queue is off.
			MaxInitialMessages: h.Initial,
			MaxCatchupMessages: h.Catchup,
			Queue: bridgeconfig.BackfillQueueConfig{
				Enabled:    false,
				BatchSize:  50,
				MaxBatches: -1,
			},
		},
	}
}
