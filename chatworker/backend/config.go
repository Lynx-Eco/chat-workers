// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/id"
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

// BridgeConfig is the framework configuration the backend fixes in code
// (spec: "Settings the backend fixes"). Nothing here is read from a file.
func BridgeConfig() *bridgeconfig.BridgeConfig {
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
			// No automatic history: history comes only on FetchHistory.
			MaxInitialMessages: 0,
			MaxCatchupMessages: 0,
			Queue: bridgeconfig.BackfillQueueConfig{
				Enabled:    false,
				BatchSize:  50,
				MaxBatches: -1,
			},
		},
	}
}
