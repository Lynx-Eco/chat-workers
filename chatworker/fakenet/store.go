// SPDX-License-Identifier: AGPL-3.0-or-later

package fakenet

import (
	"context"
	"errors"
	"sync/atomic"

	"go.mau.fi/util/dbutil"
)

// The fake network keeps its sessions in a table of its own, in the worker's
// file, through a dbutil child database with its own version table, as real
// connectors do (mautrix-telegram: telegram_version and its session table).
const (
	VersionTable = "fakenet_version"
	SessionTable = "fakenet_session"
)

// SessionToken is the prefix of the session secret a sign-in stores; tests
// check it never survives a logout in the store's files.
const SessionToken = "FAKESESSION-"

// upgrades is the child database's schema. Like a real connector's, the first
// step creates its table without IF NOT EXISTS: it runs once, when the
// version table says v0.
var upgrades = dbutil.BuildUpgradeTable().WithRaw(0, 1, 0, "fakenet sessions", dbutil.TxnModeOn,
	func(ctx context.Context, db *dbutil.Database) error {
		_, err := db.Exec(ctx, `CREATE TABLE `+SessionTable+` (
			login_id TEXT PRIMARY KEY,
			phone    TEXT NOT NULL,
			token    TEXT NOT NULL
		)`)
		return err
	}).Finish()

// SchemaVersion is the version the child database's table records.
var SchemaVersion = len(upgrades)

// starts counts the connector starts of this process (FailRestart).
var starts atomic.Int64

var errScriptedRestart = errors.New("fakenet: scripted failure to start again")

func (c *Connector) Start(ctx context.Context) error {
	if n := starts.Add(1); c.script.FailRestart && n > 1 {
		return errScriptedRestart
	}
	return c.db.Upgrade(ctx)
}

// saveSession stores what a signed-in session needs at the network, as a
// real connector stores its session.
func (c *Connector) saveSession(ctx context.Context, loginID, phone string) error {
	_, err := c.db.Exec(ctx, `INSERT INTO `+SessionTable+` (login_id, phone, token) VALUES ($1, $2, $3)
		ON CONFLICT (login_id) DO UPDATE SET phone = excluded.phone, token = excluded.token`,
		loginID, phone, SessionToken+loginID)
	return err
}
