// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"

	"lynx.invalid/ingest/chatworker/backend"
)

// What the backend needs to answer a history fetch that met Telegram's
// FLOOD_WAIT as rate_limited with the wait (backend/history.go).
//
// The connector's FetchMessages retries its getHistory request up to 5
// times, sleeping out each FLOOD_WAIT shorter than 5 minutes
// (pkg/connector/backfill.go, tgerr.FloodWait with its default maximum), and
// takes no option to stop it. A longer wait, or a fifth in a row, comes back
// as the FLOOD_WAIT error itself (floodWait reads it). A wait it sleeps out
// is seen only by the line tgerr.FloodWait logs to the context's logger just
// before it sleeps (watchFloodWaits); the backend then ends the fetch at
// once (its context) when the wait is too long to sleep out inside the
// command. TestFloodWaitHooks pins both against the connector's code.

// floodWaitLog and floodWaitField are the message and the duration field of
// the warning tgerr.FloodWait logs before it sleeps out a FLOOD_WAIT
// (pkg/gotd/tgerr/flood_wait.go: zerolog.Ctx(ctx).Warn().Dur("duration",
// d).Msg("Waiting on flood wait")).
const (
	floodWaitLog   = "Waiting on flood wait"
	floodWaitField = "duration"
)

// floodWait is the worker's Hooks.RateLimitWait: a FLOOD_WAIT (or
// FLOOD_PREMIUM_WAIT) error, wrapped or not, and the wait it names.
func floodWait(err error) (time.Duration, bool) { return tgerr.AsFloodWait(err) }

// watchFloodWaits is the worker's Hooks.WatchRateLimit.
func watchFloodWaits(ctx context.Context, onWait func(time.Duration)) context.Context {
	return backend.WatchLogWaits(ctx, floodWaitLog, floodWaitField, onWait)
}
