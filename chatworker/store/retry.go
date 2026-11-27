// SPDX-License-Identifier: AGPL-3.0-or-later

package store

import (
	"context"
	"strings"
	"time"
)

// busyLimit bounds how long a write is tried again while another writer
// holds the file. Each try already waits SQLite's busy_timeout (5 s, set by
// the driver).
const busyLimit = 2 * time.Minute

// isBusy tells whether err is SQLite's "the file is locked by another
// writer" (SQLITE_BUSY, SQLITE_LOCKED), by its text, which is the same
// however the error was wrapped on its way here.
func isBusy(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "database is locked") || strings.Contains(m, "database table is locked")
}

// retryBusy runs fn again while it fails because the file is busy, until it
// succeeds, fails otherwise, ctx ends or busyLimit passes. Without it a
// write that waited out busy_timeout on a slow, loaded host failed, and the
// event or the profile it carried was lost.
func retryBusy(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(busyLimit)
	wait := 20 * time.Millisecond
	for {
		err := fn()
		if !isBusy(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		wait = min(2*wait, time.Second)
	}
}

// RetryBusy is retryBusy for a write a worker makes itself on the same file
// (a transaction outside this package's own writers): fn runs again while
// it fails because another writer holds the file, until it succeeds, fails
// otherwise, ctx ends or busyLimit passes. fn must be safe to run again
// from the start (a transaction that failed has changed nothing).
func RetryBusy(ctx context.Context, fn func() error) error {
	return retryBusy(ctx, fn)
}
