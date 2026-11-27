// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// TestFloodWaitHooks pins the two hooks against the connector's own code,
// so that an update of mautrix-telegram that changes either fails here
// rather than turning every FLOOD_WAIT back into a fetch that sleeps for
// minutes or fails as fetch_failed:
//   - the error FetchMessages returns for a wait it does not sleep out (5
//     minutes or more, or a fifth in a row) is read with its wait, wrapped or
//     not, and no other error is taken for one;
//   - tgerr.FloodWait, the function the connector's FetchMessages sleeps in,
//     is seen about to sleep a wait out (onWait with that wait), and a cancel
//     of its context from onWait ends the sleep at once, as the backend's
//     cancel does for a wait it will not sleep out.
func TestFloodWaitHooks(t *testing.T) {
	long := tgerr.New(420, "FLOOD_WAIT_600")
	if d, ok := floodWait(fmt.Errorf("fetch: %w", long)); !ok || d != 600*time.Second {
		t.Errorf("floodWait(FLOOD_WAIT_600) = %v, %v; want 10m, true", d, ok)
	}
	if d, ok := floodWait(tgerr.New(420, "FLOOD_PREMIUM_WAIT_7")); !ok || d != 7*time.Second {
		t.Errorf("floodWait(FLOOD_PREMIUM_WAIT_7) = %v, %v; want 7s, true", d, ok)
	}
	for _, err := range []error{tgerr.New(400, "PEER_ID_INVALID"), errors.New("FLOOD_WAIT_5"), context.DeadlineExceeded} {
		if d, ok := floodWait(err); ok {
			t.Errorf("floodWait(%v) = %v, true; want not a flood wait", err, d)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan time.Duration, 4)
	ctx = watchFloodWaits(ctx, func(d time.Duration) {
		seen <- d
		cancel()
	})
	started := time.Now()
	retry, err := tgerr.FloodWait(ctx, tgerr.New(420, "FLOOD_WAIT_30"))
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("the cancel from onWait did not end the sleep: %v", took)
	}
	if retry || !errors.Is(err, context.Canceled) {
		t.Errorf("FloodWait after the cancel = %v, %v; want false, context canceled", retry, err)
	}
	select {
	case d := <-seen:
		if d != 30*time.Second {
			t.Errorf("onWait got %v, want 30s", d)
		}
	default:
		t.Fatal("onWait was not called: the connector's flood wait line is not what floodWaitLog reads")
	}
	if len(seen) != 0 {
		t.Errorf("onWait called %d more times for one wait", len(seen))
	}

	// A wait FloodWait does not sleep out (5 minutes or more) is not
	// reported as one it sleeps: it comes back as the error.
	ctx2 := watchFloodWaits(context.Background(), func(d time.Duration) { seen <- d })
	retry, err = tgerr.FloodWait(ctx2, long)
	if retry || !tgerr.Is(err, tgerr.ErrFloodWait) || len(seen) != 0 {
		t.Errorf("FloodWait(FLOOD_WAIT_600) = %v, %v, %d waits seen; want false, the error, none", retry, err, len(seen))
	}
}
