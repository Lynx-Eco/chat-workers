// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/rs/zerolog"
)

// WatchLogWaits is a Hooks.WatchRateLimit for a connector that logs a wait,
// through the zerolog logger of the context it was given, just before it
// sleeps it out: a warning (or above) whose message is message and whose
// field named field is the wait, as zerolog encodes a duration
// (Event.Dur). It returns ctx with a logger that reads such lines and calls
// onWait with each wait; it keeps nothing else and writes nothing. A
// connector that is not given a logger otherwise (the worker gives commands
// a context without one, so zerolog.Ctx answers its disabled logger) loses
// no line by it.
//
// mautrix-telegram's tgerr.FloodWait offers no other way to learn the wait
// it is about to sleep out (it sleeps inside the connector's FetchMessages,
// which takes no option for it); the line it logs is pinned by a test of
// the Telegram worker against the connector's own code
// (cmd/chat_telegram/floodwait_test.go).
func WatchLogWaits(ctx context.Context, message, field string, onWait func(wait time.Duration)) context.Context {
	l := zerolog.New(&waitLines{message: message, field: field, onWait: onWait}).Level(zerolog.WarnLevel)
	return l.WithContext(ctx)
}

// waitLines is the writer of WatchLogWaits' logger: one zerolog JSON line
// per Write.
type waitLines struct {
	message string
	field   string
	onWait  func(time.Duration)
}

func (w *waitLines) Write(p []byte) (int, error) {
	for _, line := range bytes.Split(p, []byte("\n")) {
		if wait, ok := parseWaitLine(line, w.message, w.field); ok {
			w.onWait(wait)
		}
	}
	return len(p), nil
}

// parseWaitLine reads the wait of a zerolog JSON line whose message is
// message, from its field named field, a duration as zerolog encodes it
// (a number of zerolog.DurationFieldUnit, integer or not). ok is false for
// any other line, and for a wait that is not a number or is negative.
func parseWaitLine(line []byte, message, field string) (time.Duration, bool) {
	if len(bytes.TrimSpace(line)) == 0 {
		return 0, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(line, &fields) != nil {
		return 0, false
	}
	var msg string
	if json.Unmarshal(fields[zerolog.MessageFieldName], &msg) != nil || msg != message {
		return 0, false
	}
	var n float64
	if raw, ok := fields[field]; !ok || json.Unmarshal(raw, &n) != nil || n < 0 {
		return 0, false
	}
	return time.Duration(n * float64(zerolog.DurationFieldUnit)), true
}
