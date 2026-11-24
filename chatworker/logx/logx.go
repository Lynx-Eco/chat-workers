// SPDX-License-Identifier: AGPL-3.0-or-later

// Package logx is the worker's logging policy.
//
// The worker's own lines carry a level, a fixed code, the opaque connection
// id and numbers (counts, durations, seqs) only. Never message text, names,
// phone numbers, tokens, QR or code payloads, principals, or error strings
// (an error string can quote any of those). The code parameter has an
// unexported type, so only a constant can be passed: a string built at run
// time (fmt.Sprintf, an error's text, a variable) does not compile.
//
// The framework and the connector log with zerolog and put ids and errors in
// their fields, and some format names into the message itself. No rule on
// the message can tell constant prose from a formatted name ("Joined Alice
// Example" is letters and spaces), so Filter keeps none of it: an upstream
// line keeps its level, and lines below the configured level are dropped.
package logx

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Logger is the worker's own logger.
type Logger struct {
	zl zerolog.Logger
}

// New writes JSON lines to w, each tagged with the connection id.
func New(w io.Writer, connectionID string) Logger {
	zl := zerolog.New(w).With().Timestamp().Str("conn", connectionID).Logger()
	return Logger{zl: zl}
}

// Nop discards everything.
func Nop() Logger { return Logger{zl: zerolog.Nop()} }

// Stderr is a logger for before the connection id is known.
func Stderr() Logger { return New(os.Stderr, "") }

// WithConn returns a logger tagged with the connection id.
func (l Logger) WithConn(connectionID string) Logger {
	return Logger{zl: l.zl.With().Str("conn", connectionID).Logger()}
}

// code is a log line's fixed code. It is unexported so that callers can only
// pass a constant ("store_open_failed"): a string value made at run time is
// not assignable to it.
type code string

// Info logs a fixed code with optional numbers.
func (l Logger) Info(c code, nums ...Num) { l.emit(l.zl.Info(), c, nums) }

// Warn logs a fixed code with optional numbers.
func (l Logger) Warn(c code, nums ...Num) { l.emit(l.zl.Warn(), c, nums) }

// Error logs a fixed code with optional numbers.
func (l Logger) Error(c code, nums ...Num) { l.emit(l.zl.Error(), c, nums) }

// Num is one numeric field.
type Num struct {
	Key string
	Val int64
}

// N builds a numeric field.
func N(key string, val int64) Num { return Num{key, val} }

// Ms builds a duration field in milliseconds.
func Ms(key string, d time.Duration) Num { return Num{key, d.Milliseconds()} }

func (l Logger) emit(e *zerolog.Event, c code, nums []Num) {
	if e == nil {
		return
	}
	e = e.Str("code", string(c))
	for _, n := range nums {
		e = e.Int64(n.Key, n.Val)
	}
	e.Send()
}

// Filter is an io.Writer for upstream zerolog JSON lines.
type Filter struct {
	mu   sync.Mutex
	out  io.Writer
	conn string
}

// NewFilter writes kept lines to out, tagged with the connection id.
func NewFilter(out io.Writer, connectionID string) *Filter {
	return &Filter{out: out, conn: connectionID}
}

// Upstream returns a zerolog logger for the framework and connector, at the
// given minimum level, writing through f.
func (f *Filter) Upstream(level zerolog.Level) zerolog.Logger {
	return zerolog.New(f).Level(level)
}

type keptLine struct {
	Time  string `json:"time"`
	Level string `json:"level"`
	Src   string `json:"src"`
	Conn  string `json:"conn"`
}

// Write implements io.Writer. It never fails, so upstream logging never
// errors because of the filter.
func (f *Filter) Write(p []byte) (int, error) {
	n := len(p)
	for _, line := range bytes.Split(p, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var in struct {
			Level string `json:"level"`
		}
		if json.Unmarshal(line, &in) != nil {
			continue
		}
		out := keptLine{
			Time:  time.Now().UTC().Format(time.RFC3339),
			Level: in.Level,
			Src:   "upstream",
			Conn:  f.conn,
		}
		b, err := json.Marshal(out)
		if err != nil {
			continue
		}
		f.mu.Lock()
		_, _ = f.out.Write(append(b, '\n'))
		f.mu.Unlock()
	}
	return n, nil
}
