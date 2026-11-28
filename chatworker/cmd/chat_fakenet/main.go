// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_fakenet is the worker for the fake network: the backend with the
// scripted fakenet connector. It is test support for the engine and the
// backend, never shipped for a real network.
//
//	chat_fakenet --socket-path <path>
//	chat_fakenet --source-commit    print the stamped commit and exit 0
//
// Exit codes: 0 after Shutdown or when the host closed the connection (on
// SIGTERM it logs sigterm and dies of the signal, as without a handler), 1 on
// an error before Hello (a store that could not be reached now among them:
// store_open_failed, no Hello sent), 2 on bad arguments, 3 on a scripted
// crash, 4 (backend.ExitRestartFailed) when no framework instance could be
// started again after a logout wiped the store, 5
// (backend.ExitStoreRefused) when the store must not be used as it is
// (store_key_invalid, store_not_encrypted, store_key_mismatch,
// store_corrupt: reported as ConnectionState ERROR first).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

// Stamped with -ldflags -X at build time.
var (
	version        = "0.0.0-dev"
	sourceCommit   = ""
	mautrixVersion = "v0.33.0"
)

func main() {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], sourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet("chat_fakenet", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_fakenet",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream:     map[string]string{"maunium.net/go/mautrix": mautrixVersion},
		BridgeID:     "fakenet",
		NewConnector: func(init *contract.Init) (bridgev2.NetworkConnector, error) {
			c, err := fakenet.New(init.GetStateDir(), init.GetSettings())
			if err != nil {
				return nil, &backend.SettingsError{Code: "bad_script"}
			}
			return c, nil
		},
		Hooks: backend.Hooks{
			ShowExpiresIn: func(step *bridgev2.LoginStep) uint32 { return 20 },
			// fakenet's LoadUserLogin makes a Client struct and LogoutRemote
			// writes one line to its network log: neither touches the store.
			EndRefusedSession: true,
			// A portal whose stored metadata says broadcast is a channel
			// (Script.TurnedChannel), re-IDed or not.
			IsChannel: func(q backend.ChannelQuery) bool { return fakenet.IsBroadcast(q.Portal) },
			// The fake network's rate limit (Script.HistoryRateLimits), seen
			// as the Telegram worker sees a FLOOD_WAIT: the error of a wait
			// not slept out, and the line logged before one is.
			RateLimitWait: func(err error) (time.Duration, bool) {
				var rl *fakenet.RateLimitError
				if errors.As(err, &rl) {
					return rl.Wait, true
				}
				return 0, false
			},
			WatchRateLimit: func(ctx context.Context, onWait func(time.Duration)) context.Context {
				return backend.WatchLogWaits(ctx, fakenet.FloodWaitLog, "duration", onWait)
			},
		},
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}
