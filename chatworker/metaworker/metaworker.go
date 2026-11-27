// SPDX-License-Identifier: AGPL-3.0-or-later

// Package metaworker is what the two Meta workers share, chat_instagram and
// chat_messenger: the strict reading of their settings (settings.go), the
// hooks that are the same for both connectors of mautrix-meta (hooks.go) and
// their main program's start (Main).
//
// The two workers wrap two different connectors of the same upstream module,
// go.mau.fi/mautrix-meta v0.2611.0, unmodified: chat_messenger its
// pkg/connector (Facebook Messenger, the connector of upstream's
// cmd/mautrix-meta, with a second, end-to-end encrypted connection through
// whatsmeow), chat_instagram its pkg/igconnector (Instagram, the connector
// of upstream's cmd/mautrix-instagram; v0.2611.0's pkg/connector refuses an
// Instagram login, meta-ig-not-supported). This package imports neither
// connector: each worker links its own only. What it imports of the module
// (messagix/httpclient, metaid) both connectors link anyway.
package metaworker

import (
	"context"
	"flag"
	"fmt"
	"os"

	"lynx.invalid/ingest/chatworker/backend"
)

// The upstream versions both workers report in Hello (chatworker/go.mod
// pins each; the smoke tests check it).
const (
	// MautrixVersion is maunium.net/go/mautrix's.
	MautrixVersion = "v0.33.0"
	// MetaModule and MetaVersion are the connectors' module.
	MetaModule  = "go.mau.fi/mautrix-meta"
	MetaVersion = "v0.2611.0"
	// ReqModule and ReqVersion are Beeper's fork of github.com/imroc/req/v3,
	// in its place by chatworker/go.mod's replace (the HTTP client of
	// messagix and instameow, with its TLS and HTTP/2 fingerprints of a
	// browser).
	ReqModule  = "github.com/beeper/req/v3"
	ReqVersion = "v3.0.0-20260925135712-b49593750e56"
)

// Upstream is the Hello upstream versions of a Meta worker, with extra (the
// worker's own besides: chat_messenger's whatsmeow) added.
func Upstream(extra map[string]string) map[string]string {
	up := map[string]string{
		"maunium.net/go/mautrix": MautrixVersion,
		MetaModule:               MetaVersion,
		ReqModule:                ReqVersion,
	}
	for k, v := range extra {
		up[k] = v
	}
	return up
}

// Main is a Meta worker's main program, as every chat worker's: the
// engine's build check before anything else, then one flag, the host's
// socket, and backend.Run; it exits with Run's code (2 on bad arguments).
//
//	<worker> --socket-path <path>
//	<worker> --source-commit    print the stamped commit and exit 0
func Main(w backend.Worker) {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], w.SourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet(w.Name, flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}
