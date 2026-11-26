// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !cgo

package store

// The store's SQLite driver (mattn/go-sqlite3, linked against SQLite3
// Multiple Ciphers, db.go) is cgo. Without cgo that package builds a stub,
// everything still builds, and OpenDB fails at run time. This file makes such a build fail instead: build with
// CGO_ENABLED=1 and a C compiler.
var _ = the_worker_store_needs_cgo_build_with_CGO_ENABLED_1
