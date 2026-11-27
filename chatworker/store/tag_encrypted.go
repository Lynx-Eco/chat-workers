// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build libsqlite3 && !plainstore

package store

// BuildTag is the build tag this binary was built with (tests pass it on
// when they build a worker).
const BuildTag = "libsqlite3"

// PlainBuild is false: this binary links SQLite3 Multiple Ciphers (checked
// again at every open) and never opens a plain store.
const PlainBuild = false
