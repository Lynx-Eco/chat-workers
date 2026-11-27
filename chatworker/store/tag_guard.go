// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build (libsqlite3 && plainstore) || (!libsqlite3 && !plainstore)

package store

// Exactly one of the two store build tags is required: build with
// ./build.sh (which sets -tags libsqlite3 and links SQLite3 Multiple
// Ciphers), or with -tags plainstore for a development build. This line does
// not compile on purpose.
var _ int = "store: build with ./build.sh (-tags libsqlite3), or -tags plainstore for development only"
