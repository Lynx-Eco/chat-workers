// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build plainstore && !libsqlite3

package store

// BuildTag is the build tag this binary was built with (tests pass it on
// when they build a worker).
const BuildTag = "plainstore"

// PlainBuild is true in the development build: it links the driver's own
// plain SQLite. It opens a plain store only when asked by name
// (OpenOptions.DevPlain); otherwise it refuses like any build whose library
// does not encrypt.
const PlainBuild = true
