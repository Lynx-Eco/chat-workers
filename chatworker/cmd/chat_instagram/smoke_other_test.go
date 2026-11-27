// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !linux

package main

import "testing"

// nonUnixSockets is not counted where there is no /proc (macOS): the smoke
// test's other checks run, and it logs that this one did not.
func nonUnixSockets(*testing.T, int) (n int, checked bool) { return 0, false }
