// SPDX-License-Identifier: AGPL-3.0-or-later

// Package main must not compile: it passes a code made at run time
// (TestCodesAreConstants).
package main

import (
	"fmt"
	"os"

	"lynx.invalid/ingest/chatworker/logx"
)

func main() {
	logx.New(os.Stderr, "").Error(fmt.Sprintf("joined %s", os.Args[0]))
}
