// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build linux

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// nonUnixSockets counts the process's sockets that are not Unix sockets,
// from /proc (Linux only: see smoke_other_test.go).
func nonUnixSockets(t *testing.T, pid int) (n int, checked bool) {
	t.Helper()
	unix := map[string]bool{}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/unix", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n")[1:] {
		f := strings.Fields(line)
		if len(f) >= 7 {
			unix[f[6]] = true
		}
	}
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
		if err != nil || !strings.HasPrefix(target, "socket:[") {
			continue
		}
		if !unix[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] {
			n++
		}
	}
	return n, true
}
