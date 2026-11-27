// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"testing"
)

func TestAnswerSourceCommit(t *testing.T) {
	const commit = "0123456789abcdef0123456789abcdef01234567"
	var out bytes.Buffer
	if !AnswerSourceCommit([]string{"--source-commit"}, commit, &out) {
		t.Fatal("the flag alone was not answered")
	}
	if out.String() != commit+"\n" {
		t.Fatalf("answer %q", out.String())
	}
	for _, args := range [][]string{
		nil,
		{"--socket-path", "/tmp/s"},
		{"--source-commit", "--socket-path", "/tmp/s"},
		{"--socket-path", "/tmp/s", "--source-commit"},
		{"-source-commit"},
	} {
		out.Reset()
		if AnswerSourceCommit(args, commit, &out) || out.Len() != 0 {
			t.Fatalf("%q answered %q", args, out.String())
		}
	}
}
