// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"fmt"
	"io"
)

// SourceCommitFlag is the argument a worker answers, alone, with the commit
// it was stamped with (main.sourceCommit, set by go/build.sh): the engine
// asks it before it hands the worker anything, and runs no worker of another
// build. A check for skew between the engine and a separately built worker,
// not a trust boundary.
const SourceCommitFlag = "--source-commit"

// AnswerSourceCommit answers the engine's build check: when args (os.Args[1:])
// is exactly SourceCommitFlag, it writes commit and a newline to out and
// returns true, and the worker exits 0 before it touches anything; else it
// writes nothing and returns false.
func AnswerSourceCommit(args []string, commit string, out io.Writer) bool {
	if len(args) != 1 || args[0] != SourceCommitFlag {
		return false
	}
	fmt.Fprintln(out, commit)
	return true
}
