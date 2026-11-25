// SPDX-License-Identifier: AGPL-3.0-or-later

package store

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestBuildWithoutCgoFails: a worker built without cgo would have no SQLite
// driver; the build itself must fail, and say why.
func TestBuildWithoutCgoFails(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the package")
	}
	cmd := exec.Command("go", "build", "-tags", BuildTag, "-o", os.DevNull, ".")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the store built without cgo")
	}
	if !strings.Contains(string(out), "the_worker_store_needs_cgo_build_with_CGO_ENABLED_1") {
		t.Fatalf("build failed for another reason:\n%s", out)
	}
}
