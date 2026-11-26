// SPDX-License-Identifier: AGPL-3.0-or-later

package logx

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestFilterKeepsNoUpstreamText: an upstream line keeps its level and
// nothing of its message or fields, a formatted name made of letters only
// included; lines below the level are dropped.
func TestFilterKeepsNoUpstreamText(t *testing.T) {
	var out bytes.Buffer
	up := NewFilter(&out, "conn-1").Upstream(zerolog.InfoLevel)
	up.Info().Msgf("Joined %s", "Alice Example")
	up.Warn().Str("user_id", "u-alice").Err(errors.New("dial +15550001")).Msg("Failed to connect")
	up.Debug().Msg("Below the level")
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d lines: %q", len(lines), out.String())
	}
	for _, bad := range []string{"Alice", "Joined", "u-alice", "+1555", "Failed", "Below"} {
		if strings.Contains(out.String(), bad) {
			t.Fatalf("filtered output contains %q: %s", bad, out.String())
		}
	}
	if !strings.Contains(lines[0], `"level":"info"`) || !strings.Contains(lines[1], `"level":"warn"`) ||
		!strings.Contains(lines[1], `"conn":"conn-1"`) || !strings.Contains(lines[1], `"src":"upstream"`) {
		t.Fatalf("lines: %q", lines)
	}
}

// TestCodesAreConstants: a code made at run time does not compile.
func TestCodesAreConstants(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a package")
	}
	cmd := exec.Command("go", "build", "-o", os.DevNull, "./testdata/runtimecode")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("a run-time code compiled")
	}
	if !strings.Contains(string(out), "cannot use fmt.Sprintf") {
		t.Fatalf("build failed for another reason:\n%s", out)
	}
}
