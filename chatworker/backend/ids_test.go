// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import "testing"

// TestEventIDsDoNotCollide: components that contain the separator (or the
// escape) give distinct ids; ids of components with neither are as before.
func TestEventIDsDoNotCollide(t *testing.T) {
	pairs := [][2][]string{
		{{"a:b", "c", ""}, {"a", "b:c", ""}},
		{{"a", "b", ":"}, {"a", "b:", ""}},
		{{"a%3Ab", "c"}, {"a:b", "c"}},
		{{"a%", "3Ab"}, {"a", "%3Ab"}},
	}
	for _, p := range pairs {
		if x, y := eventID("msg", p[0]...), eventID("msg", p[1]...); x == y {
			t.Errorf("%q and %q both give %q", p[0], p[1], x)
		}
	}
	for want, got := range map[string]string{
		"msg:dm-alice:m1:":       eventID("msg", "dm-alice", "m1", ""),
		"chat:grp-1:3":           eventID("chat", "grp-1", num(uint64(3))),
		"state:12:8":             eventID("state", num(uint64(12)), num(int32(8))),
		"react:c:m::u:👍:added:5": eventID("react", "c", "m", "", "u", "👍", "added", num(int64(5))),
		"msg:%3A%25:x":           eventID("msg", ":%", "x"),
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
