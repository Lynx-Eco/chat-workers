// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeElectron writes a file named electron in a directory of its own,
// with mode, and returns its path.
func fakeElectron(t *testing.T, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), electronName)
	if err := os.WriteFile(p, []byte("#!/bin/sh\nexit 0\n"), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestReadElectronPath: absent or blank is no Electron; an absolute, clean
// path whose last element is "electron" is taken; a relative path, one with
// "..", a trailing slash, another name (the macOS bundle's "Electron"
// among them), a PATH list separator or a control character is
// bad_settings. WHY: catches a setting that would put another directory, or
// several, on the worker's PATH (the connector would run whatever
// "electron" it finds there), and a name the connector never looks for.
func TestReadElectronPath(t *testing.T) {
	for in, want := range map[string]string{
		"":                             "",
		"   ":                          "",
		"/opt/electron/electron":       "/opt/electron/electron",
		" /usr/lib/electron/electron ": "/usr/lib/electron/electron",
	} {
		got, err := readElectronPath(initWith(map[string]string{settingElectronPath: in}))
		if err != nil || got != want {
			t.Errorf("%q: %q %v, want %q", in, got, err, want)
		}
	}
	if got, err := readElectronPath(initWith(nil)); err != nil || got != "" {
		t.Errorf("absent: %q %v", got, err)
	}
	for _, bad := range []string{
		"electron",
		"bin/electron",
		"/opt/electron/../bin/electron",
		"/opt//electron",
		"/opt/electron/",
		"/opt/electron/electron.sh",
		"/Applications/Electron.app/Contents/MacOS/Electron",
		"/opt/a:b/electron",
		"/opt/a\nb/electron",
		"/opt/\x00/electron",
		"/opt/" + string([]byte{0xff}) + "/electron",
	} {
		_, err := readElectronPath(initWith(map[string]string{settingElectronPath: bad}))
		wantSettingsError(t, bad, err, "bad_settings")
	}
}

// TestUseElectron: with no Electron the worker's PATH is empty, so the
// connector's own lookup (exec.LookPath("electron"), electron.go:55) finds
// none, even where the host's PATH had one; with one, the lookup finds
// exactly that file; a path that is missing, not executable or a directory
// refuses the start as electron_not_found and leaves PATH as it was. The
// connector's debug switch is unset either way. WHY: catches a worker that
// runs an Electron the host did not name, one that cannot find the one it
// did, and a debug window opened on a server.
func TestUseElectron(t *testing.T) {
	keepPath(t)
	hostOne := fakeElectron(t, 0o755)
	t.Setenv("PATH", filepath.Dir(hostOne))
	if got, err := exec.LookPath(electronName); err != nil || got != hostOne {
		t.Fatalf("the test's own electron is not found: %q %v", got, err)
	}
	t.Setenv(electronDebugEnv, "true")

	if err := useElectron(""); err != nil {
		t.Fatal(err)
	}
	if p := os.Getenv("PATH"); p != "" {
		t.Fatalf("PATH %q with no Electron", p)
	}
	if got, err := exec.LookPath(electronName); err == nil {
		t.Fatalf("an electron found with none named: %q", got)
	}
	if _, set := os.LookupEnv(electronDebugEnv); set {
		t.Fatal("the debug switch is still set")
	}

	named := fakeElectron(t, 0o755)
	t.Setenv(electronDebugEnv, "true")
	if err := useElectron(named); err != nil {
		t.Fatal(err)
	}
	if p := os.Getenv("PATH"); p != filepath.Dir(named) {
		t.Fatalf("PATH %q, want %q", p, filepath.Dir(named))
	}
	if got, err := exec.LookPath(electronName); err != nil || got != named {
		t.Fatalf("lookup %q %v, want %q", got, err, named)
	}
	if _, set := os.LookupEnv(electronDebugEnv); set {
		t.Fatal("the debug switch is still set")
	}

	dirNamed := filepath.Join(t.TempDir(), electronName)
	if err := os.Mkdir(dirNamed, 0o755); err != nil {
		t.Fatal(err)
	}
	before := os.Getenv("PATH")
	for _, bad := range []string{
		filepath.Join(t.TempDir(), electronName),
		fakeElectron(t, 0o644),
		dirNamed,
	} {
		err := useElectron(bad)
		wantSettingsError(t, bad, err, codeElectronNotFound)
		if p := os.Getenv("PATH"); p != before {
			t.Fatalf("%s: PATH changed to %q", bad, p)
		}
	}
}

// TestNewConnectorElectron: electron_path reaches the worker's PATH
// through newConnector, a missing one refuses the start as
// electron_not_found and a malformed one as bad_settings, with no connector
// made. WHY: catches a check left out of the start (the worker would run
// with texts unsigned while the host believes they are signed).
func TestNewConnectorElectron(t *testing.T) {
	keepPath(t)
	named := fakeElectron(t, 0o755)
	if _, err := newConnector(initWith(map[string]string{settingElectronPath: named})); err != nil {
		t.Fatal(err)
	}
	if got, err := exec.LookPath(electronName); err != nil || got != named {
		t.Fatalf("lookup %q %v", got, err)
	}
	missing := filepath.Join(t.TempDir(), "gone", electronName)
	nc, err := newConnector(initWith(map[string]string{settingElectronPath: missing}))
	wantSettingsError(t, missing, err, codeElectronNotFound)
	if nc != nil {
		t.Fatal("a connector with a missing Electron")
	}
	_, err = newConnector(initWith(map[string]string{settingElectronPath: strings.TrimPrefix(named, "/")}))
	wantSettingsError(t, "relative", err, "bad_settings")
	if _, err := newConnector(initWith(nil)); err != nil || os.Getenv("PATH") != "" {
		t.Fatalf("no Electron: %v, PATH %q", err, os.Getenv("PATH"))
	}
}
