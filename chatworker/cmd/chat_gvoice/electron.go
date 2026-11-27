// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// The connector's one external piece: Electron, to sign the texts it sends.
//
// mautrix-gvoice v0.2611.0 runs with Go code alone. Everything it asks
// Google is plain HTTPS from Go (libgv request.go: the cookies, and the
// SAPISIDHASH authorization header computed in Go from the SAPISID cookie,
// request.go:42-47, :86-96). One thing it can add with outside help: the
// "tracking data" of a sent text (ReqSendSMS.TrackingData), a signature
// that Google's web app computes by running a program Google serves (the
// "WAA" interpreter script, created by libgv CreateWaa). The connector runs
// that script in Electron when it finds one (electron.go runElectron):
//   - it looks for a program named "electron" on PATH (exec.LookPath,
//     electron.go:55), at each realtime connect (client.go:199); none found
//     is a debug line, and nothing else changes;
//   - it writes its embedded electron.mjs to a temporary file
//     (os.CreateTemp: $TMPDIR) and runs "electron <that file>" with no other
//     argument and the worker's environment (electron.go:60-73), speaking
//     JSON lines on the child's stdin and stdout;
//   - the script opens a hidden BrowserWindow (show: false, unless
//     MAUTRIX_GVOICE_ELECTRON_DEBUG is "true"), loads
//     https://voice.google.com/about, lets through only that page and the
//     interpreter script Google names, strips the page's CSP, and runs the
//     script to sign each text (electron.mjs:83-136);
//   - each sent text then asks Electron for a signature, 10 s at most
//     (electron.go:171); a missing or failed one is not an error.
//
// Without Electron the connector sends every text with the placeholder
// tracking data "!" (libgv client.go:118-120, handlematrix.go:70-72), as
// its own Docker image does (it ships no Electron). Whether Google takes
// those as readily is not in the source; the connector names Electron's
// status in a 429's error (handlematrix.go:113-115), as if it may not.
//
// The worker decides what the connector can find, instead of the host's
// PATH: electron_path (a setting the host gives, never one a connection
// sets: it names a program the worker runs) is the absolute path of the
// program, named "electron"; the worker's PATH is then that file's
// directory alone. Absent or blank, the worker's PATH is empty, so no
// "electron" is ever found and none is run. The worker itself runs no
// program found by PATH (the backend runs none; the connector runs only
// Electron), so nothing else depends on PATH.
//
// What Electron needs that the worker cannot check at start: a display
// (on Linux an X server or Wayland: the connector passes no flag for a
// headless mode; a server needs Xvfb, through a wrapper script named
// "electron"), Electron's own shared libraries, a version that runs an ES
// module as its main script (electron.mjs is one), and the network to
// voice.google.com. A wrapper script runs with this PATH too: it must name
// its interpreter and every program it runs by absolute path. An Electron
// that fails to start is logged by the connector (level only, logx) and the
// texts go unsigned.

const (
	// settingElectronPath is the setting naming the program.
	settingElectronPath = "electron_path"
	// electronName is the name the connector looks for (electron.go:55).
	electronName = "electron"
	// electronDebugEnv shows Electron's window and its developer tools when
	// "true" (electron.mjs:81, :86-90, :129-131): never in a worker.
	electronDebugEnv = "MAUTRIX_GVOICE_ELECTRON_DEBUG"
	// codeElectronNotFound refuses a start whose electron_path names no
	// executable file.
	codeElectronNotFound = "electron_not_found"
)

// readElectronPath reads electron_path from Init: absent or blank is ""
// (no Electron); else it must be an absolute, clean path of valid UTF-8
// with no control character and no PATH list separator, whose last element
// is exactly electronName. Anything else is bad_settings. Whether the file
// is there is useElectron's.
func readElectronPath(init *contract.Init) (string, error) {
	v := strings.TrimSpace(init.GetSettings()[settingElectronPath])
	if v == "" {
		return "", nil
	}
	if !utf8.ValidString(v) || strings.IndexFunc(v, unicode.IsControl) >= 0 ||
		strings.ContainsRune(v, os.PathListSeparator) || !filepath.IsAbs(v) ||
		filepath.Clean(v) != v || filepath.Base(v) != electronName {
		return "", &backend.SettingsError{Code: "bad_settings"}
	}
	return v, nil
}

// useElectron makes the connector find the Electron at path, or none when
// path is "": the worker's PATH becomes path's directory alone, or empty.
// The debug switch of the connector's script is always unset. A path that
// is not an executable regular file (after symlinks) refuses the start with
// codeElectronNotFound: a host that names an Electron it does not have
// cannot start the worker, and nothing else is changed by it.
func useElectron(path string) error {
	if err := os.Unsetenv(electronDebugEnv); err != nil {
		return &backend.SettingsError{Code: "bad_settings"}
	}
	dir := ""
	if path != "" {
		fi, err := os.Stat(path)
		if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm()&0o111 == 0 {
			return &backend.SettingsError{Code: codeElectronNotFound}
		}
		dir = filepath.Dir(path)
	}
	if err := os.Setenv("PATH", dir); err != nil {
		return &backend.SettingsError{Code: "bad_settings"}
	}
	return nil
}
