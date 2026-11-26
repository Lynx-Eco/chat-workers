// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build plainstore

package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// TestPlainLibraryRefused: with the driver's own plain SQLite linked, the
// fail-closed check refuses and creates nothing.
func TestPlainLibraryRefused(t *testing.T) {
	if _, err := LibraryVersion(); err == nil {
		t.Fatalf("plain SQLite passed the library check")
	}
	path := filepath.Join(t.TempDir(), "w.db")
	_, enc, err := OpenDB(path, OpenOptions{Key: bytes.Repeat([]byte{1}, KeyLen)}, zerolog.Nop())
	if enc || err == nil || err.Error() != CodeNotEncrypted {
		t.Fatalf("open: %v %v", enc, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file created: %v", err)
	}
	db, enc, err := OpenDB(path, OpenOptions{Key: bytes.Repeat([]byte{1}, KeyLen), DevPlain: true}, zerolog.Nop())
	if err != nil || enc {
		t.Fatalf("dev plain: %v %v", enc, err)
	}
	_ = db.Close()
}
