// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"lynx.invalid/ingest/chatworker/store"
)

func testSpool(t *testing.T, budget int64) (*spool, *store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	key := bytes.Repeat([]byte{9}, store.KeyLen)
	db, _, err := store.OpenDB(filepath.Join(dir, "w.db"), store.OpenOptions{Key: key, DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	st, err := store.Open(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	media := filepath.Join(dir, "media")
	if err := os.MkdirAll(media, 0o700); err != nil {
		t.Fatal(err)
	}
	sp, err := newSpool(media, key, budget)
	if err != nil {
		t.Fatal(err)
	}
	return sp, st, media
}

func marked(n int, marker string) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	for i := 0; i+len(marker) <= n; i += 4096 {
		copy(b[i:], marker)
	}
	return b
}

func readAll(t *testing.T, sp *spool, sha string) ([]byte, error) {
	t.Helper()
	r, release, err := sp.open(sha)
	if err != nil {
		return nil, err
	}
	defer release()
	return io.ReadAll(r)
}

func TestSpoolRoundTripAtRest(t *testing.T) {
	ctx := context.Background()
	sp, st, media := testSpool(t, 1<<30)
	const marker = "SPOOL-PLAINTEXT-MARKER"
	for _, n := range []int{0, 1, len(marker), spoolChunk - 1, spoolChunk, spoolChunk + 1, 3*spoolChunk + 17, 4 * spoolChunk} {
		data := marked(n, marker)
		sha, err := sp.put(ctx, st, unguarded, bytes.NewReader(data), "f.bin", "application/octet-stream")
		if err != nil {
			t.Fatalf("%d: put: %v", n, err)
		}
		want := sha256.Sum256(data)
		if sha != hex.EncodeToString(want[:]) {
			t.Fatalf("%d: digest %s", n, sha)
		}
		raw, err := os.ReadFile(filepath.Join(media, sp.name(sha)))
		if err != nil {
			t.Fatalf("%d: %v", n, err)
		}
		if n >= len(marker) && bytes.Contains(raw, []byte(marker)) {
			t.Fatalf("%d: plaintext on disk", n)
		}
		if bytes.Contains(raw, []byte(sha)) {
			t.Fatalf("%d: digest on disk", n)
		}
		got, err := readAll(t, sp, sha)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%d: round trip: %d bytes, %v", n, len(got), err)
		}
		if e, _ := st.GetSpool(ctx, sha); e == nil || e.Size != int64(n) {
			t.Fatalf("%d: row %+v", n, e)
		}
	}
	// No file is named by its digest.
	entries, _ := os.ReadDir(media)
	for _, e := range entries {
		if row, _ := st.GetSpool(ctx, e.Name()); row != nil {
			t.Fatalf("file %s is named by its digest", e.Name())
		}
	}
}

func TestSpoolTamperAndTruncation(t *testing.T) {
	ctx := context.Background()
	sp, st, media := testSpool(t, 1<<30)
	data := marked(2*spoolChunk+100, "X")
	sha, err := sp.put(ctx, st, unguarded, bytes.NewReader(data), "", "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(media, sp.name(sha))
	orig, _ := os.ReadFile(path)
	chunk := spoolChunk + 16
	for name, mutate := range map[string]func([]byte) []byte{
		"flip a byte":         func(b []byte) []byte { b[spoolHeaderLen+chunk+5] ^= 1; return b },
		"flip the salt":       func(b []byte) []byte { b[10] ^= 1; return b },
		"drop the last chunk": func(b []byte) []byte { return b[:spoolHeaderLen+2*chunk] },
		"truncate mid chunk":  func(b []byte) []byte { return b[:spoolHeaderLen+chunk+100] },
		"swap two chunks": func(b []byte) []byte {
			c := append([]byte(nil), b...)
			copy(c[spoolHeaderLen:], b[spoolHeaderLen+chunk:spoolHeaderLen+2*chunk])
			copy(c[spoolHeaderLen+chunk:], b[spoolHeaderLen:spoolHeaderLen+chunk])
			return c
		},
		"append bytes": func(b []byte) []byte { return append(b, 1, 2, 3) },
	} {
		if err := os.WriteFile(path, mutate(append([]byte(nil), orig...)), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := readAll(t, sp, sha); !errors.Is(err, errSpoolCorrupt) {
			t.Fatalf("%s: %d bytes, %v", name, len(got), err)
		}
	}
	// Another store key cannot read it.
	if err := os.WriteFile(path, orig, 0o600); err != nil {
		t.Fatal(err)
	}
	other, err := newSpool(media, bytes.Repeat([]byte{8}, store.KeyLen), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	other.nameKey = sp.nameKey
	if _, err := readAll(t, other, sha); !errors.Is(err, errSpoolCorrupt) {
		t.Fatalf("another key: %v", err)
	}
}

// TestCheckFinalShrunk: a spool file that is shorter when its final chunk
// is read than when its size was taken (it shrank in between) is corrupt, a
// miss (errMediaUnknown), not a raw io.EOF.
func TestCheckFinalShrunk(t *testing.T) {
	ctx := context.Background()
	sp, st, media := testSpool(t, 1<<30)
	sha, err := sp.put(ctx, st, unguarded, bytes.NewReader(marked(spoolChunk+100, "S")), "", "")
	if err != nil {
		t.Fatal(err)
	}
	name := sp.name(sha)
	b, err := os.ReadFile(filepath.Join(media, name))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := sp.fileAEAD(b[len(spoolMagic):spoolHeaderLen])
	if err != nil {
		t.Fatal(err)
	}
	ad := finalAD(b[:spoolHeaderLen], name)
	if err := checkFinalAt(bytes.NewReader(b), int64(len(b)), aead, ad); err != nil {
		t.Fatalf("whole file: %v", err)
	}
	for _, cut := range []int{1, 50} {
		err := checkFinalAt(bytes.NewReader(b[:len(b)-cut]), int64(len(b)), aead, ad)
		if !errors.Is(err, errSpoolCorrupt) || !errors.Is(err, errMediaUnknown) {
			t.Fatalf("shrunk by %d: %v", cut, err)
		}
	}
}

// TestSpoolFileBoundToName: a spooled file decrypts only under the name it
// was stored under. A file renamed (or copied) to another's name, the other
// file's final chunk, and a changed or old (version 1) magic fail as a miss
// (errSpoolCorrupt, which is errMediaUnknown) at open, before any byte is
// read; a chunk moved from another file fails where it is read. A put of
// the content then replaces the file that does not decrypt.
func TestSpoolFileBoundToName(t *testing.T) {
	ctx := context.Background()
	sp, st, media := testSpool(t, 1<<30)
	dataA, dataB := marked(2*spoolChunk+100, "A"), marked(2*spoolChunk+200, "B")
	shaA, err := sp.put(ctx, st, unguarded, bytes.NewReader(dataA), "", "")
	if err != nil {
		t.Fatal(err)
	}
	shaB, err := sp.put(ctx, st, unguarded, bytes.NewReader(dataB), "", "")
	if err != nil {
		t.Fatal(err)
	}
	pathA, pathB := filepath.Join(media, sp.name(shaA)), filepath.Join(media, sp.name(shaB))
	origA, _ := os.ReadFile(pathA)
	origB, _ := os.ReadFile(pathB)
	chunk := spoolChunk + 16
	for name, b := range map[string][]byte{
		"A renamed to B's name": origA,
		"A's first chunk in B": func() []byte {
			c := bytes.Clone(origB)
			copy(c[spoolHeaderLen:spoolHeaderLen+chunk], origA[spoolHeaderLen:])
			return c
		}(),
		"A's final chunk in B": append(bytes.Clone(origB[:spoolHeaderLen+2*chunk]), origA[spoolHeaderLen+2*chunk:]...),
		"changed magic":        append([]byte("LXSPOOL3"), origB[len(spoolMagic):]...),
		"version 1 magic":      append([]byte("LXSPOOL1"), origB[len(spoolMagic):]...),
	} {
		if err := os.WriteFile(pathB, b, 0o600); err != nil {
			t.Fatal(err)
		}
		r, release, err := sp.open(shaB)
		if err == nil {
			// Only a chunk moved from another file passes open (the final
			// chunk is B's): it fails where it is read.
			got, rerr := io.ReadAll(r)
			release()
			if name != "A's first chunk in B" || len(got) != 0 {
				t.Fatalf("%s: opened, read %d bytes (%v)", name, len(got), rerr)
			}
			err = rerr
		}
		if !errors.Is(err, errSpoolCorrupt) || !errors.Is(err, errMediaUnknown) {
			t.Fatalf("%s: %v, want a miss", name, err)
		}
	}
	// B's file holds A's bytes: a put of B's content replaces it.
	if err := os.WriteFile(pathB, origA, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.put(ctx, st, unguarded, bytes.NewReader(dataB), "", ""); err != nil {
		t.Fatal(err)
	}
	if got, err := readAll(t, sp, shaB); err != nil || !bytes.Equal(got, dataB) {
		t.Fatalf("B after a put over the wrong file: %d bytes, %v", len(got), err)
	}
	if got, err := readAll(t, sp, shaA); err != nil || !bytes.Equal(got, dataA) {
		t.Fatalf("A: %d bytes, %v", len(got), err)
	}
}

func TestSpoolEviction(t *testing.T) {
	ctx := context.Background()
	const budget = 100 << 10
	sp, st, media := testSpool(t, budget)
	size := func() int64 {
		var n int64
		entries, _ := os.ReadDir(media)
		for _, e := range entries {
			if info, err := e.Info(); err == nil {
				n += info.Size()
			}
		}
		return n
	}
	var shas []string
	for i := 0; i < 8; i++ {
		sha, err := sp.put(ctx, st, unguarded, bytes.NewReader(marked(30<<10, string(rune('a'+i)))), "", "")
		if err != nil {
			t.Fatal(err)
		}
		shas = append(shas, sha)
		// Distinct modification times, oldest first.
		past := time.Now().Add(time.Duration(i-100) * time.Second)
		_ = os.Chtimes(filepath.Join(media, sp.name(sha)), past, past)
		if s := size(); s > budget {
			t.Fatalf("after %d puts the spool holds %d bytes, budget %d", i+1, s, budget)
		}
	}
	// The newest three fit; the older ones went, rows and files.
	for i, sha := range shas {
		e, _ := st.GetSpool(ctx, sha)
		_, ferr := os.Stat(filepath.Join(media, sp.name(sha)))
		kept := i >= len(shas)-3
		if (e != nil) != kept || (ferr == nil) != kept {
			t.Fatalf("file %d: row %v, file %v, want kept %v", i, e != nil, ferr == nil, kept)
		}
	}
	// A file being read is never evicted, even when it is the oldest.
	oldest := shas[len(shas)-3]
	r, release, err := sp.open(oldest)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := sp.put(ctx, st, unguarded, bytes.NewReader(marked(30<<10, string(rune('A'+i)))), "", ""); err != nil {
			t.Fatal(err)
		}
	}
	if e, _ := st.GetSpool(ctx, oldest); e == nil {
		t.Fatalf("a file in use was evicted")
	}
	if b, err := io.ReadAll(r); err != nil || len(b) != 30<<10 {
		t.Fatalf("reading during eviction: %d %v", len(b), err)
	}
	release()
	if _, err := sp.put(ctx, st, unguarded, bytes.NewReader(marked(30<<10, "Z")), "", ""); err != nil {
		t.Fatal(err)
	}
	if e, _ := st.GetSpool(ctx, oldest); e != nil {
		t.Fatalf("the released file was kept over budget")
	}
	if s := size(); s > budget {
		t.Fatalf("spool holds %d bytes, budget %d", s, budget)
	}
	// A single file larger than the budget is kept (it was just asked for).
	big, err := sp.put(ctx, st, unguarded, bytes.NewReader(marked(budget+1, "B")), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readAll(t, sp, big); err != nil || len(got) != budget+1 {
		t.Fatalf("big file: %d %v", len(got), err)
	}
}

func TestSweep(t *testing.T) {
	dir := t.TempDir()
	media, tmp, transfer := filepath.Join(dir, "media"), filepath.Join(dir, "tmp"), filepath.Join(dir, "transfer")
	for _, d := range []string{media, tmp, transfer} {
		_ = os.MkdirAll(d, 0o700)
	}
	files := map[string]bool{
		filepath.Join(media, ".spool-1"):        false,
		filepath.Join(media, "kept"):            true,
		filepath.Join(tmp, "upload-1"):          false,
		filepath.Join(transfer, ".partial-1"):   false,
		filepath.Join(transfer, "the-host.png"): true,
	}
	for p := range files {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sweep(media, tmp, transfer)
	for p, kept := range files {
		if _, err := os.Stat(p); (err == nil) != kept {
			t.Fatalf("%s: present %v, want %v", p, err == nil, kept)
		}
	}
}

// unguarded writes the spool's rows with no epoch guard (the spool's own
// tests; the guard is instance.write's, tested in instance_test.go).
func unguarded(fn func() error) error { return fn() }
