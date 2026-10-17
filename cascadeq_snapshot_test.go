package cascadeq

import (
	"bytes"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// TestFailedWriteRemovesStaleSnapshot checks that a failed overflow write does
// not leave a tail snapshot on disk to be replayed after reopening.
func TestFailedWriteRemovesStaleSnapshot(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const (
			itemCount    = 49
			itemSize     = 512 * 1024
			snapInterval = time.Second
		)
		dir := t.TempDir()
		var logBuf strings.Builder
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))
		opts := []func(*Queue){
			WithGzip(true), WithMaxMemItems(32),
			WithMaxItemSize(itemSize), WithMaxMemory(64 * 1024 * 1024),
			WithSnapshotInterval(snapInterval), WithLogger(logger),
		}
		q, err := New(dir, opts...)
		if err != nil {
			t.Fatal(err)
		}

		r := rand.New(rand.NewSource(1))
		items := make([][]byte, itemCount)
		for i := range items {
			item := make([]byte, itemSize)
			r.Read(item)
			copy(item, fmt.Sprintf("%06d", i))
			items[i] = item
		}
		var wrn, rdn int
		put := func(n int) {
			for range n {
				if err := q.Put(items[wrn]); err != nil {
					t.Fatal(err)
				}
				wrn++
			}
		}
		drain := func(n int) {
			dst := make([][]byte, n)
			got := q.Drain(dst)
			for _, item := range dst[:got] {
				if !bytes.Equal(item, items[rdn]) {
					t.Fatalf("item %d does not match, got prefix %q", rdn, item[:6])
				}
				rdn++
			}
		}

		// Items 0-15 fill headQ and 16-31 fill tailQ. Item 32 starts the
		// write of items 16-31 to file 1, which succeeds.
		put(33)
		if stats := q.Stats(); len(stats.Files) != 1 {
			t.Fatalf("expected 1 overflow file, got %v", stats.Files)
		}

		// While idle, the queue snapshots item 32 to file 2.
		time.Sleep(2 * snapInterval)
		synctest.Wait()
		snapPath := filepath.Join(dir, "cq-2.dat.gz")
		if _, err := os.Stat(snapPath); err != nil {
			t.Fatalf("expected tail snapshot: %v", err)
		}

		// Gate the writer immediately before it closes and renames file 2.
		writeReady := make(chan struct{})
		allowClose := make(chan struct{})
		var releaseOnce sync.Once
		releaseWriter := func() {
			releaseOnce.Do(func() { close(allowClose) })
		}
		defer releaseWriter()
		q.beforeFileClose = func(path string) {
			if path == snapPath {
				close(writeReady)
				<-allowClose
			}
		}

		// Items 33-47 fill tailQ and item 48 starts writing items 32-47
		// to file 2. Delete its temporary file before allowing the rename.
		put(16)
		<-writeReady
		tmpFiles, err := filepath.Glob(snapPath + "-*")
		if err != nil {
			t.Fatal(err)
		}
		if len(tmpFiles) != 1 {
			t.Fatalf("expected one temporary file, got %v", tmpFiles)
		}
		if err := os.Remove(tmpFiles[0]); err != nil {
			t.Fatal(err)
		}
		releaseWriter()

		// Read the head queue and the first item of file 1, which is loaded and
		// removed while the write of file 2 is still in progress.
		drain(17)

		// Wait for the write of file 2 to fail. Its items return to tailQ.
		stats := q.Stats()
		if !strings.Contains(logBuf.String(), "failed to write overflow file") {
			t.Fatal("expected write of file 2 to fail")
		}
		if len(stats.Files) != 0 || stats.TailQLen != 17 {
			t.Fatalf("expected no files and 17 tail items, got files %v and %d tail items", stats.Files, stats.TailQLen)
		}

		// Read everything else, leaving the queue empty, and close it.
		drain(itemCount)
		if rdn != wrn {
			t.Fatalf("read %d items, wrote %d", rdn, wrn)
		}
		if err = q.Close(); err != nil {
			t.Fatal(err)
		}

		// After reopening, the queue must be empty and not replay any item.
		q, err = New(dir, opts...)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([][]byte, itemCount)
		if n := q.Drain(dst); n != 0 {
			t.Errorf("reopened queue replayed %d consumed items, first %q", n, dst[0][:6])
		}
		if err = q.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
