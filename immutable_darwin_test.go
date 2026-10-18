//go:build darwin

package cascadeq_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gammazero/cascadeq"
)

// ufImmutable is the UF_IMMUTABLE file flag from sys/stat.h. A file with this
// flag cannot be removed or replaced by a rename, while other files in the same
// directory are unaffected. The file owner can set and clear it.
const ufImmutable = 0x2

// TestStaleSnapshotRemovedAtClose checks that a stale tail snapshot is not
// replayed after the queue is reopened when its removal fails while the queue
// is running, and that Close removes it. The removal fails for real: the
// snapshot is made immutable while the last overflow file is consumed, which is
// when the queue removes the snapshot because file numbering resets.
//
// The test runs in a synctest bubble, so the idle snapshot happens without real
// waiting, and no snapshot can happen while the test is not blocked.
func TestStaleSnapshotRemovedAtClose(t *testing.T) {
	t.Parallel()
	testStaleSnapshotRemoved(t, false)
}

// TestStaleSnapshotRemovedByClear is like TestStaleSnapshotRemovedAtClose, but
// checks that Clear removes the stale tail snapshot.
func TestStaleSnapshotRemovedByClear(t *testing.T) {
	t.Parallel()
	testStaleSnapshotRemoved(t, true)
}

func testStaleSnapshotRemoved(t *testing.T, useClear bool) {
	synctest.Test(t, func(t *testing.T) {
		const snapInterval = time.Second
		dir := t.TempDir()
		opts := []func(*cascadeq.Queue){
			cascadeq.WithMaxMemItems(32), cascadeq.WithSnapshotInterval(snapInterval),
		}
		q, err := cascadeq.New(dir, opts...)
		if err != nil {
			t.Fatal(err)
		}

		// Items 0-15 fill the head queue and 16-31 fill the tail queue. Item 32 starts
		// the write of items 16-31 to file 1.
		wrn := putN(t, 33, 0, q)
		if stats := q.Stats(); len(stats.Files) != 1 {
			t.Fatalf("expected 1 overflow file, got %v", stats.Files)
		}

		// While idle, the queue snapshots the tail queue (item 32) to file 2.
		time.Sleep(2 * snapInterval)
		synctest.Wait()
		snapPath := filepath.Join(dir, "cq-2.dat")
		if _, err = os.Stat(snapPath); err != nil {
			t.Fatalf("expected tail snapshot: %v", err)
		}

		if err = syscall.Chflags(snapPath, ufImmutable); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = syscall.Chflags(snapPath, 0)
		})

		// Read all items. Consuming file 1 resets file numbering, and the removal of
		// the snapshot fails, so it is left on disk with item 32, which is also read.
		rdn := getAll(t, 0, q)
		if rdn != wrn {
			t.Fatalf("read %d items, wrote %d", rdn, wrn)
		}
		if _, err = os.Stat(snapPath); err != nil {
			t.Fatalf("expected stale tail snapshot to remain: %v", err)
		}

		// Once the snapshot can be removed, closing or clearing the queue removes it.
		if err = syscall.Chflags(snapPath, 0); err != nil {
			t.Fatal(err)
		}
		if useClear {
			if err = q.Clear(); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(snapPath); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("expected clear to remove stale tail snapshot, got %v", err)
			}
		}
		if err = q.Close(); err != nil {
			t.Fatal(err)
		}

		// After reopening, the queue must be empty and not replay item 32.
		q, err = cascadeq.New(dir, opts...)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([][]byte, wrn)
		if n := q.Drain(dst); n != 0 {
			t.Errorf("reopened queue replayed %d consumed items, first %q", n, dst[0])
		}
		if err = q.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
