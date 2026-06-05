package cascadeq_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gammazero/cascadeq"
	"github.com/gammazero/fsutil"
)

func TestBadSaveDir(t *testing.T) {
	dir := t.TempDir()
	file, err := os.CreateTemp(dir, "somefile")
	if err != nil {
		panic("cannot create temp file")
	}
	if err = file.Close(); err != nil {
		panic(err)
	}
	defer os.Remove(file.Name())

	q, err := cascadeq.New("test", file.Name())
	if err == nil {
		t.Fatal("expected error on bad dir")
	}
	if q != nil {
		t.Fatal("New should return nil queue on error")
	}

	q, err = cascadeq.New("test", filepath.Join(dir, "not-a-dir"))
	if err != nil {
		t.Fatal(err)
	}
	ok, err := fsutil.DirExists(q.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected", q.Dir(), "to exist")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = cascadeq.New("test", "~not-a-user-0932488/foo")
	if err == nil {
		t.Fatal("expected error - unexpandable user")
	}
	expect := "cannot expand user-specific home dir"
	if !strings.Contains(err.Error(), expect) {
		t.Fatalf("expected error %q got %q", expect, err)
	}

	_, err = cascadeq.New("test", filepath.Join(dir, "no-such-dir", "my-queue-dir"))
	if err == nil {
		t.Fatal("expected error - no such directory")
	}
	expect = "no such file or directory"
	if !strings.Contains(err.Error(), expect) {
		t.Fatalf("expected error %q got %q", expect, err)
	}

	wrOnlyDir := filepath.Join(dir, "wronly")
	err = os.Mkdir(wrOnlyDir, 0300)
	if err != nil {
		panic(err)
	}
	defer os.Remove(wrOnlyDir)
	_, err = cascadeq.New("test", wrOnlyDir)
	if err == nil {
		t.Fatal("expected error - permission denied")
	}
	expect = "permission denied"
	if !strings.Contains(err.Error(), expect) {
		t.Fatalf("expected error %q got %q", expect, err)
	}
}

func TestDisappearingSaveDir(t *testing.T) {
	dir := t.TempDir()
	disappearDir := filepath.Join(dir, "disappear")

	// Test save directory removed after startup/
	q, err := cascadeq.New("test", disappearDir, cascadeq.WithMaxMemItems(32))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Remove(disappearDir)
	if err != nil {
		panic(err)
	}
	putN(t, 32, 0, q) // fill memory
	if err = q.Put([]byte("hello")); err == nil {
		t.Fatal("expected error")
	}
	if err = q.Close(); err == nil {
		t.Fatal("expected error")
	}

	// Test save directory removed between save and load.
	disappearDir = filepath.Join(dir, "disappear2")
	q, err = cascadeq.New("test", disappearDir, cascadeq.WithMaxMemItems(32))
	if err != nil {
		t.Fatal(err)
	}
	putN(t, 55, 0, q) // write multiple files
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = cascadeq.New("test", disappearDir, cascadeq.WithMaxMemItems(32))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	err = os.RemoveAll(disappearDir)
	if err != nil {
		panic(err)
	}
	getN(t, 16, 0, q) // trigger reading next file
	select {
	case <-q.Out():
		t.Fatal("should not have read more items")
	case <-q.Empty():
		t.Log("ok, queue is empty")
	}
}

func TestBadSizeLimits(t *testing.T) {
	_, err := cascadeq.New("test", "", cascadeq.WithMinItemSize(10), cascadeq.WithMaxItemSize(2))
	if err == nil {
		t.Fatal("expected error on backwards item size limits")
	}

	_, err = cascadeq.New("test", "", cascadeq.WithMinItemSize(64), cascadeq.WithMaxMemory(63))
	if err == nil {
		t.Fatal("expected error on min item size > max memory size")
	}
}

func TestBadFileNames(t *testing.T) {
	dir := t.TempDir()

	for _, name := range []string{"somefile", "somefile.dat", "test-somefile.dat"} {
		file, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			panic("cannot create temp file")
		}
		file.Close()
		defer os.Remove(file.Name())
	}

	q := makeQueue(t, dir)

	stats := q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("should not have any files")
	}
}

func TestUnwritableFile(t *testing.T) {
	const maxMemItems = 32
	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(maxMemItems))

	name := filepath.Join(q.Dir(), "test-1.dat")
	blockFile, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE|os.O_EXCL, 0400)
	if err != nil {
		panic(err)
	}
	defer func() {
		blockFile.Close()
		os.Remove(name)
	}()

	// Check that opened file gets removed when queue is saved to disk.
	wrn := putN(t, maxMemItems+1, 0, q)
	rdn := getAll(t, 0, q)
	if rdn != wrn {
		t.Fatal("did not read all items from queue")
	}
}

func TestBasicOperation(t *testing.T) {
	dir := t.TempDir()
	q := makeQueue(t, dir, cascadeq.WithMinItemSize(2), cascadeq.WithMaxItemSize(10))

	if q.Name() != "test" {
		t.Fatal("wrong name")
	}
	if q.Dir() != dir {
		t.Fatal("wrong directory")
	}

	// Test empty queue starts with empty signal.
	select {
	case <-q.Empty():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("initially empty queue did not signal it is empty")
	}

	err := q.Put(nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.Out():
		t.Fatal("nothing should be in queue")
	case <-time.After(time.Millisecond):
	}

	// Check item size limits.
	err = q.Put([]byte("this message is too large"))
	if err == nil {
		t.Fatal("expected error from oversize item")
	}
	err = q.Put([]byte("X"))
	if err == nil {
		t.Fatal("expected error from undersize item")
	}

	msgs := []string{"apple", "banana", "cherry"}

	for _, msg := range msgs {
		err = q.Put([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Put", msg)
	}

	timer := time.AfterFunc(time.Second, func() {
		q.Close()
	})

	var count int
loop:
	for {
		select {
		case data := <-q.Out():
			msg := string(data)
			if msg != msgs[count] {
				t.Fatalf("%s is not equal to %s", msg, msgs[count])
			}
			count++
			t.Log("Get", msg)
		case <-q.Empty():
			break loop
		}
	}
	timer.Stop()

	for count < len(msgs) {
		t.Fatal("did not get all expected items")
	}

	select {
	case <-q.Done():
		t.Fatal("should not be done")
	default:
	}

	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-q.Done():
	default:
		t.Fatal("should be done")
	}

	err = q.Put([]byte(msgs[0]))
	if err == nil {
		t.Fatal("expected error calling Enqueue after Close")
	}

	err = q.Clear()
	if err == nil {
		t.Fatal("expected error calling Enqueue after Close")
	}

	stats := q.Stats()
	if !stats.Closed || stats.MaxQLen != 0 || stats.MaxQBytes != 0 {
		t.Fatal("expected closed empty stats")
	}
}

// TestConcurrentPut exercises Put from many goroutines at once. Each caller
// must receive its own result, and every enqueued item must come back out
// exactly once. Run with -race to catch concurrent access to queue state.
func TestConcurrentPut(t *testing.T) {
	const (
		writers     = 8
		perWriter   = 250
		maxMemItems = 256 // small enough to force overflow to disk
	)
	total := writers * perWriter
	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(maxMemItems))

	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				if err := q.Put(fmt.Appendf(nil, "%02d-%05d", w, i)); err != nil {
					t.Errorf("writer %d item %d: %v", w, i, err)
					return
				}
			}
		})
	}
	wg.Wait()

	seen := make(map[string]struct{}, total)
	for len(seen) < total {
		select {
		case item := <-q.Out():
			s := string(item)
			if _, dup := seen[s]; dup {
				t.Fatalf("item %q returned more than once", s)
			}
			seen[s] = struct{}{}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out: read %d of %d items", len(seen), total)
		}
	}
}

func TestPutBatch(t *testing.T) {
	const maxMemItems = 32
	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(maxMemItems))

	// nil and empty batch are no-ops.
	if err := q.PutBatch(nil); err != nil {
		t.Fatal("PutBatch(nil) should return nil, got:", err)
	}
	if err := q.PutBatch([][]byte{}); err != nil {
		t.Fatal("PutBatch(empty) should return nil, got:", err)
	}

	single := [][]byte{[]byte("hello")}
	if err := q.PutBatch(single); err != nil {
		t.Fatal(err)
	}
	var count int
	for done := false; !done; {
		select {
		case got := <-q.Out():
			count++
			if count == 2 {
				t.Fatal("got more than 1 item")
			}
			want := single[0]
			if !bytes.Equal(got, want) {
				t.Fatalf("item 0: got %q, want %q", got, want)
			}
		case <-q.Empty():
			done = true
		}
	}

	// Basic ordering guarantee.
	batch := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	if err := q.PutBatch(batch); err != nil {
		t.Fatal(err)
	}
	for i, want := range batch {
		got := <-q.Out()
		if !bytes.Equal(got, want) {
			t.Fatalf("item %d: got %q, want %q", i, got, want)
		}
	}

	// Error on invalid item mid-batch: items before it are enqueued, after are not.
	oversized := make([]byte, cascadeq.DefaultMaxItemSize+1)
	mixed := [][]byte{[]byte("ok1"), oversized, []byte("ok2")}
	err := q.PutBatch(mixed)
	if err == nil {
		t.Fatal("expected error for oversize item")
	}
	got := <-q.Out()
	if !bytes.Equal(got, []byte("ok1")) {
		t.Fatalf("got %q, want %q", got, "ok1")
	}
	select {
	case <-q.Out():
		t.Fatal("ok2 should not have been enqueued after error")
	case <-q.Empty():
	}

	// Batch that causes overflow to disk preserves order.
	overflow := make([][]byte, maxMemItems+1) // 33 items
	for i := range overflow {
		overflow[i] = []byte(fmt.Sprintf("%06d", i))
	}
	if err := q.PutBatch(overflow); err != nil {
		t.Fatal(err)
	}
	stats := q.Stats()
	if len(stats.Files) == 0 {
		t.Fatal("expected at least one overflow file")
	}
	for i, want := range overflow {
		got := <-q.Out()
		if !bytes.Equal(got, want) {
			t.Fatalf("overflow item %d: got %q, want %q", i, got, want)
		}
	}

	// ErrClosed after Close.
	q2, err := cascadeq.New("test2", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	q2.Close()
	if err := q2.PutBatch([][]byte{[]byte("x"), []byte("y")}); !errors.Is(err, cascadeq.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestDrain(t *testing.T) {
	const maxMemItems = 32
	maxQ := maxMemItems / 2 // 16: items per half-queue

	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(maxMemItems))

	// Drain on empty queue returns 0.
	if n := q.Drain(make([][]byte, 4)); n != 0 {
		t.Fatalf("drain on empty queue: got %d, want 0", n)
	}

	// Drain with nil/empty dst returns 0 and does not consume items.
	if err := q.Put([]byte("probe")); err != nil {
		t.Fatal(err)
	}
	if n := q.Drain(nil); n != 0 {
		t.Fatalf("drain(nil): got %d, want 0", n)
	}
	if n := q.Drain([][]byte{}); n != 0 {
		t.Fatalf("drain(empty): got %d, want 0", n)
	}
	<-q.Out() // consume the probe item

	// Drain fills up to len(dst) items and preserves order.
	for i := range 6 {
		if err := q.Put([]byte(fmt.Sprintf("%06d", i))); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([][]byte, 4)
	if n := q.Drain(dst); n != 4 {
		t.Fatalf("drain 6 items into dst[4]: got %d, want 4", n)
	}
	for i, got := range dst {
		want := fmt.Sprintf("%06d", i)
		if string(got) != want {
			t.Fatalf("item %d: got %q, want %q", i, got, want)
		}
	}
	// 2 remain.
	dst2 := make([][]byte, 4)
	if n := q.Drain(dst2); n != 2 {
		t.Fatalf("drain remainder: got %d, want 2", n)
	}

	// After Drain, Out() still works (output channel not broken).
	for i := range 3 {
		if err := q.Put([]byte(fmt.Sprintf("%06d", i))); err != nil {
			t.Fatal(err)
		}
	}
	got := <-q.Out()
	if string(got) != "000000" {
		t.Fatalf("Out() after Drain: got %q, want %q", got, "000000")
	}
	q.Drain(make([][]byte, 8)) // drain the rest

	// Drain triggers tailQ→headQ promotion.
	// With maxQ=16: items 0-15 fill headQ, item 16 goes to tailQ.
	for i := range maxQ + 1 {
		if err := q.Put([]byte(fmt.Sprintf("%06d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Drain all of headQ.
	headBuf := make([][]byte, maxQ)
	if n := q.Drain(headBuf); n != maxQ {
		t.Fatalf("drain headQ: got %d, want %d", n, maxQ)
	}
	// Next drain must find the tailQ item (promotion).
	one := make([][]byte, 4)
	if n := q.Drain(one); n != 1 {
		t.Fatalf("after tailQ promotion: got %d, want 1", n)
	}
	if string(one[0]) != fmt.Sprintf("%06d", maxQ) {
		t.Fatalf("promoted item: got %q, want %q", one[0], fmt.Sprintf("%06d", maxQ))
	}

	if err := q.Clear(); err != nil {
		t.Fatal(err)
	}

	// Drain triggers file-load promotion.
	// 33 items: headQ=0-15, file-1=16-31, tailQ=32.
	for i := range maxMemItems + 1 {
		if err := q.Put([]byte(fmt.Sprintf("%06d", i))); err != nil {
			t.Fatal(err)
		}
	}
	stats := q.Stats()
	if len(stats.Files) != 1 {
		t.Fatalf("setup: want 1 overflow file, got %d", len(stats.Files))
	}
	// Drain headQ (0-15); post-promote should load file-1 into headQ.
	h1 := make([][]byte, maxQ)
	if n := q.Drain(h1); n != maxQ {
		t.Fatalf("first drain: got %d, want %d", n, maxQ)
	}
	// Drain from file-loaded headQ (16-31).
	h2 := make([][]byte, maxQ)
	if n := q.Drain(h2); n != maxQ {
		t.Fatalf("file-load drain: got %d, want %d", n, maxQ)
	}
	for i, got := range h2 {
		want := fmt.Sprintf("%06d", maxQ+i)
		if string(got) != want {
			t.Fatalf("file item %d: got %q, want %q", i, got, want)
		}
	}
	// 1 item remaining (item 32 in tailQ, promoted to headQ by post-promote).
	rem := make([][]byte, 4)
	if n := q.Drain(rem); n != 1 {
		t.Fatalf("remainder: got %d, want 1", n)
	}

	// Drain returns 0 after Close.
	q2, err := cascadeq.New("test2", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	q2.Put([]byte("x"))
	q2.Close()
	if n := q2.Drain(make([][]byte, 4)); n != 0 {
		t.Fatalf("drain after close: got %d, want 0", n)
	}
}

func TestAlternativeLogger(t *testing.T) {
	logOpts := slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}
	var b strings.Builder
	handler := slog.NewTextHandler(&b, &logOpts)
	attrs := []slog.Attr{
		slog.String("queueName", "my-test-queue"),
	}
	logger := slog.New(handler.WithAttrs(attrs))

	makeQueue(t, t.TempDir(), cascadeq.WithLogger(logger))
	logMsg := b.String()
	const expect = "queueName=my-test-queue"
	if !strings.Contains(logMsg, expect) {
		t.Fatalf("expected to see %q in log message", expect)
	}
}

func TestLoggedErrors(t *testing.T) {
	const maxMemItems = 32
	logOpts := slog.HandlerOptions{
		AddSource: true,
	}
	var b strings.Builder
	logger := slog.New(slog.NewTextHandler(&b, &logOpts))

	q := makeQueue(t, t.TempDir(), cascadeq.WithLogger(logger), cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(time.Second))

	putN(t, maxMemItems+1, 0, q)
	stats := q.Stats()
	if len(stats.Files) != 1 {
		t.Fatal("should have overflow file")
	}
	fname := "test-2.dat"
	dirName := filepath.Join(q.Dir(), fname)

	// Create a directory with a file, so that trying to remove a snapshot by that name fails.
	err := os.Mkdir(dirName, 0750)
	if err != nil {
		panic(err)
	}
	defer os.Remove(dirName)
	subFileName := filepath.Join(dirName, "somefile")
	file, err := os.Create(subFileName)
	if err != nil {
		panic("cannot create temp file")
	}
	file.Close()
	defer os.Remove(file.Name())

	rdn := getN(t, maxMemItems/2, 0, q)
	rdn = getN(t, 1, rdn, q)

	logMsg := b.String()
	t.Log("LOG MSG:", logMsg)

	expect := "msg=\"failed to remove snapshot\""
	if !strings.Contains(logMsg, expect) {
		t.Fatal("did not find expected content in log:", expect)
	}
	expect = "test-2.dat: directory not empty"
	if !strings.Contains(logMsg, expect) {
		t.Fatal("did not find expected content in log:", expect)
	}

	rdn = getAll(t, rdn, q)
	if rdn != maxMemItems+1 {
		t.Fatal("did not read all messages")
	}

	b.Reset()

	t.Log("log:", b.String())
}

func TestClear(t *testing.T) {
	const maxMemItems = 32
	dir := t.TempDir()

	q := makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(10*time.Millisecond))

	t.Log("writing 65 items to queue")
	putN(t, 65, 0, q)
	stats := q.Stats()
	if len(stats.Files) == 0 {
		t.Fatal("should have overflow files")
	}
	if stats.HeadQLen == 0 {
		t.Fatal("should have items in head queue")
	}

	// Wait long enough for snapshot.
	time.Sleep(20 * time.Millisecond)

	t.Log("clearing queue")
	err := q.Clear()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-q.Out():
		t.Fatal("cleared queue should have no output")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for empty signal after clearing queue")
	case <-q.Empty():
	}

	entries, err := os.ReadDir(q.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("should not have any save files, but found", len(entries))
	}

	stats = q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 || stats.HeadQBytes != 0 || stats.TailQBytes != 0 {
		t.Fatal("queue is not empty")
	}

	putN(t, 33, 0, q)
	stats = q.Stats()
	if len(stats.Files) != 1 {
		t.Fatal("should have 1 file, have", len(stats.Files))
	}
	dirName := filepath.Join(q.Dir(), "test-1.dat")
	err = os.Remove(dirName)
	if err != nil {
		panic(err)
	}

	// Create a directory with a file, so that trying to remove a snapshot by that name fails.
	err = os.Mkdir(dirName, 0750)
	if err != nil {
		panic(err)
	}
	defer os.Remove(dirName)
	subFileName := filepath.Join(dirName, "somefile")
	file, err := os.Create(subFileName)
	if err != nil {
		panic("cannot create temp file")
	}
	file.Close()
	defer os.Remove(file.Name())

	dirName = filepath.Join(q.Dir(), "test-2.dat")
	err = os.Mkdir(dirName, 0750)
	if err != nil {
		panic(err)
	}
	defer os.Remove(dirName)
	subFileName = filepath.Join(dirName, "somefile")
	file, err = os.Create(subFileName)
	if err != nil {
		panic("cannot create temp file")
	}
	file.Close()
	defer os.Remove(file.Name())

	err = q.Clear()
	if err == nil {
		t.Fatal("expect error")
	}
	expect := "test-1.dat: directory not empty"
	if !strings.Contains(err.Error(), expect) {
		t.Fatal("did not get expected log message:", expect)
	}
	expect = "test-2.dat: directory not empty"
	if !strings.Contains(err.Error(), expect) {
		t.Fatal("did not get expected log message:", expect)
	}
}

func TestOrderAcrossSave(t *testing.T) {
	dir := t.TempDir()

	q := makeQueue(t, dir)
	t.Log("Created new queue")

	msgs := []string{"apple", "avocado", "banana", "blueberry", "cherry", "coconut"}

	for _, msg := range msgs[:2] {
		err := q.Put([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Put", msg)
	}
	err := q.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Closed queue")

	q = makeQueue(t, dir)
	t.Log("Created new queue")

	for _, msg := range msgs[2:4] {
		err = q.Put([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Put", msg)
	}
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Closed queue")

	q = makeQueue(t, dir)
	t.Log("Created new queue")

	for _, msg := range msgs[4:] {
		err = q.Put([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Put", msg)
	}

	timer := time.AfterFunc(time.Second, func() {
		q.Close()
	})

	var count int
	for data := range q.Out() {
		msg := string(data)
		t.Log("Get", msg)
		if msg != msgs[count] {
			t.Fatalf("%s is not equal to %s", msg, msgs[count])
		}
		count++
	}
	timer.Stop()

	for count < len(msgs) {
		t.Fatal("did not get all expected items")
	}
}

func TestGzipOnOffAcrossSave(t *testing.T) {
	const maxMemItems = 32
	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(maxMemItems))

	wrn := putN(t, 50, 0, q)
	err := q.Close()
	if err != nil {
		t.Fatal(err)
	}

	q = makeQueue(t, q.Dir(), cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithGzip(true))
	wrn = putN(t, 50, wrn, q)
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	q = makeQueue(t, q.Dir(), cascadeq.WithMaxMemItems(maxMemItems))
	wrn = putN(t, 50, wrn, q)
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	q = makeQueue(t, q.Dir(), cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithGzip(true))
	wrn = putN(t, 50, wrn, q)

	// Read both gzip and non-gzip files with gzip on.
	rdn := getAll(t, 0, q)
	if rdn != wrn {
		t.Fatalf("number of items written (%d) and read (%d) do not match", wrn, rdn)
	}

	wrn = putN(t, 50, wrn, q)
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	q = makeQueue(t, q.Dir(), cascadeq.WithMaxMemItems(maxMemItems))
	wrn = putN(t, 50, wrn, q)

	// Read both gzip and non-gzip files with gzip off.
	rdn = getAll(t, rdn, q)
	if rdn != wrn {
		t.Fatalf("number of items written (%d) and read (%d) do not match", wrn, rdn)
	}
}

func logStats(t *testing.T, stats cascadeq.Stats) {
	t.Helper()
	t.Logf("Stats:\nHeadQBytes: %d\nHeadQLen: %d\nTailQBytes: %d\nTailQLen: %d\nFiles: %d", stats.HeadQBytes, stats.HeadQLen, stats.TailQBytes, stats.TailQLen, len(stats.Files))
}

func TestChangeSizeAcrossSave(t *testing.T) {
	const (
		smallLimit = 32
		bigLimit   = 256
		msgCount   = 1024
	)
	dir := t.TempDir()

	// Test saving to small files then reading into larger memory queue.
	q := makeQueue(t, dir, cascadeq.WithMaxMemItems(smallLimit))

	// Enqueue enough items to generate multiple save files.
	for i := range msgCount {
		msg := fmt.Sprintf("%04d", i)
		err := q.Put([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
	}
	logStats(t, q.Stats())
	err := q.Close()
	if err != nil {
		t.Fatal(err)
	}

	q = makeQueue(t, dir, cascadeq.WithMaxMemItems(bigLimit))
	logStats(t, q.Stats())

	var count int
	timeout := time.After(time.Second)
	for done := false; !done; {
		select {
		case data := <-q.Out():
			msg := string(data)
			//t.Log("Got", msg)
			expect := fmt.Sprintf("%04d", count)
			if msg != expect {
				t.Fatalf("%s is not equal to %s", msg, expect)
			}
			count++
		case <-timeout:
			done = true
		}
	}
	for count != msgCount {
		t.Fatalf("did not get all expected items, expexted %d, got %d", msgCount, count)
	}

	// Test saving to large files then reading into small memory queue.
	for i := range msgCount {
		msg := fmt.Sprintf("%04d", i)
		err = q.Put([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	q = makeQueue(t, dir, cascadeq.WithMaxMemItems(smallLimit))

	count = 0
	timeout = time.After(time.Second)
	for done := false; !done; {
		select {
		case data := <-q.Out():
			msg := string(data)
			//t.Log("Got", msg)
			expect := fmt.Sprintf("%04d", count)
			if msg != expect {
				t.Fatalf("%s is not equal to %s", msg, expect)
			}
			count++
		case <-timeout:
			done = true
		}
	}

	for count != msgCount {
		t.Fatalf("did not get all expected items, expexted %d, got %d", msgCount, count)
	}

	err = q.Clear()
	if err != nil {
		t.Fatal(err)
	}
	stats := q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 || stats.HeadQBytes != 0 || stats.TailQBytes != 0 {
		t.Fatal("queue is not empty")
	}
}

func TestAllIOLoop(t *testing.T) {
	const maxMemItems = 32
	dir := t.TempDir()

	// Test saving to small files then reading into larger memory queue.
	q := makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems))

	stats := q.Stats()
	if stats.MaxQLen != maxMemItems/2 {
		t.Fatal("excpected max items per queue to be", maxMemItems/2, "but is", stats.MaxQLen)
	}
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 || stats.HeadQBytes != 0 || stats.TailQBytes != 0 {
		t.Fatal("queue is not empty")
	}

	var rdn, wrn int

	t.Log("writing 16 items to queue")
	wrn = putN(t, 16, wrn, q)
	stats = q.Stats()
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}
	t.Log("q0 is full")

	t.Log("writing 8 items to queue")
	wrn = putN(t, 8, wrn, q)
	stats = q.Stats()
	if stats.HeadQLen != 16 {
		t.Fatalf("q0 should have 16 items but has %d", stats.HeadQLen)
	}
	if stats.TailQLen != 8 {
		t.Fatalf("q1 should have 8 items but has %d", stats.TailQLen)
	}
	t.Log("q0 has 16 items and q1 has 8, for a total of 24")

	t.Log("reading 4 items from queue")
	rdn = getN(t, 4, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != 12 {
		t.Fatalf("q0 should have 12 items but has %d", stats.HeadQLen)
	}
	if stats.TailQLen != 8 {
		t.Fatalf("q1 should have 8 items but has %d", stats.HeadQLen)
	}
	t.Log("q0 has 12 items and q1 has 8, for 20 total.")

	t.Log("writing 8 items to queue")
	wrn = putN(t, 8, wrn, q)
	stats = q.Stats()
	if stats.TailQLen != stats.MaxQLen {
		t.Fatalf("q1 should be full but has %d out of %d items", stats.TailQLen, stats.MaxQLen)
	}
	if stats.HeadQLen != 12 {
		t.Fatalf("q0 should have 12 items but has %d", stats.HeadQLen)
	}
	t.Log("q1 is full and q0 still has 12 items")

	t.Log("writing 1 item to queue")
	wrn = putN(t, 1, wrn, q)
	stats = q.Stats()
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}
	if stats.TailQLen != 13 {
		t.Fatalf("q1 should have 13 items but has %d", stats.TailQLen)
	}
	t.Log("shifted 4 items from q1 to q0, q0 is now full and q1 has 13 items")

	t.Log("reading 15 items from queue")
	rdn = getN(t, 15, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != 1 {
		t.Fatalf("q0 should have 1 items but has %d", stats.HeadQLen)
	}
	t.Log("q0 has 1 item left")

	t.Log("reading 1 item from queue to empty q0...")
	rdn = getN(t, 1, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != 13 {
		t.Fatalf("q0 should have 13 items but has %d", stats.HeadQLen)
	}
	if stats.TailQLen != 0 || stats.TailQBytes != 0 {
		t.Fatalf("q1 should be empty but has %d itemsd", stats.TailQLen)
	}
	t.Log("q0 emptied and swapped with q1, so now q0 has 13 items and q1 is empty")

	t.Log("writing 3 items to queue")
	wrn = putN(t, 3, wrn, q)
	stats = q.Stats()
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}
	if stats.TailQLen != 0 {
		t.Fatalf("q1 should be empty but has %d itemsd", stats.TailQLen)
	}
	t.Log("q0 is full and q1 is empty")

	t.Log("writing 16 items to queue")
	wrn = putN(t, 16, wrn, q)
	stats = q.Stats()
	if stats.TailQLen != stats.MaxQLen {
		t.Fatalf("q1 should be full but has %d out of %d items", stats.TailQLen, stats.MaxQLen)
	}
	t.Log("q1 is now full")

	t.Log("writing one more item to queue to make q1 save to file...")
	wrn = putN(t, 1, wrn, q)
	stats = q.Stats()
	if len(stats.Files) != 1 {
		t.Fatalf("expected 1 save file, but there are %d", len(stats.Files))
	}
	if stats.TailQLen != 1 {
		t.Fatalf("q1 should have 1 items but has %d", stats.TailQLen)
	}
	t.Log("saved q1 in file", stats.Files[0])

	t.Log("reading 16 items from queue to empty q0 and make it load from file...")
	rdn = getN(t, 16, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}
	if stats.TailQLen != 1 {
		t.Fatalf("q1 should have 1 items but has %d", stats.TailQLen)
	}
	if len(stats.Files) != 0 {
		t.Fatalf("expected 0 save files, but there are %d", len(stats.Files))
	}
	t.Log("loaded q0 from file, so now q0 is full and q1 has 1 item and there are 0 saved files")

	t.Log("writing 32 items to queue, which should result in 2 saved files...")
	putN(t, 32, wrn, q)
	stats = q.Stats()
	if len(stats.Files) != 2 {
		t.Fatalf("expected 2 save files, but there are %d", len(stats.Files))
	}
	if stats.TailQLen != 1 {
		t.Fatalf("q1 should have 1 items but has %d", stats.TailQLen)
	}
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}

	t.Log("clearing queue")
	err := q.Clear()
	if err != nil {
		t.Fatal(err)
	}
	stats = q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 || stats.HeadQBytes != 0 || stats.TailQBytes != 0 {
		t.Fatal("queue is not empty")
	}
	t.Log("queue is empty")
	wrn = rdn

	t.Log("writing 20 items to queue")
	wrn = putN(t, 20, wrn, q)
	stats = q.Stats()
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}
	if stats.TailQLen != 4 {
		t.Fatalf("q1 should have 4 items but has %d", stats.TailQLen)
	}
	t.Log("q0 is full and q1 has 4 items, closing to check for save files")

	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(q.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatal("expected 2 save files, have", len(entries))
	}

	q = makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems))

	stats = q.Stats()
	if len(stats.Files) != 1 {
		t.Fatal("expected 1 save file, but have", len(stats.Files))
	}
	if stats.HeadQLen != stats.MaxQLen {
		t.Fatalf("q0 should be full but has %d out of %d items", stats.HeadQLen, stats.MaxQLen)
	}
	t.Log("q0 is full and there is 1 save file remaining")

	t.Log("reading 20 items from queue to check everything loaded, should start at", rdn, "...")
	rdn = getN(t, 20, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != 0 {
		t.Fatalf("q0 should be empty but has %d items", stats.HeadQLen)
	}
	if stats.TailQLen != 0 {
		t.Fatalf("q1 should be empty but has %d items", stats.TailQLen)
	}
	if len(stats.Files) != 0 {
		t.Fatalf("expected 0 save files, but there are %d", len(stats.Files))
	}

	t.Log("writing 50 items to queue then reading until empty")
	rdnBefore := rdn
	putN(t, 50, wrn, q)
	rdn = getAll(t, rdn, q)
	if rdn != rdnBefore+50 {
		t.Fatal("did not read all 50 items when reading until empty")
	}
	stats = q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 || stats.HeadQBytes != 0 || stats.TailQBytes != 0 {
		t.Fatal("queue is not empty")
	}
}

func TestMissingAndEmptyFiles(t *testing.T) {
	const maxMemItems = 32
	dir := t.TempDir()

	q := makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems))

	var wrn int

	t.Log("writing 129 items to queue")
	wrn = putN(t, 129, wrn, q)
	stats := q.Stats()
	t.Log("Files:", len(stats.Files))

	err := q.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Stats was taken before close so it is missing file-0. This will delete
	// file 3 and truncate4, leaving files 0, 1, 2, 6, 7 with data.
	name := filepath.Join(q.Dir(), stats.Files[2])
	os.Remove(name)
	t.Log("removed file", name)

	name = filepath.Join(q.Dir(), stats.Files[3])
	os.Truncate(name, 0)
	t.Log("truncated file", name)

	name = filepath.Join(q.Dir(), stats.Files[4])
	os.Remove(name)
	err = os.Mkdir(name, 0750)
	if err != nil {
		panic(err)
	}
	defer os.Remove(name)
	t.Log("replaced file", name, "with directory of same name")

	var rdn int

	q = makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems))

	t.Log("reading 48 item from queue, loading from files 0, 1, 2.")
	getN(t, 48, rdn, q)
	// Items 48-63 removed in file 3, and 64-79 removed in file 4.
	// Next item to read is 80.
	rdn = 96
	stats = q.Stats()
	// Truncated file removed on last read when read-from queue emptied.
	expect := 2
	if len(stats.Files) != expect {
		t.Fatalf("there should be %d files remaining, got %d", expect, len(stats.Files))
	}

	t.Log("reading 33 item from queue")
	rdn = getN(t, 33, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 {
		logStats(t, stats)
		t.Fatal("queue not empty")
	}

	t.Log("writing 129 items to queue")
	putN(t, 129, wrn, q)
	stats = q.Stats()
	if !slices.Contains(stats.Files, filepath.Base(name)) {
		t.Fatal("files should have", filepath.Base(name))
	}
	fi, err := os.Stat(name)
	if err != nil {
		panic(err)
	}
	if fi.IsDir() {
		t.Fatal("directory should have been removed and replaced by file:", name)
	}

	rdn = getAll(t, rdn, q)
	stats = q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 {
		logStats(t, stats)
		t.Fatal("queue not empty")
	}
	expect = 129 + 129
	if rdn != expect {
		t.Fatalf("expect read num to be %d, got %d", expect, rdn)
	}

	err = q.Clear()
	if err != nil {
		t.Fatal(err)
	}
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Test missing file that was previously see at start.
	q = makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(maxMemItems))
	t.Log("writing 129 items to queue")
	putN(t, 129, 0, q)
	stats = q.Stats()

	name = filepath.Join(q.Dir(), stats.Files[2])
	os.Remove(name)
	t.Log("removed file", name)

	t.Log("reading 48 item from queue, loading from files 0, 1, 2.")
	getN(t, 48, 0, q)
	stats = q.Stats()
	// Truncated file removed on last read when read-from queue emptied.
	expect = 3
	if len(stats.Files) != expect {
		t.Fatalf("there should be %d files remaining, got %d", expect, len(stats.Files))
	}
	t.Log("reading remaining item from queue")
	rdn = getAll(t, 64, q)
	stats = q.Stats()
	if stats.TailQLen != 0 || stats.HeadQLen != 0 {
		t.Fatal("queue not mepty")
	}
	if rdn != 129 {
		t.Fatal("expected to have read up to message 129")
	}
}

func TestCorruptedFiles(t *testing.T) {
	const maxMemItems = 32
	dir := t.TempDir()
	q := makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems))

	var rdn, wrn int

	// Corrupt record length.
	t.Log("writing 64 items to queue")
	wrn = putN(t, 64, wrn, q)
	stats := q.Stats()
	if len(stats.Files) != 2 {
		t.Fatal("expected 2 files saved, got", len(stats.Files))
	}
	corrupt := filepath.Join(dir, stats.Files[0])
	t.Log("corrupting item size in save file:", corrupt)
	err := os.Truncate(corrupt, 2)
	if err != nil {
		panic(err)
	}
	rdn = getN(t, 16, rdn, q)
	rdn += 16 // should have skipped corrpted file
	rdn = getN(t, 1, rdn, q)
	stats = q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("should not have any save files:", stats.Files)
	}
	rdn = getAll(t, rdn, q)

	// Corrupt record data.
	t.Log("writing 64 items to queue")
	wrn = putN(t, 64, wrn, q)
	stats = q.Stats()
	if len(stats.Files) != 2 {
		t.Fatal("expected 2 files saved, got", len(stats.Files))
	}
	corrupt = filepath.Join(dir, stats.Files[0])
	t.Log("corrupting item data in save file:", corrupt)
	err = os.Truncate(corrupt, 5)
	if err != nil {
		panic(err)
	}
	rdn = getN(t, 16, rdn, q)
	rdn += 16 // should have skipped corrpted file
	rdn = getN(t, 1, rdn, q)
	stats = q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("should not have any save files:", stats.Files)
	}
	rdn = getAll(t, rdn, q)

	// Corrupt record data.
	t.Log("writing 64 items to queue")
	wrn = putN(t, 64, wrn, q)
	stats = q.Stats()
	if len(stats.Files) != 2 {
		t.Fatal("expected 2 files saved, got", len(stats.Files))
	}
	corrupt = filepath.Join(dir, stats.Files[0])
	t.Log("corrupting item size with oversize value in save file:", corrupt)
	writeFile, err := os.OpenFile(corrupt, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer writeFile.Close()
	dataLen := int32(cascadeq.DefaultMaxItemSize * 2)
	err = binary.Write(writeFile, binary.BigEndian, dataLen)
	if err != nil {
		panic(err)
	}
	err = writeFile.Close()
	if err != nil {
		panic(err)
	}
	rdn = getN(t, 16, rdn, q)
	rdn += 16 // should have skipped corrpted file
	rdn = getN(t, 1, rdn, q)
	stats = q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("should not have any save files:", stats.Files)
	}
	rdn = getAll(t, rdn, q)

	// Corrupt record length and prevent corrupted file from being renamed.
	t.Log("writing 64 items to queue")
	putN(t, 64, wrn, q)
	stats = q.Stats()
	if len(stats.Files) != 2 {
		t.Fatal("expected 2 files saved, got", len(stats.Files))
	}
	corrupt = filepath.Join(dir, stats.Files[0])
	t.Log("corrupting item size amd preventing renaming of save file:", corrupt)
	err = os.Truncate(corrupt, 2)
	if err != nil {
		panic(err)
	}
	rename := corrupt + cascadeq.BadFileExt
	os.Remove(rename)
	err = os.Mkdir(rename, 0750)
	if err != nil {
		panic(err)
	}
	defer os.Remove(rename)
	rdn = getN(t, 16, rdn, q)
	rdn += 16 // should have skipped corrpted file
	getN(t, 1, rdn, q)
	stats = q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("should not have any save files:", stats.Files)

	}
	// Check that bad file was written with .bad.1 to avoid existing .bad file.
	rename1 := rename + ".1"
	if !fsutil.FileExists(rename1) {
		t.Fatal("Did not find", rename1)
	}
	os.Remove(rename1)

	err = q.Clear()
	if err != nil {
		t.Fatal(err)
	}
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(rename)

	t.Log("Testing that snapshots are removed when all files are consumed")
	q = makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(100*time.Millisecond))

	// Create overflow files.
	wrn = putN(t, 70, 0, q)
	stats = q.Stats()

	// Corrupt the files.
	for _, fname := range stats.Files {
		name := filepath.Join(q.Dir(), fname)
		os.Truncate(name, 0)
		t.Log("truncated file", name)
	}
	// Wait for snapshot.
	time.Sleep(200 * time.Millisecond)
	// Consume the files.
	getN(t, 16, 0, q)

	stats = q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("expected all overflow files to be gone")
	}
	entries, err := os.ReadDir(q.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatal("expected 1 snapshot left, there are", len(entries), entries)
	}

	err = q.Clear()
	if err != nil {
		t.Fatal(err)
	}

	// Test that renaming bad files is limited to 9 retrys.
	putN(t, 64, wrn, q)
	stats = q.Stats()
	if len(stats.Files) != 2 {
		t.Fatal("expected 2 files saved, got", len(stats.Files))
	}
	corrupt = filepath.Join(dir, stats.Files[0])
	err = os.Truncate(corrupt, 2)
	if err != nil {
		panic(err)
	}
	rename = corrupt + cascadeq.BadFileExt
	rn := rename
	os.Remove(rename)
	for i := range 10 {
		if i > 0 {
			rn = rename + fmt.Sprintf(".%d", i)
		}
		err = os.Mkdir(rn, 0750)
		if err != nil {
			panic(err)
		}
		defer os.Remove(rn)
	}
	rdn = 70
	rdn = getN(t, 16, rdn, q)
	rdn += 16 // should have skipped corrpted file
	getN(t, 1, rdn, q)
	stats = q.Stats()
	if len(stats.Files) != 0 {
		t.Fatal("should not have any save files:", stats.Files)

	}
	// Check that bad file was written with .bad.1 to avoid existing .bad file.
	rename10 := rename + ".10"
	if fsutil.FileExists(rename10) {
		t.Fatal("should not have bad file \".bad.10\"")
	}
}

func TestReadAll(t *testing.T) {
	const msgCount = 100
	dir := t.TempDir()

	t.Run("read-all-max-items", func(t *testing.T) {
		q := makeQueue(t, dir, cascadeq.WithMaxMemItems(32))
		putN(t, msgCount, 0, q)
		rdn := getAll(t, 0, q)
		if rdn != msgCount {
			t.Fatalf("only read %d out of %d messages", rdn, msgCount)
		}
	})

	t.Run("read-all-max-mem", func(t *testing.T) {
		q := makeQueue(t, dir, cascadeq.WithMaxMemory(119))
		putN(t, msgCount, 0, q)
		rdn := getAll(t, 0, q)
		if rdn != msgCount {
			t.Fatalf("only read %d out of %d messages", rdn, msgCount)
		}
	})
}

func TestFastWrSlowRdSlowWrFastRd(t *testing.T) {
	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(32))
	var rdn, wrn int

	t.Log("write fast read slow")
	for range 64 {
		wrn = putN(t, 2, wrn, q)
		rdn = getN(t, 1, rdn, q)
	}

	t.Log("read fast write slow")
	for range 63 {
		rdn = getN(t, 2, rdn, q)
		wrn = putN(t, 1, wrn, q)
	}

	getN(t, 1, rdn, q)
	stats := q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 {
		t.Fatal("queue is not empty")
	}
}

func TestStress(t *testing.T) {
	const count = 10000
	items := randItems(10, 10, 60)

	dir := t.TempDir()

	q := makeQueue(t, dir,
		cascadeq.WithSnapshotInterval(time.Second),
		cascadeq.WithMaxMemory(8192),
		cascadeq.WithMaxMemItems(8192),
	)
	var rdn, wrn int
	var err error
	t.Log("write fast read slow")
	for range count {
		if err = q.Put(items[wrn%len(items)]); err != nil {
			t.Fatal(err)
		}
		wrn++

		if err = q.Put(items[wrn%len(items)]); err != nil {
			t.Fatal(err)
		}
		wrn++

		data := <-q.Out()
		item := items[rdn%len(items)]
		if !bytes.Equal(data, item) {
			t.Fatalf("%s is not equal to expected %s", string(data), string(item))
		}
		rdn++
	}

	stats := q.Stats()
	t.Log("Have", len(stats.Files), "files")

	t.Log("read fast write slow")
	for range count - 1 {
		data := <-q.Out()
		item := items[rdn%len(items)]
		if !bytes.Equal(data, item) {
			t.Fatalf("%s is not equal to expected %s", string(data), string(item))
		}
		rdn++

		data = <-q.Out()
		item = items[rdn%len(items)]
		if !bytes.Equal(data, item) {
			t.Fatalf("%s is not equal to expected %s", string(data), string(item))
		}
		rdn++

		if err = q.Put(items[wrn%len(items)]); err != nil {
			t.Fatal(err)
		}
		wrn++
	}

	data := <-q.Out()
	item := items[rdn%len(items)]
	if !bytes.Equal(data, item) {
		t.Fatalf("%s is not equal to expected %s", string(data), string(item))
	}

	stats = q.Stats()
	if stats.HeadQLen != 0 || stats.TailQLen != 0 || len(stats.Files) != 0 {
		t.Fatal("queue is not empty")
	}
}

func BenchmarkStress(b *testing.B) {
	items := randItems(10, 10, 60)

	dir := b.TempDir()

	q, err := cascadeq.New("test", dir,
		cascadeq.WithSnapshotInterval(time.Second),
		cascadeq.WithMaxMemory(4096),
		cascadeq.WithMaxMemItems(8192))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	var rdn, wrn int

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if err = q.Put(items[wrn%len(items)]); err != nil {
			b.Fatal(err)
		}
		wrn++

		if err = q.Put(items[wrn%len(items)]); err != nil {
			b.Fatal(err)
		}
		wrn++

		<-q.Out()
		rdn++
	}

	for range b.N - 1 {
		<-q.Out()
		rdn++

		<-q.Out()
		rdn++

		if err = q.Put(items[wrn%len(items)]); err != nil {
			b.Fatal(err)
		}
		wrn++
	}

	data := <-q.Out()
	item := items[rdn%len(items)]
	if !bytes.Equal(data, item) {
		b.Fatalf("%s is not equal to expected %s", string(data), string(item))
	}
}

func TestZeroLength(t *testing.T) {
	q := makeQueue(t, t.TempDir(), cascadeq.WithMaxMemItems(32))
	zeroBytes := []byte{}

	for range 65 {
		if err := q.Put(zeroBytes); err != nil {
			t.Fatal(err)
		}
	}

	var data []byte
	select {
	case data = <-q.Out():
	case <-q.Empty():
		t.Fatal("should not be empty")
	}

	if len(data) != 0 {
		t.Fatal("expected zero-length data")
	}

	stats := q.Stats()
	if len(stats.Files) != 3 {
		t.Fatal("expected 3 save files, got", len(stats.Files))
	}

	for range 32 {
		select {
		case data = <-q.Out():
		case <-q.Empty():
			t.Fatal("should not be empty")
		}
		if len(data) != 0 {
			t.Fatal("expected zero-length data")
		}
	}

	stats = q.Stats()
	if len(stats.Files) != 1 {
		t.Fatal("expected 1 save file, got", len(stats.Files))
	}
}

func TestSnapshot(t *testing.T) {
	const snapInterval = time.Second
	const maxMemItems = 32
	dir := t.TempDir()

	synctest.Test(t, func(t *testing.T) {
		q := makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(snapInterval))
		wrn := putN(t, 129, 0, q)
		time.Sleep(2 * time.Second)

		rdn := getN(t, 128, 0, q)
		time.Sleep(2 * time.Second)

		wrn = putN(t, 16, wrn, q)

		err := q.Close()
		if err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(q.Dir())
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 {
			t.Fatal("expected 2 save files, got", len(entries))
		}

		q = makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(snapInterval))
		rdn = getAll(t, rdn, q)
		if rdn != wrn {
			t.Fatalf("did not read expected value: rdn=%d != wrn=%d", rdn, wrn)
		}

		entries, err = os.ReadDir(q.Dir())
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatal("should not have any files, has", len(entries))
		}

		wrn = putN(t, 30, 0, q)
		time.Sleep(2 * snapInterval)
		rdn = getN(t, 10, 0, q)
		err = q.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Make sure that no items are redelivered after reading in snapshots.
		q = makeQueue(t, dir, cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(snapInterval))
		rdn = getAll(t, rdn, q)
		if rdn != wrn {
			t.Fatal("did not read all items")
		}

		stats := q.Stats()
		if len(stats.Files) != 0 {
			t.Fatal("should have 0 file")
		}

		// Test tail snapshot optimization.
		putN(t, 48, wrn, q)
		stats = q.Stats()
		if len(stats.Files) != 1 {
			t.Fatal("should have 1 file")
		}
		if stats.TailQBytes < stats.MaxQBytes && stats.TailQLen < stats.MaxQLen {
			t.Fatal("tailQ should be full")
		}
		// Wait for snapshot.
		time.Sleep(2 * snapInterval)
		// Check that overflow was created and not snapshot.
		stats = q.Stats()
		if len(stats.Files) != 2 {
			t.Fatal("should have 2 files, but there are", len(stats.Files), stats.Files)
		}
		if stats.TailQBytes != 0 || stats.TailQLen != 0 {
			t.Log("stats.TailQBytes:", stats.TailQBytes)
			t.Log("stats.TailQLen:", stats.TailQLen)
			t.Fatal("tailQ should be empty")
		}
		if stats.HeadQBytes < stats.MaxQBytes && stats.HeadQLen < stats.MaxQLen {
			t.Fatal("headQQ should be full")
		}

		err = q.Clear()
		if err != nil {
			t.Fatal(err)
		}
		err = q.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	synctest.Test(t, func(t *testing.T) {
		logOpts := slog.HandlerOptions{
			AddSource: true,
		}
		var b strings.Builder
		logger := slog.New(slog.NewTextHandler(&b, &logOpts))

		q := makeQueue(t, dir, cascadeq.WithLogger(logger), cascadeq.WithMaxMemItems(maxMemItems), cascadeq.WithSnapshotInterval(snapInterval))
		putN(t, maxMemItems+(maxMemItems/2), 0, q)

		dirName := filepath.Join(q.Dir(), "test-1.dat")
		err := os.Remove(dirName)
		if err != nil {
			panic(err)
		}
		err = os.Mkdir(dirName, 0750)
		if err != nil {
			panic(err)
		}
		defer os.Remove(dirName)
		subFileName := filepath.Join(dirName, "somefile")
		file, err := os.Create(subFileName)
		if err != nil {
			panic("cannot create temp file")
		}
		file.Close()
		defer os.Remove(file.Name())

		dirName = filepath.Join(q.Dir(), "test-2.dat")
		err = os.Mkdir(dirName, 0750)
		if err != nil {
			panic(err)
		}
		defer os.Remove(dirName)
		subFileName = filepath.Join(dirName, "somefile")
		file, err = os.Create(subFileName)
		if err != nil {
			panic("cannot create temp file")
		}
		file.Close()
		defer os.Remove(file.Name())

		dirName = filepath.Join(q.Dir(), "test-0.dat")
		err = os.Mkdir(dirName, 0750)
		if err != nil {
			panic(err)
		}
		defer os.Remove(dirName)
		subFileName = filepath.Join(dirName, "somefile")
		file, err = os.Create(subFileName)
		if err != nil {
			panic("cannot create temp file")
		}
		file.Close()
		defer os.Remove(file.Name())

		// Wait for snapshot, then for the queue goroutine to become idle so
		// its log writes are ordered before the read below.
		time.Sleep(2 * snapInterval)
		synctest.Wait()

		logMsg := b.String()
		expect := "msg=\"failed to save tail queue to file\""
		if !strings.Contains(logMsg, expect) {
			t.Fatal("log did not contain expected message:", expect)
		}
		expect = "msg=\"failed to save snapshot to file\""
		if !strings.Contains(logMsg, expect) {
			t.Fatal("log did not contain expected message:", expect)
		}
	})
}

func BenchmarkFillEmpty(b *testing.B) {
	const (
		memLimit = 256
		msgCount = 2048
	)

	msgs := make([][]byte, 0, msgCount)
	for i := range msgCount {
		msgs = append(msgs, []byte(fmt.Sprintf("%d", i)))
	}

	dir := b.TempDir()

	// Test saving to small files then reading into larger memory queue.
	q, err := cascadeq.New("test", dir, cascadeq.WithMaxMemItems(256))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	for b.Loop() {
		for _, msg := range msgs {
			err = q.Put(msg)
			if err != nil {
				b.Fatal(err)
			}
		}

		for count := range msgCount {
			data := <-q.Out()
			if len(data) != len(msgs[count]) {
				b.Fatalf("got bad data, expected %s, got %s", string(msgs[count]), string(data))
			}
		}
	}
}

func BenchmarkFastWrSlowRdSlowWrFastRd(b *testing.B) {
	dir := b.TempDir()

	// Test saving to small files then reading into larger memory queue.
	q, err := cascadeq.New("test", dir, cascadeq.WithMaxMemItems(32))
	//q, err := cascadeq.New("test", dir, cascadeq.WithMaxMemSize(4096))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	for b.Loop() {
		var rdn, wrn int
		for range 256 {
			msg := fmt.Sprintf("%d", wrn)
			q.Put([]byte(msg))
			wrn++

			msg = fmt.Sprintf("%d", wrn)
			q.Put([]byte(msg))
			wrn++

			msg = fmt.Sprintf("%d", rdn)
			data := <-q.Out()
			if string(data) != msg {
				b.Fatalf("%s is not equal to %s", string(data), msg)
			}
			rdn++
		}

		for range 255 {
			data := <-q.Out()
			msg := fmt.Sprintf("%d", rdn)
			if string(data) != msg {
				b.Fatalf("%s is not equal to %s", string(data), msg)
			}
			rdn++

			data = <-q.Out()
			msg = fmt.Sprintf("%d", rdn)
			if string(data) != msg {
				b.Fatalf("%s is not equal to %s", string(data), msg)
			}
			rdn++

			msg = fmt.Sprintf("%d", wrn)
			q.Put([]byte(msg))
			wrn++
		}

		data := <-q.Out()
		msg := fmt.Sprintf("%d", rdn)
		if string(data) != msg {
			b.Fatalf("%s is not equal to %s", string(data), msg)
		}
	}
}

func BenchmarkLargeFilesGzipOffOn(b *testing.B) {
	const (
		maxItems = 64
		msgCount = 128 // ~256K (4 * 32Kib files))
	)

	msgs := randItems(msgCount, 512, 1536)

	b.Run("without-gzip", func(b *testing.B) {
		dir := b.TempDir()

		for b.Loop() {
			q, err := cascadeq.New("test", dir, cascadeq.WithMaxMemItems(maxItems))
			if err != nil {
				b.Fatal(err)
			}
			for _, msg := range msgs {
				err = q.Put(msg)
				if err != nil {
					b.Fatal(err)
				}
			}
			q.Close()

			q, err = cascadeq.New("test", dir, cascadeq.WithMaxMemItems(maxItems))
			if err != nil {
				b.Fatal(err)
			}
			for count := range len(msgs) {
				data := <-q.Out()
				if !bytes.Equal(data, msgs[count]) {
					b.Fatalf("got bad data, expected %s, got %s", string(msgs[count]), string(data))
				}
			}
			q.Close()
		}
	})

	b.Run("with-gzip", func(b *testing.B) {
		dir := b.TempDir()

		for b.Loop() {
			q, err := cascadeq.New("test", dir, cascadeq.WithMaxMemItems(maxItems))
			if err != nil {
				b.Fatal(err)
			}
			for _, msg := range msgs {
				err = q.Put(msg)
				if err != nil {
					b.Fatal(err)
				}
			}
			q.Close()

			q, err = cascadeq.New("test", dir, cascadeq.WithMaxMemItems(maxItems))
			if err != nil {
				b.Fatal(err)
			}
			for count := range len(msgs) {
				data := <-q.Out()
				if !bytes.Equal(data, msgs[count]) {
					b.Fatalf("got bad data, expected %s, got %s", string(msgs[count]), string(data))
				}
			}
			q.Close()
		}
	})
}

func BenchmarkPut16(b *testing.B) {
	benchmarkPut(16, b)
}
func BenchmarkPut64(b *testing.B) {
	benchmarkPut(64, b)
}
func BenchmarkPut256(b *testing.B) {
	benchmarkPut(256, b)
}
func BenchmarkPut1024(b *testing.B) {
	benchmarkPut(1024, b)
}
func BenchmarkPut4096(b *testing.B) {
	benchmarkPut(4096, b)
}
func BenchmarkPut16384(b *testing.B) {
	benchmarkPut(16384, b)
}
func BenchmarkPut65536(b *testing.B) {
	benchmarkPut(65536, b)
}
func BenchmarkPut262144(b *testing.B) {
	benchmarkPut(262144, b)
}
func BenchmarkPut1048576(b *testing.B) {
	benchmarkPut(1048576, b)
}
func benchmarkPut(size int64, b *testing.B) {
	b.ReportAllocs()
	qName := "bench_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir := b.TempDir()
	q, err := cascadeq.New(qName, tmpDir, cascadeq.WithMaxMemItems(64), cascadeq.WithMaxItemSize(int(size)), cascadeq.WithMaxMemory(4*cascadeq.DefaultMaxMemory))
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(size)
	data := make([]byte, size)
	b.StartTimer()

	for b.Loop() {
		err = q.Put(data)
		if err != nil {
			panic(err)
		}
	}
	q.Close()
}

func BenchmarkGet16(b *testing.B) {
	benchmarkGet(16, b)
}
func BenchmarkGet64(b *testing.B) {
	benchmarkGet(64, b)
}
func BenchmarkGet256(b *testing.B) {
	benchmarkGet(256, b)
}
func BenchmarkGet1024(b *testing.B) {
	benchmarkGet(1024, b)
}
func BenchmarkGet4096(b *testing.B) {
	benchmarkGet(4096, b)
}
func BenchmarkGet16384(b *testing.B) {
	benchmarkGet(16384, b)
}
func BenchmarkGet65536(b *testing.B) {
	benchmarkGet(65536, b)
}
func BenchmarkGet262144(b *testing.B) {
	benchmarkGet(262144, b)
}
func BenchmarkGet1048576(b *testing.B) {
	benchmarkGet(1048576, b)
}

func benchmarkGet(size int64, b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	qName := "bench_get" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir := b.TempDir()
	q, err := cascadeq.New(qName, tmpDir, cascadeq.WithMaxMemItems(64), cascadeq.WithMaxItemSize(int(size)), cascadeq.WithMaxMemory(4*cascadeq.DefaultMaxMemory))
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(size)
	data := make([]byte, size)
	for range b.N {
		q.Put(data)
	}
	b.StartTimer()

	for range b.N {
		<-q.Out()
	}
	q.Close()
}

func BenchmarkDrain1(b *testing.B)   { benchmarkDrain(1, b) }
func BenchmarkDrain4(b *testing.B)   { benchmarkDrain(4, b) }
func BenchmarkDrain16(b *testing.B)  { benchmarkDrain(16, b) }
func BenchmarkDrain64(b *testing.B)  { benchmarkDrain(64, b) }
func BenchmarkDrain256(b *testing.B) { benchmarkDrain(256, b) }

func benchmarkDrain(batchSize int, b *testing.B) {
	const itemSize = 256
	b.ReportAllocs()
	b.StopTimer()
	qName := "bench_drain" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	q, err := cascadeq.New(qName, b.TempDir(), cascadeq.WithMaxMemItems(64), cascadeq.WithMaxItemSize(itemSize), cascadeq.WithMaxMemory(4*cascadeq.DefaultMaxMemory))
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, itemSize)
	dst := make([][]byte, batchSize)
	for range b.N * batchSize {
		q.Put(data)
	}
	b.SetBytes(int64(itemSize * batchSize))
	b.StartTimer()

	for range b.N {
		q.Drain(dst)
	}
	q.Close()
}

func makeQueue(t *testing.T, dir string, options ...func(*cascadeq.Queue)) *cascadeq.Queue {
	t.Helper()

	q, err := cascadeq.New("test", dir, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := q.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
	return q
}

func getN(t *testing.T, n, rdn int, q *cascadeq.Queue) int {
	t.Helper()
	for range n {
		data := <-q.Out()
		msg := fmt.Sprintf("%06d", rdn)
		if string(data) != msg {
			t.Fatalf("%s is not equal to expected %s", string(data), msg)
		}
		rdn++
	}
	return rdn
}

func getAll(t *testing.T, rdn int, q *cascadeq.Queue) int {
	t.Helper()
	for {
		select {
		case data := <-q.Out():
			msg := fmt.Sprintf("%06d", rdn)
			if string(data) != msg {
				t.Fatalf("%s is not equal to expected %s", string(data), msg)
			}
			rdn++
		case <-q.Empty():
			return rdn
		}
	}
}

func putN(t *testing.T, n, wrn int, q *cascadeq.Queue) int {
	t.Helper()
	for range n {
		if err := q.Put([]byte(fmt.Sprintf("%06d", wrn))); err != nil {
			t.Fatal(err)
		}
		wrn++
	}
	return wrn
}

func randItems(n, minLen, maxLen int) [][]byte {
	const charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"
	randSrc := rand.New(rand.NewSource(time.Now().UnixNano()))
	charsetLen := len(charset)

	results := make([][]byte, 0, n)
	randMax := maxLen + 1 - minLen

	for range n {
		length := randSrc.Intn(randMax) + minLen

		b := make([]byte, length)
		for i := range length {
			b[i] = charset[randSrc.Intn(charsetLen)]
		}
		results = append(results, b)
	}
	return results
}
