package cascadeq

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/deque"
	"github.com/gammazero/fsutil"
	"github.com/gammazero/fsutil/atomicfile"
)

const (
	DefaultMaxMemory   = 1024 * 1024
	DefaultMaxMemItems = 4096
	DefaultMaxItemSize = 65536
)

const (
	BadFileExt = ".bad"

	fileExt    = ".dat"
	filePrefix = "cq-"
	gzipExt    = ".gz"
	pkgName    = "cascadeq"
)

// ErrClosed is returned when I/O is attempted on a closed Queue.
var ErrClosed = errors.New("closed")

// ErrIsDirectory is returned when trying to read a queue file that is a
// directory.
var ErrIsDirectory = errors.New("queue file is a directory")

// putReq carries an item to enqueue together with the channel on which the
// event loop sends its result. Each Put supplies its own response channel so
// that concurrent callers cannot receive each other's results.
type putReq struct {
	item  []byte
	items [][]byte // non-nil for PutBatch; nil for Put
	rsp   chan error
}

type drainReq struct {
	dst [][]byte
	rsp chan int
}

// Queue implements a filesystem backed FIFO queue.
type Queue struct {
	maxMemItems  int
	maxMemBytes  int
	maxItemSize  int
	minItemSize  int
	snapInterval time.Duration

	dir  string
	name string

	closeErr   error
	closeMutex sync.RWMutex
	logger     *slog.Logger

	done         chan struct{}
	empty        chan struct{}
	input        chan putReq
	drainReqChan chan drainReq
	output       chan []byte
	clearReqChan chan chan error
	statsReqChan chan chan Stats

	rspErrPool sync.Pool
	rspNPool   sync.Pool

	files  deque.Deque[int64]
	closed bool
	gzip   bool
}

// Stats holds information about the Queue internal state.
type Stats struct {
	MaxQBytes  int
	MaxQLen    int
	HeadQBytes int
	HeadQLen   int
	TailQBytes int
	TailQLen   int
	Files      []string
	Closed     bool
}

// WithGzip enables, if passed true, gzip compression of buffer files.
func WithGzip(enable bool) func(*Queue) {
	return func(q *Queue) {
		q.gzip = enable
	}
}

// WithLogger sets the slog.Logger instance to use for logging. This replaces
// the default cascadeq slog.Logger, which writes JSON to stderr. A nil logger
// is ignored, leaving the default in place.
func WithLogger(logger *slog.Logger) func(*Queue) {
	return func(q *Queue) {
		if logger != nil {
			q.logger = logger
		}
	}
}

// WithMaxMemory sets the maximum amount of memory used by all items in the
// queue before items are written to disk.
func WithMaxMemory(maxBytes int) func(*Queue) {
	return func(q *Queue) {
		if maxBytes > 0 {
			q.maxMemBytes = maxBytes
		}
	}
}

// WithMaxMemItems sets the maximum number of items that the queue keeps in
// memory before items are written to disk. The value is rounded up to the
// next power of two, so the effective limit may be larger than requested.
// The effective limit is never less than 32.
func WithMaxMemItems(maxItems int) func(*Queue) {
	return func(q *Queue) {
		if maxItems > 0 {
			n := 32
			for n < maxItems {
				n <<= 1
			}
			q.maxMemItems = n
		}
	}
}

// WithMaxItemSize specifies the maximum allowed size of a single []byte item
// in the queue.
func WithMaxItemSize(maxSize int) func(*Queue) {
	return func(q *Queue) {
		q.maxItemSize = min(max(1, maxSize), math.MaxInt32)
	}
}

// WithMinItemSize specifies the minimum allowed size of a single []byte item
// in the queue.
func WithMinItemSize(minSize int) func(*Queue) {
	return func(q *Queue) {
		q.minItemSize = min(max(0, minSize), math.MaxInt32)
	}
}

// WithSnapshotInterval enables snapshots and sets the amount of time that the
// queue must be idle before saving a snapshot of the items stored in memory.
// The queue must be idle for at least half of the specified time and at most
// the entire specified time. Idle snapshots are disabled by default and are
// enabled when a positive value is specified for this option.
func WithSnapshotInterval(d time.Duration) func(*Queue) {
	return func(q *Queue) {
		if d > 0 {
			q.snapInterval = max(time.Millisecond, d/2)
		}
	}
}

// New creates a new file-backed FIFO queue instance. Files for this queue are
// stored in the specified directory. A queue that stores different items must
// use a different directory.
func New(dir string, options ...func(*Queue)) (*Queue, error) {
	if dir == "" {
		return nil, errors.New("directory not specified")
	}
	var err error
	dir, err = fsutil.ExpandHome(filepath.Clean(dir))
	if err != nil {
		return nil, err
	}
	if err = fsutil.DirWritable(dir); err != nil {
		return nil, err
	}

	q := &Queue{
		maxMemBytes: DefaultMaxMemory,
		maxMemItems: DefaultMaxMemItems,
		maxItemSize: DefaultMaxItemSize,

		dir:          dir,
		name:         filepath.Base(dir),
		done:         make(chan struct{}),
		input:        make(chan putReq),
		drainReqChan: make(chan drainReq),
		empty:        make(chan struct{}),
		output:       make(chan []byte),
		clearReqChan: make(chan chan error),
		statsReqChan: make(chan chan Stats),

		rspErrPool: sync.Pool{
			New: func() any { return make(chan error, 1) },
		},
		rspNPool: sync.Pool{
			New: func() any { return make(chan int, 1) },
		},
	}
	for _, opt := range options {
		opt(q)
	}
	if q.logger == nil {
		handler := slog.NewJSONHandler(os.Stderr, nil)
		attrs := []slog.Attr{
			slog.String(pkgName, q.name),
		}
		q.logger = slog.New(handler.WithAttrs(attrs))
	}
	if q.maxItemSize < q.minItemSize {
		return nil, errors.New("minimum item size must be less than maximum")
	}
	if q.minItemSize > 0 && q.maxMemBytes < q.minItemSize*2 {
		return nil, fmt.Errorf("max memory size (%d bytes) is too small for minimum item size (%d bytes)", q.maxMemBytes, q.minItemSize)
	}

	q.logger.Debug("starting", slog.Bool("gzip", q.gzip), slog.Bool("snapshots", q.snapInterval != 0))
	err = q.readQueueDir()
	if err != nil {
		return nil, err
	}

	go q.run()

	return q, nil
}

// Clear removes all items from the queue.
func (q *Queue) Clear() error {
	q.closeMutex.RLock()
	defer q.closeMutex.RUnlock()

	if q.closed {
		return ErrClosed
	}

	rsp := q.rspErrPool.Get().(chan error)
	q.clearReqChan <- rsp
	err := <-rsp
	q.rspErrPool.Put(rsp)
	return err
}

// Close stops the queue's internal goroutine and prevents any more input or
// output with the queue. After calling Close, any attempted input or output
// results in an error.
func (q *Queue) Close() error {
	q.closeMutex.Lock()
	if !q.closed {
		q.closed = true
		close(q.input)
	}
	q.closeMutex.Unlock()

	<-q.done
	return q.closeErr
}

// Dir returns the directory where queued data files are stored.
func (q *Queue) Dir() string {
	return q.dir
}

// Done returns a channel that is closed when the Queue is closed.
func (q *Queue) Done() <-chan struct{} {
	return q.done
}

// Empty returns a channel that is signaled when the queue is empty. This is
// useful for exiting a select when there are currently no more queued items to
// read.
func (q *Queue) Empty() <-chan struct{} {
	return q.empty
}

// Name returns the base of the directory path of this Queue, which serves as
// the queue's name.
func (q *Queue) Name() string {
	return q.name
}

// Out returns the receive-only []byte channel for reading data.
func (q *Queue) Out() <-chan []byte {
	return q.output
}

// Put writes a []byte to the queue. A nil item is ignored and returns nil
// without being enqueued.
//
// When memory is full, Put starts an asynchronous write of the tail queue to
// disk, and only waits if a previous write is still in progress. If an
// asynchronous write fails, its items are kept in memory and the next write is
// retried synchronously; if that also fails, Put returns the error and the
// item is not enqueued.
func (q *Queue) Put(item []byte) error {
	if item == nil {
		return nil
	}
	dataLen := len(item)

	q.closeMutex.RLock()
	defer q.closeMutex.RUnlock()

	if q.closed {
		return ErrClosed
	}

	if dataLen < q.minItemSize || dataLen > q.maxItemSize {
		return fmt.Errorf("invalid item size (%d) min=%d max=%d", dataLen, q.minItemSize, q.maxItemSize)
	}

	rsp := q.rspErrPool.Get().(chan error)
	q.input <- putReq{item: item, rsp: rsp}
	err := <-rsp
	q.rspErrPool.Put(rsp)
	return err
}

// PutBatch enqueues all items in a single event-loop visit. If items is nil or
// empty, it returns nil immediately. Nil items are skipped. Returns the first
// error encountered, whether from size validation or from writing overflow to
// disk; items before the failing one are already enqueued. As with Put, an
// overflow write error is returned only when a failed asynchronous write is
// retried and fails again.
func (q *Queue) PutBatch(items [][]byte) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		return q.Put(items[0])
	}

	q.closeMutex.RLock()
	defer q.closeMutex.RUnlock()

	if q.closed {
		return ErrClosed
	}

	rsp := q.rspErrPool.Get().(chan error)
	q.input <- putReq{items: items, rsp: rsp}
	err := <-rsp
	q.rspErrPool.Put(rsp)
	return err
}

// Drain fills dst with up to len(dst) items currently available in the queue
// and returns the number placed. Returns 0 if the queue is empty or closed, or
// if dst is nil or zero-length.
func (q *Queue) Drain(dst [][]byte) int {
	if len(dst) == 0 {
		return 0
	}

	q.closeMutex.RLock()
	defer q.closeMutex.RUnlock()

	if q.closed {
		return 0
	}

	rsp := q.rspNPool.Get().(chan int)
	q.drainReqChan <- drainReq{dst: dst, rsp: rsp}
	n := <-rsp
	q.rspNPool.Put(rsp)
	return n
}

// Stats retrieves information about queue internal data. It waits for any
// in-progress overflow write to finish, so that Files lists only files that
// exist on disk.
func (q *Queue) Stats() Stats {
	q.closeMutex.RLock()
	defer q.closeMutex.RUnlock()

	if q.closed {
		return Stats{
			Closed: true,
		}
	}

	rspChan := make(chan Stats, 1)
	q.statsReqChan <- rspChan
	return <-rspChan
}

func (q *Queue) run() {
	var headQ, tailQ deque.Deque[[]byte]

	defer func() {
		err := q.snapshotMemQueue(&headQ, &tailQ, true)
		// A stale file whose removal failed while the queue was running is not removed
		// later if file numbering has reset, and would be replayed when the queue is
		// reopened. Remove every queue file that is not in use: the snapshots and the
		// files in q.files.
		inUse := make(map[int64]struct{}, q.files.Len()+2)
		inUse[0] = struct{}{}
		inUse[q.nextFileNum()] = struct{}{}
		for fileNum := range q.files.Iter() {
			inUse[fileNum] = struct{}{}
		}
		staleErr := q.removeUnusedFiles(func(fileNum int64) bool {
			_, ok := inUse[fileNum]
			return ok
		})
		q.closeErr = errors.Join(err, staleErr)
		q.files.Clear()
		close(q.output)
		close(q.done)
	}()

	var (
		next          []byte
		output        chan []byte
		headQBytes    int
		tailQBytes    int
		prevSnapCount int
		snapCount     int
	)

	empty := q.empty
	maxBytes := q.maxMemBytes >> 1
	maxItems := q.maxMemItems >> 1
	headQ.SetBaseCap(maxItems)
	tailQ.SetBaseCap(maxItems)

	// Overflow files are written asynchronously, one at a time. While a write is
	// in progress, writeQ is owned by the writer goroutine and must not be
	// touched by the event loop until the result is received from writeDone.
	var (
		writeQ      deque.Deque[[]byte]
		writeQBytes int
		writeNum    int64        // file number being written; valid while writeDone != nil
		writeDone   <-chan error // non-nil while an async write is in progress
		writeErr    error        // last write error; forces the next flush to be synchronous
	)
	writeRsp := make(chan error, 1)
	writeQ.SetBaseCap(maxItems)

	if q.files.Len() != 0 {
		bytesLoaded := q.loadQueueFromFile(&headQ, nil)
		if headQ.Len() != 0 {
			next = headQ.Front()
			output = q.output
			headQBytes = bytesLoaded
			empty = nil
			snapCount++
		}
	}

	var snapCheck <-chan time.Time
	if q.snapInterval > 0 {
		snapTicker := time.NewTicker(q.snapInterval)
		defer snapTicker.Stop()
		snapCheck = snapTicker.C
	}

	// startWrite hands the contents of tailQ to a goroutine that writes them to
	// the next overflow file. The file number is added to q.files immediately so
	// that numbering stays sequential; reading that file waits for the write.
	startWrite := func() {
		fileNum := q.nextFileNum()
		writePath := q.makeFilePath(fileNum)
		writeQ, tailQ = tailQ, writeQ
		writeQBytes, tailQBytes = tailQBytes, 0
		q.files.PushBack(fileNum)
		writeNum = fileNum
		writeDone = writeRsp
		go func() {
			writeRsp <- q.saveToFile(writePath, &writeQ, false)
		}()
	}

	writeComplete := func(err error) {
		writeDone = nil
		if debug && (q.files.Len() == 0 || q.files.Back() != writeNum) {
			panic(fmt.Sprintf("cascadeq: file being written (%d) is not the last queue file, files: %v", writeNum, q.getFileNames()))
		}
		if err != nil {
			writePath := q.makeFilePath(writeNum)
			q.logger.Error("failed to write overflow file", slog.String("path", writePath), slog.Any("err", err))
			// The failed write did not replace any file already at its path. That can only
			// be a tail snapshot written while this was the next file number, and it is
			// stale because the items are back in memory. Remove it, since file numbering
			// may reset before it is overwritten, which would leave it to be replayed after
			// the queue is reopened.
			if err := os.Remove(writePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				q.logger.Error("failed to remove stale snapshot", slog.String("path", writePath), slog.Any("err", err))
			}
			// No other file is added while a write is in progress, so the failed file
			// is the last one. Its items are older than everything in tailQ, so return
			// them to the front of tailQ to keep them in order.
			if q.files.Len() != 0 && q.files.Back() == writeNum {
				q.files.PopBack()
			}
			for item := range writeQ.IterPopBack() {
				tailQ.PushFront(item)
			}
			tailQBytes += writeQBytes
			writeQBytes = 0
			writeErr = err
			return
		}
		writeQ.Clear()
		writeQBytes = 0
		writeErr = nil
	}

	// finishWrite waits for any in-progress write to complete.
	finishWrite := func() {
		if writeDone != nil {
			writeComplete(<-writeDone)
		}
	}

	// awaitFile is called before reading fileNum, and waits for the file to be
	// written if it is being written.
	awaitFile := func(fileNum int64) {
		if writeDone != nil && fileNum == writeNum {
			finishWrite()
		}
	}

	// flushTail writes tailQ to the next overflow file. Only one write is allowed
	// at a time, so this first waits for any write already in progress.
	flushTail := func() error {
		finishWrite()
		if writeErr != nil {
			// The previous write failed and its items were returned to tailQ. Retry
			// synchronously so that a persistent write failure is reported to the caller
			// instead of growing memory without bound.
			startWrite()
			finishWrite()
			if writeErr != nil {
				return fmt.Errorf("failed saving queue to file: %w", writeErr)
			}
			return nil
		}
		startWrite()
		return nil
	}

	putItem := func(item []byte) error {
		if q.files.Len() == 0 && tailQ.Len() == 0 {
			if headQ.Len() < maxItems && headQBytes < maxBytes {
				headQ.PushBack(item)
				headQBytes += len(item)
				if next == nil {
					next = headQ.Front()
					output = q.output
					empty = nil
				}
				return nil
			}
			tailQ.PushBack(item)
			tailQBytes += len(item)
			return nil
		}
		if tailQ.Len() < maxItems && tailQBytes < maxBytes {
			tailQ.PushBack(item)
			tailQBytes += len(item)
			return nil
		}
		// tailQ is full: shift into headQ if possible, otherwise flush to disk.
		if q.files.Len() == 0 && headQ.Len() < maxItems && headQBytes < maxBytes {
			var deltaBytes int
			for fromTailQ := range tailQ.IterPopFront() {
				headQ.PushBack(fromTailQ)
				deltaBytes += len(fromTailQ)
				if headQ.Len() >= maxItems || headQBytes >= maxBytes {
					break
				}
			}
			headQBytes += deltaBytes
			tailQBytes -= deltaBytes
		} else if err := flushTail(); err != nil {
			return err
		}
		tailQ.PushBack(item)
		tailQBytes += len(item)
		return nil
	}

	refillHead := func() bool {
		if q.files.Len() != 0 {
			var bytesLoaded int
			if snapCheck != nil {
				// When snapshots are enabled, track the current tail-snapshot file number
				// before loading. If no overflow files remain after the load, file numbers
				// reset to 1, so any stale tail snapshot at that slot must be removed now,
				// otherwise it will be replayed incorrectly on restart.
				tailSnapNum := q.nextFileNum()
				bytesLoaded = q.loadQueueFromFile(&headQ, awaitFile)
				if q.files.Len() == 0 {
					err := os.Remove(q.makeFilePath(tailSnapNum))
					if err != nil && !errors.Is(err, fs.ErrNotExist) {
						q.logger.Error("failed to remove snapshot", slog.Any("err", err))
					}
				}
			} else {
				bytesLoaded = q.loadQueueFromFile(&headQ, awaitFile)
			}
			if headQ.Len() != 0 {
				headQBytes = bytesLoaded
				return true
			}
		}
		if tailQ.Len() != 0 {
			// O(1) promotion: swap instead of copying all items.
			headQ, tailQ = tailQ, headQ
			headQBytes, tailQBytes = tailQBytes, 0
			return true
		}
		next = nil
		output = nil
		empty = q.empty
		return false
	}

	for {
		select {
		case req, open := <-q.input:
			if !open {
				finishWrite()
				return
			}
			snapCount++
			if req.items == nil {
				// Single-item Put path.
				req.rsp <- putItem(req.item)
				continue
			}
			// Batch put: enqueue each item with the same logic as single Put.
			var err error
		batchLoop:
			for _, item := range req.items {
				if item == nil {
					continue batchLoop
				}
				dataLen := len(item)
				if dataLen < q.minItemSize || dataLen > q.maxItemSize {
					err = fmt.Errorf("invalid item size (%d) min=%d max=%d", dataLen, q.minItemSize, q.maxItemSize)
					break batchLoop
				}
				err = putItem(item)
				if err != nil {
					break batchLoop
				}
			}
			req.rsp <- err

		case output <- next:
			snapCount++
			headQ.PopFront()
			headQBytes -= len(next)
			if headQ.Len() != 0 || refillHead() {
				next = headQ.Front()
			}

		case req := <-q.drainReqChan:
			space := len(req.dst)
			var dstFull bool
			var n int
		drainLoop:
			for {
				for item := range headQ.IterPopFront() {
					req.dst[n] = item
					headQBytes -= len(item)
					n++
					if n == space {
						dstFull = true
						break
					}
				}
				if headQ.Len() == 0 && !refillHead() {
					break drainLoop
				}
				if dstFull {
					next = headQ.Front()
					break drainLoop
				}
			}
			snapCount += n
			req.rsp <- n

		case err := <-writeDone:
			writeComplete(err)

		case errChan := <-q.clearReqChan:
			// Wait for any in-progress write so its file is not created after the
			// files are removed.
			finishWrite()
			writeErr = nil
			snapCount = 0
			headQ.Clear()
			headQBytes = 0
			tailQ.Clear()
			tailQBytes = 0
			output = nil
			next = nil
			empty = q.empty

			// Remove every queue file in the directory, not only those in q.files, to
			// include snapshots and any file whose earlier removal failed.
			q.files.Clear()
			errChan <- q.removeUnusedFiles(func(int64) bool { return false })

		case rspChan := <-q.statsReqChan:
			// Wait for any in-progress write so that Files lists only files that
			// exist on disk.
			finishWrite()
			rspChan <- Stats{
				MaxQBytes:  maxBytes,
				MaxQLen:    maxItems,
				HeadQBytes: headQBytes,
				HeadQLen:   headQ.Len(),
				TailQBytes: tailQBytes,
				TailQLen:   tailQ.Len(),
				Files:      q.getFileNames(),
			}

		case empty <- struct{}{}:

		case <-snapCheck:
			// snapCount == prevSnapCount means no Put or read since last tick (idle). Skip
			// snapshot under load: overflow I/O already handles persistence.
			if snapCount > 0 && snapCount == prevSnapCount {
				// Snapshot files are numbered after the overflow files, so any
				// in-progress write must complete first.
				finishWrite()
				tq := &tailQ
				// If overflow files exist and tailQ is full, flush tailQ as a regular overflow
				// file instead of a tail snapshot: the tail cannot move directly to head
				// anyway, so this avoids creating both a tail snapshot and, after another
				// queue input, a tail overflow file.
				if q.files.Len() != 0 && (tailQ.Len() >= maxItems || tailQBytes >= maxBytes) {
					err := q.saveTailToNextFile(&tailQ)
					if err != nil {
						q.logger.Error("failed to save tail queue to file", slog.Any("err", err))
					} else {
						tailQBytes = 0
					}
					tq = nil
				}

				err := q.snapshotMemQueue(&headQ, tq, false)
				if err != nil {
					q.logger.Error("failed to save snapshot to file", slog.Any("err", err))
					// Increment so next tick sees a changed count and skips, giving two full
					// intervals before next attempt.
					snapCount++
					continue
				}
				snapCount = 0
			}
			prevSnapCount = snapCount
		}
	}
}

func (q *Queue) getFileNames() []string {
	if q.files.Len() == 0 {
		return nil
	}
	files := make([]string, 0, q.files.Len())
	for fileNum := range q.files.Iter() {
		files = append(files, q.makeFileName(fileNum))
	}
	return files
}

func (q *Queue) readQueueDir() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		// io.EOF is returned by some implementations for an empty directory.
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	files := make([]int64, 0, len(entries))
	for _, ent := range entries {
		fileNum, ok := parseFileNum(ent.Name())
		if !ok {
			if isTempFileName(ent.Name()) && !ent.IsDir() {
				// A queue file write was interrupted, such as by the process being killed,
				// before its temp file was renamed. Only this Queue uses the directory, so no
				// write can be in progress.
				err = os.Remove(filepath.Join(q.dir, ent.Name()))
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					q.logger.Error("failed to remove temp file", slog.String("name", ent.Name()), slog.Any("err", err))
				}
			}
			continue
		}
		if ent.IsDir() {
			return fmt.Errorf("cannot read queue file %q: %w", ent.Name(), ErrIsDirectory)
		}
		files = append(files, fileNum)
	}
	slices.Sort(files)
	q.files.CopyInSlice(files)

	return nil
}

// removeUnusedFiles removes each queue file in the queue directory whose number
// is not in use, as reported by inUse. Files are matched by number under either
// extension, since the gzip setting may have changed since a file was written.
func (q *Queue) removeUnusedFiles(inUse func(fileNum int64) bool) error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		// io.EOF is returned by some implementations for an empty directory.
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	var errs []error
	for _, ent := range entries {
		fileNum, ok := parseFileNum(ent.Name())
		if !ok || inUse(fileNum) {
			continue
		}
		err = os.Remove(filepath.Join(q.dir, ent.Name()))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// isTempFileName reports whether name is the name of a temp file written by
// saveToFile. atomicfile writes each queue file to a temp file named after the
// queue file, followed by "-" and a random decimal number, and then renames it.
func isTempFileName(name string) bool {
	i := strings.LastIndexByte(name, '-')
	if i < 0 {
		return false
	}
	random := name[i+1:]
	if random == "" || strings.Trim(random, "0123456789") != "" {
		return false
	}
	_, ok := parseFileNum(name[:i])
	return ok
}

// parseFileNum returns the number of the queue file with the given name, which
// may have the gzip extension. It returns false if name is not the name of a
// queue file.
func parseFileNum(name string) (int64, bool) {
	name = strings.TrimSuffix(name, gzipExt)
	name, found := strings.CutSuffix(name, fileExt)
	if !found {
		return 0, false
	}
	name, found = strings.CutPrefix(name, filePrefix)
	if !found {
		return 0, false
	}
	fileNum, err := strconv.ParseInt(name, 16, 64)
	if err != nil {
		return 0, false
	}
	return fileNum, true
}

func (q *Queue) makeFileName(fileNum int64) string {
	b := make([]byte, 0, len(filePrefix)+16+len(fileExt)+len(gzipExt))
	b = append(b, filePrefix...)
	b = strconv.AppendInt(b, fileNum, 16)
	b = append(b, fileExt...)
	if q.gzip {
		b = append(b, gzipExt...)
	}
	return string(b)
}

func (q *Queue) makeFilePath(fileNum int64) string {
	return filepath.Join(q.dir, q.makeFileName(fileNum))
}

// toggleGzipExt returns filePath with the gzip extension removed if it has one,
// or added if it does not.
func toggleGzipExt(filePath string) string {
	if p, ok := strings.CutSuffix(filePath, gzipExt); ok {
		return p
	}
	return filePath + gzipExt
}

func (q *Queue) handleReadError(readPath string) {
	badName := readPath + BadFileExt
	var (
		badn        int
		badNameBase string
	)
retry:
	err := os.Rename(readPath, badName)
	if err != nil {
		// If the bad file already exists, try to add a number suffix to rename it.
		// Limit retries to not go past ".9" suffix.
		if errors.Is(err, fs.ErrExist) && badn < 9 {
			if badn == 0 {
				badNameBase = badName
			}
			badn++
			badName = fmt.Sprintf("%s.%d", badNameBase, badn)
			goto retry
		}
		q.logger.Error("failed to rename bad file", slog.Any("err", err))
		return
	}
	q.logger.Info("renamed bad file", slog.String("newName", filepath.Base(badName)))
}

// loadQueueFromFile loads the next readable file into headQ. On error the file
// is renamed to .bad rather than deleted. A partial read returns whatever
// items were loaded successfully. If awaitFile is not nil, it is called with
// each file number before that file is read.
func (q *Queue) loadQueueFromFile(headQ *deque.Deque[[]byte], awaitFile func(int64)) int {
	var bytesLoaded int

	for q.files.Len() != 0 && headQ.Len() == 0 {
		if awaitFile != nil {
			// Wait for the file if it is still being written. If that write fails,
			// the file is removed from q.files.
			awaitFile(q.files.Front())
			if q.files.Len() == 0 {
				break
			}
		}
		readPath := q.makeFilePath(q.files.PopFront())

		var err error
		bytesLoaded, err = readQueueFile(readPath, q.minItemSize, q.maxItemSize, headQ)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// The gzip setting may have changed since the file was written; try the
				// opposite extension before giving up.
				readPath = toggleGzipExt(readPath)
				bytesLoaded, err = readQueueFile(readPath, q.minItemSize, q.maxItemSize, headQ)
				if errors.Is(err, fs.ErrNotExist) {
					continue // read next file
				}
			}
			if err != nil {
				q.logger.Error("failed to load file", slog.String("path", readPath), slog.Any("err", err))
				if !errors.Is(err, ErrIsDirectory) {
					q.handleReadError(readPath)
				}
				continue // read next file
			}
		}
		err = os.Remove(readPath)
		if err != nil {
			q.logger.Error("failed to remove file", slog.String("path", readPath), slog.Any("err", err))
			if !errors.Is(err, os.ErrNotExist) {
				q.handleReadError(readPath)
			}
		}
	}

	return bytesLoaded
}

func readQueueFile(readPath string, minItemSize, maxItemSize int, headQ *deque.Deque[[]byte]) (int, error) {
	readFile, err := os.Open(readPath) //nolint:gosec
	if err != nil {
		return 0, err
	}
	defer readFile.Close() //nolint:errcheck

	fi, err := readFile.Stat()
	if err != nil {
		return 0, err
	}
	if fi.IsDir() {
		return 0, ErrIsDirectory
	}
	if fi.Size() == 0 {
		return 0, nil // empty file
	}

	var (
		bytesLoaded int
		itemSize    int
		szbuf       [4]byte
		r           io.Reader
	)
	reader := bufio.NewReader(readFile)
	r = reader

	if strings.HasSuffix(readPath, gzipExt) {
		gzr, err := gzip.NewReader(reader)
		if err != nil {
			return 0, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzr.Close() //nolint:errcheck
		r = gzr
	}

	for {
		_, err = io.ReadFull(r, szbuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return bytesLoaded, fmt.Errorf("failed to read item size: %w", err)
		}
		itemSize = int(binary.BigEndian.Uint32(szbuf[:]))

		// A size out of range means the file is corrupt; there's no safe way to skip
		// to the next record, so return what we have and let the caller rename it.
		if itemSize < minItemSize || itemSize > maxItemSize {
			return bytesLoaded, fmt.Errorf("read invalid item size: %d", itemSize)
		}

		readBuf := make([]byte, itemSize)
		_, err = io.ReadFull(r, readBuf)
		if err != nil {
			return bytesLoaded, fmt.Errorf("failed to read item: %w", err)
		}
		headQ.PushBack(readBuf)
		bytesLoaded += itemSize
	}

	return bytesLoaded, nil
}

func (q *Queue) nextFileNum() int64 {
	if q.files.Len() == 0 {
		return 1
	}
	return q.files.Back() + 1
}

func (q *Queue) saveTailToNextFile(tailQ *deque.Deque[[]byte]) error {
	fileNum := q.nextFileNum()
	if err := q.saveToFile(q.makeFilePath(fileNum), tailQ, false); err != nil {
		return err
	}

	q.files.PushBack(fileNum)
	tailQ.Clear()
	return nil
}

func (q *Queue) snapshotMemQueue(headQ, tailQ *deque.Deque[[]byte], sync bool) error {
	var errs []error

	if headQ != nil {
		// File 0 is the head snapshot; it is always loaded first on restart.
		writePath := q.makeFilePath(0)
		if headQ.Len() != 0 {
			if err := q.saveToFile(writePath, headQ, sync); err != nil {
				errs = append(errs, err)
			}
		} else {
			err := os.Remove(writePath)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, err)
			}
		}
	}

	if tailQ != nil {
		writePath := q.makeFilePath(q.nextFileNum())
		if tailQ.Len() != 0 {
			if err := q.saveToFile(writePath, tailQ, sync); err != nil {
				errs = append(errs, err)
			}
		} else {
			err := os.Remove(writePath)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

func (q *Queue) saveToFile(writePath string, memQ *deque.Deque[[]byte], sync bool) error {
	writeFile, err := atomicfile.Create(writePath, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = writeFile.Discard()
		}
	}()

	writer := bufio.NewWriter(writeFile)
	var w io.Writer
	var gzw *gzip.Writer

	w = writer
	if q.gzip {
		gzw = gzip.NewWriter(writer)
		w = gzw
	}

	var szbuf [4]byte
	for item := range memQ.Iter() {
		binary.BigEndian.PutUint32(szbuf[:], uint32(len(item))) //nolint:gosec // item length limited to MaxInt32
		_, err = w.Write(szbuf[:])
		if err != nil {
			return err
		}
		_, err = w.Write(item)
		if err != nil {
			return err
		}
	}
	if gzw != nil {
		err = gzw.Close()
		if err != nil {
			return err
		}
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	if sync {
		return writeFile.CloseSync()
	}
	return writeFile.Close()
}
