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
)

const (
	DefaultMaxMemory   = 1024 * 1024
	DefaultMaxMemItems = 4096
	DefaultMaxItemSize = 65536
)

const (
	BadFileExt = ".bad"

	fileExt = ".dat"
	gzipExt = ".gz"
	pkgName = "cascadeq"
)

// ErrClosed is returned when I/O is attempted on a closed Queue.
var ErrClosed = errors.New("closed")

// putReq carries an item to enqueue together with the channel on which the
// event loop sends its result. Each Put supplies its own response channel so
// that concurrent callers cannot receive each other's results.
type putReq struct {
	item []byte
	rsp  chan error
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
	output       chan []byte
	clearReqChan chan chan error
	statsReqChan chan chan Stats

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
// the default cascadeq slog.Logger with a JSON handler that writes to stderr.
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
// memory before items are written to disk.
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

// New creates a new file-backed FIFO queue instance.
func New(name, dir string, options ...func(*Queue)) (*Queue, error) {
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

		name:         name,
		dir:          dir,
		done:         make(chan struct{}),
		input:        make(chan putReq),
		empty:        make(chan struct{}),
		output:       make(chan []byte),
		clearReqChan: make(chan chan error),
		statsReqChan: make(chan chan Stats),
	}
	for _, opt := range options {
		opt(q)
	}
	if q.logger == nil {
		handler := slog.NewJSONHandler(os.Stderr, nil)
		attrs := []slog.Attr{
			slog.String(pkgName, name),
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

	rspChan := make(chan error, 1)
	q.clearReqChan <- rspChan
	return <-rspChan
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

// Name returns the name of the Queue instance.
func (q *Queue) Name() string {
	return q.name
}

// Out returns the receive-only []byte channel for reading data.
func (q *Queue) Out() <-chan []byte {
	return q.output
}

// Put writes a []byte to the queue.
func (q *Queue) Put(item []byte) (err error) {
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

	req := putReq{item: item, rsp: make(chan error, 1)}
	q.input <- req
	return <-req.rsp
}

// Stats retrieves information about queue internal data.
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
		q.closeErr = q.snapshotMemQueue(&headQ, &tailQ, true)
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

	if q.files.Len() != 0 {
		bytesLoaded := q.loadQueueFromFile(&headQ)
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

	for {
		select {
		case req, open := <-q.input:
			if !open {
				return
			}
			item := req.item
			snapCount++
			if q.files.Len() == 0 && tailQ.Len() == 0 {
				if headQ.Len() < maxItems && headQBytes < maxBytes {
					headQ.PushBack(item)
					headQBytes += len(item)
					if next == nil {
						next = headQ.Front()
						output = q.output
						empty = nil
					}
				} else {
					tailQ.PushBack(item)
					tailQBytes += len(item)
				}
				req.rsp <- nil
				continue
			}
			if tailQ.Len() < maxItems && tailQBytes < maxBytes {
				tailQ.PushBack(item)
				tailQBytes += len(item)
				req.rsp <- nil
				continue
			}
			// tailQ is full. When no overflow files exist and headQ has room, shift tailQ
			// items into headQ to avoid a disk write. Otherwise flush tailQ to the next
			// numbered file (overwriting any existing tail snapshot).
			if q.files.Len() == 0 && headQ.Len() < maxItems && headQBytes < maxBytes {
				for fromTailQ := range tailQ.IterPopFront() {
					headQ.PushBack(fromTailQ)
					itemBytes := len(fromTailQ)
					headQBytes += itemBytes
					tailQBytes -= itemBytes
					if headQ.Len() >= maxItems || headQBytes >= maxBytes {
						break
					}
				}
			} else {
				err := q.saveTailToNextFile(&tailQ)
				if err != nil {
					req.rsp <- fmt.Errorf("failed saving queue to file: %w", err)
					continue
				}
				tailQBytes = 0
			}

			// After a shift, headQ is always full, so the incoming item goes to tailQ.
			tailQ.PushBack(item)
			tailQBytes += len(item)
			req.rsp <- nil

		case output <- next:
			snapCount++
			headQ.PopFront()
			headQBytes -= len(next)
			if headQ.Len() == 0 {
				if q.files.Len() != 0 {
					var bytesLoaded int
					if snapCheck != nil {
						// When snapshots are enabled, track the current tail-snapshot file number
						// before loading. If no overflow files remain after the load, file numbers
						// reset to 1, so any stale tail snapshot at that slot must be removed now,
						// otherwise it will be replayed incorrectly on restart.
						tailSnapNum := q.nextFileNum()
						bytesLoaded = q.loadQueueFromFile(&headQ)
						if q.files.Len() == 0 {
							err := os.Remove(q.makeFilePath(tailSnapNum))
							if err != nil && !errors.Is(err, fs.ErrNotExist) {
								q.logger.Error("failed to remove snapshot", slog.Any("err", err))
							}
						}
					} else {
						bytesLoaded = q.loadQueueFromFile(&headQ)
					}

					if headQ.Len() != 0 {
						headQBytes = bytesLoaded
						next = headQ.Front()
						continue
					}
				}
				if tailQ.Len() != 0 {
					// O(1) promotion: swap instead of copying all items.
					headQ, tailQ = tailQ, headQ
					headQBytes = tailQBytes
					tailQBytes = 0
				} else {
					output = nil
					next = nil
					empty = q.empty
					continue
				}
			}
			next = headQ.Front()

		case errChan := <-q.clearReqChan:
			snapCount = 0
			headQ.Clear()
			headQBytes = 0
			tailQ.Clear()
			tailQBytes = 0
			output = nil
			next = nil
			empty = q.empty

			var errs []error
			if snapCheck != nil {
				err := q.snapshotMemQueue(&headQ, &tailQ, false)
				if err != nil {
					errs = append(errs, err)
				}
			}
			for fileNum := range q.files.IterPopFront() {
				writePath := q.makeFilePath(fileNum)
				err := os.Remove(writePath)
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					errs = append(errs, err)
				}
			}
			errChan <- errors.Join(errs...)

		case rspChan := <-q.statsReqChan:
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
	filePrefix := q.name + "-"

	files := make([]int64, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := strings.TrimSuffix(ent.Name(), gzipExt)
		name, found := strings.CutSuffix(name, fileExt)
		if !found {
			continue
		}
		name, found = strings.CutPrefix(name, filePrefix)
		if !found {
			continue
		}
		fileNum, err := strconv.ParseInt(name, 16, 64)
		if err != nil {
			continue
		}
		files = append(files, fileNum)
	}
	slices.Sort(files)
	q.files.CopyInSlice(files)

	return nil
}

func (q *Queue) makeFileName(fileNum int64) string {
	if q.gzip {
		return fmt.Sprintf("%s-%x%s%s", q.name, fileNum, fileExt, gzipExt)
	}
	return fmt.Sprintf("%s-%x%s", q.name, fileNum, fileExt)
}

func (q *Queue) makeFilePath(fileNum int64) string {
	return filepath.Join(q.dir, q.makeFileName(fileNum))
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
// items were loaded successfully.
func (q *Queue) loadQueueFromFile(headQ *deque.Deque[[]byte]) int {
	var bytesLoaded int

	for q.files.Len() != 0 && headQ.Len() == 0 {
		readPath := q.makeFilePath(q.files.PopFront())

		var err error
		bytesLoaded, err = readQueueFile(readPath, q.minItemSize, q.maxItemSize, headQ)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// The gzip setting may have changed since the file was written; try the
				// opposite extension before giving up.
				var wasGzip bool
				readPath, wasGzip = strings.CutSuffix(readPath, gzipExt)
				if !wasGzip {
					readPath += gzipExt
				}
				bytesLoaded, err = readQueueFile(readPath, q.minItemSize, q.maxItemSize, headQ)
				if errors.Is(err, fs.ErrNotExist) {
					continue // read next file
				}
			}
			if err != nil {
				q.logger.Error("failed to load file", slog.String("path", readPath), slog.Any("err", err))
				q.handleReadError(readPath)
				continue // read next file
			}
		}
		err = os.Remove(readPath)
		if err != nil {
			q.logger.Error("failed to remove file", slog.String("path", readPath), slog.Any("err", err))
			q.handleReadError(readPath)
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
			if fi, statErr := readFile.Stat(); statErr == nil && fi.Size() == 0 {
				return 0, nil // empty file
			}
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
	writePathTmp := writePath + ".tmp"
	canRetry := true
retry:
	writeFile, err := os.OpenFile(writePathTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600) //nolint:gosec
	if err != nil {
		if os.Remove(writePathTmp) == nil {
			// Removed unopenable file, so retry once.
			if canRetry {
				canRetry = false
				goto retry
			}
		}
		return err
	}
	defer func() {
		if err != nil {
			if writeFile != nil {
				_ = writeFile.Close()
			}
			_ = os.Remove(writePathTmp)
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
		binary.BigEndian.PutUint32(szbuf[:], uint32(len(item)))
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
		err = writeFile.Sync()
		if err != nil {
			return err
		}
	}
	err = writeFile.Close()
	writeFile = nil
	if err != nil {
		return err
	}
	canRetry = true
retryRename:
	err = os.Rename(writePathTmp, writePath)
	if err != nil {
		// writePath already exists (stale tmp or leftover dir): remove and retry once.
		if errors.Is(err, fs.ErrExist) && canRetry && os.Remove(writePath) == nil {
			canRetry = false
			goto retryRename
		}
		return err
	}
	return nil
}
