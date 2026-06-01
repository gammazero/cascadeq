package cascadeq

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/deque"
)

const (
	DefaultMaxMemory   = 1024 * 1024
	DefaultMaxMemItems = 4096
	DefaultMaxItemSize = 65536

	BadFileExt = ".bad"

	minItemsPerBlock
	minBlocksPerBuf
)

const (
	fileExt = ".dat"
	gzipExt = ".gz"
	pkgName = "cascadeq"
)

// ErrClosed is returned when I/O is attempted on a closed Queue.
var ErrClosed = errors.New("closed")

// Queue implements a filesystem backed FIFO queue.
type Queue struct {
	maxMemItems int
	maxMemBytes int
	maxItemSize int
	minItemSize int

	closed       bool
	closeErr     error
	closeMutex   sync.RWMutex
	dir          string
	gzip         bool
	name         string
	snapInterval time.Duration

	done         chan struct{}
	empty        chan struct{}
	input        chan []byte
	inputRsp     chan error
	output       chan []byte
	clearReqChan chan chan error
	statsReqChan chan chan Stats

	files deque.Deque[int64]
}

// Stats holds information about the Queue internal state.
type Stats struct {
	Closed     bool
	MaxQBytes  int
	MaxQLen    int
	HeadQBytes int
	HeadQLen   int
	TailQBytes int
	TailQLen   int
	Files      []string
}

// WithGzip enables, if passed true, gzip compression of buffer files.
func WithGzip(enable bool) func(*Queue) {
	return func(fq *Queue) {
		fq.gzip = enable
	}
}

// WithMaxMemory sets the maximum amount of memory used by all items in the
// queue before items are written to disk.
func WithMaxMemory(maxBytes int) func(*Queue) {
	return func(fq *Queue) {
		if maxBytes > 0 {
			fq.maxMemBytes = maxBytes
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
	q := &Queue{
		maxMemBytes: DefaultMaxMemory,
		maxMemItems: DefaultMaxMemItems,
		maxItemSize: DefaultMaxItemSize,

		name:         name,
		dir:          filepath.Clean(dir),
		done:         make(chan struct{}),
		input:        make(chan []byte),
		inputRsp:     make(chan error, 1),
		empty:        make(chan struct{}),
		output:       make(chan []byte),
		clearReqChan: make(chan chan error),
		statsReqChan: make(chan chan Stats),
	}
	for _, opt := range options {
		opt(q)
	}
	if q.maxItemSize < q.minItemSize {
		return nil, errors.New("minimum item size must be less than maximum")
	}
	if q.minItemSize > 0 && q.maxMemBytes < q.minItemSize*2 {
		return nil, fmt.Errorf("max memory size (%d bytes) is too small for minimum item size (%d bytes)", q.maxMemBytes, q.minItemSize)
	}

	err := q.readQueueDir()
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
		return fmt.Errorf("%s-%s: invalid item size (%d) min=%d max=%d", pkgName, q.name, dataLen, q.minItemSize, q.maxItemSize)
	}

	q.input <- item
	return <-q.inputRsp
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
		// Save the contents of the in-memory queues.
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

	empty := q.empty // signal that queue is empty
	maxBytes := q.maxMemBytes >> 1
	maxItems := q.maxMemItems >> 1
	headQ.SetBaseCap(maxItems)
	tailQ.SetBaseCap(maxItems)

	if q.files.Len() != 0 {
		// Load lowest numbered file into main queue.
		bytesLoaded := q.loadQueueFromFile(&headQ)
		if headQ.Len() != 0 {
			next = headQ.Front()
			output = q.output
			headQBytes = bytesLoaded
			empty = nil // queue is not empty
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
		case item, open := <-q.input:
			if !open {
				return
			}
			snapCount++
			if q.files.Len() == 0 && tailQ.Len() == 0 {
				if headQ.Len() < maxItems && headQBytes < maxBytes {
					headQ.PushBack(item)
					headQBytes += len(item)
					if next == nil {
						next = headQ.Front()
						output = q.output
						empty = nil // stop signal that queue is empty
					}
				} else {
					tailQ.PushBack(item)
					tailQBytes += len(item)
				}
				q.inputRsp <- nil
				continue
			}
			if tailQ.Len() < maxItems && tailQBytes < maxBytes {
				tailQ.PushBack(item)
				tailQBytes += len(item)
				q.inputRsp <- nil
				continue
			}
			// Getting here means that tailQ is full, and headQ cannot be empty
			// if there were any items in files or in tailQ.

			// If no items in files and headQ is not full, then shift items
			// from tailQ to headQ.
			if q.files.Len() == 0 && headQ.Len() < maxItems && headQBytes < maxBytes {
				for fromTailQ := range tailQ.IterPopFront() {
					// Shift items from tailQ to headQ.
					headQ.PushBack(fromTailQ)
					itemBytes := len(fromTailQ)
					headQBytes += itemBytes
					tailQBytes -= itemBytes
					if headQ.Len() >= maxItems || headQBytes >= maxBytes {
						// headQ is full, stop shifting items.
						break
					}
				}
			} else {
				// Previous items in files or headQ full, so save tailQ in
				// file. This will overwrite any existing tailQ snapshot.
				err := q.saveTailToNextFile(&tailQ)
				if err != nil {
					q.inputRsp <- fmt.Errorf("%s-%s: failed saving queue to file: %w", pkgName, q.name, err)
					continue
				}
				tailQBytes = 0
			}

			// Since headQ cannot be empty, shifting as many items as possible
			// from tailQ to headQ always results in headQ being full and tailQ
			// having at least one item remaining. Since headQ must be full,
			// always put the incoming item on tailQ.
			tailQ.PushBack(item)
			tailQBytes += len(item)
			q.inputRsp <- nil

		case output <- next:
			snapCount++
			// Item was read, so remove it from the read-from queue.
			headQ.PopFront()
			headQBytes -= len(next)
			if headQ.Len() == 0 {
				// headQ (read-from queue) is empty, so refill it if possible.
				if q.files.Len() != 0 {
					// There are files to load items from, so load items from
					// the next file.
					var bytesLoaded int
					if snapCheck != nil { // if snapshots enabled
						tailSnapNum := q.nextFileNum()
						bytesLoaded = q.loadQueueFromFile(&headQ)
						if q.files.Len() == 0 {
							// If there are no more regular files, then file
							// names are reset back to 1, and any tail snapshot
							// must be removed. Otherwise, if it does not get
							// overwritten, then it will be picked up on
							// restart and incorrectly replayed.
							err := os.Remove(q.makeFilePath(tailSnapNum))
							if err != nil && !errors.Is(err, fs.ErrNotExist) {
								log.Printf("failed to remove snapshot: %s", err)
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
					// headQ is empty, no files to load, so swap tailQ and
					// headQ as a faster way of pulling all entries into headQ
					// from tailQ.
					headQ, tailQ = tailQ, headQ
					headQBytes = tailQBytes
					tailQBytes = 0
				} else {
					// headQ is empty and no more to items read
					output = nil
					next = nil
					empty = q.empty // signal that queue is empty
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
			// headQ is empty and no more to items read
			output = nil
			next = nil
			empty = q.empty // signal that queue is empty

			var errs []error
			if snapCheck != nil { // if snapshots enabled
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
			if snapCount > 0 {
				// Do not create snapshot while under load, since normal I/O
				// will save and retrieve data from files.
				if snapCount == prevSnapCount {
					tq := &tailQ
					// Optimization: Write the tailQ directly to an overflow
					// file, and not to a tail snapshot, if there are existing
					// overflow files (tail will not move directly to head) and
					// the tailQ is full.
					if q.files.Len() != 0 && tailQ.Len() >= maxItems || tailQBytes >= maxBytes {
						err := q.saveTailToNextFile(&tailQ)
						if err != nil {
							log.Printf("%s-%s: failed saving tail queue to file: %s", pkgName, q.name, err)
						} else {
							tailQBytes = 0
						}
						tq = nil
					}

					err := q.snapshotMemQueue(&headQ, tq, false)
					if err != nil {
						log.Printf("%s-%s: failed saving snapshot to file: %s", pkgName, q.name, err)
						// Try again in two more snapCheck intervals.
						snapCount++
						continue
					}
					snapCount = 0
				}
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
		// If directory does not exist or is empty.
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

func (q *Queue) handleReadError(readPath, qname string, err error) error {
	badName := readPath + BadFileExt
	var (
		badn        int
		badNameBase string
	)
retry:
	err = os.Rename(readPath, badName)
	if err != nil {
		// If the bad file already exists, try to add a number suffix to rename
		// it. Limit retries to not go past ".9" suffix.
		if errors.Is(err, fs.ErrExist) && badn < 9 {
			if badn == 0 {
				badNameBase = badName
			}
			badn++
			badName = fmt.Sprintf("%s.%d", badNameBase, badn)
			goto retry
		}
		return fmt.Errorf("failed to rename bad file: %s", err)
	}
	log.Printf("%s-%s: renamed %s to %s", pkgName, qname, readPath, badName)
	return nil
}

// loadQueueFromFile loads the given deque with the contents of the next
// readable file. Errors reading the a file are handled by renaming the file to
// have a ".bad" extension. If a partial read occurs, then returns with the
// items that were loaded.
func (q *Queue) loadQueueFromFile(headQ *deque.Deque[[]byte]) int {
	var bytesLoaded int

	for q.files.Len() != 0 && headQ.Len() == 0 {
		readPath := q.makeFilePath(q.files.PopFront())

		var err error
		bytesLoaded, err = readQueueFile(readPath, q.minItemSize, q.maxItemSize, headQ)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// In case the file was created previously using a different
				// gzip setting. This should be rare, so the filename was not
				// preserved from the initial read of the directory.
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
				log.Printf("%s-%s: failed to load file %q: %s", pkgName, q.name, readPath, err)
				err = q.handleReadError(readPath, q.name, err)
				if err != nil {
					log.Printf("%s-%s: %s", pkgName, q.name, err)
				}
				continue // read next file
			}
		}
		err = os.Remove(readPath)
		if err != nil {
			log.Printf("%s-%s: failed to remove file %q: %s", pkgName, q.name, readPath, err)
			err = q.handleReadError(readPath, q.name, err)
			if err != nil {
				log.Printf("%s-%s: %s", pkgName, q.name, err)
			}
		}
	}

	return bytesLoaded
}

func readQueueFile(readPath string, minItemSize, maxItemSize int, headQ *deque.Deque[[]byte]) (int, error) {
	readFile, err := os.Open(readPath)
	if err != nil {
		return 0, err
	}
	defer readFile.Close()

	var (
		bytesLoaded int
		itemSize    int
		itemSize32  int32
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
		defer gzr.Close()
		r = gzr
	}

	for {
		err = binary.Read(r, binary.BigEndian, &itemSize32)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Nothing left to read.
				break
			}
			return bytesLoaded, fmt.Errorf("failed to read item size: %w", err)
		}
		itemSize = int(itemSize32)

		// If file is corrupt then no way to tell where good items begin.
		if itemSize < minItemSize || itemSize > maxItemSize {
			return bytesLoaded, fmt.Errorf("invalid item read size: %d", itemSize)
		}

		// Read item and put it into the in-mem queue.
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
		// headQ contains the oldest unread items. Write it to file 0 so that
		// it is loaded first.
		writePath := q.makeFilePath(0)
		if headQ.Len() != 0 {
			if err := q.saveToFile(writePath, headQ, sync); err != nil {
				errs = append(errs, err)
			}
		} else {
			// headQ is empty, so delete old head snapshot.
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
			// tailQ is empty, so delete old tail snapshot.
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
	writeFile, err := os.OpenFile(writePathTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
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
				writeFile.Close()
			}
			os.Remove(writePathTmp)
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

	var size int32
	for item := range memQ.Iter() {
		dataLen := int32(len(item))
		err = binary.Write(w, binary.BigEndian, dataLen)
		if err != nil {
			return err
		}

		_, err = w.Write(item)
		if err != nil {
			return err

		}
		size += dataLen + 4
	}
	if gzw != nil {
		gzw.Close()
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
		// OK to delete writePath as this is stale data or a file or directory
		// that should not be here.
		if errors.Is(err, fs.ErrExist) && canRetry && os.Remove(writePath) == nil {
			canRetry = false
			goto retryRename
		}
		return err
	}
	return nil
}
