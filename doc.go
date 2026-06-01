// Package cascadeq implements a persistent, disk-backed FIFO queue for []byte
// items. It operates primarily in memory and spills to disk only when the
// configured memory limits are reached, making it efficient for workloads that
// fit in memory while remaining durable under backpressure.
//
// # Memory model
//
// The queue maintains two in-memory deques at all times:
//   - headQ: the read-from queue, holding the oldest unconsumed items
//   - tailQ: the write-to queue, holding the newest items
//
// The configured memory/item limits are divided equally between headQ and
// tailQ. When tailQ is full, its contents are flushed to a numbered .dat file
// on disk unless headQ has room to absorb them directly. Items are read back
// by loading the next numbered file into headQ; when no files remain, tailQ
// and headQ are swapped in O(1).
//
// # Usage
//
// Create a Queue with New, write items with Put, and consume them by ranging
// over the channel returned by Out:
//
//	q, err := cascadeq.New("myqueue", "/var/data/queues")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer q.Close()
//
//	// Producer
//	if err := q.Put([]byte("hello")); err != nil {
//	    log.Println(err)
//	}
//
//	// Consumer
//	for {
//	    select {
//	    case item := <-q.Out():
//	        process(item)
//	    case <-q.Empty():
//	        return // no more items right now
//	    case <-q.Done():
//	        return // queue closed
//	    }
//	}
//
// # Options
//
// Behaviour is configured via functional options passed to New:
//   - [WithMaxMemory]: cap total in-memory bytes (default 1 MiB)
//   - [WithMaxMemItems]: cap total in-memory item count (default 4096)
//   - [WithMaxItemSize]: reject items above this byte size (default 64 KiB)
//   - [WithMinItemSize]: reject items below this byte size (default 0)
//   - [WithGzip]: enable gzip compression of disk files
//   - [WithSnapshotInterval]: periodically persist in-memory state when idle
//   - [WithLogger]: replace the default JSON slog.Logger
//
// # Persistence and file format
//
// Overflow files are written to the directory supplied to New and named
// {name}-{hexnum}.dat (or .dat.gz when compression is enabled). Each file is a
// sequence of big-endian int32 length-prefixed byte records. File number 0 is
// reserved for the headQ snapshot written on close or idle snapshot; higher
// numbers are sequential tailQ overflow files. On restart, New re-discovers
// these files and resumes from where the queue left off. Corrupt files are
// renamed with a .bad extension rather than deleted.
//
// # Concurrency
//
// All state mutations run inside a single goroutine. Put, Clear, and Stats are
// safe to call from multiple goroutines concurrently. Close is idempotent and
// safe to call from any goroutine.
package cascadeq
