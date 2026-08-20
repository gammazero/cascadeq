# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

# cascadeq

Fast disk-backed FIFO queue for `[]byte` items. Operates as an in-memory queue until full, and then overflows onto disk. Writes the tail half of the in-memory queue to disk only when in-memory space is full, and reads files back into the head half as items are consumed.

Optionally supports gzip compression of data written to disk. Periodic snapshots can be enabled to save in-memory data when the queue is idle.

## Commands

```bash
# Run all tests
go test ./...

# Run a single test
go test -run TestBasicOperation ./...

# Run tests with verbose output and race detector
go test -v -race ./...

# Lint (requires golangci-lint v2)
golangci-lint run
```

## Design

`cascadeq` is a single-package Go library. All implementation lives in `cascadeq.go`. It depends on `github.com/gammazero/deque` for its in-memory double-ended queues and on `github.com/gammazero/fsutil` (`fsutil` for path checks, `fsutil/atomicfile` for atomic file writes).

### Dual-queue memory model

The queue keeps two in-memory deques at all times:
- **headQ** - the read-from queue; holds the oldest unconsumed items
- **tailQ** - the write-to queue; holds the newest items

The configured memory/item limits (`maxMemBytes`, `maxMemItems`) are divided in half; each half applies independently to headQ and tailQ. `Stats.MaxQBytes`/`MaxQLen` reflect this per-queue half, not the raw config value. `WithMaxMemItems` rounds the configured value up to a power of two (minimum 32).

### Write path

New items go to headQ when both queues are empty and headQ has space. Otherwise they go to tailQ. When tailQ is full:
- If no files exist and headQ has room, shift items from tailQ into headQ.
- Otherwise, flush tailQ to the next numbered `.dat` file and clear tailQ.

`PutBatch` enqueues a slice of items in a single event-loop visit using the same per-item logic; it stops at the first size-validation error, leaving earlier items enqueued.

### Read path

Items are consumed from the front of headQ. When headQ empties:
1. Load the next numbered file into headQ (then delete the file).
2. If no files, swap tailQ and headQ (O(1)).
3. If neither, signal `Empty()` and stop sending on the output channel.

`Drain` fills a caller-provided slice with up to len(dst) items in one event-loop visit, refilling headQ from files/tailQ as needed.

### File format and naming

Files are named `cq-{hexnum}.dat` (or `.dat.gz` when compression is enabled) and stored in the directory passed to `New`. Each file is a sequence of big-endian `uint32` length-prefixed byte records. File number `0` is reserved for the headQ snapshot written on close or idle snapshot; higher-numbered files are tailQ overflows written sequentially. Corrupt files are renamed with a `.bad` extension rather than deleted. When loading, if a file is not found under the current gzip setting, the opposite extension is tried, so the gzip option can be toggled between runs without losing data.

### Single-goroutine event loop

All state mutation happens inside one goroutine (`run`) via a `select` over:
- `input` channel (Put and PutBatch requests, each carrying its own response channel)
- `output` channel (Out)
- `drainReqChan` (Drain)
- `clearReqChan` (Clear)
- `statsReqChan` (Stats)
- `empty` channel (Empty signal)
- `snapCheck` ticker (optional idle snapshot)
- `q.input` closed (Close - `Close()` closes the input channel to signal the loop to exit)

`closeMutex` (RWMutex) only protects the `closed` bool and gates `Put`/`PutBatch`/`Drain`/`Clear`/`Stats` from racing with `Close`. No other synchronization is needed because the loop is single-threaded. Response channels for Put/Drain are recycled through `sync.Pool`s.

### Snapshot feature

When `WithSnapshotInterval(d)` is set, a ticker fires every `d/2`. If `snapCount` has not advanced since the last tick (idle), the in-memory queues are written to disk inside the event loop. On `Close`, snapshots are written synchronously (fsync) for any non-empty queue. `snapCount` is incremented on every Put or read so that a busy queue is never snapshotted mid-operation. A tail snapshot is written at the next file number; stale tail snapshots are cleaned up when the file numbering resets after the last overflow file is consumed.
