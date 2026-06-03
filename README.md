# cascadeq

[![GoDoc](https://pkg.go.dev/badge/github.com/gammazero/cascadeq)](https://pkg.go.dev/github.com/gammazero/cascadeq)
[![Build Status](https://github.com/gammazero/cascadeq/actions/workflows/go.yml/badge.svg)](https://github.com/gammazero/cascadeq/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/gammazero/cascadeq)](https://goreportcard.com/report/github.com/gammazero/cascadeq)
[![codecov](https://codecov.io/gh/gammazero/cascadeq/graph/badge.svg?token=CTXB2UJ7U7)](https://codecov.io/gh/gammazero/cascadeq)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`cascadeq` (cascade-queue) is a persistent, disk-backed FIFO queue for `[]byte` items. It runs entirely in memory under normal load and spills to disk only when the configured memory limits are reached, making it fast when the queue is shallow and able to grow to any depth, limited only by disk space. Data in the queue is persisted to disk when the queue is closed and is available for reading when the queue is opened again.

## Features

- **Memory-first**: items stay in memory until configurable byte or item-count limits are hit
- **Disk overflow**: tailQ is flushed to sequentially numbered `.dat` files; files are reloaded into headQ as items are consumed
- **Persisted**: in-memory state is persisted on `Close`; existing files are picked up automatically on restart
- **Optional compression**: pass `WithGzip(true)` to compress overflow files
- **Idle snapshots**: pass `WithSnapshotInterval` to periodically persist in-memory data when the queue is idle, to increase crash safety with minimal throughput cost.
- **Channel-based API**: consume items with a plain `<-q.Out()` in a `select`

## Installation

```
go get github.com/gammazero/cascadeq
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/gammazero/cascadeq"
)

func main() {
    q, err := cascadeq.New("myqueue", "/tmp/myqueue")
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    // Write items.
    for i := range 5 {
        if err := q.Put([]byte(fmt.Sprintf("item-%d", i))); err != nil {
            log.Fatal(err)
        }
    }

    // Read items until the queue is empty.
    for {
        select {
        case item := <-q.Out():
            fmt.Println(string(item))
        case <-q.Empty():
            return
        }
    }
}
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithMaxMemory(n)` | 1 MiB | Maximum bytes held across both in-memory queues |
| `WithMaxMemItems(n)` | 4096 | Maximum item count held across both in-memory queues |
| `WithMaxItemSize(n)` | 64 KiB | Reject items larger than this |
| `WithMinItemSize(n)` | 0 | Reject items smaller than this |
| `WithGzip(true)` | disabled | Compress overflow files with gzip |
| `WithSnapshotInterval(d)` | disabled | Write in-memory state to disk after this much idle time |
| `WithLogger(l)` | JSON→stderr | Replace the default slog.Logger |

## Design

### Dual-queue memory model

The queue maintains two in-memory deques at all times:

- **headQ** — the read-from queue; holds the oldest unconsumed items
- **tailQ** — the write-to queue; holds the newest items

The configured memory/item limits are divided equally between headQ and tailQ. `Stats.MaxQBytes` and `Stats.MaxQLen` reflect this per-queue half.

### Write path

New items go to headQ when both queues are empty and headQ has space. Otherwise they go to tailQ. When tailQ is full:

- If no overflow files exist and headQ has room, shift items from tailQ into headQ.
- Otherwise, flush tailQ to the next numbered `.dat` file and clear tailQ.

### Read path

Items are consumed from the front of headQ. When headQ empties:

1. Load the next numbered file into headQ, then delete the file.
2. If no files exist, swap tailQ ↔ headQ (O(1)).
3. If neither, signal `Empty()` and stop sending on the output channel.

### File format and naming

Files are stored in the directory passed to `New` and named `{name}-{hexnum}.dat` (or `.dat.gz` when compression is enabled). Each file is a sequence of big-endian `int32` length-prefixed byte records. File number `0` is reserved for the headQ snapshot written on `Close` or on an idle snapshot tick; higher numbers are tailQ overflow files written in sequence. Corrupt files are renamed with a `.bad` extension rather than deleted.

### Single-goroutine event loop

All state mutation happens inside one goroutine via a `select` over the input channel (Put), output channel (Out), clear and stats request channels, the empty signal channel, an optional snapshot ticker, and the close signal. A single `sync.RWMutex` only protects the `closed` flag, gating `Put`/`Clear`/`Stats` from racing with `Close`. No other synchronization is needed.

### Snapshot feature

When `WithSnapshotInterval` is set, a ticker fires at half the configured interval. If no items have been written or read since the previous tick (idle), the in-memory queues are written to disk. On `Close`, a synchronous snapshot is always written for any non-empty queue.
