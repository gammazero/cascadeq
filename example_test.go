package cascadeq_test

import (
	"fmt"
	"log"
	"os"

	"github.com/gammazero/cascadeq"
)

// Example_basic demonstrates creating a queue, writing items, and reading them
// back until the queue is empty.
func Example_basic() {
	dir, err := os.MkdirTemp("", "cascadeq-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	q, err := cascadeq.New("example", dir)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	for i := range 3 {
		if err := q.Put(fmt.Appendf(nil, "item-%d", i)); err != nil {
			log.Fatal(err)
		}
	}

	for {
		select {
		case item := <-q.Out():
			fmt.Println(string(item))
		case <-q.Empty():
			return
		}
	}
	// Output:
	// item-0
	// item-1
	// item-2
}

// Example_options shows configuring memory limits and gzip compression.
func Example_options() {
	dir, err := os.MkdirTemp("", "cascadeq-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	q, err := cascadeq.New("example", dir,
		cascadeq.WithMaxMemory(64*1024),   // 64 KiB in-memory budget
		cascadeq.WithMaxMemItems(128),     // at most 128 items in memory
		cascadeq.WithMaxItemSize(4*1024),  // reject items larger than 4 KiB
		cascadeq.WithGzip(true),           // compress overflow files
	)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	if err := q.Put([]byte("hello")); err != nil {
		log.Fatal(err)
	}

	item := <-q.Out()
	fmt.Println(string(item))
	// Output:
	// hello
}
