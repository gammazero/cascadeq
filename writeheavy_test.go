package cascadeq_test

import (
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/gammazero/cascadeq"
)

// makeItem returns an item of the given size. Compressible items look like
// text so that gzip has real work to do.
func makeItem(size int, compressible bool) []byte {
	r := rand.New(rand.NewSource(int64(size)))
	b := make([]byte, size)
	if compressible {
		const words = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
		for i := range b {
			b[i] = words[(i+r.Intn(4))%len(words)]
		}
		return b
	}
	r.Read(b)
	return b
}

func reportLatency(b *testing.B, lat []time.Duration) {
	slices.Sort(lat)
	pct := func(p float64) float64 {
		i := int(float64(len(lat)-1) * p)
		return float64(lat[i].Nanoseconds()) / 1e3
	}
	b.ReportMetric(pct(0.50), "p50-µs")
	b.ReportMetric(pct(0.99), "p99-µs")
	b.ReportMetric(pct(0.999), "p99.9-µs")
	b.ReportMetric(float64(lat[len(lat)-1].Nanoseconds())/1e3, "max-µs")
}

func newQueue(b *testing.B, gzip bool) *cascadeq.Queue {
	q, err := cascadeq.New(b.TempDir(), cascadeq.WithGzip(gzip))
	if err != nil {
		b.Fatal(err)
	}
	return q
}

// BenchmarkPutOnly is a sustained producer with no consumer, so the queue is
// continually overflowing to disk. Uses default memory limits (1 MiB, 4096
// items).
func BenchmarkPutOnly(b *testing.B) {
	for _, gz := range []bool{false, true} {
		for _, size := range []int{64, 1024, 16384} {
			name := "size=" + strconv.Itoa(size)
			if gz {
				name += "/gzip"
			}
			b.Run(name, func(b *testing.B) {
				item := makeItem(size, gz)
				q := newQueue(b, gz)
				lat := make([]time.Duration, 0, b.N)
				b.SetBytes(int64(size))
				b.ResetTimer()
				for range b.N {
					start := time.Now()
					if err := q.Put(item); err != nil {
						b.Fatal(err)
					}
					lat = append(lat, time.Since(start))
				}
				b.StopTimer()
				reportLatency(b, lat)
				q.Clear()
				q.Close()
			})
		}
	}
}

// BenchmarkProducerConsumer runs a producer and a slower concurrent consumer
// that reads one item for every four written. The queue grows, so overflow
// files are written while the consumer reads files back.
func BenchmarkProducerConsumer(b *testing.B) {
	for _, gz := range []bool{false, true} {
		for _, size := range []int{256, 4096} {
			name := "size=" + strconv.Itoa(size)
			if gz {
				name += "/gzip"
			}
			b.Run(name, func(b *testing.B) {
				item := makeItem(size, gz)
				q := newQueue(b, gz)
				lat := make([]time.Duration, 0, b.N)
				reads := b.N / 4
				consumerDone := make(chan struct{})
				b.SetBytes(int64(size))
				b.ResetTimer()
				go func() {
					defer close(consumerDone)
					for range reads {
						<-q.Out()
					}
				}()
				for range b.N {
					start := time.Now()
					if err := q.Put(item); err != nil {
						b.Fatal(err)
					}
					lat = append(lat, time.Since(start))
				}
				<-consumerDone
				b.StopTimer()
				reportLatency(b, lat)
				q.Clear()
				q.Close()
			})
		}
	}
}

// BenchmarkPutBatch writes batches of 100 items with no consumer.
func BenchmarkPutBatch(b *testing.B) {
	for _, gz := range []bool{false, true} {
		name := "size=1024"
		if gz {
			name += "/gzip"
		}
		b.Run(name, func(b *testing.B) {
			item := makeItem(1024, gz)
			batch := make([][]byte, 100)
			for i := range batch {
				batch[i] = item
			}
			q := newQueue(b, gz)
			lat := make([]time.Duration, 0, b.N)
			b.SetBytes(int64(1024 * len(batch)))
			b.ResetTimer()
			for range b.N {
				start := time.Now()
				if err := q.PutBatch(batch); err != nil {
					b.Fatal(err)
				}
				lat = append(lat, time.Since(start))
			}
			b.StopTimer()
			reportLatency(b, lat)
			q.Clear()
			q.Close()
		})
	}
}
