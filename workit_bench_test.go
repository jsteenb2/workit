package workit_test

import (
	"errors"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/jsteenb2/workit"
)

func BenchmarkQueue(b *testing.B) {
	tests := []int{1, 2, 4, 8, 16, 32, 64, 128, 1024, 10000}

	b.Run("unbuffered", func(b *testing.B) {
		b.Run("no errFn", func(b *testing.B) {
			for _, numWorkers := range tests {
				b.Run("workers-"+strconv.Itoa(numWorkers), benchQueue1MillionNoErr(numWorkers))
			}
		})

		b.Run("has errFn", func(b *testing.B) {
			for _, numWorkers := range tests {
				b.Run("workers-"+strconv.Itoa(numWorkers), benchQueue1MillionWithErr(numWorkers))
			}
		})
	})

	b.Run("buffered", func(b *testing.B) {
		sizes := []int{1, 2, 4, 8, 16, 32, 64, 128, 1024, 10000}

		for _, size := range sizes {
			b.Run("size-"+strconv.Itoa(size), func(b *testing.B) {
				b.Run("no errFn", func(b *testing.B) {
					for _, numWorkers := range tests {
						b.Run("workers-"+strconv.Itoa(numWorkers), benchQueue1MillionNoErr(numWorkers))
					}
				})

				b.Run("has errFn", func(b *testing.B) {
					for _, numWorkers := range tests {
						b.Run("workers-"+strconv.Itoa(numWorkers), benchQueue1MillionWithErr(numWorkers))
					}
				})
			})
		}
	})
}

func benchQueue1MillionNoErr(numWorkers int, opts ...workit.QueueOptFn) func(*testing.B) {
	return func(b *testing.B) {
		queue := workit.New(numWorkers, opts...)
		queue.Start()

		var count int64
		expectedCalls := 1000000
		for i := 0; i < expectedCalls; i++ {
			queue.Add(func() error {
				atomic.AddInt64(&count, 1)
				return nil
			}, nil)
		}

		err := isCount(&count, expectedCalls)
		if err != nil {
			b.Errorf("failed call count: got=%s", err)
		}

		queue.Close()
		err = isFinished(queue, 0)
		if err != nil {
			b.Errorf("failed to finish: got=%s", err)
		}
	}
}

func benchQueue1MillionWithErr(numWorkers int, opts ...workit.QueueOptFn) func(*testing.B) {
	return func(b *testing.B) {
		queue := workit.New(numWorkers, opts...)
		queue.Start()

		var count int64
		expectedCalls := 1000000
		for i := 0; i < expectedCalls; i++ {
			queue.Add(func() error {
				return errors.New("an error")
			}, func(e error) {
				atomic.AddInt64(&count, 1)
			})
		}

		err := isCount(&count, expectedCalls)
		if err != nil {
			b.Errorf("failed count: got=%s", err)
		}

		queue.Close()
		err = isFinished(queue, 0)
		if err != nil {
			b.Errorf("failed to finish: got=%s", err)
		}
	}
}
