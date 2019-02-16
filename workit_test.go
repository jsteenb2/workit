package workit_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsteenb2/workit"
)

func TestQueue(t *testing.T) {
	t.Run("lifecycle", func(t *testing.T) {
		t.Run("with default ctx", func(t *testing.T) {
			queue := workit.New(3)
			if depth := queue.Depth(); depth != 0 {
				t.Fatalf("startup began to early got=%d", depth)
			}

			queue.Start()
			finishes(t, queue, 3)

			queue.Close()
			finishes(t, queue, 0)
		})

		t.Run("with Ctx option", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.TODO())
			queue := workit.New(3, workit.Ctx(ctx))
			if depth := queue.Depth(); depth != 0 {
				t.Fatalf("startup began to early got=%d", depth)
			}

			queue.Start()
			finishes(t, queue, 3)

			cancel()
			finishes(t, queue, 0)
		})
	})

	t.Run("Add", func(t *testing.T) {
		t.Run("unbuffered work stream", func(t *testing.T) {
			tests := []struct {
				name          string
				numWorkers    int
				expectedCalls int
			}{
				{
					name:          "1 worker 5 adds",
					numWorkers:    1,
					expectedCalls: 5,
				},
				{
					name:          "3 worker 15 adds",
					numWorkers:    3,
					expectedCalls: 15,
				},
				{
					name:          "5 worker 500 adds",
					numWorkers:    5,
					expectedCalls: 500,
				},
				{
					name:          "50 worker 50000 adds",
					numWorkers:    50,
					expectedCalls: 50000,
				},
			}
			for _, tt := range tests {
				t.Run("no error func/"+tt.name, testQueueNoErrFn(tt.numWorkers, tt.expectedCalls))
			}

			for _, tt := range tests {
				t.Run("has error func/"+tt.name, testQueueNoErrFn(tt.numWorkers, tt.expectedCalls))
			}
		})

		t.Run("buffered work stream", func(t *testing.T) {
			tests := []struct {
				name          string
				buffer        int
				workers       int
				expectedCalls int
			}{
				{
					name:          "1 worker 5 adds",
					buffer:        1,
					workers:       1,
					expectedCalls: 5,
				},
				{
					name:          "3 worker 15 adds",
					buffer:        3,
					workers:       3,
					expectedCalls: 15,
				},
				{
					name:          "5 worker 500 adds",
					buffer:        5,
					workers:       5,
					expectedCalls: 500,
				},
				{
					name:          "50 worker 50000 adds",
					buffer:        100,
					workers:       50,
					expectedCalls: 50000,
				},
			}
			for _, tt := range tests {
				t.Run("no error func/"+tt.name, testQueueNoErrFn(tt.workers, tt.expectedCalls, workit.Buffer(tt.buffer)))
			}

			for _, tt := range tests {
				t.Run("has error func/"+tt.name, testQueueWithErrFn(tt.workers, tt.expectedCalls, workit.Buffer(tt.buffer)))
			}
		})

		t.Run("with backoffPolicy policy", func(t *testing.T) {
			tests := []struct {
				name          string
				backoffPolicy workit.BackoffOptFn
			}{
				{
					name:          "zero",
					backoffPolicy: workit.NewZeroBackoff(10),
				},
				{
					name:          "exponential",
					backoffPolicy: workit.NewExponentialBackoff(0, time.Millisecond, 10),
				},
				{
					name:          "simple",
					backoffPolicy: workit.NewSimpleBackoff(10, false, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
				},
				{
					name:          "constant",
					backoffPolicy: workit.NewConstantBackoff(time.Nanosecond, 10),
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					queue := workit.New(1, workit.BackoffPolicy(tt.backoffPolicy))
					queue.Start()

					var count int64
					queue.Add(func() error {
						atomic.AddInt64(&count, 1)
						return errors.New("an error")
					}, func(e error) {
						atomic.AddInt64(&count, 1)
					})

					getsCount(t, &count, 11)

					queue.Close()
					finishes(t, queue, 0)
				}
				t.Run(tt.name, fn)
			}
		})
	})
}

func testQueueNoErrFn(numWorkers, expectedCalls int, opts ...workit.QueueOptFn) func(t *testing.T) {
	return func(t *testing.T) {
		queue := workit.New(numWorkers, opts...)
		queue.Start()

		var count int64
		for i := 0; i < expectedCalls; i++ {
			queue.Add(func() error {
				atomic.AddInt64(&count, 1)
				return nil
			}, nil)
		}

		getsCount(t, &count, expectedCalls)

		queue.Close()
		finishes(t, queue, 0)
	}
}

func testQueueWithErrFn(numWorkers, expectedCalls int, opts ...workit.QueueOptFn) func(t *testing.T) {
	return func(t *testing.T) {

		queue := workit.New(numWorkers, opts...)
		queue.Start()

		var count int64
		for i := 0; i < expectedCalls; i++ {
			queue.Add(func() error {
				return errors.New("an error")
			}, func(e error) {
				atomic.AddInt64(&count, 1)
			})
		}

		getsCount(t, &count, expectedCalls)

		queue.Close()
		finishes(t, queue, 0)
	}
}

func ExampleQueue() {
	queue := workit.New(3)
	queue.Start()

	var count int64
	expectedCalls := 100
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			atomic.AddInt64(&count, 1)
			return nil
		}, nil)
	}

	err := isCount(&count, expectedCalls)
	fmt.Println(err)

	queue.Close()
	err = isFinished(queue, 0)
	fmt.Println(err)
	// Output:
	// <nil>
	// <nil>
}

func ExampleQueue_Add() {
	queue := workit.New(3)
	queue.Start()

	var count int64
	expectedCalls := 3
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			return errors.New("an error")
		}, func(e error) {
			atomic.AddInt64(&count, 1)
		})
	}

	err := isCount(&count, expectedCalls)
	fmt.Println(err)

	queue.Close()
	err = isFinished(queue, 0)
	fmt.Println(err)
	// Output:
	// <nil>
	// <nil>
}

func ExampleQueue_Add_WithErrStream() {
	queue := workit.New(3)
	queue.Start()

	var count int64
	expectedCalls := 3
	errStream := make(chan error, expectedCalls)
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			atomic.AddInt64(&count, 1)
			return errors.New("an error")
		}, workit.ErrStream(errStream))
	}

	err := isCount(&count, expectedCalls)
	fmt.Println(err)

	for i := 0; i < expectedCalls; i++ {
		err := <-errStream
		fmt.Println(err)
	}

	queue.Close()
	err = isFinished(queue, 0)
	fmt.Println(err)
	fmt.Println(count)
	// Output:
	// <nil>
	// an error
	// an error
	// an error
	// <nil>
	// 3
}

func isCount(count *int64, expectedCount int) error {
	for i := 0; i < 500; i++ {
		if int(*count) == expectedCount {
			return nil
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	return fmt.Errorf("count did not match: expected=%d got=%d", expectedCount, *count)
}

func getsCount(t *testing.T, count *int64, expectedCount int) {
	err := isCount(count, expectedCount)
	if err != nil {
		t.Fatal(err)
	}
}

func isFinished(q *workit.Queue, expectedDepth int) error {
	var lastDepth int
	for i := 0; i < 500; i++ {
		lastDepth = q.Depth()
		if lastDepth == expectedDepth {
			return nil
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	return fmt.Errorf("depth did not match: expected=%d got=%d", expectedDepth, lastDepth)
}

func finishes(t *testing.T, queue *workit.Queue, expectedDepth int) {
	t.Helper()

	if err := isFinished(queue, expectedDepth); err != nil {
		t.Fatal(err)
	}
}
