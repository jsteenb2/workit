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
					faker := newFakeFn()

					queue := workit.New(1, workit.BackoffPolicy(tt.backoffPolicy))
					queue.Start()

					called := make(chan struct{}, 1)
					queue.Add(func() error {
						faker.incr()
						return errors.New("an error")
					}, func(e error) {
						faker.incr()
						called <- struct{}{}
					})

					getsCalled(t, called, 1)
					if count := faker.callCount(); count != 11 {
						t.Errorf("wrong call count: expected=%d got=%d", 11, count)
					}

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
		faker := newFakeFn()

		queue := workit.New(numWorkers, opts...)
		queue.Start()

		called := make(chan struct{}, expectedCalls)
		for i := 0; i < expectedCalls; i++ {
			queue.Add(func() error {
				faker.incr()
				called <- struct{}{}
				return nil
			}, nil)
		}

		getsCalled(t, called, expectedCalls)
		if count := faker.callCount(); expectedCalls != count {
			t.Errorf("unexpected call count: expected=%d got%d", expectedCalls, count)
		}

		queue.Close()
		finishes(t, queue, 0)
	}
}

func testQueueWithErrFn(numWorkers, expectedCalls int, opts ...workit.QueueOptFn) func(t *testing.T) {
	return func(t *testing.T) {

		queue := workit.New(numWorkers, opts...)
		queue.Start()

		faker := newFakeFn()
		called := make(chan struct{}, expectedCalls)
		for i := 0; i < expectedCalls; i++ {
			queue.Add(func() error {
				return errors.New("an error")
			}, func(e error) {
				faker.incr()
				called <- struct{}{}
			})
		}

		getsCalled(t, called, expectedCalls)
		if count := faker.callCount(); expectedCalls != count {
			t.Errorf("unexpected call count: expected=%d got%d", expectedCalls, count)
		}

		queue.Close()
		finishes(t, queue, 0)
	}
}

func ExampleQueue() {
	faker := newFakeFn()

	queue := workit.New(3)
	queue.Start()

	expectedCalls := 100
	called := make(chan struct{}, expectedCalls)
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			faker.incr()
			called <- struct{}{}
			return nil
		}, nil)
	}

	err := isCalled(called, expectedCalls)
	fmt.Println(err)

	queue.Close()
	err = isFinished(queue, 0)
	fmt.Println(err)
	// Output:
	// <nil>
	// <nil>
}

func ExampleQueue_Add() {
	faker := newFakeFn()

	queue := workit.New(3)
	queue.Start()

	expectedCalls := 3
	called := make(chan struct{}, expectedCalls)
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			return errors.New("an error")
		}, func(e error) {
			faker.incr()
			called <- struct{}{}
		})
	}

	err := isCalled(called, expectedCalls)
	fmt.Println(err)

	queue.Close()
	err = isFinished(queue, 0)
	fmt.Println(err)
	// Output:
	// <nil>
	// <nil>
}

func ExampleQueue_Add_WithErrStream() {
	faker := newFakeFn()

	queue := workit.New(3)
	queue.Start()

	expectedCalls := 3
	errStream := make(chan error, expectedCalls)
	called := make(chan struct{}, expectedCalls)
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			faker.incr()
			called <- struct{}{}
			return errors.New("an error")
		}, workit.ErrStream(errStream))
	}

	err := isCalled(called, expectedCalls)
	fmt.Println(err)

	for i := 0; i < expectedCalls; i++ {
		err := <-errStream
		fmt.Println(err)
	}

	queue.Close()
	err = isFinished(queue, 0)
	fmt.Println(err)
	fmt.Println(faker.callCount())
	// Output:
	// <nil>
	// an error
	// an error
	// an error
	// <nil>
	// 3
}

type fakeFn struct {
	calls *int64
}

func newFakeFn() *fakeFn {
	var init int64
	return &fakeFn{
		calls: &init,
	}
}

func (f *fakeFn) incr() {
	atomic.AddInt64(f.calls, 1)
}

func (f *fakeFn) callCount() (i int) {
	return int(*f.calls)
}

func isCalled(calledChan <-chan struct{}, expectedCount int) error {
	for i := 0; i < expectedCount; i++ {
		select {
		case <-calledChan:
		case <-time.After(100 * time.Millisecond):
			return errors.New("call not made in time")
		}
	}
	return nil
}

func getsCalled(t *testing.T, calledChan <-chan struct{}, expectedCount int) {
	if err := isCalled(calledChan, expectedCount); err != nil {
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
