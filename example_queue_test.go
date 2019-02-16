package workit_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jsteenb2/workit"
)

func Example() {
	queue := workit.New(2)
	queue.Start()

	var count int64
	expectedCalls := 3
	errStream := make(chan error, expectedCalls)
	for i := 0; i < expectedCalls; i++ {
		queue.Add(func() error {
			atomic.AddInt64(&count, 1)
			return errors.New("an error")
		}, func(e error) {
			errStream <- e
		})
	}

	err := IsCount(&count, expectedCalls)
	fmt.Println(err)

	for i := 0; i < expectedCalls; i++ {
		err := <-errStream
		fmt.Println(err)
	}

	queue.Close()
	err = IsFinished(queue, 0)
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

func IsCount(count *int64, expectedCount int) error {
	for i := 0; i < 50; i++ {
		if int(*count) == expectedCount {
			return nil
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	return fmt.Errorf("count did not match: expected=%d got=%d", expectedCount, *count)
}

func IsFinished(q *workit.Queue, expectedDepth int) error {
	var lastDepth int
	for i := 0; i < 50; i++ {
		lastDepth = q.Depth()
		if lastDepth == expectedDepth {
			return nil
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	return fmt.Errorf("depth did not match: expected=%d got=%d", expectedDepth, lastDepth)
}
