package workit

import "context"

// QueueOptFn is a functional option for setting fields on the queue type.
type QueueOptFn func(*Queue)

// Ctx provides a context cancellation to the queue type that will dictate
// the close of the queue.
func Ctx(ctx context.Context) QueueOptFn {
	return func(q *Queue) {
		newCtx, cancel := context.WithCancel(ctx)
		q.doneChan = newCtx.Done()
		q.cancelFn = cancel
	}
}


// Buffer sets the work stream to a buffered stream. If the buffered stream is
// full, the Add call will be blocking until availability.
func Buffer(buffer int) QueueOptFn {
	return func(q *Queue) {
		q.workStream = make(chan workDeets, buffer)
	}
}

// TODO: add Retry policy
