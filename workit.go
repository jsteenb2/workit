package workit

import (
	"context"
)

// ErrFn is an error handler function type. This func type is called when
// the WorkFn is called and returns an error.
type ErrFn func(error)

// ErrStream is a helpful constructor to provide a stream and build a ErrFn from it.
func ErrStream(errStream chan<- error) ErrFn {
	return func(err error) {
		if errStream == nil {
			return
		}
		errStream <- err
	}
}

// WorkFn is the work function each worker calls when processing an added request.
type WorkFn func() error

type workDeets struct {
	errFn  ErrFn
	workFn WorkFn
}

// Queue is a worker queue that orchestrates the workers to process all added messages.
// It is thread safe to add more work to the worker queue.
type Queue struct {
	numWorkers    int
	backoffPolicy BackoffOptFn

	cancelFn context.CancelFunc
	doneChan <-chan struct{}

	workStream   chan workDeets
	workerStream chan chan workDeets
}

// New is a constructor for a Queue. The functional options provided can overwrite
// the default values for canceling via context as opposed to the Close. In addition
// you can set the work stream to be a buffered chan as well. If numWorkers is set to < 1
// it'll overwritten to be 1.
func New(numWorkers int, opts ...QueueOptFn) *Queue {
	newCtx, cancel := context.WithCancel(context.TODO())

	if numWorkers < 1 {
		numWorkers = 1
	}

	q := &Queue{
		numWorkers:    numWorkers,
		backoffPolicy: NewStopBackoff(),

		cancelFn: cancel,
		doneChan: newCtx.Done(),

		workStream:   make(chan workDeets),
		workerStream: make(chan chan workDeets, numWorkers),
	}

	for _, o := range opts {
		o(q)
	}

	return q
}

// Start kicks off the numWorker workers.
func (q *Queue) Start() {
	for i := 0; i < q.numWorkers; i++ {
		newWorker(q.workerStream, q.doneChan, q.backoffPolicy).start()
	}

	go func() {
		defer func() { q.workerStream = nil }()
		for {
			select {
			case deets := <-q.workStream:
				w := <-q.workerStream
				w <- deets
			case <-q.doneChan:
				q.Close()
				return
			}
		}
	}()
}

// Close calls the cancel on all worker children. Note, does not wait for all to cleanup.
func (q *Queue) Close() {
	q.cancelFn()
}

// Add creates work for the next available worker. If the work stream is not buffered,
// this call will block until there is available worker to ack the newly scheduled work.
// If the queue has been closed, this will return immediately and not process the work.
func (q *Queue) Add(workFn WorkFn, errFn ErrFn) {
	if q.workStream == nil {
		return
	}

	q.workStream <- workDeets{
		workFn: workFn,
		errFn:  errFn,
	}
}

// Depth returns the depth of the worker queue, available workers.
func (q *Queue) Depth() int {
	return len(q.workerStream)
}

type worker struct {
	work        chan workDeets
	workerQueue chan chan workDeets
	doneChan    <-chan struct{}
	retryPolicy BackoffOptFn
}

func newWorker(queue chan chan workDeets, doneChan <-chan struct{}, backoff BackoffOptFn) *worker {
	return &worker{
		work:        make(chan workDeets),
		workerQueue: queue,
		doneChan:    doneChan,
		retryPolicy: backoff,
	}
}

func (w *worker) start() {
	go func() {
		for {
			if w.workerQueue == nil {
				return
			}
			select {
			case w.workerQueue <- w.work:
				w.do()
			case <-w.doneChan:
				return
			}
		}
	}()
}

func (w *worker) do() {
	select {
	case deets := <-w.work:
		err := retry(w.doneChan, deets.workFn, w.retryPolicy)
		if err == nil || deets.errFn == nil {
			return
		}
		deets.errFn(err)
	case <-w.doneChan:
		return
	}
}
