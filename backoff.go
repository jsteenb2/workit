package workit

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// RetryNotify calls notify function with the error and wait duration
// for each failed attempt before sleep.
func retry(doneChan <-chan struct{}, operation WorkFn, b BackoffOptFn) error {
	var err error
	var wait time.Duration
	var retry bool
	var n int

	backoffPolicy := b()
	for {
		if err = operation(); err == nil {
			return nil
		}

		n++
		wait, retry = backoffPolicy.Next(n)
		if !retry {
			return err
		}

		select {
		case <-doneChan:
			return err
		case <-time.After(wait):
		}
	}
}

// BackoffFunc specifies the signature of a function that returns the
// time to wait before the next call to a resource. To stop retrying
// return false in the 2nd return value.
type BackoffFunc func(retry int) (time.Duration, bool)

type BackoffOptFn func() Backoffer

// Backoffer allows callers to implement their own Backoffer strategy.
type Backoffer interface {
	// Next implements a BackoffFunc.
	Next(retry int) (time.Duration, bool)
}

// -- ZeroBackoff --

// ZeroBackoff is a fixed backoff policy whose backoff time is always zero,
// meaning that the operation is retried immediately without waiting,
// indefinitely.
type ZeroBackoff struct {
	maxCalls int
}

// NewZeroBackoff creates a zero backoff with max set calls. When set to 0,
// will backoff without end.
func NewZeroBackoff(maxCalls int) BackoffOptFn {
	return func() Backoffer {
		return ZeroBackoff{maxCalls: maxCalls}
	}
}

// Next implements BackoffFunc for ZeroBackoff.
func (b ZeroBackoff) Next(retry int) (time.Duration, bool) {
	if b.maxCalls > 0 && retry == b.maxCalls {
		return 0, false
	}
	return 0, true
}

// -- StopBackoff --

// StopBackoff is a fixed backoff policy that always returns false for
// Next(), meaning that the operation should never be retried.
type StopBackoff struct{}

func NewStopBackoff() BackoffOptFn {
	return func() Backoffer {
		return StopBackoff{}
	}
}

// Next implements BackoffFunc for StopBackoff.
func (b StopBackoff) Next(retry int) (time.Duration, bool) {
	return 0, false
}

// -- ConstantBackoff --

// ConstantBackoff is a backoff policy that always returns the same delay.
type ConstantBackoff struct {
	interval time.Duration
	maxCalls int
}

// NewConstantBackoff returns a new ConstantBackoff.
func NewConstantBackoff(interval time.Duration, maxCalls int) BackoffOptFn {
	return func() Backoffer {
		return &ConstantBackoff{
			interval: interval,
			maxCalls: maxCalls,
		}
	}
}

// Next implements BackoffFunc for ConstantBackoff.
func (b *ConstantBackoff) Next(retry int) (time.Duration, bool) {
	if b.maxCalls > 0 && retry == b.maxCalls {
		return 0, false
	}
	return b.interval, true
}

// -- Exponential --

// ExponentialBackoff implements the simple exponential backoff described by
// Douglas Thain at http://dthain.blogspot.de/2009/02/exponential-backoff-in-distributed.html.
type ExponentialBackoff struct {
	t        float64 // initial timeout (in msec)
	f        float64 // exponential factor (e.g. 2)
	m        float64 // maximum timeout (in msec)
	maxCalls int
}

// NewExponentialBackoff returns a ExponentialBackoff backoff policy.
// Use initialTimeout to set the first/minimal interval
// and maxTimeout to set the maximum wait interval.
func NewExponentialBackoff(initialTimeout, maxTimeout time.Duration, maxCalls int) BackoffOptFn {
	return func() Backoffer {
		return &ExponentialBackoff{
			t:        float64(int64(initialTimeout / time.Millisecond)),
			f:        2.0,
			m:        float64(int64(maxTimeout / time.Millisecond)),
			maxCalls: maxCalls,
		}
	}
}

// Next implements BackoffFunc for ExponentialBackoff.
func (b *ExponentialBackoff) Next(retry int) (time.Duration, bool) {
	if b.maxCalls > 0 && retry == b.maxCalls {
		return 0, false
	}
	r := 1.0 + rand.Float64() // random number in [1..2]
	m := math.Min(r*b.t*math.Pow(b.f, float64(retry)), b.m)
	if m >= b.m {
		return 0, false
	}
	d := time.Duration(int64(m)) * time.Millisecond
	return d, true
}

// -- Simple Backoffer --

// SimpleBackoff takes a list of fixed values for backoff intervals.
// Each call to Next returns the next value from that fixed list.
// After each value is returned, subsequent calls to Next will only return
// the last element. The values are optionally "jittered" (off by default).
type SimpleBackoff struct {
	sync.Mutex
	ticks    []int
	jitter   bool
	maxCalls int
}

// NewSimpleBackoff creates a SimpleBackoff algorithm with the specified
// list of fixed intervals in milliseconds.
func NewSimpleBackoff(maxCalls int, jitter bool, ticks ...int) BackoffOptFn {
	return func() Backoffer {
		return &SimpleBackoff{
			ticks:    ticks,
			jitter:   jitter,
			maxCalls: maxCalls,
		}
	}
}

// Next implements BackoffFunc for SimpleBackoff.
func (b *SimpleBackoff) Next(retry int) (time.Duration, bool) {
	if b.maxCalls > 0 && retry == b.maxCalls {
		return 0, false
	}

	b.Lock()
	defer b.Unlock()

	if retry >= len(b.ticks) {
		return 0, false
	}

	ms := b.ticks[retry]
	if b.jitter {
		ms = jitter(ms)
	}
	return time.Duration(ms) * time.Millisecond, true
}

// jitter randomizes the interval to return a value of [0.5*millis .. 1.5*millis].
func jitter(millis int) int {
	if millis <= 0 {
		return 0
	}
	return millis/2 + rand.Intn(millis)
}
