package toc

import (
	"context"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/binaryphile/fluentfp/wrap"
	"github.com/binaryphile/fluentfp/rslt"
)

// PanicRecovery returns a decorator that catches panics in fn, increments
// the panicked counter, and converts the panic to a *[rslt.PanicError]
// with a stack trace. Built once per worker at spawn time.
func PanicRecovery[T, R any](panicked *atomic.Int64) wrap.Decorator[T, R] {
	return func(fn wrap.Fn[T, R]) wrap.Fn[T, R] {
		return func(ctx context.Context, t T) (r R, err error) {
			defer func() {
				if v := recover(); v != nil {
					panicked.Add(1)
					err = &rslt.PanicError{Value: v, Stack: debug.Stack()}
				}
			}()
			return fn(ctx, t)
		}
	}
}

// ServiceTiming returns a decorator that records fn execution duration
// in nanoseconds to serviceNs. If onDuration is non-nil, it is called
// with the measured duration (e.g. for per-worker histogram recording).
// Built once per worker at spawn time.
func ServiceTiming[T, R any](serviceNs *atomic.Int64, onDuration func(time.Duration)) wrap.Decorator[T, R] {
	return func(fn wrap.Fn[T, R]) wrap.Fn[T, R] {
		return func(ctx context.Context, t T) (R, error) {
			start := time.Now()
			r, err := fn(ctx, t)
			d := time.Since(start)
			serviceNs.Add(int64(d))
			if onDuration != nil {
				onDuration(d)
			}
			return r, err
		}
	}
}
