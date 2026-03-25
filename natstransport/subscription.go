package natstransport

import (
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/nats-io/nats.go"
)

// subscription implements toc.Subscription with a precise lifecycle
// state machine. Fields are set by the subscribe methods in transport.go
// before any callback can fire. See package doc for handler contract.
type subscription struct {
	natsSub *nats.Subscription
	cancel  func()
	once    sync.Once

	mu      sync.Mutex
	closing bool
	active  int
	done    chan struct{}
}

// enterCallback attempts to enter the callback path. Returns false if
// the subscription is closing (handler should not run).
func (s *subscription) enterCallback() bool {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return false
	}
	s.active++
	s.mu.Unlock()
	return true
}

// exitCallback decrements the active count and signals done if closing.
func (s *subscription) exitCallback() {
	s.mu.Lock()
	s.active--
	if s.closing && s.active == 0 {
		close(s.done)
	}
	s.mu.Unlock()
}

// recoverPanic recovers a panic, formats it with stack trace, and routes
// to onError. Swallows panics from onError itself.
func recoverPanic(onError func(error)) {
	r := recover()
	if r == nil {
		return
	}
	stack := debug.Stack()
	err := fmt.Errorf("handler panic: %v\n%s", r, stack)
	func() {
		defer func() { recover() }()
		onError(err)
	}()
}

// safeOnError calls onError, swallowing any panic from it.
func safeOnError(onError func(error), err error) {
	defer func() { recover() }()
	onError(err)
}

// Close cancels the subscription and waits for any in-flight handler
// to return. Close is idempotent.
//
// Close may block indefinitely if a handler does not return. Callers
// requiring bounded shutdown should cancel the parent context, which
// cancels the handler's context.
func (s *subscription) Close() error {
	var unsubErr error
	s.once.Do(func() {
		s.mu.Lock()
		s.closing = true
		needWait := s.active > 0
		if !needWait {
			close(s.done)
		}
		s.mu.Unlock()

		s.cancel()
		unsubErr = s.natsSub.Unsubscribe()
		<-s.done
	})
	return unsubErr
}
