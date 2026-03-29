package toc

import "context"

// NewTestRopeHandle creates a RopeHandle for testing. The handle's Done
// channel closes when Stop is called.
func NewTestRopeHandle(drum string) *RopeHandle {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(done)
	}()
	return &RopeHandle{drum: drum, cancel: cancel, done: done}
}
