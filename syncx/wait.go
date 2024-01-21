package syncx

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	waitC := make(chan struct{})
	go func() {
		defer close(waitC)
		wg.Wait()
	}()
	select {
	case <-waitC:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout")
	}
}

func IsContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func WaitCtx(ctx context.Context, timeout time.Duration) {
	if IsContextDone(ctx) {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(timeout):
	}
}
