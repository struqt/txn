package txn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func RunTicker(ctx context.Context, interval time.Duration, tickCount int, tick func(context.Context, int32)) {
	const intervalMin = 50 * time.Millisecond
	if interval < intervalMin {
		interval = intervalMin
	}
	if tickCount < 1 {
		tickCount = 1
	}
	var wg sync.WaitGroup
	wg.Add(tickCount)
	go func(wg *sync.WaitGroup) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var count atomic.Int32
		for {
			select {
			case <-ctx.Done():
				//log.Info("Demo Ticker is stopping ...")
				return
			case <-ticker.C:
				count.Add(1)
				if count.Load() > int32(tickCount) {
					return
				}
				go func() {
					defer wg.Done()
					tick(ctx, count.Load())
				}()
			}
		}
	}(&wg)
	wg.Wait()
}
