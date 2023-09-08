package txn

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"time"
)

// PingCount is a type alias for a function that takes an integer count and a time duration as arguments.
// It is designed to keep track of the number of ping attempts and the delay before the next attempt.
type PingCount = func(cnt int, delay time.Duration)

// Ping is a function that performs a repeatable ping operation with retry logic.
// Parameters:
// - limit: The maximum number of retry attempts. If set to <= 0, a default of 3 is used.
// - count: A function of type PingCount to report the number of attempts and delay.
// - ping: A function that takes a context and returns an error. It performs the actual ping operation.
// Returns:
// - cnt: The total number of attempts made.
// - err: Any error encountered
func Ping(limit int, count PingCount, ping func(context.Context) error) (cnt int, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v --- debug.Stack --- %s", p, debug.Stack())
		}
	}()
	var cancel context.CancelFunc
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	const timeout = 2 * time.Second
	const sleepMax = 64 * time.Second

	var ctx context.Context
	cnt = 0
	if limit <= 0 {
		limit = 3
	}
	for {
		if ping != nil {
			if cancel != nil {
				cancel()
			}
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			err = ping(ctx)
		} else {
			err = errors.Join(ErrNilArgument, errors.New("[txn.Ping ping]"))
		}
		cnt++
		if cnt > limit {
			if err != nil {
				err = fmt.Errorf("reached retry limit (%d), last error: %v", limit, err)
			}
			break
		}
		sleep := time.Duration(cnt*cnt) * time.Second
		if sleep > sleepMax {
			sleep = sleepMax
		}
		if count != nil {
			count(cnt, sleep)
		}
		if err == nil {
			break
		}
		jitter := time.Duration(float64(sleep) * (rand.Float64()*0.1 + 0.95))
		time.Sleep(jitter)
	}
	if cancel != nil {
		cancel()
	}
	return
}
