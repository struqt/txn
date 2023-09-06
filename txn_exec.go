package txn

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// DoFunc defines the function type for transaction execution.
type DoFunc[O any, B any, D Doer[O, B]] func(ctx context.Context, do D) error

// Execute executes a transaction with the given Doer and function.
func Execute[O any, B any, D Doer[O, B]](ctx context.Context, db B, doer D, fn DoFunc[O, B, D]) (err error) {
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w [txn context done]", ctx.Err())
	default:
		if doer.Timeout() > time.Millisecond {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, doer.Timeout())
			defer cancel()
		}
		var tx Txn
		if tx, err = doer.BeginTxn(ctx, db); err != nil {
			return fmt.Errorf("%w [txn begin]", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if doer.RethrowPanic() {
					panic(p)
				}
				err = fmt.Errorf("%v --- debug.Stack --- %s", p, debug.Stack())
				if tx.IsNil() {
					return
				}
				if x := tx.Rollback(ctx); x != nil {
					err = fmt.Errorf("%w [txn recover] %w [rollback]", err, x)
				} else {
					err = fmt.Errorf("%w [txn recover]", err)
				}
			}
		}()
		if err = fn(ctx, doer); err != nil {
			if x := tx.Rollback(ctx); x != nil {
				return fmt.Errorf("%w [txn do] %w [rollback]", err, x)
			} else {
				return fmt.Errorf("%w [txn do]", err)
			}
		}
		if err = tx.Commit(ctx); err != nil {
			if x := tx.Rollback(ctx); x != nil {
				return fmt.Errorf("%w [txn commit] %w [rollback]", err, x)
			} else {
				return fmt.Errorf("%w [txn commit]", err)
			}
		} else {
			return nil
		}
	}
}

// Ping performs a ping operation with the given Doer.
func Ping[O any, B any](
	ctx context.Context, doer Doer[O, B], sleep func(time.Duration, int), ping func(context.Context) error) (int, error) {
	if ping == nil {
		return 0, fmt.Errorf("ping func(..) is nil")
	}
	var (
		ctx0   context.Context
		cancel context.CancelFunc
	)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if doer.MaxPing() <= 0 {
		return 0, nil
	}
	var retryIntervals = [4]time.Duration{
		time.Second * 1,
		time.Second * 4,
		time.Second * 9,
		time.Second * 16,
	}
	const retryIntervalsLen = len(retryIntervals)
	cnt := 0
	for {
		ctx0, cancel = context.WithTimeout(ctx, time.Second)
		err := ping(ctx0)
		cancel()
		i := retryIntervals[cnt%retryIntervalsLen]
		cnt++
		if err == nil {
			return cnt, nil
		}
		if cnt > doer.MaxPing() {
			return cnt, err
		}
		if sleep != nil {
			sleep(i, cnt)
		}
		time.Sleep(i)
	}
}
