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
				if doer.Rethrow() {
					panic(p)
				}
				err = fmt.Errorf("%v --- debug.Stack --- %s", p, debug.Stack())
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
