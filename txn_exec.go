package txn

import (
	"context"
	"fmt"
	"runtime/debug"
)

// DoFunc defines the function type for transaction execution.
type DoFunc[O any, B any, D Doer[O, B]] func(ctx context.Context, do D) error

// Execute executes a transaction with the given Doer and function.
func Execute[
	O any,
	B any,
	D Doer[O, B],
	F DoFunc[O, B, D],
](ctx context.Context, db B, doer D, fn F) (err error) {
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w [txn context done]", ctx.Err())
	default:
		var txn Txn
		if txn, err = doer.BeginTxn(ctx, db); err != nil {
			return fmt.Errorf("%w [txn begin]", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if doer.Rethrow() {
					panic(p)
				}
				err = fmt.Errorf("%v --- debug.Stack --- %s", p, debug.Stack())
				if x := txn.Rollback(ctx); x != nil {
					err = fmt.Errorf("%w [txn recover] %w [rollback]", err, x)
				} else {
					err = fmt.Errorf("%w [txn recover]", err)
				}
			}
		}()
		if err = fn(ctx, doer); err != nil {
			if x := txn.Rollback(ctx); x != nil {
				return fmt.Errorf("%w [txn do] %w [rollback]", err, x)
			} else {
				return fmt.Errorf("%w [txn do]", err)
			}
		}
		if err = txn.Commit(ctx); err != nil {
			if x := txn.Rollback(ctx); x != nil {
				return fmt.Errorf("%w [txn commit] %w [rollback]", err, x)
			} else {
				return fmt.Errorf("%w [txn commit]", err)
			}
		} else {
			return nil
		}
	}
}
