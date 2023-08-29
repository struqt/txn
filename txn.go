package txn

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

type Txn interface {
	Commit(context.Context) error
	Rollback(context.Context) error
}

type Doer[TTxn Txn, TBeginner any] interface {
	RethrowPanic() bool
	SetRethrowPanic(bool)
	Title() string
	SetTitle(string)
	Timeout() time.Duration
	SetTimeout(time.Duration)
	IsReadOnly() bool
	BeginTxn(ctx context.Context, db TBeginner) (TTxn, error)
}

type DoFunc[T Txn, B any, D Doer[T, B]] func(ctx context.Context, do D) error

func ExecuteTxn[T Txn, B any, D Doer[T, B]](ctx context.Context, db B, doer D, fn DoFunc[T, B, D]) (err error) {
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w [txn context done]", ctx.Err())
	default:
		if doer.Timeout() > 1*time.Millisecond {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, doer.Timeout())
			defer cancel()
		}
		var tx T
		if tx, err = doer.BeginTxn(ctx, db); err != nil {
			return fmt.Errorf("%w [txn begin]", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if doer.RethrowPanic() {
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

type DoerBase[TOptions any] struct {
	rethrow bool
	title   string
	timeout time.Duration
	options TOptions
}

func (receiver *DoerBase[any]) RethrowPanic() bool {
	return receiver.rethrow
}

func (receiver *DoerBase[any]) SetRethrowPanic(rethrow bool) {
	receiver.rethrow = rethrow
}

func (receiver *DoerBase[any]) Title() string {
	return receiver.title
}

func (receiver *DoerBase[any]) SetTitle(title string) {
	receiver.title = title
}

func (receiver *DoerBase[any]) Timeout() time.Duration {
	return receiver.timeout
}

func (receiver *DoerBase[any]) SetTimeout(t time.Duration) {
	receiver.timeout = t
}

func (receiver *DoerBase[any]) Options() any {
	return receiver.options
}

func (receiver *DoerBase[any]) SetOptions(options any) {
	receiver.options = options
}
