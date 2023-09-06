package txn

import (
	"context"
	"time"
)

type Txn interface {
	Commit(context.Context) error
	Rollback(context.Context) error
	IsNil() bool
}

type Doer[TOptions any, TBeginner any] interface {
	RethrowPanic() bool
	SetRethrowPanic(bool)
	Title() string
	SetTitle(string)
	Timeout() time.Duration
	SetTimeout(time.Duration)
	MaxPing() int
	SetMaxPing(int)
	MaxRetry() int
	SetMaxRetry(int)
	Options() TOptions
	SetOptions(options TOptions)
	SetReadOnly(string)
	SetReadWrite(string)
	IsReadOnly() bool
	BeginTxn(context.Context, TBeginner) (Txn, error)
}

type DoerBase[TOptions any, TBeginner any] struct {
	rethrow bool
	title   string
	timeout time.Duration
	options TOptions

	maxPing  int
	maxRetry int
}

func (do *DoerBase[_, _]) RethrowPanic() bool {
	return do.rethrow
}

func (do *DoerBase[_, _]) SetRethrowPanic(rethrow bool) {
	do.rethrow = rethrow
}

func (do *DoerBase[_, _]) Title() string {
	return do.title
}

func (do *DoerBase[_, _]) SetTitle(title string) {
	do.title = title
}

func (do *DoerBase[_, _]) Timeout() time.Duration {
	return do.timeout
}

func (do *DoerBase[_, _]) SetTimeout(t time.Duration) {
	do.timeout = t
}

func (do *DoerBase[T, _]) MaxPing() int {
	return do.maxPing
}

func (do *DoerBase[_, _]) SetMaxPing(v int) {
	do.maxPing = v
}

func (do *DoerBase[T, _]) MaxRetry() int {
	return do.maxRetry
}

func (do *DoerBase[_, _]) SetMaxRetry(v int) {
	do.maxRetry = v
}

func (do *DoerBase[T, _]) Options() T {
	return do.options
}

func (do *DoerBase[T, _]) SetOptions(options T) {
	do.options = options
}

func (do *DoerBase[_, _]) IsReadOnly() bool {
	panic("implement me")
}

func (do *DoerBase[_, _]) SetReadOnly(string) {
	panic("implement me")
}

func (do *DoerBase[_, _]) SetReadWrite(string) {
	panic("implement me")
}

func (do *DoerBase[_, B]) BeginTxn(context.Context, B) (Txn, error) {
	panic("implement me")
}
