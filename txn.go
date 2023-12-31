package txn

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Txn defines the basic database transaction interface.
type Txn interface {
	Commit(context.Context) error   // Commit the transaction.
	Rollback(context.Context) error // Rollback the transaction.
}

// Doer defines the interface for database transaction operations.
type Doer[TOptions any, TBeginner any] interface {
	Mutate(setters ...DoerFieldSetter)
	BeginTxn(context.Context, TBeginner) (Txn, error)
	Title() string
	Rethrow() bool
	Timeout() time.Duration
	MaxPing() int
	MaxRetry() int
	Options() TOptions
}

// DoerFields provides data fields for DoerBase struct.
type DoerFields struct {
	title    string
	rethrow  bool
	timeout  time.Duration
	maxPing  int
	maxRetry int
	options  any
}

// DoerBase provides a base implementation for the Doer interface.
type DoerBase[TOptions any, TBeginner any] struct {
	mutex  sync.Mutex
	fields DoerFields
}

// Mutate applies field setters to modify DoerBase's fields.
func (do *DoerBase[_, _]) Mutate(setters ...DoerFieldSetter) {
	do.mutex.Lock()
	defer do.mutex.Unlock()
	for _, setter := range setters {
		setter(&do.fields)
	}
}

// BeginTxn begins a new transaction.
func (do *DoerBase[_, B]) BeginTxn(context.Context, B) (Txn, error) {
	return nil, errors.Join(ErrNotImplemented, errors.New("[txn.DoerBase.BeginTxn]"))
}

// Title gets the title.
func (do *DoerBase[_, _]) Title() string {
	return do.fields.title
}

// Rethrow gets the rethrow panic flag.
func (do *DoerBase[_, _]) Rethrow() bool {
	return do.fields.rethrow
}

// Timeout gets the timeout duration.
func (do *DoerBase[_, _]) Timeout() time.Duration {
	return do.fields.timeout
}

// MaxPing gets the maximum ping count.
func (do *DoerBase[_, _]) MaxPing() int {
	return do.fields.maxPing
}

// MaxRetry gets the maximum retry count.
func (do *DoerBase[_, _]) MaxRetry() int {
	return do.fields.maxRetry
}

// Options gets the options.
func (do *DoerBase[T, _]) Options() T {
	if t, ok := do.fields.options.(T); ok {
		return t
	} else {
		var empty T
		return empty
	}
}

// DoerFieldSetter defines a function signature for setting DoerFields.
type DoerFieldSetter func(*DoerFields)

// WithTitle creates a field setter for the title.
func WithTitle(value string) DoerFieldSetter {
	return func(do *DoerFields) {
		do.title = value
	}
}

// WithRethrow creates a field setter for the rethrow flag.
func WithRethrow(value bool) DoerFieldSetter {
	return func(do *DoerFields) {
		do.rethrow = value
	}
}

// WithTimeout creates a field setter for the timeout duration.
func WithTimeout(value time.Duration) DoerFieldSetter {
	return func(do *DoerFields) {
		do.timeout = value
	}
}

// WithMaxPing creates a field setter for the maximum ping count.
func WithMaxPing(value int) DoerFieldSetter {
	return func(do *DoerFields) {
		do.maxPing = value
	}
}

// WithMaxRetry creates a field setter for the maximum retry count.
func WithMaxRetry(value int) DoerFieldSetter {
	return func(do *DoerFields) {
		do.maxRetry = value
	}
}

// WithOptions creates a field setter for options.
func WithOptions(value any) DoerFieldSetter {
	return func(do *DoerFields) {
		do.options = value
	}
}

var (
	ErrNilArgument    = errors.New("nil argument")
	ErrNotImplemented = errors.New("not implemented")
)
