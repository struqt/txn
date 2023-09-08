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
	MultiSet(setters ...DoerFieldSetter)
	Reset(*DoerFields) error
	ResetAsReadOnly(string) error
	ResetAsReadWrite(string) error
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

// MultiSet applies field setters to modify DoerBase's fields.
func (do *DoerBase[_, _]) MultiSet(setters ...DoerFieldSetter) {
	do.mutex.Lock()
	defer do.mutex.Unlock()
	for _, setter := range setters {
		setter(&do.fields)
	}
}

// Reset resets DoerBase's fields to the provided values.
func (do *DoerBase[_, _]) Reset(fields *DoerFields) error {
	if fields == nil {
		return errors.Join(ErrNilArgument, errors.New("[txn.DoerBase.Reset fields]"))
	}
	if fields.options == nil {
		return errors.Join(ErrNilArgument, errors.New("[txn.DoerBase.Reset fields.options]"))
	}
	do.mutex.Lock()
	defer do.mutex.Unlock()
	do.fields = *fields
	return nil
}

// ResetAsReadOnly sets read-only mode.
func (do *DoerBase[_, _]) ResetAsReadOnly(string) error {
	return errors.Join(ErrNotImplemented, errors.New("[txn.DoerBase.ResetAsReadOnly]"))
}

// ResetAsReadWrite sets read-write mode.
func (do *DoerBase[_, _]) ResetAsReadWrite(string) error {
	return errors.Join(ErrNotImplemented, errors.New("[txn.DoerBase.ResetAsReadWrite]"))
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

const (
	DefaultTitle    = "Txn`Nameless"
	DefaultTimeout  = 10 * time.Second
	DefaultMaxPing  = 4
	DefaultMaxRetry = 4
)

// NewDoerFields creates and initializes a new DoerFields instance with optional setters.
func NewDoerFields(setters ...DoerFieldSetter) *DoerFields {
	fields := &DoerFields{
		title:    DefaultTitle,
		rethrow:  false,
		timeout:  DefaultTimeout,
		maxPing:  DefaultMaxPing,
		maxRetry: DefaultMaxRetry,
	}
	for _, setter := range setters {
		if setter != nil {
			setter(fields)
		}
	}
	return fields
}

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
