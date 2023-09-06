package txn

import (
	"context"
	"time"
)

// Txn defines the basic transaction interface.
type Txn interface {
	Commit(context.Context) error   // Commit the transaction.
	Rollback(context.Context) error // Rollback the transaction.
	IsNil() bool                    // Check if the transaction is nil.
}

// Doer defines the interface for transaction operations.
type Doer[TOptions any, TBeginner any] interface {
	RethrowPanic() bool                               // Get the rethrow panic flag.
	SetRethrowPanic(bool)                             // Set the rethrow panic flag.
	Title() string                                    // Get the title.
	SetTitle(string)                                  // Set the title.
	Timeout() time.Duration                           // Get the timeout duration.
	SetTimeout(time.Duration)                         // Set the timeout duration.
	MaxPing() int                                     // Get the maximum ping count.
	SetMaxPing(int)                                   // Set the maximum ping count.
	MaxRetry() int                                    // Get the maximum retry count.
	SetMaxRetry(int)                                  // Set the maximum retry count.
	Options() TOptions                                // Get the options.
	SetOptions(options TOptions)                      // Set the options.
	SetReadOnly(string)                               // Set read-only mode.
	SetReadWrite(string)                              // Set read-write mode.
	IsReadOnly() bool                                 // Check if read-only mode is enabled.
	BeginTxn(context.Context, TBeginner) (Txn, error) // Begin a new transaction.
}

// DoerBase provides a base implementation for the Doer interface.
type DoerBase[TOptions any, TBeginner any] struct {
	rethrow  bool
	title    string
	timeout  time.Duration
	options  TOptions
	maxPing  int
	maxRetry int
}

// RethrowPanic gets the rethrow panic flag.
func (do *DoerBase[_, _]) RethrowPanic() bool {
	return do.rethrow
}

// SetRethrowPanic sets the rethrow panic flag.
func (do *DoerBase[_, _]) SetRethrowPanic(rethrow bool) {
	do.rethrow = rethrow
}

// Title gets the title.
func (do *DoerBase[_, _]) Title() string {
	return do.title
}

// SetTitle sets the title.
func (do *DoerBase[_, _]) SetTitle(title string) {
	do.title = title
}

// Timeout gets the timeout duration.
func (do *DoerBase[_, _]) Timeout() time.Duration {
	return do.timeout
}

// SetTimeout sets the timeout duration.
func (do *DoerBase[_, _]) SetTimeout(t time.Duration) {
	do.timeout = t
}

// MaxPing gets the maximum ping count.
func (do *DoerBase[T, _]) MaxPing() int {
	return do.maxPing
}

// SetMaxPing sets the maximum ping count.
func (do *DoerBase[_, _]) SetMaxPing(v int) {
	do.maxPing = v
}

// MaxRetry gets the maximum retry count.
func (do *DoerBase[T, _]) MaxRetry() int {
	return do.maxRetry
}

// SetMaxRetry sets the maximum retry count.
func (do *DoerBase[_, _]) SetMaxRetry(v int) {
	do.maxRetry = v
}

// Options gets the options.
func (do *DoerBase[T, _]) Options() T {
	return do.options
}

// SetOptions sets the options.
func (do *DoerBase[T, _]) SetOptions(options T) {
	do.options = options
}

// IsReadOnly checks if read-only mode is enabled.
func (do *DoerBase[_, _]) IsReadOnly() bool {
	panic("implement me")
}

// SetReadOnly sets read-only mode.
func (do *DoerBase[_, _]) SetReadOnly(string) {
	panic("implement me")
}

// SetReadWrite sets read-write mode.
func (do *DoerBase[_, _]) SetReadWrite(string) {
	panic("implement me")
}

// BeginTxn begins a new transaction.
func (do *DoerBase[_, B]) BeginTxn(context.Context, B) (Txn, error) {
	panic("implement me")
}
