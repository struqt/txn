package txn_sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	//
	//
	"github.com/struqt/txn"
)

// Beginner is an alias for *sql.DB.
type Beginner = *sql.DB

// Options is an alias for *sql.TxOptions.
type Options = *sql.TxOptions

// Doer defines the interface for SQL transaction operations.
type Doer[Stmt any] interface {
	txn.Doer[Options, Beginner]
	Stmt() Stmt
	SetStmt(Stmt)
}

// DoerBase provides a base implementation for the Doer interface.
type DoerBase[Stmt any] struct {
	txn.DoerBase[Options, Beginner]
	stmt Stmt
}

// Stmt returns the statement.
func (do *DoerBase[S]) Stmt() S {
	return do.stmt
}

// SetStmt sets the statement.
func (do *DoerBase[S]) SetStmt(s S) {
	do.stmt = s
}

// IsReadOnly checks if the transaction is read-only.
func (do *DoerBase[_]) IsReadOnly() bool {
	if do.Options() == nil {
		return false
	}
	return do.Options().ReadOnly
}

// SetReadOnly sets the transaction to read-only mode.
func (do *DoerBase[_]) SetReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(300 * time.Millisecond)
	do.SetMaxPing(2)
	do.SetMaxRetry(1)
	do.SetOptions(&sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: true,
	})
}

// SetReadWrite sets the transaction to read-write mode.
func (do *DoerBase[_]) SetReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(500 * time.Millisecond)
	do.SetMaxPing(8)
	do.SetMaxRetry(2)
	do.SetOptions(&sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: false,
	})
}

// Txn wraps a raw sql.Tx transaction.
type Txn struct {
	Raw *sql.Tx
}

// Commit commits the transaction.
func (w *Txn) Commit(context.Context) error {
	if w.Raw == nil {
		return fmt.Errorf("cancelling Commit, Raw is nil")
	}
	return w.Raw.Commit()
}

// Rollback rolls back the transaction.
func (w *Txn) Rollback(context.Context) error {
	if w.Raw == nil {
		return fmt.Errorf("cancelling Rollback, Raw is nil")
	}
	return w.Raw.Rollback()
}

// ExecuteOnce executes an SQL transaction.
func ExecuteOnce[D txn.Doer[Options, Beginner]](
	ctx context.Context, db Beginner, do D, fn txn.DoFunc[Options, Beginner, D]) (D, error) {
	return do, txn.Execute(ctx, db, do, fn)
}

// Ping performs a ping operation.
func Ping(beginner Beginner, limit int, count txn.PingCount) (int, error) {
	return txn.Ping(limit, count, func(ctx context.Context) error {
		return beginner.PingContext(ctx)
	})
}

// BeginTxn begins an SQL transaction.
func BeginTxn(ctx context.Context, db Beginner, opt Options) (*Txn, error) {
	var o *sql.TxOptions
	if opt != nil {
		o = opt
	} else {
		o = &sql.TxOptions{}
	}
	if raw, err := db.BeginTx(ctx, o); err != nil {
		return nil, err
	} else {
		return &Txn{Raw: raw}, nil
	}
}
