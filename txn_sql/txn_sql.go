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

// SqlBeginner is an alias for *sql.DB.
type SqlBeginner = *sql.DB

// SqlOptions is an alias for *sql.TxOptions.
type SqlOptions = *sql.TxOptions

// SqlDoer defines the interface for SQL transaction operations.
type SqlDoer[Stmt any] interface {
	txn.Doer[SqlOptions, SqlBeginner]
	Stmt() Stmt
	SetStmt(Stmt)
}

// SqlDoerBase provides a base implementation for the SqlDoer interface.
type SqlDoerBase[Stmt any] struct {
	txn.DoerBase[SqlOptions, SqlBeginner]
	stmt Stmt
}

// Stmt returns the statement.
func (do *SqlDoerBase[S]) Stmt() S {
	return do.stmt
}

// SetStmt sets the statement.
func (do *SqlDoerBase[S]) SetStmt(s S) {
	do.stmt = s
}

// IsReadOnly checks if the transaction is read-only.
func (do *SqlDoerBase[_]) IsReadOnly() bool {
	if do.Options() == nil {
		return false
	}
	return do.Options().ReadOnly
}

// SetReadOnly sets the transaction to read-only mode.
func (do *SqlDoerBase[_]) SetReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(150 * time.Millisecond)
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
func (do *SqlDoerBase[_]) SetReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(200 * time.Millisecond)
	do.SetMaxPing(8)
	do.SetMaxRetry(2)
	do.SetOptions(&sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: false,
	})
}

// SqlTxn wraps a raw sql.Tx transaction.
type SqlTxn struct {
	Raw *sql.Tx
}

// Commit commits the transaction.
func (w *SqlTxn) Commit(context.Context) error {
	if w.Raw == nil {
		return fmt.Errorf("cancelling Commit, Raw is nil")
	}
	return w.Raw.Commit()
}

// Rollback rolls back the transaction.
func (w *SqlTxn) Rollback(context.Context) error {
	if w.Raw == nil {
		return fmt.Errorf("cancelling Rollback, Raw is nil")
	}
	return w.Raw.Rollback()
}

// SqlExecute executes an SQL transaction.
func SqlExecute[D txn.Doer[SqlOptions, SqlBeginner]](
	ctx context.Context, db SqlBeginner, do D, fn txn.DoFunc[SqlOptions, SqlBeginner, D]) (D, error) {
	return do, txn.Execute(ctx, db, do, fn)
}

// SqlPing performs a ping operation.
func SqlPing[T any](
	ctx context.Context, beginner SqlBeginner, doer SqlDoer[T], sleep func(time.Duration, int)) (int, error) {
	return txn.Ping[SqlOptions, SqlBeginner](ctx, doer, sleep, func(ctx context.Context) error {
		return beginner.PingContext(ctx)
	})
}

// SqlBeginTxn begins an SQL transaction.
func SqlBeginTxn(ctx context.Context, db SqlBeginner, opt SqlOptions) (*SqlTxn, error) {
	var o *sql.TxOptions
	if opt != nil {
		o = opt
	} else {
		o = &sql.TxOptions{}
	}
	if raw, err := db.BeginTx(ctx, o); err != nil {
		return nil, err
	} else {
		return &SqlTxn{Raw: raw}, nil
	}
}
