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

// ResetAsReadOnly sets the transaction to read-only mode.
func (do *DoerBase[_]) ResetAsReadOnly(title string) error {
	if title != "" {
		title = fmt.Sprintf("TxnRo`%s", title)
	} else {
		title = do.Title()
	}
	options := &sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: true,
	}
	return do.Reset(txn.NewDoerFields(
		txn.WithRethrow(false),
		txn.WithTitle(title),
		txn.WithMaxPing(2),
		txn.WithMaxRetry(1),
		txn.WithTimeout(300*time.Millisecond),
		txn.WithOptions(options),
	))
}

// ResetAsReadWrite sets the transaction to read-write mode.
func (do *DoerBase[_]) ResetAsReadWrite(title string) error {
	if title != "" {
		title = fmt.Sprintf("TxnRw`%s", title)
	} else {
		title = do.Title()
	}
	options := &sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: false,
	}
	return do.Reset(txn.NewDoerFields(
		txn.WithRethrow(false),
		txn.WithTitle(title),
		txn.WithMaxPing(8),
		txn.WithMaxRetry(2),
		txn.WithTimeout(500*time.Millisecond),
		txn.WithOptions(options),
	))
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
	var clone sql.TxOptions
	if opt != nil {
		clone = sql.TxOptions{Isolation: opt.Isolation, ReadOnly: opt.ReadOnly}
	} else {
		clone = sql.TxOptions{}
	}
	if raw, err := db.BeginTx(ctx, &clone); err != nil {
		return nil, err
	} else {
		return &Txn{Raw: raw}, nil
	}
}
