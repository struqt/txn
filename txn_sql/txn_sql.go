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
	ReadOnlySetters(title string) []txn.DoerFieldSetter
	ReadWriteSetters(title string) []txn.DoerFieldSetter
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

func (do *DoerBase[_]) ReadOnlySetters(title string) []txn.DoerFieldSetter {
	options := &sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: true,
	}
	return []txn.DoerFieldSetter{
		txn.WithTitle(fmt.Sprintf("TxnRo`%s", title)),
		txn.WithRethrow(false),
		txn.WithTimeout(2 * time.Second),
		txn.WithMaxPing(2),
		txn.WithMaxRetry(1),
		txn.WithOptions(options),
	}
}

func (do *DoerBase[_]) ReadWriteSetters(title string) []txn.DoerFieldSetter {
	options := &sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: false,
	}
	return []txn.DoerFieldSetter{
		txn.WithTitle(fmt.Sprintf("TxnRw`%s", title)),
		txn.WithRethrow(false),
		txn.WithTimeout(5 * time.Second),
		txn.WithMaxPing(8),
		txn.WithMaxRetry(2),
		txn.WithOptions(options),
	}
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
	if do.Timeout() > time.Millisecond {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, do.Timeout())
		defer cancel()
	}
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
