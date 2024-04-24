package txn_pgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/struqt/txn"
)

type (
	RawTx    = pgx.Tx
	Beginner = *pgxpool.Pool
	Options  = *pgx.TxOptions
)

type RawTxn interface {
	txn.Txn
	Raw() RawTx
}

// Doer defines the interface for PGX transaction operations.
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
	options := &pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
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
	options := &pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
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

// rawTx wraps a raw pgx.Tx transaction.
type rawTx struct {
	raw RawTx
}

func (w *rawTx) Raw() RawTx {
	return w.raw
}

// Commit commits the transaction.
func (w *rawTx) Commit(ctx context.Context) error {
	if w.raw == nil {
		return errors.New("cancelling Commit, Raw is nil")
	}
	return w.raw.Commit(ctx)
}

// Rollback rolls back the transaction.
func (w *rawTx) Rollback(ctx context.Context) error {
	if w.raw == nil {
		return errors.New("cancelling Rollback, Raw is nil")
	}
	return w.raw.Rollback(ctx)
}

// ExecuteOnce executes a pgx transaction.
func ExecuteOnce[
	D txn.Doer[Options, Beginner],
](ctx context.Context, beginner Beginner, do D, fn txn.DoFunc[Options, Beginner, D]) error {
	if do.Timeout() > time.Millisecond {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, do.Timeout())
		defer cancel()
	}
	return txn.Execute(ctx, beginner, do, fn)
}

// Ping performs a ping operation.
func Ping(beginner Beginner, limit int, count txn.PingCount) (int, error) {
	return txn.Ping(limit, count, func(ctx context.Context) error {
		return beginner.Ping(ctx)
	})
}

// BeginTxn begins a pgx transaction.
func BeginTxn(ctx context.Context, beginner Beginner, opt Options) (RawTxn, error) {
	var clone pgx.TxOptions
	if opt != nil {
		clone = *opt
	} else {
		clone = pgx.TxOptions{}
	}
	if tx, err := beginner.BeginTx(ctx, clone); err != nil {
		return nil, err
	} else {
		return &rawTx{raw: tx}, nil
	}
}
