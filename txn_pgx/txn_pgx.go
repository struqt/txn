package txn_pgx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/struqt/txn"
)

// PgxBeginner is an alias for *pgxpool.Pool.
type PgxBeginner = *pgxpool.Pool

// PgxOptions is an alias for *pgx.TxOptions.
type PgxOptions = *pgx.TxOptions

// PgxDoer defines the interface for PGX transaction operations.
type PgxDoer[Stmt any] interface {
	txn.Doer[PgxOptions, PgxBeginner]
	Stmt() Stmt
	SetStmt(Stmt)
}

// PgxDoerBase provides a base implementation for the PgxDoer interface.
type PgxDoerBase[Stmt any] struct {
	txn.DoerBase[PgxOptions, PgxBeginner]
	stmt Stmt
}

// Stmt returns the statement.
func (do *PgxDoerBase[S]) Stmt() S {
	return do.stmt
}

// SetStmt sets the statement.
func (do *PgxDoerBase[S]) SetStmt(s S) {
	do.stmt = s
}

// IsReadOnly checks if the transaction is read-only.
func (do *PgxDoerBase[_]) IsReadOnly() bool {
	if do.Options() == nil {
		return false
	}
	return strings.Compare(string(pgx.ReadOnly), string(do.Options().AccessMode)) == 0
}

// SetReadOnly sets the transaction to read-only mode.
func (do *PgxDoerBase[_]) SetReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(150 * time.Millisecond)
	do.SetMaxPing(2)
	do.SetMaxRetry(1)
	do.SetOptions(&pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
	})
}

// SetReadWrite sets the transaction to read-write mode.
func (do *PgxDoerBase[_]) SetReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(200 * time.Millisecond)
	do.SetMaxPing(8)
	do.SetMaxRetry(2)
	do.SetOptions(&pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
	})
}

// PgxTxn wraps a raw pgx.Tx transaction.
type PgxTxn struct {
	Raw pgx.Tx
}

// Commit commits the transaction.
func (w *PgxTxn) Commit(ctx context.Context) error {
	if w.Raw == nil {
		return fmt.Errorf("cancelling Commit, Raw is nil")
	}
	return w.Raw.Commit(ctx)
}

// Rollback rolls back the transaction.
func (w *PgxTxn) Rollback(ctx context.Context) error {
	if w.Raw == nil {
		return fmt.Errorf("cancelling Rollback, Raw is nil")
	}
	return w.Raw.Rollback(ctx)
}

// PgxExecute executes a pgx transaction.
func PgxExecute[D txn.Doer[PgxOptions, PgxBeginner]](
	ctx context.Context, db PgxBeginner, do D, fn txn.DoFunc[PgxOptions, PgxBeginner, D]) (D, error) {
	return do, txn.Execute(ctx, db, do, fn)
}

// PgxPing performs a ping operation.
func PgxPing[T any](
	ctx context.Context, beginner PgxBeginner, doer PgxDoer[T], sleep func(time.Duration, int)) (int, error) {
	return txn.Ping[PgxOptions, PgxBeginner](ctx, doer, sleep, func(ctx context.Context) error {
		return beginner.Ping(ctx)
	})
}

// PgxBeginTxn begins a pgx transaction.
func PgxBeginTxn(ctx context.Context, db PgxBeginner, opt PgxOptions) (*PgxTxn, error) {
	var o pgx.TxOptions
	if opt != nil {
		o = *opt
	} else {
		o = pgx.TxOptions{}
	}
	if raw, err := db.BeginTx(ctx, o); err != nil {
		return nil, err
	} else {
		return &PgxTxn{Raw: raw}, nil
	}
}
