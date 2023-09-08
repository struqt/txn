package txn_pgx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/struqt/txn"
)

// Beginner is an alias for *pgxpool.Pool.
type Beginner = *pgxpool.Pool

// Options is an alias for *pgx.TxOptions.
type Options = *pgx.TxOptions

// Doer defines the interface for PGX transaction operations.
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
	return strings.Compare(string(pgx.ReadOnly), string(do.Options().AccessMode)) == 0
}

// ResetAsReadOnly sets the transaction to read-only mode.
func (do *DoerBase[_]) ResetAsReadOnly(title string) error {
	options := &pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
	}
	fields := txn.NewDoerFields(
		txn.WithTitle(fmt.Sprintf("TxnRo`%s", title)),
		txn.WithRethrow(false),
		txn.WithTimeout(2*time.Second),
		txn.WithMaxPing(2),
		txn.WithMaxRetry(1),
		txn.WithOptions(options),
	)
	return do.Reset(fields)
}

// ResetAsReadWrite sets the transaction to read-write mode.
func (do *DoerBase[_]) ResetAsReadWrite(title string) error {
	options := &pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
	}
	fields := txn.NewDoerFields(
		txn.WithTitle(fmt.Sprintf("TxnRw`%s", title)),
		txn.WithRethrow(false),
		txn.WithTimeout(5*time.Second),
		txn.WithMaxPing(8),
		txn.WithMaxRetry(2),
		txn.WithOptions(options),
	)
	return do.Reset(fields)
}

// Txn wraps a raw pgx.Tx transaction.
type Txn struct {
	Raw pgx.Tx
}

// Commit commits the transaction.
func (w *Txn) Commit(ctx context.Context) error {
	if w.Raw == nil {
		return errors.New("cancelling Commit, Raw is nil")
	}
	return w.Raw.Commit(ctx)
}

// Rollback rolls back the transaction.
func (w *Txn) Rollback(ctx context.Context) error {
	if w.Raw == nil {
		return errors.New("cancelling Rollback, Raw is nil")
	}
	return w.Raw.Rollback(ctx)
}

// ExecuteOnce executes a pgx transaction.
func ExecuteOnce[D txn.Doer[Options, Beginner]](
	ctx context.Context, beginner Beginner, do D, fn txn.DoFunc[Options, Beginner, D]) (D, error) {
	return do, txn.Execute(ctx, beginner, do, fn)
}

// Ping performs a ping operation.
func Ping(beginner Beginner, limit int, count txn.PingCount) (int, error) {
	return txn.Ping(limit, count, func(ctx context.Context) error {
		return beginner.Ping(ctx)
	})
}

// BeginTxn begins a pgx transaction.
func BeginTxn(ctx context.Context, beginner Beginner, opt Options) (*Txn, error) {
	var clone pgx.TxOptions
	if opt != nil {
		clone = *opt
	} else {
		clone = pgx.TxOptions{}
	}
	if raw, err := beginner.BeginTx(ctx, clone); err != nil {
		return nil, err
	} else {
		return &Txn{Raw: raw}, nil
	}
}
