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

type PgxBeginner = *pgxpool.Pool
type PgxOptions = *pgx.TxOptions

type PgxDoer[Stmt any] interface {
	txn.Doer[PgxOptions, PgxBeginner]
	Stmt() Stmt
	SetStmt(Stmt)
}

type PgxDoerBase[Stmt any] struct {
	txn.DoerBase[PgxOptions, PgxBeginner]
	stmt Stmt
}

func (do *PgxDoerBase[S]) Stmt() S {
	return do.stmt
}

func (do *PgxDoerBase[S]) SetStmt(s S) {
	do.stmt = s
}

func (do *PgxDoerBase[_]) IsReadOnly() bool {
	return strings.Compare(string(pgx.ReadOnly), string(do.Options().AccessMode)) == 0
}

func (do *PgxDoerBase[_]) SetReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(150 * time.Millisecond)
	do.SetOptions(&pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
	})
}

func (do *PgxDoerBase[_]) SetReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw`%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(200 * time.Millisecond)
	do.SetOptions(&pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
		BeginQuery:     "",
	})
}

type PgxTxn struct {
	Raw pgx.Tx
}

func (w *PgxTxn) Commit(ctx context.Context) error {
	return w.Raw.Commit(ctx)
}

func (w *PgxTxn) Rollback(ctx context.Context) error {
	return w.Raw.Rollback(ctx)
}

func (w *PgxTxn) IsNil() bool {
	return w.Raw == nil
}

func PgxExecute[D txn.Doer[PgxOptions, PgxBeginner]](
	ctx context.Context, db PgxBeginner, do D, fn txn.DoFunc[PgxOptions, PgxBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}

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
