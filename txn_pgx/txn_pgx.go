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

type Txn = txn.Txn
type TxnDoer = txn.Doer[Txn, PgxBeginner]

type PgxBeginner = *pgxpool.Pool
type PgxOptions = *pgx.TxOptions

type PgxDoer[Stmt any] interface {
	TxnDoer
	Options() PgxOptions
	SetOptions(options PgxOptions)
	Stmt() Stmt
	SetStmt(Stmt)
	ReadOnly(title string)
	ReadWrite(title string)
}

type PgxDoerBase[Stmt any] struct {
	txn.DoerBase[PgxOptions]
	stmt Stmt
}

func (do *PgxDoerBase[any]) IsReadOnly() bool {
	return strings.Compare(string(pgx.ReadOnly), string(do.Options().AccessMode)) == 0
}

func (do *PgxDoerBase[Stmt]) Stmt() Stmt {
	return do.stmt
}

func (do *PgxDoerBase[Stmt]) SetStmt(s Stmt) {
	do.stmt = s
}

func (do *PgxDoerBase[Stmt]) ReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo.%s", title))
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

func (do *PgxDoerBase[Stmt]) ReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw.%s", title))
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

func (do *PgxDoerBase[Stmt]) BeginTxn(context.Context, PgxBeginner) (Txn, error) {
	panic("implement me")
}

type PgxTx = pgx.Tx

type PgxTxn struct {
	Raw PgxTx
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

func PgxBeginTxn(ctx context.Context, db PgxBeginner, opt PgxOptions) (*PgxTxn, error) {
	if raw, err := db.BeginTx(ctx, *opt); err != nil {
		return nil, err
	} else {
		return &PgxTxn{Raw: raw}, nil
	}
}

func PgxExecute[D TxnDoer](
	ctx context.Context, db PgxBeginner, do D, fn txn.DoFunc[Txn, PgxBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}
