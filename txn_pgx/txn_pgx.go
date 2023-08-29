package txn_pgx

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/struqt/txn"
)

type Txn = txn.Txn

type PgxBeginner = *pgxpool.Pool

type PgxOptions = *pgx.TxOptions

type PgxDoerBase struct {
	txn.DoerBase[PgxOptions]
}

func (d *PgxDoerBase) IsReadOnly() bool {
	return strings.Compare(string(pgx.ReadOnly), string(d.Options().AccessMode)) == 0
}

type PgxTx = pgx.Tx

type PgxWrapper struct {
	Raw PgxTx
}

func (w *PgxWrapper) Commit(ctx context.Context) error {
	return w.Raw.Commit(ctx)
}

func (w *PgxWrapper) Rollback(ctx context.Context) error {
	return w.Raw.Rollback(ctx)
}

func (w *PgxWrapper) IsNil() bool {
	return w.Raw == nil
}

func PgxBeginTxn(ctx context.Context, db PgxBeginner, opt PgxOptions) (*PgxWrapper, error) {
	if raw, err := db.BeginTx(ctx, *opt); err != nil {
		return nil, err
	} else {
		return &PgxWrapper{Raw: raw}, nil
	}
}

func PgxExecute[D txn.Doer[Txn, PgxBeginner]](
	ctx context.Context, db PgxBeginner, do D, fn txn.DoFunc[Txn, PgxBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}
