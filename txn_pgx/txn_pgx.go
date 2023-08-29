package txn_pgx

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/struqt/txn"
)

type PgxBeginner = *pgxpool.Pool

func PgxExecute[D txn.Doer[txn.Txn, PgxBeginner]](
	ctx context.Context, db PgxBeginner, do D, fn txn.DoFunc[txn.Txn, PgxBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}

type PgxOptions = *pgx.TxOptions

type PgxDoerBase struct {
	txn.DoerBase[PgxOptions]
}

func (d *PgxDoerBase) IsReadOnly() bool {
	return strings.Compare(string(pgx.ReadOnly), string(d.Options().AccessMode)) == 0
}
