package txn_sql

import (
	"context"
	"database/sql"

	"github.com/struqt/txn"
)

type SqlBeginner = *sql.DB

func SqlExecute[D txn.Doer[txn.Txn, SqlBeginner]](
	ctx context.Context, db SqlBeginner, do D, fn txn.DoFunc[txn.Txn, SqlBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}

type SqlOptions = *sql.TxOptions

type SqlDoerBase struct {
	txn.DoerBase[SqlOptions]
}

func (d *SqlDoerBase) IsReadOnly() bool {
	return d.Options().ReadOnly
}

type SqlTx = *sql.Tx

type SqlWrapper struct {
	Raw SqlTx
}

func (w *SqlWrapper) Commit(context.Context) error {
	return w.Raw.Commit()
}

func (w *SqlWrapper) Rollback(context.Context) error {
	return w.Raw.Rollback()
}
