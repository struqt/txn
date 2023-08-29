package txn_sql

import (
	"context"
	"database/sql"

	"github.com/struqt/txn"
)

type Txn = txn.Txn

type SqlBeginner = *sql.DB

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

func (w *SqlWrapper) IsNil() bool {
	return w.Raw == nil
}

func SqlBeginTxn(ctx context.Context, db SqlBeginner, opt SqlOptions) (*SqlWrapper, error) {
	if raw, err := db.BeginTx(ctx, opt); err != nil {
		return nil, err
	} else {
		return &SqlWrapper{Raw: raw}, nil
	}
}

func SqlExecute[D txn.Doer[Txn, SqlBeginner]](
	ctx context.Context, db SqlBeginner, do D, fn txn.DoFunc[Txn, SqlBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}
