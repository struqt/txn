package txn_sql

import (
	"context"
	"database/sql"

	"github.com/struqt/txn"
)

type Txn = txn.Txn

type SqlBeginner = *sql.DB

type SqlOptions = *sql.TxOptions

type SqlDoer[Stmt any] interface {
	txn.Doer[txn.Txn, SqlBeginner]
	Stmt() Stmt
	SetStmt(Stmt)
}

type SqlDoerBase[Stmt any] struct {
	txn.DoerBase[SqlOptions]
	stmt Stmt
}

func (do *SqlDoerBase[any]) IsReadOnly() bool {
	return do.Options().ReadOnly
}

func (do *SqlDoerBase[any]) Stmt() any {
	return do.stmt
}

func (do *SqlDoerBase[any]) SetStmt(s any) {
	do.stmt = s
}

type SqlTx = *sql.Tx

type SqlTxn struct {
	Raw SqlTx
}

func (w *SqlTxn) Commit(context.Context) error {
	return w.Raw.Commit()
}

func (w *SqlTxn) Rollback(context.Context) error {
	return w.Raw.Rollback()
}

func (w *SqlTxn) IsNil() bool {
	return w.Raw == nil
}

func SqlBeginTxn(ctx context.Context, db SqlBeginner, opt SqlOptions) (*SqlTxn, error) {
	if raw, err := db.BeginTx(ctx, opt); err != nil {
		return nil, err
	} else {
		return &SqlTxn{Raw: raw}, nil
	}
}

func SqlExecute[D txn.Doer[Txn, SqlBeginner]](
	ctx context.Context, db SqlBeginner, do D, fn txn.DoFunc[Txn, SqlBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}
