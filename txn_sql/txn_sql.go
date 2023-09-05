package txn_sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	//
	//
	"github.com/struqt/txn"
)

type Txn = txn.Txn
type TxnDoer = txn.Doer[Txn, SqlBeginner]

type SqlBeginner = *sql.DB
type SqlOptions = *sql.TxOptions

type SqlDoer[Stmt any] interface {
	TxnDoer
	Options() SqlOptions
	SetOptions(options SqlOptions)
	Stmt() Stmt
	SetStmt(Stmt)
	ReadOnly(title string)
	ReadWrite(title string)
}

type SqlDoerBase[Stmt any] struct {
	txn.DoerBase[SqlOptions]
	stmt Stmt
}

func (do *SqlDoerBase[Stmt]) IsReadOnly() bool {
	return do.Options().ReadOnly
}

func (do *SqlDoerBase[Stmt]) Stmt() Stmt {
	return do.stmt
}

func (do *SqlDoerBase[Stmt]) SetStmt(s Stmt) {
	do.stmt = s
}

func (do *SqlDoerBase[Stmt]) ReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo.%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(150 * time.Millisecond)
	do.SetOptions(&sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: true,
	})
}

func (do *SqlDoerBase[Stmt]) ReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw.%s", title))
	}
	do.SetRethrowPanic(false)
	do.SetTimeout(200 * time.Millisecond)
	do.SetOptions(&sql.TxOptions{
		//
		Isolation: sql.LevelReadCommitted,
		//
		ReadOnly: false,
	})
}

func (do *SqlDoerBase[Stmt]) BeginTxn(context.Context, SqlBeginner) (Txn, error) {
	panic("implement me")
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

func SqlExecute[D TxnDoer](
	ctx context.Context, db SqlBeginner, do D, fn txn.DoFunc[Txn, SqlBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}
