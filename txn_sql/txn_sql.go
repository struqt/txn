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

type SqlBeginner = *sql.DB
type SqlOptions = *sql.TxOptions

type SqlDoer[Stmt any] interface {
	txn.Doer[SqlOptions, SqlBeginner]
	Stmt() Stmt
	SetStmt(Stmt)
}

type SqlDoerBase[Stmt any] struct {
	txn.DoerBase[SqlOptions, SqlBeginner]
	stmt Stmt
}

func (do *SqlDoerBase[S]) Stmt() S {
	return do.stmt
}

func (do *SqlDoerBase[S]) SetStmt(s S) {
	do.stmt = s
}

func (do *SqlDoerBase[_]) IsReadOnly() bool {
	return do.Options().ReadOnly
}

func (do *SqlDoerBase[_]) SetReadOnly(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRo`%s", title))
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

func (do *SqlDoerBase[_]) SetReadWrite(title string) {
	if title != "" {
		do.SetTitle(fmt.Sprintf("TxnRw`%s", title))
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

type SqlTxn struct {
	Raw *sql.Tx
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

func SqlExecute[D txn.Doer[SqlOptions, SqlBeginner]](
	ctx context.Context, db SqlBeginner, do D, fn txn.DoFunc[SqlOptions, SqlBeginner, D]) (D, error) {
	return do, txn.ExecuteTxn(ctx, db, do, fn)
}

func SqlBeginTxn(ctx context.Context, db SqlBeginner, opt SqlOptions) (*SqlTxn, error) {
	if raw, err := db.BeginTx(ctx, opt); err != nil {
		return nil, err
	} else {
		return &SqlTxn{Raw: raw}, nil
	}
}
