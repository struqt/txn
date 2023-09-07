package txn_pgx

import (
	"context"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"github.com/struqt/txn"
)

type PgxStmt = any

type PgxModule[Stmt PgxStmt] interface {
	Beginner() PgxBeginner
}

type PgxModuleBase[Stmt PgxStmt] struct {
	beginner PgxBeginner
}

func (b *PgxModuleBase[_]) Init(beginner PgxBeginner) {
	b.beginner = beginner
}

func (b *PgxModuleBase[_]) Beginner() PgxBeginner {
	return b.beginner
}

func title[Stmt PgxStmt, Doer PgxDoer[Stmt]](do Doer) string {
	if do.Title() != "" {
		return ""
	}
	t := reflect.TypeOf(do)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func PgxRwExecute[Stmt PgxStmt, Doer PgxDoer[Stmt]](
	ctx context.Context, log logr.Logger, mod PgxModule[Stmt], doer Doer,
	fn txn.DoFunc[PgxOptions, PgxBeginner, Doer],
) (Doer, error) {
	doer.SetReadWrite(title[Stmt](doer))
	return exec(ctx, log, mod, doer, fn)
}

func PgxRoExecute[Stmt PgxStmt, Doer PgxDoer[Stmt]](
	ctx context.Context, log logr.Logger, mod PgxModule[Stmt], doer Doer,
	fn txn.DoFunc[PgxOptions, PgxBeginner, Doer],
) (Doer, error) {
	doer.SetReadOnly(title[Stmt](doer))
	return exec(ctx, log, mod, doer, fn)
}

func exec[Stmt PgxStmt, Doer PgxDoer[Stmt]](
	ctx context.Context, log0 logr.Logger, mod PgxModule[Stmt], doer Doer,
	fn txn.DoFunc[PgxOptions, PgxBeginner, Doer],
) (Doer, error) {
	log := log0.WithName(doer.Title())
	log.V(1).Info("  +")
	var beginner = mod.Beginner()
	var x, err error
	var pings int
	var retries = -1
	t1 := time.Now()
retry:
	retries++
	if retries > doer.MaxRetry() && doer.MaxRetry() > 0 {
		if err != nil {
			log.Error(err, "", "retries", retries, "pings", pings)
		}
		return doer, err
	}
	if doer, err = PgxExecute(ctx, beginner, doer, fn); err == nil {
		log.V(1).Info("  +", "duration", time.Now().Sub(t1))
		return doer, nil
	}
	pings, x = PgxPing(ctx, beginner, doer.MaxPing(), func(cnt int, i time.Duration) {
		log.Info("PgxPing", "retries", retries, "pings", cnt, "interval", i)
	})
	if x == nil && pings <= 1 {
		log.Error(err, "", "retries", retries, "pings", pings)
		return doer, err
	}
	log.Info("", "retries", retries, "pings", pings, "err", err)
	goto retry
}
