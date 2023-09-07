package txn_sql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/struqt/txn"
)

type SqlStmt interface {
	comparable
	io.Closer
}

type SqlModule[Stmt SqlStmt] interface {
	io.Closer
	Prepare(ctx context.Context, do SqlDoer[Stmt]) error
	Beginner() SqlBeginner
}

type SqlModuleBase[Stmt SqlStmt] struct {
	beginner   SqlBeginner
	cacheMaker func(context.Context, SqlBeginner) (Stmt, error)
	cache      Stmt
	mu         sync.Mutex
}

func (b *SqlModuleBase[Stmt]) Init(
	beginner SqlBeginner, maker func(context.Context, SqlBeginner) (Stmt, error)) {
	b.beginner = beginner
	b.cacheMaker = maker
}

func (b *SqlModuleBase[Stmt]) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var empty Stmt
	if b.cache != empty {
		defer func() { b.cache = empty }()
		return b.cache.Close()
	}
	return nil
}

func (b *SqlModuleBase[Stmt]) Beginner() SqlBeginner {
	return b.beginner
}

func (b *SqlModuleBase[Stmt]) Prepare(ctx context.Context, do SqlDoer[Stmt]) (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var empty Stmt
	if b.cache == empty {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second*3)
		defer cancel()
		b.cache, err = b.cacheMaker(ctx, b.beginner)
		if err != nil {
			do.SetStmt(empty)
			return
		}
	}
	do.SetStmt(b.cache)
	return
}

func title[Stmt SqlStmt, Doer SqlDoer[Stmt]](do Doer) string {
	if do.Title() != "" {
		return ""
	}
	t := reflect.TypeOf(do)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func SqlRwExecute[Stmt SqlStmt, Doer SqlDoer[Stmt]](
	ctx context.Context, log logr.Logger, module SqlModule[Stmt], do Doer,
	fn txn.DoFunc[SqlOptions, SqlBeginner, Doer],
) (Doer, error) {
	do.SetReadWrite(title[Stmt](do))
	return exec(ctx, log, module, do, fn)
}

func SqlRoExecute[Stmt SqlStmt, Doer SqlDoer[Stmt]](
	ctx context.Context, log logr.Logger, module SqlModule[Stmt], do Doer,
	fn txn.DoFunc[SqlOptions, SqlBeginner, Doer],
) (Doer, error) {
	do.SetReadOnly(title[Stmt](do))
	return exec(ctx, log, module, do, fn)
}

func exec[Stmt SqlStmt, Doer SqlDoer[Stmt]](
	ctx context.Context, log0 logr.Logger, module SqlModule[Stmt], doer Doer,
	fn txn.DoFunc[SqlOptions, SqlBeginner, Doer],
) (Doer, error) {
	log := log0.WithName(doer.Title())
	log.V(2).Info("~", "state", "Preparing")
	var x, err error
	var pings = 0
	var retries = -1
	t0 := time.Now()
retry:
	retries++
	if retries > doer.MaxRetry() && doer.MaxRetry() > 0 {
		if err != nil {
			log.Error(err, "", "retries", retries, "pings", pings)
		}
		return doer, err
	}
	err = module.Prepare(ctx, doer)
	if err != nil {
		pings, x = SqlPing(ctx, module.Beginner(), doer.MaxPing(), func(cnt int, i time.Duration) {
			log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
		})
		if x == nil && pings <= 1 {
			log.Error(err, "", "retries", retries, "pings", pings)
			return doer, err
		}
		log.Info("", "retries", retries, "pings", pings, "err", err)
		goto retry
	}
	t1 := time.Now()
	log.V(2).Info("~", "state", "Prepared", "duration", t1.Sub(t0))
	log.V(1).Info("+")
	if _, err = SqlExecute(ctx, module.Beginner(), doer, fn); err == nil {
		log.V(1).Info("+", "duration", time.Now().Sub(t1))
		return doer, nil
	}
	if x := module.Close(); x != nil {
		log.Error(x, "")
		err = fmt.Errorf("%w [exec] %w [Close]", err, x)
	} else {
		err = fmt.Errorf("%w [exec]", err)
	}
	pings, x = SqlPing(ctx, module.Beginner(), doer.MaxPing(), func(cnt int, i time.Duration) {
		log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
	})
	if x == nil && pings <= 1 {
		log.Error(err, "", "retries", retries, "pings", pings)
		return doer, err
	}
	log.Info("", "retries", retries, "pings", pings, "err", err)
	goto retry
}
