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

type StmtHolder interface {
	comparable
	io.Closer
}

type Module[Stmt StmtHolder] interface {
	Beginner() Beginner
	Prepare(ctx context.Context, do Doer[Stmt]) error
	io.Closer
}

type ModuleBase[Stmt StmtHolder] struct {
	beginner   Beginner
	cacheMaker func(context.Context, Beginner) (Stmt, error)
	mu         sync.Mutex
	cache      Stmt
}

func (b *ModuleBase[Stmt]) Init(
	beginner Beginner, maker func(context.Context, Beginner) (Stmt, error)) {
	b.beginner = beginner
	b.cacheMaker = maker
}

func (b *ModuleBase[Stmt]) Beginner() Beginner {
	return b.beginner
}

func (b *ModuleBase[Stmt]) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var empty Stmt
	if b.cache != empty {
		defer func() { b.cache = empty }()
		return b.cache.Close()
	}
	return nil
}

func (b *ModuleBase[Stmt]) Prepare(ctx context.Context, do Doer[Stmt]) (err error) {
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

func title[Stmt StmtHolder, D Doer[Stmt]](do D) string {
	if do.Title() != "" {
		return ""
	}
	t := reflect.TypeOf(do)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func ExecuteRw[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, log logr.Logger, module Module[Stmt], do D,
	fn txn.DoFunc[Options, Beginner, D],
) (D, error) {
	do.SetReadWrite(title[Stmt](do))
	return Execute(ctx, log, module, do, fn)
}

func ExecuteRo[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, log logr.Logger, module Module[Stmt], do D,
	fn txn.DoFunc[Options, Beginner, D],
) (D, error) {
	do.SetReadOnly(title[Stmt](do))
	return Execute(ctx, log, module, do, fn)
}

func Execute[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, log0 logr.Logger, mod Module[Stmt], doer D,
	fn txn.DoFunc[Options, Beginner, D],
) (D, error) {
	log := log0.WithName(doer.Title())
	log.V(2).Info("~", "state", "Preparing")
	var beginner = mod.Beginner()
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
	err = mod.Prepare(ctx, doer)
	if err != nil {
		pings, x = Ping(beginner, doer.MaxPing(), func(cnt int, i time.Duration) {
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
	if _, err = ExecuteOnce(ctx, beginner, doer, fn); err == nil {
		log.V(1).Info("+", "duration", time.Now().Sub(t1))
		return doer, nil
	}
	if x = mod.Close(); x != nil {
		log.Error(x, "")
		err = fmt.Errorf("%w [exec] %w [Close]", err, x)
	} else {
		err = fmt.Errorf("%w [exec]", err)
	}
	pings, x = Ping(beginner, doer.MaxPing(), func(cnt int, i time.Duration) {
		log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
	})
	if x == nil && pings <= 1 {
		log.Error(err, "", "retries", retries, "pings", pings)
		return doer, err
	}
	log.Info("", "retries", retries, "pings", pings, "err", err)
	goto retry
}
