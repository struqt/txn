package txn_sql

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"time"

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

func (b *ModuleBase[Stmt]) Stmt() Stmt {
	if b.cache != nil {
		return b.cache
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cache
}

func (b *ModuleBase[Stmt]) Init(
	beginner Beginner, maker func(context.Context, Beginner) (Stmt, error)) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
	ctx context.Context, module Module[Stmt], do D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	s := append(do.ReadWriteSetters(title[Stmt](do)), setters...)
	return Execute(ctx, module, do, fn, s...)
}

func ExecuteRo[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, module Module[Stmt], do D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	s := append(do.ReadOnlySetters(title[Stmt](do)), setters...)
	return Execute(ctx, module, do, fn, s...)
}

func Execute[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, mod Module[Stmt], doer D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	doer.Mutate(setters...)
	var logger *slog.Logger
	if v, ok := ctx.Value("logger").(*slog.Logger); ok {
		logger = v
	}
	log := logger.With("T", doer.Title())
	log.Debug("~", "state", "Preparing")
	var x, err error
	var pings = 0
	var retries = -1
	t0 := time.Now()
retry:
	retries++
	if retries > doer.MaxRetry() && doer.MaxRetry() > 0 {
		if err != nil {
			log.Error(err.Error(), "retries", retries, "pings", pings)
		}
		return doer, err
	}
	err = mod.Prepare(ctx, doer)
	if err != nil {
		pings, x = Ping(mod.Beginner(), doer.MaxPing(), func(cnt int, i time.Duration) {
			log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
		})
		connected := x == nil && pings <= 1
		if connected && retries > 0 {
			log.Error(err.Error(), "retries", retries, "pings", pings)
			return doer, err
		}
		log.Info("", "retries", retries, "pings", pings, "err", err)
		goto retry
	}
	t1 := time.Now()
	log.Debug("~", "state", "Prepared", "duration", t1.Sub(t0))
	log.Info("+")
	if _, err = ExecuteOnce(ctx, mod.Beginner(), doer, fn); err == nil {
		log.Info("+", "duration", time.Now().Sub(t1))
		return doer, nil
	}
	if x = mod.Close(); x != nil {
		log.Error(x.Error())
		err = fmt.Errorf("%w [exec] %w [Close]", err, x)
	} else {
		err = fmt.Errorf("%w [exec]", err)
	}
	pings, x = Ping(mod.Beginner(), doer.MaxPing(), func(cnt int, i time.Duration) {
		log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
	})
	connected := x == nil && pings <= 1
	if connected && retries > 0 {
		log.Error(err.Error(), "retries", retries, "pings", pings)
		return doer, err
	}
	log.Info("", "retries", retries, "pings", pings, "err", err)
	goto retry
}
