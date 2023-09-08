package txn_pgx

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/struqt/txn"
)

type StmtHolder = any

type Module[Stmt StmtHolder] interface {
	Beginner() Beginner
}

type ModuleBase[Stmt StmtHolder] struct {
	mutex    sync.Mutex
	beginner Beginner
}

func (b *ModuleBase[_]) Beginner() Beginner {
	return b.beginner
}

func (b *ModuleBase[_]) Init(beginner Beginner) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.beginner = beginner
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
	ctx context.Context, mod Module[Stmt], do D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	s := append(do.ReadWriteSetters(title[Stmt](do)), setters...)
	return Execute(ctx, mod, do, fn, s...)
}

func ExecuteRo[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, mod Module[Stmt], do D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	s := append(do.ReadOnlySetters(title[Stmt](do)), setters...)
	return Execute(ctx, mod, do, fn, s...)
}

func Execute[Stmt StmtHolder, D Doer[Stmt]](
	ctx context.Context, mod Module[Stmt], doer D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	doer.Mutate(setters...)
	var logger logr.Logger
	if v, ok := ctx.Value("logger").(logr.Logger); ok {
		logger = v
	}
	log := logger.WithName(doer.Title())
	log.V(1).Info("+")
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
	if doer, err = ExecuteOnce(ctx, mod.Beginner(), doer, fn); err == nil {
		log.V(1).Info("+", "duration", time.Now().Sub(t1))
		return doer, nil
	}
	pings, x = Ping(mod.Beginner(), doer.MaxPing(), func(cnt int, i time.Duration) {
		log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
	})
	connected := x == nil && pings <= 1
	if connected && retries > 0 {
		log.Error(err, "", "retries", retries, "pings", pings)
		return doer, err
	}
	log.Info("", "retries", retries, "pings", pings, "err", err)
	goto retry
}
