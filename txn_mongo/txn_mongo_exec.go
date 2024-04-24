package txn_mongo

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/struqt/txn"
)

type ModuleSetter func(*ModuleBase)

type Module interface {
	Beginner() Beginner
}

type ModuleBase struct {
	mutex    sync.Mutex
	beginner Beginner
}

func (b *ModuleBase) Beginner() Beginner {
	return b.beginner
}

func (b *ModuleBase) Mutate(setters ...ModuleSetter) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for _, setter := range setters {
		setter(b)
	}
}

func WithBeginner(value Beginner) ModuleSetter {
	return func(do *ModuleBase) {
		do.beginner = value
	}
}

func Execute[T any, D Doer[T]](
	ctx context.Context, mod Module, do D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	s := append(do.DefaultSetters(title[T](do)), setters...)
	return execute[T](ctx, mod, do, fn, s...)
}

func title[T any, D Doer[T]](do D) string {
	if do.Title() != "" {
		return ""
	}
	t := reflect.TypeOf(do)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func execute[T any, D Doer[T]](
	ctx context.Context, mod Module, doer D,
	fn txn.DoFunc[Options, Beginner, D], setters ...txn.DoerFieldSetter,
) (D, error) {
	doer.Mutate(setters...)
	var logger *slog.Logger
	if v, ok := ctx.Value("logger").(*slog.Logger); ok {
		logger = v
	} else {
		logger = slog.Default()
	}
	log := logger.With("T", doer.Title())
	log.Info("+")
	var x, err error
	var pings int
	var retries = -1
	t1 := time.Now()
retry:
	retries++
	if retries > doer.MaxRetry() && doer.MaxRetry() > 0 {
		if err != nil {
			log.Error(err.Error(), "retries", retries, "pings", pings)
		}
		return doer, err
	}
	if err = ExecuteOnce(ctx, mod.Beginner(), doer, fn); err == nil {
		log.Info("+", "duration", time.Now().Sub(t1))
		return doer, nil
	}
	pings, x = Ping(mod.Beginner(), doer.MaxPing(), func(cnt int, i time.Duration) {
		log.Info("Ping", "retries", retries, "pings", cnt, "interval", i)
	})
	connected := x == nil && pings <= 1
	//if connected && retries > 0 {
	if connected {
		log.Error(err.Error(), "retries", retries, "pings", pings)
		return doer, err
	}
	log.Info("", "retries", retries, "pings", pings, "err", err)
	goto retry
}
