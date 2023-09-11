package txn_mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/struqt/txn"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MgoOptions struct {
	session     []*options.SessionOptions
	transaction []*options.TransactionOptions
}

// Beginner is an alias for *pgxpool.Pool.
type Beginner = *mongo.Client

// Options is an alias for *pgx.TxOptions.
type Options = *MgoOptions

// Doer defines the interface for PGX transaction operations.
type Doer[T any] interface {
	txn.Doer[Options, Beginner]
	DefaultSetters(title string) []txn.DoerFieldSetter
	Client() T
	SetClient(T)
}

// DoerBase provides a base implementation for the Doer interface.
type DoerBase[T any] struct {
	txn.DoerBase[Options, Beginner]
	client T
}

// Client returns the client.
func (do *DoerBase[T]) Client() T {
	return do.client
}

// SetClient sets the client.
func (do *DoerBase[T]) SetClient(value T) {
	do.client = value
}

func (do *DoerBase[_]) DefaultSetters(title string) []txn.DoerFieldSetter {
	opts := &MgoOptions{
		session:     []*options.SessionOptions{},
		transaction: []*options.TransactionOptions{},
	}
	return []txn.DoerFieldSetter{
		txn.WithTitle(fmt.Sprintf("Txn`%s", title)),
		txn.WithRethrow(false),
		txn.WithTimeout(5 * time.Second),
		txn.WithMaxPing(4),
		txn.WithMaxRetry(2),
		txn.WithOptions(opts),
	}
}

// Txn wraps a raw pgx.Tx transaction.
type Txn struct {
	Raw mongo.Session
	opt []*options.TransactionOptions
}

// Commit commits the transaction.
func (w *Txn) Commit(ctx context.Context) error {
	if w.Raw == nil {
		return errors.New("cancelling Commit, Raw is nil")
	}
	defer w.Raw.EndSession(context.Background())
	return w.Raw.CommitTransaction(ctx)
}

// Rollback rolls back the transaction.
func (w *Txn) Rollback(ctx context.Context) error {
	session := w.Raw
	if session == nil {
		return errors.New("cancelling Rollback, Raw is nil")
	}
	defer session.EndSession(context.Background())
	return session.AbortTransaction(ctx)
}

// ExecuteOnce executes a pgx transaction.
func ExecuteOnce[D txn.Doer[Options, Beginner]](
	ctx context.Context, beginner Beginner, do D, fn txn.DoFunc[Options, Beginner, D]) (D, error) {
	o := do.Options()
	var err error
	var session mongo.Session
	if o == nil {
		session, err = beginner.StartSession()
	} else {
		session, err = beginner.StartSession(o.session...)
	}
	if err != nil {
		return do, err
	}
	defer session.EndSession(context.Background())
	if do.Timeout() > time.Millisecond {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, do.Timeout())
		defer cancel()
	}
	c1 := mongo.NewSessionContext(ctx, session)
	return do, txn.Execute(c1, beginner, do, fn)
}

// Ping performs a ping operation.
func Ping(beginner Beginner, limit int, count txn.PingCount) (int, error) {
	return txn.Ping(limit, count, func(ctx context.Context) error {
		return beginner.Ping(ctx, readpref.Primary())
	})
}

// BeginTxn begins a pgx transaction.
func BeginTxn(ctx context.Context, _ Beginner, opt Options) (*Txn, error) {
	session, ok := ctx.(mongo.SessionContext)
	if !ok {
		return nil, errors.New("no mongodb_session on current context")
	}
	var err error
	if opt == nil {
		err = session.StartTransaction()
	} else {
		err = session.StartTransaction(opt.transaction...)
	}
	if err != nil {
		return nil, err
	}
	return &Txn{Raw: session}, nil
}
