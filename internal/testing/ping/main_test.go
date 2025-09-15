package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"iter"
	"os"
	"testing"
	"time"

	migrate "github.com/adoublef/ot/internal/database/postgres"
	"github.com/adoublef/ot/internal/device"
	"github.com/adoublef/ot/internal/net/nats"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats-server/v2/server"
	servertest "github.com/nats-io/nats-server/v2/test"
	"github.com/testcontainers/testcontainers-go"
	"go.adoublef.dev/runtime/container/postgres"
	"go.adoublef.dev/testing/is"
	"go.adoublef.dev/testing/wait"
	"golang.org/x/sync/errgroup"
)

var dbConns int
var blobSize int
var numOfMsgs int
var numOfDevs int
var firstSeen time.Time = time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC)

func init() {
	flag.IntVar(&dbConns, "n.db", 1, "db connection pool")
	flag.IntVar(&blobSize, "n.blob", 8, "blob size")
	flag.IntVar(&numOfMsgs, "n.msg", 1, "messages per device")
	flag.IntVar(&numOfDevs, "n.dev", 1, "concurrent devices")
}

func Test(t *testing.T) {
	if testing.Verbose() {
		t.Logf("%s/FLAGS dbConns=%d blobSize=%d numOfMsgs=%d numOfDevs=%d", t.Name(), dbConns, blobSize, numOfMsgs, numOfDevs)
	}

	db := &device.DB{RWC: newPool(t)}
	nc := newNATS(t, db)

	// metrics := make(chan any, 1)

	ids, g := ping(t.Context(), db, nc) // non-blocking
	err := poll(t.Context(), ids, db)   // blocking
	is.OK(t, cmp.Or(g.Wait(), err))
}

func poll(ctx context.Context, ids <-chan uuid.UUID, db *device.DB) error {
	g, ctx := errgroup.WithContext(ctx)
	// one at a time for now
	// can expand when i am happy
	g.SetLimit(max(1, numOfDevs/4))
	for id := range ids {
		g.Go(func() error {
			err := wait.ForFunc(ctx, time.Second*60, func() error {
				d, err := db.Device(ctx, id)
				if err != nil {
					return wait.SkipRetry // not found
				}
				if !d.Meta.LastSeen.Equal(firstSeen.AddDate(0, 0, numOfMsgs)) {
					return fmt.Errorf("device %s not ready", id)
				}
				return nil
			})
			if err := cmp.Or(err, ctx.Err()); err != nil {
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func ping(ctx context.Context, db *device.DB, nc *nats.Conn) (<-chan uuid.UUID, *errgroup.Group) {
	ch := make(chan uuid.UUID)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer func() { close(ch) }()

		g, ctx := errgroup.WithContext(ctx)
		// if one device we still need to send
		//
		// how many should we process at once?
		// with 10 devices do we need 10 routines? maybe not
		g.SetLimit(max(1, numOfDevs/2))
		for id, err := range devices(ctx, db) {
			// we want to check context before the next stage
			// more verbose, but best we can do when not using jetstream
			g.Go(func() error {
				if err := cmp.Or(err, ctx.Err()); err != nil {
					return err
				}
				for day := 1; day <= numOfMsgs; day++ {
					v := struct {
						ID       uuid.UUID `json:"id"`
						LastSeen time.Time `json:"lastSeen"`
					}{
						ID: id,
						// greater than -n.msg=1000000 and we get a marshalJson error with time
						LastSeen: firstSeen.AddDate(0, 0, day),
					}
					p, err := json.Marshal(v)
					if err := cmp.Or(err, ctx.Err()); err != nil {
						return err
					}
					if err := nc.Publish("ping", p); err != nil {
						return fmt.Errorf("failed to send message: %w", err)
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- id:
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		return nil
	})
	return ch, g
}

func devices(ctx context.Context, d *device.DB) iter.Seq2[uuid.UUID, error] {
	return func(yield func(uuid.UUID, error) bool) {
		blob, err1 := newBlobN(blobSize * 1024)
		for range numOfDevs {
			id, err2 := d.AddDevice(ctx, device.Metadata{LastSeen: firstSeen, Blob: blob})
			if !yield(id, cmp.Or(err1, err2)) {
				return
			}
		}
	}
}

func newBlobN(size int) ([]byte, error) {
	buf := make([]byte, size)

	n, err := rand.Read(buf)
	if err != nil {
		return nil, err
	}

	encodedLen := base64.StdEncoding.EncodedLen(len(buf))
	b64 := make([]byte, encodedLen)
	base64.StdEncoding.Encode(b64, buf[:n])

	return b64, nil
}

func newNATS(t testing.TB, db *device.DB) *nats.Conn {
	t.Helper()

	ns := servertest.RunServer(&server.Options{Debug: testing.Verbose()})
	t.Cleanup(func() { ns.Shutdown() })

	serverAddr := ns.ClientURL()

	nc, err := nats.Connect(serverAddr)
	is.OK(t, err) // Connect
	t.Cleanup(func() { is.OK(t, nc.Drain()) /* Drain */ })

	is.OK(t, nats.Handler(nc, db)) // Handler

	return nc
}

func newPool(t testing.TB) *sqlx.DB {
	t.Helper()
	ctx := t.Context()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	is.OK(t, err) // postgresContainer.ConnectionString

	err = migrate.Up(ctx, dsn)
	is.OK(t, err)
	t.Cleanup(func() { is.OK(t, migrate.Down(context.Background(), dsn)) })

	db, err := sql.Open("postgres", dsn)
	is.OK(t, err) // sql.Open
	if dbConns > 0 {
		// avoid making and closing lots of connections, set the maximum idle size
		// db.SetMaxIdleConns(maxConns)
		// If n <= 0, then there is no limit on the number of open connections.
		db.SetMaxOpenConns(dbConns)
	}
	return sqlx.NewDb(db, "postgres")
}

func TestMain(m *testing.M) {
	err := setup(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	err = cleanup(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

var postgresContainer *postgres.Container

// setup initialises containers within the pacakge.
func setup(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		postgresContainer, err = postgres.Run(ctx, "")
		return
	})
	return g.Wait()
}

// cleanup stops all running containers for the pacakge.
func cleanup(ctx context.Context) (err error) {
	g := new(errgroup.Group)
	var cc = []testcontainers.Container{postgresContainer}
	for _, c := range cc {
		// if c != nil {
		g.Go(func() error { return c.Terminate(ctx) })
		// }
	}
	return g.Wait()
}
