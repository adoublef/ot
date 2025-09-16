package nats_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	migrate "github.com/adoublef/ot/internal/database/postgres"
	"github.com/adoublef/ot/internal/device"
	"github.com/adoublef/ot/internal/net/nats"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats-server/v2/server"
	servertest "github.com/nats-io/nats-server/v2/test"
	"github.com/testcontainers/testcontainers-go"
	"go.adoublef.dev/runtime/container/postgres"
	"go.adoublef.dev/testing/is"
	"golang.org/x/sync/errgroup"
)

func newNATS(t testing.TB, db *device.DB, subCount int) *nats.Conn {
	t.Helper()

	ns := servertest.RunServer(&server.Options{Debug: testing.Verbose()})
	t.Cleanup(func() { ns.Shutdown() })

	serverAddr := ns.ClientURL()

	nc, err := nats.Connect(serverAddr)
	is.OK(t, err) // Connect
	t.Cleanup(func() { is.OK(t, nc.Drain()) /* Drain */ })

	is.OK(t, nats.Handler(nc, db, subCount)) // Handler

	return nc
}

func newPool(t testing.TB, maxConns int) *sqlx.DB {
	t.Helper()
	ctx := t.Context()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	is.OK(t, err) // postgresContainer.ConnectionString

	err = migrate.Up(ctx, dsn)
	is.OK(t, err)
	t.Cleanup(func() { is.OK(t, migrate.Down(context.Background(), dsn)) })

	db, err := sql.Open("postgres", dsn)
	is.OK(t, err) // sql.Open
	if maxConns > 0 {
		// avoid making and closing lots of connections, set the maximum idle size
		// db.SetMaxIdleConns(maxConns)
		// If n <= 0, then there is no limit on the number of open connections.
		db.SetMaxOpenConns(maxConns)
	}
	t.Logf("%d = db.Stats().MaxOpenConnections", db.Stats().MaxOpenConnections)
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
