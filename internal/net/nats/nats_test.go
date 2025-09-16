package nats_test

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/adoublef/ot/internal/device"
	"github.com/adoublef/ot/internal/net/nats"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"go.adoublef.dev/testing/is"
	"go.adoublef.dev/testing/wait"
	"golang.org/x/sync/errgroup"
)

var testConfig struct {
	msgCount int
	msgLimit int
	pubLimit int
	pubCount int
	subCount int
	dbCount  int
}

func init() {
	flag.IntVar(&testConfig.msgCount, "msg.count", 1, "message count")
	flag.IntVar(&testConfig.msgLimit, "msg.limit", 1, "message limit")
	flag.IntVar(&testConfig.pubCount, "pub.count", 1, "publisher count")
	flag.IntVar(&testConfig.pubLimit, "pub.limit", 1, "publisher limit")
	flag.IntVar(&testConfig.subCount, "sub.count", 1, "subscriber count")
	flag.IntVar(&testConfig.dbCount, "db.count", 1, "database count")
}

func TestConsume(t *testing.T) {
	t.Logf("%s/testConfig%+v", t.Name(), testConfig)

	var (
		p  = newPool(t, testConfig.dbCount)
		db = &device.DB{RWC: p}
		nc = newNATS(t, db, testConfig.subCount)

		firstSeen = time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC)
	)

	ids, sends := send(t.Context(), db, nc, testConfig.pubCount, testConfig.pubLimit, testConfig.msgCount, testConfig.msgLimit, firstSeen)
	polls := poll(t.Context(), ids, db, testConfig.pubLimit, testConfig.msgCount, firstSeen)
	is.OK(t, cmp.Or(sends.Wait(), polls.Wait()))
}

func poll(ctx context.Context, ids <-chan uuid.UUID, db *device.DB, pubLimit, msgCount int, firstSeen time.Time) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(pubLimit)
	for id := range ids {
		g.Go(func() error {
			return wait.ForFunc(ctx, time.Second*60, func() error {
				d, err := db.Device(ctx, id)
				if err != nil {
					return wait.SkipRetry // not found
				}
				if !d.Metadata.LastSeen.Equal(firstSeen.AddDate(0, 0, msgCount)) {
					return fmt.Errorf("device %s not ready", id)
				}
				return nil
			})
		})
	}
	return g
}

func send(ctx context.Context, db *device.DB, nc *nats.Conn, pubCount, pubLimit, msgCount, msgLimit int, firstSeen time.Time) (<-chan uuid.UUID, *errgroup.Group) {
	ch := make(chan uuid.UUID)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(ch)

		g, ctx := errgroup.WithContext(ctx)
		// if one device we still need to send
		//
		// how many should we process at once?
		// with 10 devices do we need 10 routines? maybe not
		g.SetLimit(pubLimit)
		for id, err := range devices(ctx, db, pubCount, firstSeen) {
			// we want to check context before the next stage
			// more verbose, but best we can do when not using jetstream
			g.Go(func() error {
				if err != nil {
					return err
				}
				g, ctx := errgroup.WithContext(ctx)
				g.SetLimit(msgLimit)
				for day := 1; day <= msgCount; day++ {
					g.Go(func() error {
						v := struct {
							ID       uuid.UUID `json:"id"`
							LastSeen time.Time `json:"lastSeen"`
						}{
							ID: id,
							// greater than -n.msg=1000000 and we get a marshalJson error with time
							LastSeen: firstSeen.AddDate(0, 0, day),
						}
						p, err1 := json.Marshal(v)
						err2 := nc.Publish("ping", p)
						return cmp.Or(err1, err2, ctx.Err())
					})
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- id:
				}
				return nil
			})
		}
		return g.Wait()
	})
	return ch, g
}

func devices(ctx context.Context, d *device.DB, pubCount int, firstSeen time.Time) iter.Seq2[uuid.UUID, error] {
	return func(yield func(uuid.UUID, error) bool) {
		for range pubCount {
			id, err := d.AddDevice(ctx, device.Metadata{LastSeen: firstSeen})
			if !yield(id, err) {
				return
			}
		}
	}
}
