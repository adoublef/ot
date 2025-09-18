package nats_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"iter"
	"sync"
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

var tc struct {
	msgCount   int
	msgLimit   int
	pubLimit   int
	pubCount   int
	subCount   int
	subTimeout time.Duration
	dbCount    int
	blobSize   int
}

func init() {
	flag.IntVar(&tc.msgCount, "msg.count", 1, "message count")
	flag.IntVar(&tc.msgLimit, "msg.limit", 1, "message limit")
	flag.IntVar(&tc.pubCount, "pub.count", 1, "publisher count")
	flag.IntVar(&tc.pubLimit, "pub.limit", 1, "publisher limit")
	flag.IntVar(&tc.subCount, "sub.count", 1, "subscriber count")
	flag.DurationVar(&tc.subTimeout, "sub.timeout", time.Millisecond*100, "subscriber timeout")
	flag.IntVar(&tc.dbCount, "db.count", 1, "database count")
	flag.IntVar(&tc.blobSize, "blob.size", 0, "blob size")
}

func TestConsume(t *testing.T) {
	t.Logf("%s/testConfig%+v", t.Name(), tc)

	var (
		p  = newPool(t, tc.dbCount)
		db = &device.DB{RWC: p}
		nc = newNATS(t, db, tc.subCount, tc.subTimeout)

		firstSeen = time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC)

		ctx = t.Context()
	)

	g, ctx := errgroup.WithContext(ctx)

	ids, sends := send(ctx, g, db, nc, tc.pubCount, tc.pubLimit, tc.msgCount, tc.msgLimit, tc.blobSize, firstSeen)
	polls := poll(ctx, g, ids, db, tc.pubLimit, tc.msgCount, firstSeen)

	records := merge(sends, polls)
	err := writeTo(io.Discard, records)
	is.OK(t, cmp.Or(g.Wait(), err))
}

func poll(ctx context.Context, g *errgroup.Group, ids <-chan uuid.UUID, db *device.DB, pubLimit, msgCount int, firstSeen time.Time) <-chan []string {
	records := make(chan []string, pubLimit)
	g.Go(func() error {
		defer func() { close(records) }()

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(pubLimit)
		for id := range ids {
			g.Go(func() error {
				err := wait.ForFunc(ctx, time.Second*60, func() error {
					d, err := db.Device(ctx, id)
					if err != nil {
						return wait.SkipRetry // not found
					}
					if !d.Metadata.LastSeen.Equal(firstSeen.AddDate(0, 0, msgCount)) {
						return fmt.Errorf("device %s not ready", id)
					}
					return nil
				})
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case records <- []string{"poll", id.String()}:
				}
				return nil
			})
		}
		return g.Wait()
	})
	return records
}

func send(ctx context.Context, g *errgroup.Group, db *device.DB, nc *nats.Conn, pubCount, pubLimit, msgCount, msgLimit, blobSize int, firstSeen time.Time) (<-chan uuid.UUID, <-chan []string) {
	ids := make(chan uuid.UUID, pubLimit)
	records := make(chan []string, pubLimit*msgLimit)
	g.Go(func() error {
		defer func() { close(ids); close(records) }()

		g, ctx := errgroup.WithContext(ctx)
		// if one device we still need to send
		//
		// how many should we process at once?
		// with 10 devices do we need 10 routines? maybe not
		g.SetLimit(pubLimit)
		for id, err := range devices(ctx, db, pubCount, blobSize, firstSeen) {
			// we want to check context before the next stage
			// more verbose, but best we can do when not using jetstream
			g.Go(func() error {
				if err != nil {
					return err
				}
				g, ctx := errgroup.WithContext(ctx)
				g.SetLimit(msgLimit)
				for day := 1; day <= msgCount; day++ {
					g.Go(func() error { // context canceled
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
						if err := cmp.Or(err1, err2, ctx.Err()); err != nil {
							return err
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case records <- []string{"send", id.String()}:
						}
						return nil
					})
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ids <- id:
				}
				return g.Wait()
			})
		}
		return g.Wait()
	})
	return ids, records
}

func devices(ctx context.Context, d *device.DB, pubCount, blobSize int, firstSeen time.Time) iter.Seq2[uuid.UUID, error] {
	buf := make([]byte, blobSize)
	n, err := rand.Read(buf)
	if err != nil {
		panic("failed to read")
	}
	encodedLen := base64.StdEncoding.EncodedLen(len(buf))
	blob := make([]byte, encodedLen)
	base64.StdEncoding.Encode(blob, buf[:n])
	// 3mb takes longer to query & modify under 100ms
	// 4mb takes longer to query & modify under 250ms
	return func(yield func(uuid.UUID, error) bool) {
		for range pubCount {
			id, err := d.AddDevice(ctx, device.Metadata{Blob: blob, LastSeen: firstSeen})
			if !yield(id, err) {
				return
			}
		}
	}
}

func writeTo(w io.Writer, records <-chan []string) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"type", "id"}); err != nil {
		return err
	}
	for record := range records {
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// https://go.dev/blog/pipelines#fan-out-fan-in
func merge[V any](cs ...<-chan V) <-chan V {
	var wg sync.WaitGroup
	out := make(chan V)
	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan V) {
		defer wg.Done()
		for n := range c {
			out <- n
		}
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}
	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
