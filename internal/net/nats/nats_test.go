package nats_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"iter"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/adoublef/ot/internal/device"
	"github.com/adoublef/ot/internal/net/nats"
	"github.com/adoublef/ot/internal/testing/wait"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"go.adoublef.dev/testing/is"
	"golang.org/x/sync/errgroup"
)

var layout = "15:04:05.000"

var tc struct {
	msgCount   int
	msgLimit   int
	pubLimit   int
	pubCount   int
	pubSize    int
	subCount   int
	subTimeout time.Duration
	dbCount    int
	dbSize     int
}

func init() {
	flag.IntVar(&tc.msgCount, "msg.count", 1, "message count")
	flag.IntVar(&tc.msgLimit, "msg.limit", 1, "message limit")
	flag.IntVar(&tc.pubCount, "pub.count", 1, "publisher count")
	flag.IntVar(&tc.pubLimit, "pub.limit", 1, "publisher limit")
	flag.IntVar(&tc.pubSize, "pub.size", 0, "publisher size")
	flag.IntVar(&tc.subCount, "sub.count", 1, "subscriber count")
	flag.DurationVar(&tc.subTimeout, "sub.timeout", time.Millisecond*100, "subscriber timeout")
	flag.IntVar(&tc.dbCount, "db.count", 1, "database count")
	flag.IntVar(&tc.dbSize, "db.size", 0, "database size")
}

func TestConsume(t *testing.T) {
	t.Logf("%s/testConfig%+v", t.Name(), tc)

	p := newPool(t, tc.dbCount)
	db := &device.DB{RWC: p}

	s := newHTTP(t, db)
	nc := newNATS(t, db, tc.subCount, tc.subTimeout)
	firstSeen := time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC)

	g, ctx := errgroup.WithContext(t.Context())

	ids, sends := send(ctx, g, db, nc, tc.pubCount, tc.pubLimit, tc.pubSize, tc.msgCount, tc.msgLimit, tc.dbSize, firstSeen)
	polls := poll(ctx, g, ids, s, tc.pubLimit, tc.msgCount, firstSeen)

	records := merge(sends, polls)

	w :=
		// io.Discard
		os.Stderr
	err := writeTo(w, records)
	is.OK(t, cmp.Or(g.Wait(), err))
}

func poll(ctx context.Context, g *errgroup.Group, ids <-chan uuid.UUID, s *httptest.Server, pubLimit, msgCount int, firstSeen time.Time) <-chan []string {
	records := make(chan []string, pubLimit)
	g.Go(func() error {
		defer func() { close(records) }()

		c, baseURL := s.Client(), s.URL

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(pubLimit)
		for id := range ids {
			g.Go(func() error {
				err := wait.ForFunc(ctx, time.Second*60, func() error {
					start := time.Now()
					resp, err := get(ctx, c, baseURL, id)
					if err != nil {
						return wait.SkipRetry // failed to complete http reqeust
					}
					defer resp.Body.Close()
					select {
					case <-ctx.Done():
						return wait.SkipRetry // context canceled
					case records <- []string{"poll", id.String(), time.Since(start).String(), start.Format(layout)}:
					}
					// check the status code
					if resp.StatusCode != 200 {
						return fmt.Errorf("failed to fetch device")
					}
					// decode since its valid
					var d struct {
						ID       uuid.UUID `json:"id"`
						Blob     []byte    `json:"blob,omitempty"`
						LastSeen time.Time `json:"lastSeen"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
						return wait.SkipRetry // not found
					}
					// handle reading message at this point
					if !d.LastSeen.Equal(firstSeen.AddDate(0, 0, msgCount)) {
						return fmt.Errorf("device %s not ready", id)
					}
					return nil
				})
				if err != nil {
					return err
				}
				return nil
			})
		}
		return g.Wait()
	})
	return records
}

func send(ctx context.Context, g *errgroup.Group, db *device.DB, nc *nats.Conn, pubCount, pubLimit, pubSize, msgCount, msgLimit, dbSize int, firstSeen time.Time) (<-chan uuid.UUID, <-chan []string) {
	// generate a blob that is shared amongst all publishers
	// default max size for NATS is 1mib (can i convert)
	// panic at 4mb
	buf := make([]byte, pubSize)
	n, err := rand.Read(buf)
	if err != nil {
		panic("failed to read")
	}
	buf = buf[:n]

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
		for id, err := range devices(ctx, db, pubCount, dbSize, firstSeen) {
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
						start := time.Now()
						v := struct {
							ID       uuid.UUID `json:"id"`
							Blob     []byte    `json:"blob"`
							LastSeen time.Time `json:"lastSeen"`
						}{
							ID:       id,
							Blob:     buf,
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
						case records <- []string{"send", id.String(), time.Since(start).String(), start.Format(layout)}:
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

func devices(ctx context.Context, d *device.DB, pubCount, dbSize int, firstSeen time.Time) iter.Seq2[uuid.UUID, error] {
	buf := make([]byte, dbSize)
	n, err := rand.Read(buf)
	if err != nil {
		panic("failed to read")
	}
	buf = buf[:n]
	return func(yield func(uuid.UUID, error) bool) {
		for range pubCount {
			id, err := d.AddDevice(ctx, device.Metadata{Blob: buf, LastSeen: firstSeen})
			if !yield(id, err) {
				return
			}
		}
	}
}

func writeTo(w io.Writer, records <-chan []string) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"type", "id", "elapsed", "start"}); err != nil {
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
