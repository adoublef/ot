package nats

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/adoublef/ot/internal/device"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.adoublef.dev/runtime/debug"
	"golang.org/x/sync/errgroup"
)

type MsgHandler func(ctx context.Context, msg *nats.Msg)

type Conn = nats.Conn

func Connect(url string) (*nats.Conn, error) {
	return nats.Connect(url)
}

func Handler(nc *nats.Conn, db *device.DB, subCount int, subTimeout time.Duration) error {
	if subCount < 1 {
		panic("subCount not set")
	}

	handleMsg := func(h MsgHandler) nats.MsgHandler {
		return func(msg *nats.Msg) {
			ctx, cancel := context.WithTimeout(context.Background(), subTimeout)
			defer cancel()
			/* err :=  */ h(ctx, msg)
		}
	}

	g := new(errgroup.Group)
	g.Go(func() error {
		for range subCount {
			_, err := nc.QueueSubscribe("ping", "queue", handleMsg(handlePing(db)))
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to subscribe to nats: %w", err)
	}
	return nil
}

func handlePing(db *device.DB) MsgHandler {
	panicf := func(format string, v ...any) { panic(fmt.Sprintf(format, v...)) }
	logf := func(format string, v ...any) { debug.Printf(format, v...) }
	return func(ctx context.Context, msg *nats.Msg) {
		var v struct {
			ID       uuid.UUID `json:"id"`
			LastSeen time.Time `json:"lastSeen"`
		}
		err1 := json.Unmarshal(msg.Data, &v)
		d, err2 := db.Device(ctx, v.ID)
		if v.LastSeen.Before(d.Metadata.LastSeen) {
			logf("server received message out of order")
			return
		}
		// debug.Printf("len(d.Metadata.Blob) = %d", len(d.Metadata.Blob))
		d.Metadata.LastSeen = v.LastSeen
		err3 := db.ModDevice(ctx, d.ID, d.Metadata)
		if err := cmp.Or(err1, err2, err3); err != nil {
			panicf("failed to process handlePing message: %v", err)
		}
	}
}
