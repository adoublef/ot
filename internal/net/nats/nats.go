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

type Conn = nats.Conn

func Connect(url string) (*nats.Conn, error) {
	return nats.Connect(url)
}

var defaultTimeout = 100 * time.Millisecond

func Handler(nc *nats.Conn, db *device.DB) error {
	g := new(errgroup.Group)
	g.Go(func() error {
		for range 1 {
			_, err := nc.QueueSubscribe("ping", "queue", handlePing(db))
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to subscribe to nats: %w", err)
	}
	return nil
}

func handlePing(db *device.DB) nats.MsgHandler {
	return func(msg *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		var v struct {
			ID       uuid.UUID `json:"id"`
			LastSeen time.Time `json:"lastSeen"`
		}
		err1 := json.Unmarshal(msg.Data, &v)
		d, err2 := db.Device(ctx, v.ID)
		if v.LastSeen.Before(d.Meta.LastSeen) {
			panic("server received invalid last seen")
		}
		d.Meta.LastSeen = v.LastSeen
		err3 := db.ModDevice(ctx, d.ID, d.Meta)
		if err := cmp.Or(err1, err2, err3); err != nil {
			debug.Printf("failed to process handlePing message: %v", err)
		}
	}
}
