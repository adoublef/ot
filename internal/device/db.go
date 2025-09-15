package device

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Device struct {
	ID   uuid.UUID `json:"id" db:"id"`
	Meta Metadata  `json:"metadata" db:"metadata"`
}

type DB struct {
	RWC *sqlx.DB
}

func (d *DB) Device(ctx context.Context, id uuid.UUID) (Device, error) {
	var found Device
	err := d.RWC.QueryRowxContext(ctx, "select d.id, d.metadata from iot.device d where d.id = $1", id).StructScan(&found)
	return found, err
}

type Metadata struct {
	Blob     []byte    `json:"blob,omitempty"`
	LastSeen time.Time `json:"lastSeen"`
}

func (m *Metadata) Scan(src any) error {
	if src == nil {
		*m = Metadata{}
		return nil
	}

	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return errors.New("unsupported type for Metadata Scan")
	}
	return json.Unmarshal(data, m)
}

func (m Metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (d *DB) AddDevice(ctx context.Context, m Metadata) (uuid.UUID, error) {
	id := uuid.Must(uuid.NewV7())
	arg := &struct {
		ID       uuid.UUID
		Metadata Metadata
	}{
		ID:       id,
		Metadata: m,
	}
	_, err := d.RWC.NamedExecContext(ctx, "insert into iot.device (id, metadata) values (:id, :metadata)", arg)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (d *DB) ModDevice(ctx context.Context, id uuid.UUID, m Metadata) error {
	arg := &struct {
		ID       uuid.UUID
		Metadata Metadata
	}{
		ID:       id,
		Metadata: m,
	}
	ct, err := d.RWC.NamedExecContext(ctx, "update iot.device set metadata = :metadata where id = :id", arg)
	if hasErr := cmp.Or(err != nil, omit(ct.RowsAffected()) < 1); hasErr {
		return cmp.Or(err, sql.ErrNoRows)
	}
	return nil
}

func omit[V any](v V, _ error) V {
	return v
}
