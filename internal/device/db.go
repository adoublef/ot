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

type DB struct {
	RWC *sqlx.DB
}

type Device struct {
	ID       uuid.UUID `db:"id"`
	Metadata Metadata  `db:"metadata"`
}

func (d *DB) Device(ctx context.Context, id uuid.UUID) (Device, error) {
	var found Device
	err := d.RWC.QueryRowxContext(ctx, "select d.id, d.metadata from ot.device d where d.id = $1", id).StructScan(&found)
	return found, err
}

type Metadata struct {
	Blob     []byte    `json:"blob"`
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
	_, err := d.RWC.NamedExecContext(ctx, "insert into ot.device (id, metadata) values (:id, :metadata)", arg)
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
	ct, err := d.RWC.NamedExecContext(ctx, "update ot.device set metadata = :metadata where id = :id", arg)
	if err != nil {
		return err
	} else if n, err := ct.RowsAffected(); err != nil || n < 1 {
		return cmp.Or(err, sql.ErrNoRows)
	}
	return nil
}
