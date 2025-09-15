package device_test

import (
	"testing"
	"time"

	. "github.com/adoublef/ot/internal/device"
	"go.adoublef.dev/testing/is"
)

func Test_DB(t *testing.T) {
	var (
		db = &DB{RWC: newDB(t, 1)}
	)

	m := Metadata{
		LastSeen: time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC),
	}
	id, err := db.AddDevice(t.Context(), m)
	is.OK(t, err) // DB.AddDevice

	m.LastSeen = m.LastSeen.AddDate(0, 0, 1)
	err = db.ModDevice(t.Context(), id, m)
	is.OK(t, err) // DB.ModDevice

	dev, err := db.Device(t.Context(), id)
	is.OK(t, err) // DB.Device

	is.Equal(t, dev.Meta.LastSeen, m.LastSeen) // +1 day
}
