package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/adoublef/ot/internal/device"
	"github.com/google/uuid"
)

func Handler(db *device.DB) http.Handler {
	mux := http.NewServeMux()
	handleFunc := func(pattern string, h http.Handler) {
		mux.Handle(pattern, h)
	}
	handleFunc("GET /devices/{device}", handleDevice(db))

	return mux
}

func handleDevice(db *device.DB) http.HandlerFunc {
	parse := func(_ http.ResponseWriter, r *http.Request) (uuid.UUID, error) {
		return uuid.Parse(r.PathValue("device"))
	}

	type response struct {
		ID       uuid.UUID `json:"id"`
		Blob     []byte    `json:"blob,omitempty"`
		LastSeen time.Time `json:"lastSeen"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		id, err := parse(w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		d, err := db.Device(r.Context(), id)
		if err != nil {
			w.WriteHeader(http.StatusFailedDependency)
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response{d.ID, d.Metadata.Blob, d.Metadata.LastSeen})
	}
}
