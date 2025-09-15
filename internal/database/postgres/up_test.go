// Copyright The IOT Authors 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	. "github.com/adoublef/ot/internal/database/postgres"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"go.adoublef.dev/runtime/container/postgres"
	"go.adoublef.dev/testing/is"
)

func TestUp(t *testing.T) {
	ctx := t.Context()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	is.OK(t, err) // container.ConnectionString

	is.OK(t, Up(ctx, dsn))   // Up
	is.OK(t, Down(ctx, dsn)) // Down
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

var container *postgres.Container

// setup initialises containers within the pacakge.
func setup(ctx context.Context) (err error) {
	container, err = postgres.Run(ctx, "")
	if err != nil {
		return
	}
	return
}

// cleanup stops all running containers for the pacakge.
func cleanup(ctx context.Context) (err error) {
	var cc = []testcontainers.Container{container}
	for _, c := range cc {
		if c != nil {
			err = errors.Join(err, c.Terminate(ctx))
		}
	}
	return err
}
