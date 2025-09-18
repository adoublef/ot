package wait

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v5"
)

// ForFunc waits for function to return non-nil error within the specified timeout.
// It uses exponential backoff to retry requests until the endpoint responds successfully
// or the context is canceled.
func ForFunc(ctx context.Context, timeout time.Duration, f func() error) error {
	o := func() (bool, error) {
		if err := f(); err != nil {
			if errors.Is(err, SkipRetry) {
				return false, backoff.Permanent(err)
			}
			return false, err
		}
		return true, nil
	}
	_, err := backoff.Retry(ctx, o, backoff.WithBackOff(backoff.NewExponentialBackOff()), backoff.WithMaxElapsedTime(timeout))
	return err
}

// SkipRetry is used as a return value from [ForFunc]
//
//lint:ignore ST1012 following pattern from stdlib with fs.SkipAll & fs.SkipDir
var SkipRetry = errors.New("skip retry")
