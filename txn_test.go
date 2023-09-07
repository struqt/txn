package txn

import (
	"context"
	"errors"
	"testing"
)

func TestPing(t *testing.T) {
	// Test 1: Basic functionality
	t.Run("basic functionality", func(t *testing.T) {
		pingFunc := func(ctx context.Context) error {
			return nil
		}
		cnt, err := Ping(5, nil, pingFunc)
		if cnt != 1 || err != nil {
			t.Errorf("Expected cnt=1 and err=nil, got cnt=%d and err=%v", cnt, err)
		}
	})

	// Test 2: Retry mechanism
	t.Run("retry mechanism", func(t *testing.T) {
		retry := 0
		pingFunc := func(ctx context.Context) error {
			retry++
			if retry == 2 {
				return nil
			}
			return errors.New("failed")
		}
		cnt, err := Ping(2, nil, pingFunc)
		if cnt != 2 || err != nil {
			t.Errorf("Expected cnt=2 and err=nil, got cnt=%d and err=%v", cnt, err)
		}
	})

	// Test 3: Reach retry limit
	t.Run("reach retry limit", func(t *testing.T) {
		pingFunc := func(ctx context.Context) error {
			return errors.New("failed")
		}
		cnt, err := Ping(2, nil, pingFunc)
		if cnt != 3 || err == nil || err.Error() != "reached retry limit (2), last error: failed" {
			t.Errorf("Expected cnt=3 and specific error, got cnt=%d and err=%v", cnt, err)
		}
	})

	// Test 4: Invalid parameters
	t.Run("invalid parameters", func(t *testing.T) {
		cnt, err := Ping(2, nil, nil)
		if cnt != 3 || err == nil || err.Error() != "reached retry limit (2), last error: ping func is nil" {
			t.Errorf("Expected cnt=1 and specific error, got cnt=%d and err=%v", cnt, err)
		}
	})

}
