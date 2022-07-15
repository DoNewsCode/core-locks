package core_locks

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-redis/redis/v8"
)

func TestDistLock(t *testing.T) {
	if os.Getenv("REDIS_ADDR") == "" {
		t.Skip("set REDIS_ADDR to run TestDistLock")
		return
	}
	ctx := context.Background()
	addrs := strings.Split(os.Getenv("REDIS_ADDR"), ",")
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs})
	l := NewLockManager(client, log.NewNopLogger(), WithLeaseTTL(time.Second))
	unlock, err := l.Lock(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.Lock(ctx, "foo")
	if err == nil {
		t.Fatal("expected error")
	}
	time.Sleep(time.Second * 2) // lock should be renewed
	_, err = l.Lock(ctx, "foo")
	if err == nil {
		t.Fatal("expected error")
	}
	unlock()
	_, err = l.Lock(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}
}
