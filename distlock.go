// Package core_locks provides lock implementation using redis.
package core_locks

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-kit/log"
	"github.com/go-redis/redis/v8"
)

// Lua script to unlock a redis lock
const unlockLuaScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

// Lua script to renew redis lock
const renewLuaScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
else
	return 0
end
`

var ErrLockHeld = fmt.Errorf("lock held")

// LockManager is based on the redis database and implements a distributed lock
// that automatically renews the lease in a timely manner.
type LockManager struct {
	prefix      string
	client      redis.UniversalClient
	logger      log.Logger
	leaseTTL    time.Duration
	idGenerator func() int
}

// Option is the option to change LockManager behavior.
type Option func(*LockManager)

// WithPrefix change key prefix.
func WithPrefix(prefix string) func(c *LockManager) {
	return func(c *LockManager) {
		c.prefix = prefix
	}
}

// WithLeaseTTL change LockManager's leaseTTL and can't be zero value.
func WithLeaseTTL(leaseTTL time.Duration) func(c *LockManager) {
	return func(c *LockManager) {
		if leaseTTL == 0 {
			return
		}
		c.leaseTTL = leaseTTL
	}
}

// WithIDGenerator change the function that generates the lock check value.
func WithIDGenerator(f func() int) func(c *LockManager) {
	return func(c *LockManager) {
		c.idGenerator = f
	}
}

// NewLockManager to new LockManager.
func NewLockManager(client redis.UniversalClient, logger log.Logger, opts ...Option) *LockManager {
	m := &LockManager{
		prefix:      "lock:",
		leaseTTL:    time.Second * 2,
		idGenerator: rand.Int,
		client:      client,
		logger:      logger,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Lock set lock and returned unlock function. If acquiring the lock fails, return ErrLockHeld.
// The expiration time of the key is automatically updated every half of LockManager's leaseTTL.
// Because time.Ticker is used, the unlock method must be executed, otherwise it will cause an overflow.
func (l *LockManager) Lock(ctx context.Context, key string) (unlock func(), err error) {
	lockKey := l.prefix + key
	lockValue := l.idGenerator()
	succeed, err := l.client.SetNX(ctx, lockKey, lockValue, l.leaseTTL).Result()
	if err != nil {
		return nil, fmt.Errorf("lock %s failed: %w", lockKey, err)
	}
	if !succeed {
		return nil, ErrLockHeld
	}
	var ticker = time.NewTicker(l.leaseTTL / 2)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case _, ok := <-ticker.C:
				// the ticker is closed
				if !ok {
					return
				}
				if !l.renew(ctx, lockKey, lockValue) {
					// the lock has been released
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() {
		ticker.Stop()
		err := l.client.Eval(ctx, unlockLuaScript, []string{lockKey}, lockValue).Err()
		if err != nil {
			l.logger.Log("msg", "unlock failed", "key", lockKey, "value", lockValue, "error", err)
		}
	}, nil
}

func (l *LockManager) renew(ctx context.Context, lockKey string, lockValue interface{}) (renewed bool) {
	r, err := l.client.Eval(ctx, renewLuaScript, []string{lockKey}, lockValue, int(l.leaseTTL.Milliseconds())).Result()
	if err != nil {
		l.logger.Log("msg", "renew failed", "key", lockKey, "value", lockValue, "error", err)
		return false
	}
	if r == 0 {
		return false
	}
	return true
}
