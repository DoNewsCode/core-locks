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

type LockManager struct {
	prefix      string
	client      redis.UniversalClient
	logger      log.Logger
	leaseTTL    time.Duration
	idGenerator func() int
}

type LockManagerConfig struct {
	Prefix      string
	LeaseTTL    time.Duration
	IDGenerator func() int
}

func NewLockManager(client redis.UniversalClient, logger log.Logger, config LockManagerConfig) *LockManager {
	if config.Prefix == "" {
		config.Prefix = "lock:"
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = time.Second * 2
	}
	if config.IDGenerator == nil {
		config.IDGenerator = rand.Int
	}
	return &LockManager{
		prefix:      config.Prefix,
		client:      client,
		logger:      logger,
		leaseTTL:    config.LeaseTTL,
		idGenerator: config.IDGenerator,
	}
}

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
		for range ticker.C {
			if !l.renew(ctx, lockKey, lockValue) {
				break
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
