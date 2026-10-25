package lock

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ErrRedisReentrantLockNotOwner 表示解锁时锁属于其他 token。
var ErrRedisReentrantLockNotOwner = errors.New("lock: redis reentrant lock is not owned by token")

// RedisReentrantLock 按 token 判断重入，同一 token 的调用共享锁所有权。
type RedisReentrantLock struct {
	client *goredis.Client
	token  string
}

// RedisReentrantGuard 对应一次重入获取，必须且只能解锁一次。
type RedisReentrantGuard struct {
	lock  *RedisReentrantLock
	key   string
	token string
}

// NewRedisReentrantLock 创建可重入锁；token 为空时生成随机 token。
func NewRedisReentrantLock(client *goredis.Client, token string) *RedisReentrantLock {
	if token == "" {
		token, _ = randomValue()
	}
	return &RedisReentrantLock{client: client, token: token}
}

// TryLock 尝试获取或重入锁；ttl 必须至少为一毫秒。
func (l *RedisReentrantLock) TryLock(ctx context.Context, key string, ttl time.Duration) (*RedisReentrantGuard, bool, error) {
	ok, err := redisReentrantAcquire.Run(ctx, l.client, []string{key}, l.token, ttl.Milliseconds()).Bool()
	if err != nil || !ok {
		return nil, ok, err
	}
	return &RedisReentrantGuard{lock: l, key: key, token: l.token}, true, nil
}

// Lock 持续重试获取或重入锁，等待过程可通过 ctx 取消。
func (l *RedisReentrantLock) Lock(ctx context.Context, key string, ttl, retryInterval time.Duration) (*RedisReentrantGuard, error) {
	if retryInterval <= 0 {
		retryInterval = 100 * time.Millisecond
	}
	for {
		guard, ok, err := l.TryLock(ctx, key, ttl)
		if err != nil {
			return nil, err
		}
		if ok {
			return guard, nil
		}
		if err := sleepWithContext(ctx, retryInterval); err != nil {
			return nil, err
		}
	}
}

// Do 加锁执行 fn，并在 fn 返回 error 或发生 panic 时自动解锁。
//
// 如果获取锁失败，fn 不会执行；如果 fn 发生 panic，Do 会先解锁再继续抛出原 panic。
func (l *RedisReentrantLock) Do(ctx context.Context, key string, ttl, retryInterval time.Duration, fn func() error) (err error) {
	guard, err := l.Lock(ctx, key, ttl, retryInterval)
	if err != nil {
		return err
	}
	defer func() {
		unlockErr := guard.Unlock(context.Background())
		if err != nil {
			err = errors.Join(err, unlockErr)
			return
		}
		err = unlockErr
	}()
	return fn()
}

// Unlock 减少一次重入计数，计数归零后释放锁。
func (g *RedisReentrantGuard) Unlock(ctx context.Context) error {
	result, err := redisReentrantRelease.Run(ctx, g.lock.client, []string{g.key}, g.token).Int()
	if err != nil {
		return err
	}
	if result < 0 {
		return ErrRedisReentrantLockNotOwner
	}
	return nil
}

// Refresh 在 token 匹配时更新 TTL；返回值仅反映 Redis 执行错误，不能证明仍持有锁。
func (g *RedisReentrantGuard) Refresh(ctx context.Context, ttl time.Duration) error {
	return redisReentrantRefresh.Run(ctx, g.lock.client, []string{g.key}, g.token, ttl.Milliseconds()).Err()
}

// Token 返回可重入锁的所有权标识。
func (g *RedisReentrantGuard) Token() string {
	return g.token
}

var redisReentrantAcquire = goredis.NewScript(`
local owner = redis.call("HGET", KEYS[1], "owner")
if not owner then
  redis.call("HSET", KEYS[1], "owner", ARGV[1], "count", 1)
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  return 1
end
if owner == ARGV[1] then
  redis.call("HINCRBY", KEYS[1], "count", 1)
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  return 1
end
return 0`)

var redisReentrantRelease = goredis.NewScript(`
local owner = redis.call("HGET", KEYS[1], "owner")
if not owner then
  return 0
end
if owner ~= ARGV[1] then
  return -1
end
local count = redis.call("HINCRBY", KEYS[1], "count", -1)
if count <= 0 then
  redis.call("DEL", KEYS[1])
end
return count`)

var redisReentrantRefresh = goredis.NewScript(`
if redis.call("HGET", KEYS[1], "owner") == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)
