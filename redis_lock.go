package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisLock 提供基于 Redis SET NX 的互斥锁，不自动续期。
type RedisLock struct {
	client *goredis.Client
}

// Guard 持有一次普通锁的随机所有权凭证。
type Guard struct {
	client *goredis.Client
	key    string
	value  string
}

// NewRedisLock 使用调用方管理的非空 Redis 客户端创建锁。
func NewRedisLock(client *goredis.Client) *RedisLock {
	return &RedisLock{client: client}
}

// TryLock 尝试一次加锁；未取得锁时返回 ok=false。ttl 应为正数。
func (l *RedisLock) TryLock(ctx context.Context, key string, ttl time.Duration) (*Guard, bool, error) {
	value, err := randomValue()
	if err != nil {
		return nil, false, err
	}
	ok, err := l.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil || !ok {
		return nil, ok, err
	}
	return &Guard{client: l.client, key: key, value: value}, true, nil
}

// Lock 持续重试直到获得锁、Redis 返回错误或 ctx 取消。
func (l *RedisLock) Lock(ctx context.Context, key string, ttl, retryInterval time.Duration) (*Guard, error) {
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
func (l *RedisLock) Do(ctx context.Context, key string, ttl, retryInterval time.Duration, fn func() error) (err error) {
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

// Unlock 仅删除匹配自身凭证的锁；已过期或被替换时不执行删除。
func (g *Guard) Unlock(ctx context.Context) error {
	// 只删除自己持有的锁值，避免误删其他实例刚抢到的新锁。
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`
	return g.client.Eval(ctx, script, []string{g.key}, g.value).Err()
}

func randomValue() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
