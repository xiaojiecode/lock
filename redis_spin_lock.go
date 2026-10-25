package lock

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisSpinLock 持续尝试获取锁，不在重试之间主动等待。
type RedisSpinLock struct {
	lock *RedisLock
}

// NewRedisSpinLock 使用调用方管理的 Redis 客户端创建自旋锁。
func NewRedisSpinLock(client *goredis.Client) *RedisSpinLock {
	return &RedisSpinLock{lock: NewRedisLock(client)}
}

// TryLock 尝试一次获取锁。
func (l *RedisSpinLock) TryLock(ctx context.Context, key string, ttl time.Duration) (*Guard, bool, error) {
	return l.lock.TryLock(ctx, key, ttl)
}

// Lock 自旋到获得锁、Redis 返回错误或 ctx 取消。
func (l *RedisSpinLock) Lock(ctx context.Context, key string, ttl time.Duration) (*Guard, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		guard, ok, err := l.TryLock(ctx, key, ttl)
		if err != nil {
			return nil, err
		}
		if ok {
			return guard, nil
		}
	}
}

// Do 自旋加锁执行 fn，并在 fn 返回 error 或发生 panic 时自动解锁。
//
// 如果获取锁失败，fn 不会执行；如果 fn 发生 panic，Do 会先解锁再继续抛出原 panic。
func (l *RedisSpinLock) Do(ctx context.Context, key string, ttl time.Duration, fn func() error) (err error) {
	guard, err := l.Lock(ctx, key, ttl)
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
