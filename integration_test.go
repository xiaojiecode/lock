package lock

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func integrationClient(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("设置 REDIS_TEST_ADDR 运行真实 Redis 集成测试")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	token, err := randomValue()
	if err != nil {
		t.Fatal(err)
	}
	key := "lock-test:" + token
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Del(ctx, key).Err(); err != nil {
			t.Error(err)
		}
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client, key
}

func TestRedisIntegrationMutualExclusion(t *testing.T) {
	client, key := integrationClient(t)
	locker := NewRedisLock(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var active atomic.Int64
	var completed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				err := locker.Do(ctx, key, 10*time.Second, time.Millisecond, func() error {
					if n := active.Add(1); n != 1 {
						t.Errorf("同时进入临界区：%d", n)
					}
					time.Sleep(100 * time.Microsecond)
					active.Add(-1)
					completed.Add(1)
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if completed.Load() != 120 {
		t.Fatalf("完成次数：%d", completed.Load())
	}
}

func TestRedisIntegrationExpiredGuardCannotDeleteNewOwner(t *testing.T) {
	client, key := integrationClient(t)
	ctx := context.Background()
	locker := NewRedisLock(client)
	old, ok, err := locker.TryLock(ctx, key, 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("首次加锁：%v %v", ok, err)
	}
	waitForExpiry(t, client, key)
	current, ok, err := locker.TryLock(ctx, key, time.Minute)
	if err != nil || !ok {
		t.Fatalf("再次加锁：%v %v", ok, err)
	}
	if err := old.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	if value, err := client.Get(ctx, key).Result(); err != nil || value != current.value {
		t.Fatalf("旧解锁破坏了新持有者：%v", err)
	}
	if err := current.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRedisIntegrationReentrantRefreshAndOwner(t *testing.T) {
	client, key := integrationClient(t)
	ctx := context.Background()
	locker := NewRedisReentrantLock(client, "first")
	first, err := locker.Lock(ctx, key, 100*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Refresh(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if ttl, err := client.PTTL(ctx, key).Result(); err != nil || ttl < time.Second {
		t.Fatalf("续期失败：%v %v", ttl, err)
	}
	second, err := locker.Lock(ctx, key, 100*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := NewRedisReentrantLock(client, "other").TryLock(ctx, key, time.Minute); err != nil || ok {
		t.Fatalf("嵌套锁提前释放：%v %v", ok, err)
	}
	waitForExpiry(t, client, key)
	current, ok, err := NewRedisReentrantLock(client, "other").TryLock(ctx, key, time.Minute)
	if err != nil || !ok {
		t.Fatalf("过期后获取：%v %v", ok, err)
	}
	if err := first.Unlock(ctx); !errors.Is(err, ErrRedisReentrantLockNotOwner) {
		t.Fatalf("错误持有者解锁：%v", err)
	}
	if err := current.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRedisIntegrationCanceledCallbackStillUnlocks(t *testing.T) {
	for _, kind := range []string{"normal", "spin", "reentrant"} {
		t.Run(kind, func(t *testing.T) {
			client, key := integrationClient(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			callback := func() error { cancel(); return context.Canceled }
			var err error
			switch kind {
			case "normal":
				err = NewRedisLock(client).Do(ctx, key, time.Minute, time.Millisecond, callback)
			case "spin":
				err = NewRedisSpinLock(client).Do(ctx, key, time.Minute, callback)
			case "reentrant":
				err = NewRedisReentrantLock(client, "owner").Do(ctx, key, time.Minute, time.Millisecond, callback)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if n, err := client.Exists(context.Background(), key).Result(); err != nil || n != 0 {
				t.Fatalf("取消后未解锁：%d %v", n, err)
			}
		})
	}
}

func waitForExpiry(t *testing.T, client *redis.Client, key string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, err := client.Exists(context.Background(), key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("锁没有按时过期")
}

func TestLocalMutualExclusionAndWrongOwner(t *testing.T) {
	locker := NewReentrantLock()
	locker.Lock()
	done := make(chan error, 1)
	go func() { done <- locker.Unlock() }()
	if err := <-done; !errors.Is(err, ErrReentrantLockNotOwner) {
		t.Fatal(err)
	}
	if err := locker.Unlock(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	counter := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := locker.Do(func() error { counter++; return nil }); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if counter != 200 {
		t.Fatal(counter)
	}
}
