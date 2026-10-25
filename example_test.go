package lock_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xiaojiecode/lock"
)

func ExampleReentrantLock_Do() {
	locker := lock.NewReentrantLock()
	err := locker.Do(func() error {
		return locker.Do(func() error { fmt.Println("嵌套执行"); return nil })
	})
	fmt.Println(err)
	// Output:
	// 嵌套执行
	// <nil>
}

func ExampleRedisLock_Do() {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := lock.NewRedisLock(client).Do(ctx, "lock:example", time.Minute, 10*time.Millisecond, func() error {
		// 临界区代码需要在 TTL 内完成。
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}
