# lock

[![测试](https://github.com/xiaojiecode/lock/actions/workflows/test.yml/badge.svg)](https://github.com/xiaojiecode/lock/actions/workflows/test.yml)

Go 本地可重入锁和 Redis 分布式锁工具，支持回调自动解锁、阻塞等待、自旋和按 token 重入。

## 安装

需要 Go 1.26 或更新版本。Redis 集成测试覆盖 Redis 7 和 8。

```sh
go get github.com/xiaojiecode/lock@v0.1.1
```

## 使用

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/xiaojiecode/lock"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
    defer client.Close()
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    err := lock.NewRedisLock(client).Do(ctx, "jobs:daily", time.Minute, 50*time.Millisecond, func() error {
        // 在锁有效期内执行业务。
        return nil
    })
    if err != nil { log.Fatal(err) }
}
```

| 类型 | 用途 | 获取方式 |
| --- | --- | --- |
| `ReentrantLock` | 同一 Go 协程可重入的进程内锁 | `Lock`、`LockContext`、`Do`、`DoContext` |
| `RedisLock` | 按随机凭证互斥 | `TryLock`、`Lock`、`Do` |
| `RedisSpinLock` | 持续重试的 Redis 锁 | `TryLock`、`Lock`、`Do` |
| `RedisReentrantLock` | 同一 token 可重入的 Redis 锁 | `TryLock`、`Lock`、`Do` |

优先使用 `Do` / `DoContext`。回调返回 error 或 panic 时都会解锁；panic 解锁后继续传播，回调和解锁同时失败时用 `errors.Join` 保留两个错误。Redis 解锁不继承调用方 context 的取消。

手动获取时，普通锁通过 `Guard.Unlock(ctx)` 释放；可重入锁每次获取都必须恰好解锁一次。`RedisReentrantGuard.Refresh(ctx, ttl)` 可由调用方按需续期。

## 行为边界

- 客户端由调用方创建和关闭，必须非空；锁对象通过构造函数创建，不使用零值，不复制本地锁。
- Redis TTL 应至少为 1 毫秒且明显大于临界区耗时。普通锁和自旋锁不会自动续期。TTL 到期后，旧业务可能仍在执行；本库不提供 fencing token，不能阻止旧业务写入外部存储。
- 本地重入身份通过解析 `runtime.Stack` 中的协程 ID 获得，这是 Go 运行时的非正式接口。普通互斥需求优先使用标准库 `sync.Mutex`。
- Redis 重入按调用方提供的 token 判断，**不会按协程区分**。不同独立任务不能共用同一 token，否则会被视为重入。
- 可重入 Guard 必须只解锁一次；不要在同一 token 已过期并重新获取后继续使用旧 Guard。
- `Refresh` 的返回值仅反映 Redis 命令错误。锁不存在或属于其他 token 时不会续期，但仍可能返回 nil；不能用它判断是否继续拥有锁。
- 自旋锁没有重试间隔，会持续请求 Redis。一般使用带重试间隔的 `RedisLock`，等待应传入可取消 context。
- 本库使用单 Redis 客户端模型，不实现多节点共识锁或故障切换下的强互斥保证。

## 测试

```sh
go test -race -count=1 ./...
REDIS_TEST_ADDR=127.0.0.1:6379 go test -race -shuffle=on -count=3 ./...
go vet ./...
```

默认单元测试使用 miniredis，真实 Redis 测试未配置地址时会明确跳过。配置地址后连接失败会使测试失败。测试只清理自己随机生成的键，不清空数据库。

测试覆盖本地互斥、重入计数、超时取消、回调错误与 panic、Redis 并发互斥、过期后旧 Guard 安全解锁、续期、不同 token 隔离和取消后的解锁。

## 许可证

采用 [MIT](LICENSE) 许可证。
