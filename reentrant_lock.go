package lock

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// ErrReentrantLockNotOwner 表示当前协程没有持有本地可重入锁。
var ErrReentrantLockNotOwner = errors.New("lock: current goroutine does not own reentrant lock")

// ReentrantLock 是进程内可重入互斥锁。
//
// 同一个 goroutine 可多次 Lock；必须调用相同次数 Unlock 才会真正释放。
// 它只适合本进程内同步，跨进程/多副本请使用 RedisReentrantLock。
type ReentrantLock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	owner   uint64
	entries int
}

// NewReentrantLock 创建本地可重入锁；请通过构造函数初始化。
func NewReentrantLock() *ReentrantLock {
	l := &ReentrantLock{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Lock 阻塞到获取锁；同一协程调用会增加重入计数。
func (l *ReentrantLock) Lock() {
	id := goroutineID()
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.owner != 0 && l.owner != id {
		l.cond.Wait()
	}
	l.owner = id
	l.entries++
}

// LockContext 在 ctx 有效期间等待或重入锁。
func (l *ReentrantLock) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := goroutineID()
	stop := context.AfterFunc(ctx, func() {
		l.mu.Lock()
		l.cond.Broadcast()
		l.mu.Unlock()
	})
	defer stop()

	l.mu.Lock()
	defer l.mu.Unlock()
	for l.owner != 0 && l.owner != id {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.owner = id
	l.entries++
	return nil
}

// Do 加锁执行 fn，并在 fn 返回 error 或发生 panic 时自动解锁。
//
// 如果 fn 发生 panic，Do 会先解锁再继续抛出原 panic。
func (l *ReentrantLock) Do(fn func() error) (err error) {
	l.Lock()
	defer func() {
		unlockErr := l.Unlock()
		if err != nil {
			err = errors.Join(err, unlockErr)
			return
		}
		err = unlockErr
	}()
	return fn()
}

// DoContext 使用可取消的加锁方式执行 fn，并在 fn 返回 error 或发生 panic 时自动解锁。
//
// 如果获取锁失败，fn 不会执行；如果 fn 发生 panic，DoContext 会先解锁再继续抛出原 panic。
func (l *ReentrantLock) DoContext(ctx context.Context, fn func() error) (err error) {
	if err := l.LockContext(ctx); err != nil {
		return err
	}
	defer func() {
		unlockErr := l.Unlock()
		if err != nil {
			err = errors.Join(err, unlockErr)
			return
		}
		err = unlockErr
	}()
	return fn()
}

// Unlock 减少当前协程的重入计数，归零后唤醒等待者。
func (l *ReentrantLock) Unlock() error {
	id := goroutineID()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner != id || l.entries == 0 {
		return ErrReentrantLockNotOwner
	}
	l.entries--
	if l.entries == 0 {
		l.owner = 0
		l.cond.Signal()
	}
	return nil
}

func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}
