package lock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestReentrantLockAllowsSameGoroutine(t *testing.T) {
	l := NewReentrantLock()
	l.Lock()
	l.Lock()
	if err := l.Unlock(); err != nil {
		t.Fatalf("first Unlock returned error: %v", err)
	}
	if err := l.Unlock(); err != nil {
		t.Fatalf("second Unlock returned error: %v", err)
	}
}

func TestReentrantLockDoUnlocksAfterError(t *testing.T) {
	l := NewReentrantLock()
	wantErr := errors.New("callback failed")
	if err := l.Do(func() error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Do err=%v, want %v", err, wantErr)
	}
	if err := l.Do(func() error {
		return nil
	}); err != nil {
		t.Fatalf("Do after error returned error: %v", err)
	}
}

func TestReentrantLockDoUnlocksBeforePanic(t *testing.T) {
	l := NewReentrantLock()
	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Fatalf("panic=%v, want boom", recovered)
			}
		}()
		_ = l.Do(func() error {
			panic("boom")
		})
	}()
	if err := l.Do(func() error {
		return nil
	}); err != nil {
		t.Fatalf("Do after panic returned error: %v", err)
	}
}

func TestReentrantLockDoContextDoesNotRunCallbackWhenLockWaitCanceled(t *testing.T) {
	l := NewReentrantLock()
	l.Lock()
	defer func() {
		if err := l.Unlock(); err != nil {
			t.Fatalf("Unlock returned error: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	calledCh := make(chan struct{}, 1)
	go func() {
		errCh <- l.DoContext(ctx, func() error {
			calledCh <- struct{}{}
			return nil
		})
	}()

	if err := <-errCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DoContext err=%v, want context deadline", err)
	}
	select {
	case <-calledCh:
		t.Fatal("callback was called")
	default:
	}
}

func TestRedisLockBlocksUntilAvailable(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisLock(client)
	ctx := context.Background()
	first, ok, err := locker.TryLock(ctx, "lock:test:block", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock ok=%v err=%v, want ok", ok, err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := locker.Lock(waitCtx, "lock:test:block", time.Minute, 5*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lock err=%v, want context deadline", err)
	}
	if err := first.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
	second, err := locker.Lock(ctx, "lock:test:block", time.Minute, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock after release returned error: %v", err)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatalf("second Unlock returned error: %v", err)
	}
}

func TestRedisLockDoUnlocksAfterError(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisLock(client)
	ctx := context.Background()
	wantErr := errors.New("callback failed")

	if err := locker.Do(ctx, "lock:test:do:error", time.Minute, 5*time.Millisecond, func() error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Do err=%v, want %v", err, wantErr)
	}
	guard, ok, err := locker.TryLock(ctx, "lock:test:do:error", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock after Do error ok=%v err=%v, want ok", ok, err)
	}
	if err := guard.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
}

func TestRedisLockDoUnlocksBeforePanic(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisLock(client)
	ctx := context.Background()

	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Fatalf("panic=%v, want boom", recovered)
			}
		}()
		_ = locker.Do(ctx, "lock:test:do:panic", time.Minute, 5*time.Millisecond, func() error {
			panic("boom")
		})
	}()
	guard, ok, err := locker.TryLock(ctx, "lock:test:do:panic", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock after Do panic ok=%v err=%v, want ok", ok, err)
	}
	if err := guard.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
}

func TestRedisSpinLockBlocksUntilAvailable(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisSpinLock(client)
	ctx := context.Background()
	first, ok, err := locker.TryLock(ctx, "lock:test:spin:block", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock ok=%v err=%v, want ok", ok, err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := locker.Lock(waitCtx, "lock:test:spin:block", time.Minute); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lock err=%v, want context deadline", err)
	}
	if err := first.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
	second, err := locker.Lock(ctx, "lock:test:spin:block", time.Minute)
	if err != nil {
		t.Fatalf("Lock after release returned error: %v", err)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatalf("second Unlock returned error: %v", err)
	}
}

func TestRedisSpinLockDoUnlocksAfterError(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisSpinLock(client)
	ctx := context.Background()
	wantErr := errors.New("callback failed")

	if err := locker.Do(ctx, "lock:test:spin:do:error", time.Minute, func() error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Do err=%v, want %v", err, wantErr)
	}
	guard, ok, err := locker.TryLock(ctx, "lock:test:spin:do:error", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock after Do error ok=%v err=%v, want ok", ok, err)
	}
	if err := guard.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
}

func TestRedisSpinLockDoUnlocksBeforePanic(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisSpinLock(client)
	ctx := context.Background()

	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Fatalf("panic=%v, want boom", recovered)
			}
		}()
		_ = locker.Do(ctx, "lock:test:spin:do:panic", time.Minute, func() error {
			panic("boom")
		})
	}()
	guard, ok, err := locker.TryLock(ctx, "lock:test:spin:do:panic", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock after Do panic ok=%v err=%v, want ok", ok, err)
	}
	if err := guard.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
}

func TestRedisReentrantLockCountsEntries(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisReentrantLock(client, "owner-a")
	ctx := context.Background()

	first, err := locker.Lock(ctx, "lock:test:reentrant", time.Minute, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("first Lock returned error: %v", err)
	}
	second, err := locker.Lock(ctx, "lock:test:reentrant", time.Minute, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("second Lock returned error: %v", err)
	}
	other := NewRedisReentrantLock(client, "owner-b")
	if _, ok, err := other.TryLock(ctx, "lock:test:reentrant", time.Minute); err != nil || ok {
		t.Fatalf("other TryLock ok=%v err=%v, want locked", ok, err)
	}
	if err := second.Unlock(ctx); err != nil {
		t.Fatalf("second Unlock returned error: %v", err)
	}
	if _, ok, err := other.TryLock(ctx, "lock:test:reentrant", time.Minute); err != nil || ok {
		t.Fatalf("other TryLock after partial unlock ok=%v err=%v, want locked", ok, err)
	}
	if err := first.Unlock(ctx); err != nil {
		t.Fatalf("first Unlock returned error: %v", err)
	}
	otherGuard, ok, err := other.TryLock(ctx, "lock:test:reentrant", time.Minute)
	if err != nil || !ok {
		t.Fatalf("other TryLock after full unlock ok=%v err=%v, want ok", ok, err)
	}
	if err := otherGuard.Unlock(ctx); err != nil {
		t.Fatalf("other Unlock returned error: %v", err)
	}
}

func TestRedisReentrantLockDoUnlocksAfterError(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisReentrantLock(client, "owner-a")
	other := NewRedisReentrantLock(client, "owner-b")
	ctx := context.Background()
	wantErr := errors.New("callback failed")

	if err := locker.Do(ctx, "lock:test:reentrant:do:error", time.Minute, 5*time.Millisecond, func() error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Do err=%v, want %v", err, wantErr)
	}
	guard, ok, err := other.TryLock(ctx, "lock:test:reentrant:do:error", time.Minute)
	if err != nil || !ok {
		t.Fatalf("other TryLock after Do error ok=%v err=%v, want ok", ok, err)
	}
	if err := guard.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
}

func TestRedisReentrantLockDoUnlocksBeforePanic(t *testing.T) {
	client := newRedisClient(t)
	locker := NewRedisReentrantLock(client, "owner-a")
	other := NewRedisReentrantLock(client, "owner-b")
	ctx := context.Background()

	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Fatalf("panic=%v, want boom", recovered)
			}
		}()
		_ = locker.Do(ctx, "lock:test:reentrant:do:panic", time.Minute, 5*time.Millisecond, func() error {
			panic("boom")
		})
	}()
	guard, ok, err := other.TryLock(ctx, "lock:test:reentrant:do:panic", time.Minute)
	if err != nil || !ok {
		t.Fatalf("other TryLock after Do panic ok=%v err=%v, want ok", ok, err)
	}
	if err := guard.Unlock(ctx); err != nil {
		t.Fatalf("Unlock returned error: %v", err)
	}
}

func newRedisClient(t *testing.T) *goredis.Client {
	t.Helper()
	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

// TestDoJoinsCallbackAndUnlockErrors 保留业务失败与解锁失败两个原因。
func TestDoJoinsCallbackAndUnlockErrors(t *testing.T) {
	client := newRedisClient(t)
	want := errors.New("业务失败")
	err := NewRedisLock(client).Do(context.Background(), "lock:test:joined", time.Minute, time.Millisecond, func() error {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		return want
	})
	if !errors.Is(err, want) || !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("错误信息丢失：%v", err)
	}
}
