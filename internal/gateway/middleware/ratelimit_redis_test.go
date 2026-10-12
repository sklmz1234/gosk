package middleware

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func setupRedisLimiter(t *testing.T, r rate.Limit, burst int) (*redisRateLimiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return newRedisRateLimiter(rdb, r, burst), mr
}

// 窗口内放行、超限拦截：r=1/s burst=2 → 1 秒窗口内最多 2 个请求。
func TestRedisRateLimit_WindowCap(t *testing.T) {
	limiter, _ := setupRedisLimiter(t, rate.Limit(1), 2)
	now := time.Unix(1_700_000_000, 0)

	assert.True(t, limiter.allow("1.2.3.4", now))
	assert.True(t, limiter.allow("1.2.3.4", now))
	assert.False(t, limiter.allow("1.2.3.4", now), "窗口内第 3 个请求应被拦截")
}

// 窗口随时间滚动：下一秒（新窗口号）计数重置，重新放行。
func TestRedisRateLimit_WindowRolls(t *testing.T) {
	limiter, _ := setupRedisLimiter(t, rate.Limit(1), 2)
	now := time.Unix(1_700_000_000, 0)

	assert.True(t, limiter.allow("1.2.3.4", now))
	assert.True(t, limiter.allow("1.2.3.4", now))
	assert.False(t, limiter.allow("1.2.3.4", now))

	nextWindow := now.Add(2 * time.Second)
	assert.True(t, limiter.allow("1.2.3.4", nextWindow), "新窗口应重新放行")
}

// 不同 IP 的计数互相独立（和单机版 PerIPIsolation 同一语义，只是搬到了 Redis）。
func TestRedisRateLimit_PerIPIsolation(t *testing.T) {
	limiter, _ := setupRedisLimiter(t, rate.Limit(1), 1)
	now := time.Unix(1_700_000_000, 0)

	assert.True(t, limiter.allow("1.1.1.1", now))
	assert.False(t, limiter.allow("1.1.1.1", now))
	assert.True(t, limiter.allow("2.2.2.2", now), "另一个 IP 有自己的窗口")
}

// Redis 故障 → 回落单机令牌桶：网关可用性不绑定 Redis。
// 补药速率取极小值：Eval 连接失败要走 go-redis 的重试退避（秒级），
// 正常速率会在这期间把令牌桶重新填满，测不出"burst 用完即拦截"。
func TestRedisRateLimit_RedisDownFallsBack(t *testing.T) {
	limiter, mr := setupRedisLimiter(t, rate.Limit(0.000001), 2)
	mr.Close()
	now := time.Unix(1_700_000_000, 0)

	assert.True(t, limiter.allow("1.2.3.4", now), "Redis 挂了应回落单机版放行")
	assert.True(t, limiter.allow("1.2.3.4", now))
	assert.False(t, limiter.allow("1.2.3.4", now), "单机版 burst=2 用完后同样拦截")
}
