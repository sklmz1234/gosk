// ratelimit_redis.go Redis + Lua 固定窗口限流：计数收敛到 Redis，
// 多副本共享同一份额度，填 ratelimit.go 注释里"生产方案是 Redis + Lua"的坑。
// Redis 故障时回落单机令牌桶——限流器挂了不能拖死网关。
package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// windowScript 原子完成"计数 + 首访设过期"。
// 为什么必须 Lua：INCR 和 EXPIRE 是两条命令，若进程在两者之间崩溃，
// 这个 key 永不过期——该 IP 的窗口计数永久卡死（内存泄漏 + 永久误限流）。
const windowScript = `
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
if n > tonumber(ARGV[1]) then return 0 end
return 1
`

type redisRateLimiter struct {
	rdb       *redis.Client
	limit     int // 窗口内允许的最大请求数
	windowSec int
	// fallback 是 Redis 故障时的退路（单机内存桶）。多副本下它的阈值
	// 会放大为 配置×副本数，但可用性优先：宁可限流失准，不可网关瘫痪。
	fallback *ipRateLimiter
}

func newRedisRateLimiter(rdb *redis.Client, r rate.Limit, burst int) *redisRateLimiter {
	// 窗口长度 = burst / r：窗口内放满 burst 个请求，折算速率恰为 r，
	// 与单机令牌桶"burst 突发 + r 匀速"的语义对齐。
	// 已知边界（面试要能讲）：固定窗口在窗口交界处最多放过约 2 倍流量；
	// 要更平滑需升级滑动窗口（ZSET 存时间戳）或 Redis 令牌桶，属于下一步。
	windowSec := int(float64(burst) / float64(r))
	if windowSec < 1 {
		windowSec = 1
	}
	l := &redisRateLimiter{
		rdb:       rdb,
		limit:     burst,
		windowSec: windowSec,
		fallback:  newIPRateLimiter(r, burst),
	}
	go l.fallback.cleanupLoop()
	return l
}

func (l *redisRateLimiter) allow(ip string, now time.Time) bool {
	// 窗口号进 key 名：窗口随时间自然滚动、靠 EXPIRE 自动回收，
	// 不需要单机版 cleanupLoop 那样的清扫协程。
	key := fmt.Sprintf("rl:%s:%d", ip, now.Unix()/int64(l.windowSec))
	v, err := l.rdb.Eval(context.Background(), windowScript, []string{key}, l.limit, l.windowSec).Int()
	if err != nil {
		return l.fallback.allow(ip)
	}
	return v == 1
}

// RateLimitRedis 是 RateLimit 的分布式版本，参数语义完全一致。
func RateLimitRedis(rdb *redis.Client, r rate.Limit, burst int) gin.HandlerFunc {
	limiter := newRedisRateLimiter(rdb, r, burst)

	return func(c *gin.Context) {
		if !limiter.allow(c.ClientIP(), time.Now()) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded, please slow down"})
			return
		}
		c.Next()
	}
}
