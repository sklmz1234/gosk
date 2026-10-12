package cache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBloom(t *testing.T, n uint64, p float64) *BloomFilter {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewBloomFilter(rdb, "bloom:test", n, p)
}

// 参数推导：n=10万、p=1% → m 约 96 万 bit（约 117KB）、k 约 7。
func TestNewBloomFilter_Params(t *testing.T) {
	f := NewBloomFilter(nil, "bloom:x", 100000, 0.01)
	assert.InDelta(t, 958506, f.m, 1000)
	assert.Equal(t, uint64(7), f.k)
}

// 零假阴性：Add 过的 id 必须全部判"可能存在"，一个都不许漏。
func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	f := setupBloom(t, 1000, 0.01)
	ctx := context.Background()

	for id := uint64(1); id <= 200; id++ {
		require.NoError(t, f.Add(ctx, id))
	}
	for id := uint64(1); id <= 200; id++ {
		ok, err := f.MightContain(ctx, id)
		require.NoError(t, err)
		assert.True(t, ok, "Add 过的 id=%d 绝不能判不存在（假阴性）", id)
	}
}

// 假阳性只多不少地"放行"，但绝不能漏放已 Add 的；抽样验证未 Add 的 id
// 大部分被拦截（n 远未满时误判率应远低于 1% 的目标值）。
func TestBloomFilter_RejectsMostUnknown(t *testing.T) {
	f := setupBloom(t, 100000, 0.01)
	ctx := context.Background()

	for id := uint64(1); id <= 100; id++ {
		require.NoError(t, f.Add(ctx, id))
	}
	rejected := 0
	for id := uint64(1001); id <= 2000; id++ {
		ok, err := f.MightContain(ctx, id)
		require.NoError(t, err)
		if !ok {
			rejected++
		}
	}
	assert.Greater(t, rejected, 900, "1000 个未 Add 的 id 应拦截掉绝大多数")
}
