// bloom.go 基于 Redis bitmap 的布隆过滤器：把"一定不存在"的 id 挡在
// 缓存和 DB 之前，是防穿透的第一道闸（空值缓存是第二道兜底）。
//
// 两个特性就是它的全部取舍（面试必答）：
//   - 零假阴性：Add 过的 id 永远判"可能存在"——不会误杀真实商品；
//   - 有假阳性：没 Add 过的 id 可能判"可能存在"——误判只会放行，
//     穿透到空值缓存/DB 兜底，不会产生错误结果；
//   - 不支持删除：清一个 bit 可能误伤映射到同一位的其他 id。
//     要删除得换计数布隆或定期重建，本项目靠"启动预热 + seed 重建"对账。
package cache

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/redis/go-redis/v9"
)

type BloomFilter struct {
	rdb *redis.Client
	key string
	m   uint64 // 位数组大小（bit）
	k   uint64 // 哈希函数个数
}

// NewBloomFilter 按预期元素数 n 和目标误判率 p 推导参数：
//
//	m = -n·ln(p) / (ln2)²      k = (m/n)·ln2
//
// 例：n=10 万、p=1% → m≈95.9 万 bit（约 117KB），k≈7。
func NewBloomFilter(rdb *redis.Client, key string, n uint64, p float64) *BloomFilter {
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	k := uint64(math.Ceil(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	return &BloomFilter{rdb: rdb, key: key, m: m, k: k}
}

func fnvSum(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// offsets 用双哈希公式 h_i = h1 + i·h2 从两个独立哈希派生 k 个位置
// （Kirsch-Mitzenmacher 技巧），省去 k 次完整哈希计算，效果等价。
func (f *BloomFilter) offsets(id uint64) []uint64 {
	var b [9]byte
	binary.BigEndian.PutUint64(b[:8], id)
	h1 := fnvSum(b[:8])
	b[8] = 0x9e // 追加一个盐字节，得到第二个独立哈希
	h2 := fnvSum(b[:9])

	offs := make([]uint64, f.k)
	for i := uint64(0); i < f.k; i++ {
		offs[i] = (h1 + i*h2) % f.m
	}
	return offs
}

func (f *BloomFilter) Add(ctx context.Context, id uint64) error {
	pipe := f.rdb.Pipeline()
	for _, off := range f.offsets(id) {
		pipe.SetBit(ctx, f.key, int64(off), 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// MightContain 返回 false = 一定不存在；true = 可能存在（含误判）。
func (f *BloomFilter) MightContain(ctx context.Context, id uint64) (bool, error) {
	pipe := f.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, f.k)
	for _, off := range f.offsets(id) {
		cmds = append(cmds, pipe.GetBit(ctx, f.key, int64(off)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	for _, c := range cmds {
		if c.Val() == 0 {
			return false, nil
		}
	}
	return true, nil
}
