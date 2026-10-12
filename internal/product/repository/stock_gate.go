// stock_gate.go 库存前置闸门：用 Redis 单线程原子性做"查够不够 + 预扣"，
// 把注定失败的请求挡在 MySQL 行锁之外。
//
// 设计定调：MySQL 的条件更新（product_repository.go 的 DeductStock）仍是
// 库存唯一权威，闸门只是加速器。因此闸门只做"宁可少卖、不可超卖"的取舍：
//   - 闸门判定不足 → 微秒级拦截，不碰 DB；
//   - key 未预热 / Redis 故障 → 放行，DB 兜底（和缓存降级同一原则）；
//   - 闸门已预扣但 DB 失败 → 不回滚补偿，代价是闸门短暂偏低，
//     下次成功落库后的 Sync 用 DB 真值纠偏。
package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type StockGate struct {
	rdb *redis.Client
}

func NewStockGate(rdb *redis.Client) *StockGate {
	return &StockGate{rdb: rdb}
}

// stockKey 与详情缓存 key（product:%d）刻意分开：详情缓存是"副本"，
// 写后失效；库存 key 是"状态计数器"，绝不能被缓存失效逻辑误删。
func stockKey(id uint64) string {
	return fmt.Sprintf("stock:%d", id)
}

// deductScript 原子执行"读库存 → 判断 → 扣减"。
// 为什么必须 Lua：GET 再 DECRBY 是两条命令，两个并发请求可能都读到 1、
// 都去扣，超卖。Lua 脚本在 Redis 单线程里作为整体执行，中间不会插队。
// 返回值：1=放行且已预扣；0=库存不足；-1=key 不存在（未预热）→ 放行。
const deductScript = `
local stock = redis.call('GET', KEYS[1])
if not stock then return -1 end
local s = tonumber(stock)
local q = tonumber(ARGV[1])
if s < q then return 0 end
redis.call('DECRBY', KEYS[1], q)
return 1
`

// TryDeduct 尝试过闸。allowed=false 表示闸门判定库存不足；
// err != nil 表示 Redis 故障——调用方应放行降级，由 DB 条件更新兜底。
func (g *StockGate) TryDeduct(ctx context.Context, id uint64, qty int32) (allowed bool, err error) {
	v, err := g.rdb.Eval(ctx, deductScript, []string{stockKey(id)}, qty).Int()
	if err != nil {
		return false, err
	}
	// v == 0 才是明确拦截；1（已预扣）和 -1（未预热）都放行
	return v != 0, nil
}

// Sync 对账式回写：用 DB 返回的真值覆盖闸门。
// 作用：① 首次扣减顺带完成预热（懒加载，不需要单独的预热流程）；
// ② 每次 DB 成功后纠偏，闸门最多在"上次失败到这次成功之间"偏低，
// 不会累积漂移。不设 TTL——库存 key 是状态计数器，不是缓存副本。
func (g *StockGate) Sync(ctx context.Context, id uint64, stock int32) error {
	return g.rdb.Set(ctx, stockKey(id), stock, 0).Err()
}
