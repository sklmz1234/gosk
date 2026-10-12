package repository

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apperrors "go-ecom-admin/pkg/errors"
)

func setupGate(t *testing.T) (*StockGate, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStockGate(rdb), mr
}

// 预热后扣减放行，且闸门值原子地减少。
func TestStockGate_DeductWithinStock(t *testing.T) {
	gate, mr := setupGate(t)
	ctx := context.Background()

	require.NoError(t, gate.Sync(ctx, 42, 10))

	allowed, err := gate.TryDeduct(ctx, 42, 3)
	require.NoError(t, err)
	assert.True(t, allowed)

	assert.Equal(t, "7", mustGet(t, mr, stockKey(42)))
}

// 不足被拦截，且闸门值不被改动（Lua 整体执行，不存在"扣了一半"）。
func TestStockGate_InsufficientRejected(t *testing.T) {
	gate, mr := setupGate(t)
	ctx := context.Background()

	require.NoError(t, gate.Sync(ctx, 42, 2))

	allowed, err := gate.TryDeduct(ctx, 42, 5)
	require.NoError(t, err)
	assert.False(t, allowed)

	assert.Equal(t, "2", mustGet(t, mr, stockKey(42)))
}

// key 不存在（未预热）→ 放行去 DB，由 DB 条件更新兜底；
// 且不能因为一次放行就凭空创建出库存 key。
func TestStockGate_MissingKeyPassesThrough(t *testing.T) {
	gate, mr := setupGate(t)

	allowed, err := gate.TryDeduct(context.Background(), 999, 1)
	require.NoError(t, err)
	assert.True(t, allowed)
	assert.False(t, mr.Exists(stockKey(999)))
}

// Redis 故障 → 返回 error，由调用方决定放行降级（不是静默拦截）。
func TestStockGate_RedisDownReturnsError(t *testing.T) {
	gate, mr := setupGate(t)
	mr.Close()

	_, err := gate.TryDeduct(context.Background(), 42, 1)
	require.Error(t, err)
}

func mustGet(t *testing.T, mr *miniredis.Miniredis, key string) string {
	t.Helper()
	v, err := mr.Get(key)
	require.NoError(t, err)
	return v
}

// 闸门拦截与 DB 判定不足返回同一错误语义（CodeFailedPrecondition），
// 上游 handler 不需要关心请求是被哪一层拒的。
func TestCachedRepository_DeductStock_GateBlocksInsufficient(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)

	require.NoError(t, mr.Set(stockKey(42), "0")) // 闸门预热为 0 库存

	_, err := repo.DeductStock(context.Background(), 42, 1)
	require.Error(t, err)

	var appErr *apperrors.AppError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, apperrors.CodeFailedPrecondition, appErr.Code)
}

// DB 扣减成功后闸门被对账式回写为 DB 真值（countingRepo 返回的 product
// Stock=10），这一步同时承担"懒预热"的职责。
func TestCachedRepository_DeductStock_SyncsGate(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)

	_, err := repo.DeductStock(context.Background(), 42, 1)
	require.NoError(t, err)

	assert.Equal(t, "10", mustGet(t, mr, stockKey(42)))
}

// 回补库存成功后闸门同步变大。
func TestCachedRepository_RestoreStock_SyncsGate(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)

	_, err := repo.RestoreStock(context.Background(), 42, 5)
	require.NoError(t, err)

	assert.Equal(t, "10", mustGet(t, mr, stockKey(42)))
}
