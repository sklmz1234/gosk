package repository

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apperrors "go-ecom-admin/pkg/errors"

	"go-ecom-admin/internal/product/model"
)

// countingRepo 是手写假实现而不是 mockery mock：缓存装饰器的测试核心是
// "下游被调了几次"（回源计数），mock 框架的断言反而不如一个原子计数器直白。
type countingRepo struct {
	product  *model.Product // 回源时返回的商品；nil 表示返回 NotFound
	getCalls atomic.Int32   // GetByID 回源次数
	delay    time.Duration  // 模拟 DB 耗时，给并发测试留出"同时在途"的窗口
}

func (f *countingRepo) GetByID(ctx context.Context, id uint64) (*model.Product, error) {
	f.getCalls.Add(1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.product == nil {
		return nil, apperrors.NotFound("product not found", nil)
	}
	return f.product, nil
}

func (f *countingRepo) Create(ctx context.Context, p *model.Product) error { return nil }

func (f *countingRepo) Update(ctx context.Context, p *model.Product) error { return nil }

func (f *countingRepo) Delete(ctx context.Context, id uint64) error { return nil }

func (f *countingRepo) List(ctx context.Context, page, pageSize int) ([]*model.Product, int64, error) {
	return nil, 0, nil
}

func (f *countingRepo) SearchByKeyword(ctx context.Context, keyword string, page, pageSize int) ([]*model.Product, int64, error) {
	return nil, 0, nil
}

func (f *countingRepo) ListByIDs(ctx context.Context, ids []uint64) ([]*model.Product, error) {
	return nil, nil
}

func (f *countingRepo) DeductStock(ctx context.Context, productID uint64, quantity int32) (*model.Product, error) {
	if f.product == nil {
		return nil, apperrors.NotFound("product not found", nil)
	}
	return f.product, nil
}

func (f *countingRepo) RestoreStock(ctx context.Context, productID uint64, quantity int32) (*model.Product, error) {
	if f.product == nil {
		return nil, apperrors.NotFound("product not found", nil)
	}
	return f.product, nil
}

func (f *countingRepo) RestoreStockIdempotent(ctx context.Context, messageID string, productID uint64, quantity int32) (*model.Product, error) {
	if f.product == nil {
		return nil, apperrors.NotFound("product not found", nil)
	}
	return f.product, nil
}

// setup 起一台内存假 Redis（miniredis），返回装饰器和它的组件。
// miniredis 让单测不依赖真 Redis，CI/本地都能跑——和 sqlite :memory: 测 GORM 同一思路。
func setup(t *testing.T, next Repository) (*cachedRepository, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewCachedRepository(next, rdb, NewStockGate(rdb))
	cached, ok := repo.(*cachedRepository)
	require.True(t, ok, "NewCachedRepository should return *cachedRepository")
	return cached, mr
}

var testProduct = &model.Product{ID: 42, Name: "机械键盘", PriceCents: 19900, Stock: 10}

// 未命中回源并回填：第一次读打 DB，第二次读命中缓存，DB 只被查一次。
func TestCachedRepository_GetByID_MissThenHit(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, _ := setup(t, next)
	ctx := context.Background()

	p1, err := repo.GetByID(ctx, 42)
	require.NoError(t, err)
	assert.Equal(t, testProduct.Name, p1.Name)

	p2, err := repo.GetByID(ctx, 42)
	require.NoError(t, err)
	assert.Equal(t, testProduct.Name, p2.Name)

	assert.Equal(t, int32(1), next.getCalls.Load(), "第二次读应该命中缓存，不再回源")
}

// 防雪崩：回填的 TTL 必须落在 [30min, 35min) 区间——带随机抖动而不是固定值。
func TestCachedRepository_GetByID_TTLHasJitter(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, 42)
	require.NoError(t, err)

	ttl := mr.TTL(productKey(42))
	assert.GreaterOrEqual(t, ttl, productCacheTTL, "TTL 不应低于基础值")
	assert.Less(t, ttl, productCacheTTL+cacheTTLJitter, "TTL 不应超过基础值+抖动上限")
}

// 防穿透：查不存在的 id 会缓存空值占位符，重复查不再打 DB。
func TestCachedRepository_GetByID_NullCaching(t *testing.T) {
	next := &countingRepo{product: nil} // 回源永远 NotFound
	repo, mr := setup(t, next)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, 999)
	require.Error(t, err)
	assert.True(t, isNotFound(err))

	// 空值确实写进了 Redis（占位符 + 短 TTL）
	val, err := mr.Get(productKey(999))
	require.NoError(t, err)
	assert.Equal(t, nullValue, val)
	assert.Equal(t, nullCacheTTL, mr.TTL(productKey(999)))

	_, err = repo.GetByID(ctx, 999)
	require.Error(t, err)
	assert.True(t, isNotFound(err))
	assert.Equal(t, int32(1), next.getCalls.Load(), "命中空值缓存后不应再回源")
}

// 写后失效：Update 后缓存被删，下次读回源拿到新值。
func TestCachedRepository_Update_InvalidatesCache(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, 42) // 让缓存里先有旧值
	require.NoError(t, err)
	require.True(t, mr.Exists(productKey(42)))

	err = repo.Update(ctx, &model.Product{ID: 42, Name: "新键盘", PriceCents: 29900, Stock: 5})
	require.NoError(t, err)
	assert.False(t, mr.Exists(productKey(42)), "Update 后缓存 key 应被删除")
}

// Delete 后缓存同样被删。
func TestCachedRepository_Delete_InvalidatesCache(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, 42)
	require.NoError(t, err)
	require.True(t, mr.Exists(productKey(42)))

	err = repo.Delete(ctx, 42)
	require.NoError(t, err)
	assert.False(t, mr.Exists(productKey(42)), "Delete 后缓存 key 应被删除")
}

// 阶段 3：扣库存后缓存必须失效——否则下单成功后商品详情页的 stock
// 还是旧值，最长脏读 30 分钟（productCacheTTL）。
func TestCachedRepository_DeductStock_InvalidatesCache(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, 42) // 让缓存里先有旧值
	require.NoError(t, err)
	require.True(t, mr.Exists(productKey(42)))

	_, err = repo.DeductStock(ctx, 42, 1)
	require.NoError(t, err)
	assert.False(t, mr.Exists(productKey(42)), "DeductStock 后缓存 key 应被删除")
}

// 回补库存同理。
func TestCachedRepository_RestoreStock_InvalidatesCache(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, 42)
	require.NoError(t, err)
	require.True(t, mr.Exists(productKey(42)))

	_, err = repo.RestoreStock(ctx, 42, 1)
	require.NoError(t, err)
	assert.False(t, mr.Exists(productKey(42)), "RestoreStock 后缓存 key 应被删除")
}

// 防击穿：N 个并发请求同时打同一个未缓存的 key，
// singleflight 合并后 DB 只被回源一次，且所有请求拿到同样的结果。
func TestCachedRepository_GetByID_Singleflight(t *testing.T) {
	// delay 是关键：没有它第一个请求可能在其他 goroutine 启动前就完成回源，
	// 测不出"同时在途被合并"。
	next := &countingRepo{product: testProduct, delay: 100 * time.Millisecond}
	repo, _ := setup(t, next)
	ctx := context.Background()

	const concurrency = 20
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p, err := repo.GetByID(ctx, 42)
			if err == nil {
				assert.Equal(t, testProduct.Name, p.Name)
			}
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), next.getCalls.Load(),
		"singleflight 应该把 %d 个并发回源合并成 1 次", concurrency)
}

// Redis 故障降级：缓存挂了读路径仍然可用（直连 DB），只是没有加速。
func TestCachedRepository_RedisDown_FallbackToDB(t *testing.T) {
	next := &countingRepo{product: testProduct}
	repo, mr := setup(t, next)
	mr.Close() // 把 Redis 杀掉，模拟故障

	p, err := repo.GetByID(context.Background(), 42)
	require.NoError(t, err, "Redis 挂了应该降级直连 DB，而不是整个读路径挂掉")
	assert.Equal(t, testProduct.Name, p.Name)
	assert.Equal(t, int32(1), next.getCalls.Load())
}
