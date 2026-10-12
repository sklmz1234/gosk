// order 服务 repository 层单元测试：sqlite :memory: 跑真实 GORM 逻辑，
// 与 internal/product/repository 同一策略。
package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	apperrors "go-ecom-admin/pkg/errors"

	"go-ecom-admin/internal/order/model"
)

func newSQLiteDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	// OutboxMessage 必须一起建：repo.Create 在同事务写 order.timeout 消息。
	require.NoError(t, db.AutoMigrate(&model.Order{}, &model.OrderItem{}, &model.OutboxMessage{}))
	return db
}

func requireAppCode(t *testing.T, err error, want apperrors.Code) {
	t.Helper()
	require.Error(t, err)
	var appErr *apperrors.AppError
	require.True(t, errors.As(err, &appErr), "应该返回 *AppError，实际是 %T: %v", err, err)
	assert.Equal(t, want, appErr.Code)
}

// Create 落主表 + 明细后，GetByID 能 Preload 回完整聚合。
func TestCreateAndGetByID(t *testing.T) {
	repo := NewGormRepository(newSQLiteDB(t))
	ctx := context.Background()

	o := &model.Order{
		UserID: 42, Status: model.StatusPending, TotalCents: 239700,
		Items: []model.OrderItem{
			{ProductID: 1, ProductName: "机械键盘", Quantity: 2, UnitPriceCents: 19900},
			{ProductID: 2, ProductName: "显示器", Quantity: 1, UnitPriceCents: 199900},
		},
	}
	require.NoError(t, repo.Create(ctx, o))
	require.NotZero(t, o.ID)

	got, err := repo.GetByID(ctx, o.ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got.UserID)
	assert.Equal(t, int64(239700), got.TotalCents)
	require.Len(t, got.Items, 2, "明细必须随聚合一起落库、一起读回")
	assert.Equal(t, "显示器", got.Items[1].ProductName)
	assert.Equal(t, int64(199900), got.Items[1].UnitPriceCents)
}

func TestGetByID_NotFound(t *testing.T) {
	repo := NewGormRepository(newSQLiteDB(t))

	_, err := repo.GetByID(context.Background(), 999)

	requireAppCode(t, err, apperrors.CodeNotFound)
}

// ListByUser 只返回指定用户的订单（越权隔离在 SQL 层），按 id 倒序（新订单在前）。
func TestListByUser(t *testing.T) {
	repo := NewGormRepository(newSQLiteDB(t))
	ctx := context.Background()

	for _, uid := range []uint64{42, 42, 7} {
		require.NoError(t, repo.Create(ctx, &model.Order{UserID: uid, Status: model.StatusPending}))
	}

	orders, total, err := repo.ListByUser(ctx, 42, 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(2), total, "user 42 恰有 2 单，user 7 的单不能漏进来")
	require.Len(t, orders, 2)
	assert.Greater(t, orders[0].ID, orders[1].ID, "新订单在前")

	// 非法分页参数兜底（与 product 一致）
	_, total, err = repo.ListByUser(ctx, 42, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
}

func TestUpdateStatus(t *testing.T) {
	repo := NewGormRepository(newSQLiteDB(t))
	ctx := context.Background()
	o := &model.Order{UserID: 42, Status: model.StatusPending}
	require.NoError(t, repo.Create(ctx, o))

	require.NoError(t, repo.UpdateStatus(ctx, o.ID, model.StatusPending, model.StatusCancelled))

	got, err := repo.GetByID(ctx, o.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCancelled, got.Status)

	// 第二次迁移：状态已不是 PENDING，条件不满足
	requireAppCode(t, repo.UpdateStatus(ctx, o.ID, model.StatusPending, model.StatusCancelled),
		apperrors.CodeFailedPrecondition)
}
