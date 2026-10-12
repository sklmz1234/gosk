// Package repository 负责 order 服务的数据访问，与其他服务同一模式：
// GORM 细节隔离在这一层，service 只面向接口编程（单测用 mockery 替身）。
package repository

import (
	"context"
	"errors"

	"gorm.io/gorm"

	apperrors "go-ecom-admin/pkg/errors"

	"go-ecom-admin/internal/order/model"
)

type Repository interface {
	Create(ctx context.Context, o *model.Order) error
	GetByID(ctx context.Context, id uint64) (*model.Order, error)

	ListByUser(ctx context.Context, userID uint64, page, pageSize int) ([]*model.Order, int64, error)

	UpdateStatus(ctx context.Context, id uint64, from, to string) error
}

type gormRepository struct {
	db *gorm.DB
}

func NewGormRepository(db *gorm.DB) Repository {
	return &gormRepository{db: db}
}

// Create 在一个事务里落订单 + 写支付超时定时消息（outbox 模式）。
// 超时消息与订单同生同死：订单落库成功才可能需要"15 分钟后关单"，
// 分开写会出现"订单在、超时消息丢"的漏关单窗口。Create 保持无返回
// 副作用的旧签名，调用方（service）不需要知道这条消息的存在。
func (r *gormRepository) Create(ctx context.Context, o *model.Order) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(o).Error; err != nil {
			return apperrors.Internal("failed to create order", err)
		}
		msg, err := model.TimeoutMessageFor(o)
		if err != nil {
			return apperrors.Internal("failed to encode timeout message", err)
		}
		if err := tx.Create(msg).Error; err != nil {
			return apperrors.Internal("failed to create timeout outbox message", err)
		}
		return nil
	})
}

func (r *gormRepository) GetByID(ctx context.Context, id uint64) (*model.Order, error) {
	var o model.Order
	if err := r.db.WithContext(ctx).Preload("Items").First(&o, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, apperrors.NotFound("order not found", err)
		}
		return nil, apperrors.Internal("failed to query order", err)
	}
	return &o, nil
}

// ListByUser 用最朴素的 offset/limit 分页（与 product 一致的理由）。
// 明细不 Preload：列表页只需要主表字段，N+1 次明细查询是纯浪费；
// 要看明细调 GetByID。
func (r *gormRepository) ListByUser(ctx context.Context, userID uint64, page, pageSize int) ([]*model.Order, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}

	var orders []*model.Order
	var total int64

	db := r.db.WithContext(ctx).Model(&model.Order{}).Where("user_id = ?", userID)
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, apperrors.Internal("failed to count orders", err)
	}

	offset := (page - 1) * pageSize
	if err := db.Order("id DESC").Offset(offset).Limit(pageSize).Find(&orders).Error; err != nil {
		return nil, 0, apperrors.Internal("failed to list orders", err)
	}

	return orders, total, nil
}

func (r *gormRepository) UpdateStatus(ctx context.Context, id uint64, from, to string) error {
	result := r.db.WithContext(ctx).Model(&model.Order{}).
		Where("id = ? AND status = ?", id, from).
		Update("status", to)
	if result.Error != nil {
		return apperrors.Internal("failed to update order status", result.Error)
	}
	if result.RowsAffected == 0 {
		return apperrors.FailedPrecondition("order status changed concurrently", nil)
	}
	return nil
}
