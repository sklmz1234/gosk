// Package service 承载 order 服务的业务逻辑，实现 gRPC 生成的 OrderServiceServer。
//
// 本包的核心是 CreateOrder 的下单编排——Saga 模式的最小形态
package service

import (
	"context"

	"go.uber.org/zap"

	apperrors "go-ecom-admin/pkg/errors"
	"go-ecom-admin/pkg/identity"

	"go-ecom-admin/internal/order/model"
	"go-ecom-admin/internal/order/repository"
	orderpb "go-ecom-admin/proto/order"
)

type Service struct {
	orderpb.UnimplementedOrderServiceServer

	repo     repository.Repository
	outbox   repository.OutboxRepository
	products repository.ProductClient
	log      *zap.Logger
}

func New(repo repository.Repository, outbox repository.OutboxRepository, products repository.ProductClient, log *zap.Logger) *Service {
	return &Service{repo: repo, outbox: outbox, products: products, log: log}
}

// deductedItem 记录一笔已成功扣减，补偿时按它逐项回补。
type deductedItem struct {
	productID uint64
	quantity  int32
}

func (s *Service) CreateOrder(ctx context.Context, req *orderpb.CreateOrderRequest) (*orderpb.CreateOrderResponse, error) {
	// 先验身份再验参数（401 优先于 400），与 product-service 同一惯例。
	callerID, err := identity.FromIncoming(ctx)
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	if len(req.GetItems()) == 0 {
		return nil, apperrors.ToGRPCStatus(apperrors.InvalidArgument("items must not be empty", nil))
	}
	for _, it := range req.GetItems() {
		if it.GetProductId() == 0 || it.GetQuantity() <= 0 {
			return nil, apperrors.ToGRPCStatus(apperrors.InvalidArgument("product_id is required and quantity must be positive", nil))
		}
	}

	callCtx := identity.InjectOutgoing(ctx, callerID)

	var deducted []deductedItem
	order := &model.Order{
		UserID: callerID,
		Status: model.StatusPending,
	}
	for _, it := range req.GetItems() {
		resp, err := s.products.DeductStock(callCtx, it.GetProductId(), it.GetQuantity())
		if err != nil {
			// 第 N 项扣减失败：回补前 N-1 项，然后把原始错误（库存不足的
			// FailedPrecondition / 商品不存在的 NotFound）透传给调用方。
			s.compensate(callCtx, deducted)
			return nil, err
		}
		deducted = append(deducted, deductedItem{productID: it.GetProductId(), quantity: it.GetQuantity()})
		order.Items = append(order.Items, model.OrderItem{
			ProductID:      it.GetProductId(),
			ProductName:    resp.GetName(),
			Quantity:       it.GetQuantity(),
			UnitPriceCents: resp.GetPriceCents(), // 下单时刻价格快照
		})
		order.TotalCents += resp.GetPriceCents() * int64(it.GetQuantity())
	}

	// 第二步：落订单（orders + order_items 单事务）。失败同样全额回补。
	if err := s.repo.Create(ctx, order); err != nil {
		s.log.Error("create order failed after stock deducted, compensating",
			zap.Uint64("user_id", callerID), zap.Error(err))
		s.compensate(callCtx, deducted)
		return nil, apperrors.ToGRPCStatus(err)
	}

	s.log.Info("order created",
		zap.Uint64("order_id", order.ID),
		zap.Uint64("user_id", callerID),
		zap.Int64("total_cents", order.TotalCents),
		zap.Int("items", len(order.Items)),
	)
	return &orderpb.CreateOrderResponse{Order: toProto(order)}, nil
}

// compensate 逐项回补已扣库存。补偿失败的后果是"库存少了但订单没建成"——
// 数据不一致，必须留下足够醒目的日志供对账（生产环境还会配告警）。
// 补偿不回传错误：编排已经失败，调用方只需要知道原始失败原因。
func (s *Service) compensate(ctx context.Context, deducted []deductedItem) {
	for _, d := range deducted {
		// messageID 传空串：同步补偿走 product 侧的老语义（不幂等）——
		// 这里调用方自己保证只补一次，重试/去重是 outbox 路径的事。
		if err := s.products.RestoreStock(ctx, d.productID, d.quantity, ""); err != nil {
			s.log.Error("COMPENSATION FAILED: stock not restored, manual reconciliation required",
				zap.Uint64("product_id", d.productID),
				zap.Int32("quantity", d.quantity),
				zap.Error(err),
			)
		}
	}
}

func (s *Service) GetOrder(ctx context.Context, req *orderpb.GetOrderRequest) (*orderpb.GetOrderResponse, error) {
	callerID, err := identity.FromIncoming(ctx)
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	o, err := s.repo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	if o.UserID != callerID {
		s.log.Warn("order access denied",
			zap.Uint64("order_id", o.ID),
			zap.Uint64("owner_id", o.UserID),
			zap.Uint64("caller_id", callerID),
		)
		return nil, apperrors.ToGRPCStatus(apperrors.NotFound("order not found", nil))
	}

	return &orderpb.GetOrderResponse{Order: toProto(o)}, nil
}

// ListMyOrders 只返回调用者自己的订单，user_id 过滤从 repo 层就带上，
// 不存在"查全部再过滤"的越权窗口。
func (s *Service) ListMyOrders(ctx context.Context, req *orderpb.ListMyOrdersRequest) (*orderpb.ListMyOrdersResponse, error) {
	callerID, err := identity.FromIncoming(ctx)
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	orders, total, err := s.repo.ListByUser(ctx, callerID, int(req.GetPage()), int(req.GetPageSize()))
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	pbOrders := make([]*orderpb.Order, 0, len(orders))
	for _, o := range orders {
		pbOrders = append(pbOrders, toProto(o))
	}
	return &orderpb.ListMyOrdersResponse{Orders: pbOrders, Total: total}, nil
}

// CancelOrder
func (s *Service) CancelOrder(ctx context.Context, req *orderpb.CancelOrderRequest) (*orderpb.CancelOrderResponse, error) {
	callerID, err := identity.FromIncoming(ctx)
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	o, err := s.repo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	// 与 GetOrder 同一语义：越权返回 404，不暴露订单号存在性。
	if o.UserID != callerID {
		s.log.Warn("order cancel denied",
			zap.Uint64("order_id", o.ID),
			zap.Uint64("owner_id", o.UserID),
			zap.Uint64("caller_id", callerID),
		)
		return nil, apperrors.ToGRPCStatus(apperrors.NotFound("order not found", nil))
	}

	// 状态机守卫：只允许 PENDING → CANCELLED 单方向流转（提前 409，
	// 省一次注定失败的事务；repo 层的条件更新仍是并发兜底）。
	if o.Status != model.StatusPending {
		return nil, apperrors.ToGRPCStatus(apperrors.FailedPrecondition("only pending orders can be cancelled", nil))
	}

	// 每个订单项组装一条回补消息（FIFO 投递，逐项对应扣减记录）。
	// 构造逻辑收敛到 model.StockRestoreMessagesFor：手动取消和超时
	// 自动关单（consumer/order_timeout.go）两条路径共用同一份。
	messages := model.StockRestoreMessagesFor(o)

	// 一个事务：条件迁移状态（WHERE status='PENDING'）+ 批量写消息。
	// 并发取消只有一个能命中；不命中的整体回滚，返回 409。
	if err := s.outbox.CancelWithOutbox(ctx, o.ID, model.StatusPending, model.StatusCancelled, messages); err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	o.Status = model.StatusCancelled
	s.log.Info("order cancelled, outbox messages queued",
		zap.Uint64("order_id", o.ID),
		zap.Uint64("user_id", callerID),
		zap.Int("queued_items", len(messages)),
	)
	return &orderpb.CancelOrderResponse{Order: toProto(o)}, nil
}

func (s *Service) PayOrder(ctx context.Context, req *orderpb.PayOrderRequest) (*orderpb.PayOrderResponse, error) {
	callerID, err := identity.FromIncoming(ctx)
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	o, err := s.repo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	// 越权 404：与 GetOrder/CancelOrder 同一语义，不暴露订单存在性。
	if o.UserID != callerID {
		s.log.Warn("order pay denied",
			zap.Uint64("order_id", o.ID),
			zap.Uint64("owner_id", o.UserID),
			zap.Uint64("caller_id", callerID),
		)
		return nil, apperrors.ToGRPCStatus(apperrors.NotFound("order not found", nil))
	}

	if o.Status != model.StatusPending {
		return nil, apperrors.ToGRPCStatus(apperrors.FailedPrecondition("only pending orders can be paid", nil))
	}

	if err := s.repo.UpdateStatus(ctx, o.ID, model.StatusPending, model.StatusPaid); err != nil {
		return nil, apperrors.ToGRPCStatus(err)
	}

	o.Status = model.StatusPaid
	s.log.Info("order paid (simulated)",
		zap.Uint64("order_id", o.ID),
		zap.Uint64("user_id", callerID),
		zap.Int64("total_cents", o.TotalCents),
	)
	return &orderpb.PayOrderResponse{Order: toProto(o)}, nil
}

func toProto(o *model.Order) *orderpb.Order {
	items := make([]*orderpb.OrderItem, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, &orderpb.OrderItem{
			ProductId:      it.ProductID,
			ProductName:    it.ProductName,
			Quantity:       it.Quantity,
			UnitPriceCents: it.UnitPriceCents,
		})
	}
	return &orderpb.Order{
		Id:         o.ID,
		UserId:     o.UserID,
		Status:     statusToProto(o.Status),
		TotalCents: o.TotalCents,
		Items:      items,
		CreatedAt:  o.CreatedAt.Unix(),
	}
}

func statusToProto(status string) orderpb.OrderStatus {
	switch status {
	case model.StatusPending:
		return orderpb.OrderStatus_ORDER_STATUS_PENDING
	case model.StatusPaid:
		return orderpb.OrderStatus_ORDER_STATUS_PAID
	case model.StatusCancelled:
		return orderpb.OrderStatus_ORDER_STATUS_CANCELLED
	default:
		return orderpb.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
