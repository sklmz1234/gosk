// Package consumer 承载 order 服务的 MQ 消费端。
//
// TimeoutConsumer 是"支付超时自动关单"（Phase 2）的执行者：消费
// order-timeout topic 的定时消息（下单时经 outbox 投递、broker
// 延迟 15 分钟后放出），对仍是 PENDING 的订单做一次系统取消。
package consumer

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go-ecom-admin/internal/order/model"
	"go-ecom-admin/internal/order/repository"
	"go-ecom-admin/pkg/mq"
)

type TimeoutConsumer struct {
	repo   repository.Repository
	outbox repository.OutboxRepository
	log    *zap.Logger
}

func NewTimeoutConsumer(repo repository.Repository, outbox repository.OutboxRepository, log *zap.Logger) *TimeoutConsumer {
	return &TimeoutConsumer{repo: repo, outbox: outbox, log: log}
}

// Handle 实现 mq.Handler。错误语义（见 pkg/mq 的 Handler 注释）：
//   - nil：消费成功，Ack。包括"订单已不需要关"的正常路径（已支付/
//     已取消/不存在）——它们不是失败，重投也不会变出不同的结果。
//   - error：瞬时失败（DB 抖动等），不 Ack，invisible 到期重投。
//     消费端幂等由 CancelWithOutbox 的条件更新 + stock_restores
//     去重表兜底，重投安全。
func (c *TimeoutConsumer) Handle(ctx context.Context, d mq.Delivered) error {
	var env mq.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		// Envelope 损坏是永久性失败：重投一万次也解不开。
		// 记 ERROR（毒消息丢弃，人工对账）并 Ack，防止无限重投。
		c.log.Error("order-timeout: undecodable envelope, discarded",
			zap.String("topic", d.Topic), zap.String("message_id", d.MessageID), zap.Error(err))
		return nil
	}
	if env.Type != mq.TypeOrderTimeout {
		// topic 订阅的是 SUB_ALL，出现其他类型说明路由配错了，
		// 丢弃并告警，不能替别的消费者 Ack 掉它的消息。
		c.log.Error("order-timeout: unexpected message type, discarded",
			zap.String("topic", d.Topic), zap.String("message_id", d.MessageID), zap.String("type", env.Type))
		return nil
	}

	var p model.OrderTimeoutPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		c.log.Error("order-timeout: undecodable payload, discarded",
			zap.String("message_id", env.MessageID), zap.Error(err))
		return nil
	}

	o, err := c.repo.GetByID(ctx, p.OrderID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// 订单不存在：数据已手工清理或消息是陈年重投，丢弃。
			c.log.Warn("order-timeout: order not found, discarded",
				zap.Uint64("order_id", p.OrderID))
			return nil
		}
		return err // 查询失败视为瞬时，重投
	}

	// 15 分钟里订单可能已支付/已取消——非 PENDING 一律视为"这单已经
	// 处理完"，正常 Ack。不做任何动作。
	if o.Status != model.StatusPending {
		c.log.Info("order-timeout: order no longer pending, skipped",
			zap.Uint64("order_id", o.ID), zap.String("status", o.Status))
		return nil
	}

	// PENDING：系统取消。复用 CancelWithOutbox（条件更新 WHERE
	// status='PENDING' + 同事务写回补 outbox 消息），回补消息经 relay
	// 上 RocketMQ，走 product 消费端的幂等回补——即 Phase 1b 链路。
	messages := model.StockRestoreMessagesFor(o)
	if err := c.outbox.CancelWithOutbox(ctx, o.ID, model.StatusPending, model.StatusCancelled, messages); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			// 用户在定时消息到点的同一瞬间手动取消了：并发赢家已处理，
			// 不是错误。
			c.log.Info("order-timeout: order cancelled concurrently, skipped",
				zap.Uint64("order_id", o.ID))
			return nil
		}
		return err
	}

	c.log.Info("order auto-cancelled by timeout",
		zap.Uint64("order_id", o.ID),
		zap.Int("queued_items", len(messages)),
	)
	return nil
}
