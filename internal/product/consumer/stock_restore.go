// Package consumer 承载 product 服务的 MQ 消费端。
//
// StockRestoreConsumer 是"取消回补链路 MQ 化"（Phase 1b）的下游：
// 消费 stock-restore topic 的回补消息，把取消订单对应的库存幂等加回。
package consumer

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go-ecom-admin/internal/product/repository"
	"go-ecom-admin/pkg/mq"
)

type StockRestoreConsumer struct {
	repo repository.Repository
	log  *zap.Logger
}

func NewStockRestoreConsumer(repo repository.Repository, log *zap.Logger) *StockRestoreConsumer {
	return &StockRestoreConsumer{repo: repo, log: log}
}

// stockRestorePayload 是 StockRestorePayload 在 product 侧的本地副本。
//
// 刻意不 import order/model：服务边界上的 DTO 各自定义，order 改字段
// 时 product 编译不报错、运行时 json 解码自然兼容——这是防"共享模型
// 耦合成单体"的纪律（进程内共享 struct 会把两个服务的 schema 演进
// 绑死）。
type stockRestorePayload struct {
	ProductID uint64 `json:"product_id"`
	Quantity  int32  `json:"quantity"`
	OrderID   uint64 `json:"order_id"`
}

// Handle 实现 mq.Handler。错误语义：
//   - nil：Ack。含"永久性失败"路径（商品已删等），ERROR 日志 + 丢弃。
//   - error：瞬时失败（DB 抖动），不 Ack 重投。
//
// 幂等：MQ 是至少一次投递，同一条消息可能重复消费（Ack 丢失、消费者
// 崩溃后重投）。RestoreStockIdempotent 用 message_id 在 stock_restores
// 去重表做主键插入，重复消费直接短路返回——重投安全是本链路能"最终
// 一致"的前提。
func (c *StockRestoreConsumer) Handle(ctx context.Context, d mq.Delivered) error {
	var env mq.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		c.log.Error("stock-restore: undecodable envelope, discarded",
			zap.String("topic", d.Topic), zap.String("message_id", d.MessageID), zap.Error(err))
		return nil
	}
	if env.Type != mq.TypeStockRestore {
		c.log.Error("stock-restore: unexpected message type, discarded",
			zap.String("topic", d.Topic), zap.String("message_id", d.MessageID), zap.String("type", env.Type))
		return nil
	}

	var p stockRestorePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		c.log.Error("stock-restore: undecodable payload, discarded",
			zap.String("message_id", env.MessageID), zap.Error(err))
		return nil
	}

	if _, err := c.repo.RestoreStockIdempotent(ctx, env.MessageID, p.ProductID, p.Quantity); err != nil {
		if status.Code(err) == codes.NotFound {
			// 商品已被删除：库存无处可补，重投也不会好。
			// 记 ERROR 供对账（这笔是真实差异：扣了库存没回补，商品
			// 又没了——等价于库存随商品一起销毁，业务上可接受），
			// Ack 丢弃。
			c.log.Error("stock-restore: product deleted, restore dropped",
				zap.Uint64("order_id", p.OrderID),
				zap.Uint64("product_id", p.ProductID),
				zap.Int32("quantity", p.Quantity),
			)
			return nil
		}
		return err
	}

	c.log.Info("stock restored",
		zap.Uint64("order_id", p.OrderID),
		zap.Uint64("product_id", p.ProductID),
		zap.Int32("quantity", p.Quantity),
	)
	return nil
}
