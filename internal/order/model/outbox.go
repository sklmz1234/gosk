// Package model 的 outbox.go：本地消息表模型（阶段 4）。
package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"go-ecom-admin/pkg/mq"
)

type OutboxMessage struct {
	// ID 自增主键，同时是投递顺序依据（FIFO）。
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	MessageID string `gorm:"column:message_id;type:varchar(36);not null;uniqueIndex"`
	Type      string `gorm:"column:type;type:varchar(32);not null"`
	// Payload 存 JSON（product_id/quantity/order_id）：消息体不与表结构耦合，
	// 未来加新消息类型不用 ALTER。用 datatypes.JSON 语义上更准，
	// 但 string + json.Marshal 更直白，也不用引 gorm.io/datatypes 依赖。
	Payload    string `gorm:"column:payload;type:json;not null"`
	Status     string `gorm:"column:status;type:varchar(16);not null;index"`
	RetryCount int    `gorm:"column:retry_count;not null;default:0"`
	// NextRetryAt 是退避重试的"下次可投递时间"：Claim 只扫
	// next_retry_at <= NOW() 的行，失败后按指数退避推远。
	// sqlite/MySQL 都用 time.Time 映射 datetime。
	NextRetryAt time.Time  `gorm:"column:next_retry_at;not null"`
	CreatedAt   time.Time  `gorm:"column:created_at;autoCreateTime"`
	SentAt      *time.Time `gorm:"column:sent_at"`
	// DeliverAt 是定时消息的"应投递时刻"：非定时消息为 NULL。
	// order.timeout 消息在 Create 订单时写入 now+15min，relay 透传给
	// RocketMQ 5.x 的 SetDelayTimestamp（timer wheel 秒级定时），
	// 到点前消息对消费者不可见。这就是"延迟消息实现支付超时关单"的落点。
	DeliverAt *time.Time `gorm:"column:deliver_at"`
}

func (OutboxMessage) TableName() string {
	return "outbox_messages"
}

// 消息状态机。status 用 string 存（与订单状态同一取舍：库里直接可读）。
const (
	OutboxStatusPending = "PENDING"
	OutboxStatusSent    = "SENT"
	OutboxStatusDead    = "DEAD"
)

// 消息类型常量直接引用 pkg/mq：topic 路由由 mq.TopicForType 做单一
// 事实来源，这里只保留 outbox 侧的别名，防止字符串漂移。
const (
	OutboxTypeStockRestore = mq.TypeStockRestore
	OutboxTypeOrderTimeout = mq.TypeOrderTimeout
)

// OrderTimeoutDelay 是支付超时窗口：下单后 15 分钟未支付即自动关单。
// 写死在 model 层而不是 config：它是业务规则（和"订单只能 PENDING→CANCELLED"
// 一样），不是部署环境差异。
const OrderTimeoutDelay = 15 * time.Minute

// StockRestorePayload 是 OutboxTypeStockRestore 的消息体。
type StockRestorePayload struct {
	ProductID uint64 `json:"product_id"`
	Quantity  int32  `json:"quantity"`
	OrderID   uint64 `json:"order_id"`
}

// OrderTimeoutPayload 是 OutboxTypeOrderTimeout 的消息体。
// 只带 OrderID：消费端（order 服务自己）会回查订单现状，
// 消息体不冗余 status——15 分钟里状态随时可能变。
type OrderTimeoutPayload struct {
	OrderID uint64 `json:"order_id"`
}

// StockRestoreMessagesFor 为一个订单的每个订单项组装一条回补消息。
// 从 service.CancelOrder 抽到 model：取消链路有两个人要造这组消息
// （用户手动取消 + 超时自动关单），逻辑必须只有一份。
func StockRestoreMessagesFor(o *Order) []*OutboxMessage {
	messages := make([]*OutboxMessage, 0, len(o.Items))
	for _, it := range o.Items {
		payload, _ := json.Marshal(StockRestorePayload{
			ProductID: it.ProductID,
			Quantity:  it.Quantity,
			OrderID:   o.ID,
		})
		messages = append(messages, &OutboxMessage{
			MessageID:   uuid.NewString(),
			Type:        OutboxTypeStockRestore,
			Payload:     string(payload),
			Status:      OutboxStatusPending,
			NextRetryAt: time.Now(),
		})
	}
	return messages
}

// TimeoutMessageFor 组装订单的支付超时定时消息。
// DeliverAt = 下单时刻 + OrderTimeoutDelay，relay 透传给 RocketMQ
// 的 SetDelayTimestamp；非定时消息该字段为 nil。
func TimeoutMessageFor(o *Order) (*OutboxMessage, error) {
	payload, err := json.Marshal(OrderTimeoutPayload{OrderID: o.ID})
	if err != nil {
		return nil, err
	}
	deliverAt := o.CreatedAt.Add(OrderTimeoutDelay)
	return &OutboxMessage{
		MessageID:   uuid.NewString(),
		Type:        OutboxTypeOrderTimeout,
		Payload:     string(payload),
		Status:      OutboxStatusPending,
		NextRetryAt: time.Now(),
		DeliverAt:   &deliverAt,
	}, nil
}
