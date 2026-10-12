// Package mq 是消息中间件的接入层，与 pkg/cache 对称：业务代码只依赖
// 这里的接口与常量，RocketMQ 的客户端细节隔离在 rocketmq.go。
//
// 线格式（wire contract）：所有经 MQ 传递的消息都是 Envelope——
// message_id 是 outbox 侧生成的 uuid，也是消费端幂等去重的钥匙；
// payload 是业务 JSON，由各服务的 consumer 自行解码。
package mq

import (
	"context"
	"encoding/json"
	"time"
)

// Topic 与消费组：全项目唯一的命名登记处，生产端和消费端都从这里取，
// 避免两处字符串字面量对不上（topic 写错 = 消息发进黑洞）。
const (
	TopicStockRestore = "stock-restore"
	TopicOrderTimeout = "order-timeout"

	GroupStockRestore = "product-stock-restore"
	GroupOrderTimeout = "order-timeout-checker"
)

// 消息类型：与 outbox 表的 type 列一一对应（order/model 引用这里的
// 常量，单一事实来源）。
const (
	TypeStockRestore = "stock.restore"
	TypeOrderTimeout = "order.timeout"
)

// TopicForType 把 outbox 的消息类型映射到投递 topic。
// 第二个返回值 false = 未知类型（写代码时的笔误或未来的新类型还没接线），
// 调用方（relay）应对未知类型死信，而不是发进默认 topic 里悄悄丢掉。
func TopicForType(msgType string) (string, bool) {
	switch msgType {
	case TypeStockRestore:
		return TopicStockRestore, true
	case TypeOrderTimeout:
		return TopicOrderTimeout, true
	default:
		return "", false
	}
}

// Envelope 是 MQ 上传递的统一信封。
type Envelope struct {
	// MessageID 是 outbox 侧生成的 uuid：消费端拿它做幂等去重
	//（product 的 stock_restores 表主键），重复投递只生效一次。
	MessageID string `json:"message_id"`
	// Type 即 outbox 的消息类型，消费端按它分发。
	Type string `json:"type"`
	// Payload 是业务 JSON（如 StockRestorePayload / OrderTimeoutPayload）。
	Payload json.RawMessage `json:"payload"`
}

// Message 是业务侧发给 Publisher 的消息。DeliverAt 零值 = 立即投递；
// 非零 = RocketMQ 5.x 定时消息（任意秒级延迟，broker 的 timer wheel
// 负责到期投递——经典版本的 18 个固定延迟级别没有 15min 档，这是
// 选 5.x 定时消息的原因）。
type Message struct {
	Topic      string
	Key        string // 幂等键，dashboard 上按 key 检索消息轨迹
	Tag        string // 消息类型，dashboard 过滤用（与 Envelope.Type 同值）
	Body       []byte // Envelope 的 JSON
	DeliverAt  time.Time
}

// Publisher 是生产端抽象：relay 的唯一依赖，测试用假实现替换。
type Publisher interface {
	Send(ctx context.Context, m Message) error
	Close() error
}

// Delivered 是消费端拿到的消息（脱去客户端类型，handler 可单测）。
type Delivered struct {
	Topic     string
	MessageID string // broker 侧消息 id（与 Envelope.MessageID 不同：那是业务幂等键）
	Key       string
	Tag       string
	Body      []byte
}

// Handler 处理一条消息。返回 nil = 成功（Run 会 Ack）；
// 返回 error = 失败（不 Ack，invisible 到期后 broker 自动重投——
// 至少一次投递的"至少"来源，消费端必须幂等）。
//
// ⚠️ 永久性失败（payload 损坏、目标不存在等重试也不会好的）不要返回
// error——那会造成无限重投。Handler 应自己打 ERROR 日志并返回 nil
//（ack 丢弃，人工对账），与 relay 对不可解码 payload 直接死信同一策略。
type Handler func(ctx context.Context, d Delivered) error

// Consumer 是消费端抽象：Run 阻塞直到 ctx 取消（与 relay.Run 同一
// 生命周期模式，由各服务组合根的 goroutine 托管）。
type Consumer interface {
	Run(ctx context.Context, h Handler) error
	Close() error
}
