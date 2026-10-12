// Package outbox 承载本地消息表的投递端：一个内嵌在 order-service 里的
// relay goroutine，周期性扫描 outbox_messages，把到期的消息通过
// mq.Publisher 发到 RocketMQ（Phase 1b：投递通道从 gRPC 直连换成 MQ）。

package outbox

import (
	"context"
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"go-ecom-admin/internal/order/model"
	"go-ecom-admin/internal/order/repository"
	"go-ecom-admin/pkg/mq"
)

const (
	DefaultInterval = 2 * time.Second
	// DefaultBatchSize 是单轮最多投递的消息数，防单轮拖太久；
	// 积压大时每 tick 消化一批，逐步排空。
	DefaultBatchSize = 20

	deliverTimeout = 3 * time.Second
)

type Relay struct {
	outbox    repository.OutboxRepository
	publisher mq.Publisher
	log       *zap.Logger
	tracer    trace.Tracer
	interval  time.Duration
	batchSize int

	delivered metric.Int64Counter
	dead      metric.Int64Counter
	pending   metric.Int64Gauge
}

// NewRelay 组装 relay。interval/batchSize 传 0 用默认值。
// 依赖是 mq.Publisher 接口而不是具体 RocketMQ 客户端——单测注入
// fake，未来换 MQ 实现不动这里。
func NewRelay(outbox repository.OutboxRepository, publisher mq.Publisher, log *zap.Logger, interval time.Duration, batchSize int) *Relay {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	meter := otel.Meter("go-ecom-admin/order/outbox")

	delivered, _ := meter.Int64Counter("outbox_delivered_total")
	dead, _ := meter.Int64Counter("outbox_dead_total")
	pending, _ := meter.Int64Gauge("outbox_pending")

	return &Relay{
		outbox:    outbox,
		publisher: publisher,
		log:       log,
		tracer:    otel.GetTracerProvider().Tracer("go-ecom-admin/order/outbox"),
		interval:  interval,
		batchSize: batchSize,
		delivered: delivered,
		dead:      dead,
		pending:   pending,
	}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("outbox relay stopped")
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *Relay) tick(ctx context.Context) {
	ctx, span := r.tracer.Start(ctx, "outbox-relay.tick")
	defer span.End()

	// 先上报积压再抢消息（此时无事务持有连接）：PENDING 总量含退避
	// 等待中的，是"这个集群还有多少回没送达"的真实读数。
	if n, err := r.outbox.CountPending(ctx); err == nil && r.pending != nil {
		r.pending.Record(ctx, n)
	}

	sess, err := r.outbox.ClaimPending(ctx, r.batchSize)
	if err != nil {
		r.log.Error("outbox claim failed", zap.Error(err))
		span.SetStatus(codes.Error, "claim failed")
		return
	}
	if sess == nil {
		return // 本轮无可投递，最常见的路径
	}

	// 投递完成后无论成败都要 Close（提交标记）；漏掉 Close 会让事务
	// 悬着直到连接回收——行锁一直被持有，阻塞其他副本。
	defer func() {
		if err := sess.Close(); err != nil {
			r.log.Error("outbox session close failed", zap.Error(err))
		}
	}()

	for i := range sess.Messages {
		r.deliver(ctx, sess, &sess.Messages[i])
	}
}

// deliver 投递单条消息。成功 MarkSent；失败 MarkFailed（退避/死信在
// session 里算好，dead=true 时补 ERROR 日志）。
//
// Phase 1b 后投递目标从"product 的 gRPC RestoreStock"变成"RocketMQ 的
// topic"：relay 不再知道消息的业务含义（也不再反解 payload 成
// StockRestorePayload），只负责把 Envelope 原样上船。业务解码移到
// 消费端（product/consumer/stock_restore.go），投递端与消费端通过
// Envelope 的 type 字段解耦——这是"管道与内容分离"的落点。
func (r *Relay) deliver(ctx context.Context, sess *repository.OutboxSession, m *model.OutboxMessage) {
	ctx, span := r.tracer.Start(ctx, "outbox-relay.deliver",
		trace.WithAttributes(
			attribute.String("outbox.message_id", m.MessageID),
			attribute.String("outbox.type", m.Type),
			attribute.Int("outbox.retry_count", m.RetryCount),
		))
	defer span.End()

	// 永久性失败就地死信，不上 MQ：
	// 1. payload 不是合法 JSON——消费端必然解析失败，重试无意义；
	// 2. 未知消息类型——没有对应的 topic 可投（typo 场景），发布即丢失。
	topic, known := mq.TopicForType(m.Type)
	if !json.Valid([]byte(m.Payload)) || !known {
		r.log.Error("OUTBOX DEAD: undeliverable message",
			zap.Uint64("outbox_id", m.ID), zap.String("message_id", m.MessageID),
			zap.String("type", m.Type), zap.String("topic", topic))
		span.SetStatus(codes.Error, "undeliverable message")
		if err := sess.MarkDead(ctx, m.ID); err != nil {
			r.log.Error("mark dead after undeliverable message", zap.Error(err))
		}
		if r.dead != nil {
			r.dead.Add(ctx, 1)
		}
		return
	}

	// 组 Envelope 上船：MessageID 贯穿 outbox 表、MQ、消费端幂等表，
	// 是全链路对账与去重的唯一键。Key=MessageID 方便按消息查轨迹，
	// Tag=Type 方便消费端按类型过滤。
	env, err := json.Marshal(mq.Envelope{
		MessageID: m.MessageID,
		Type:      m.Type,
		Payload:   json.RawMessage(m.Payload),
	})
	if err != nil {
		r.log.Error("OUTBOX DEAD: failed to encode envelope",
			zap.Uint64("outbox_id", m.ID), zap.String("message_id", m.MessageID), zap.Error(err))
		span.SetStatus(codes.Error, "envelope encode failed")
		if err := sess.MarkDead(ctx, m.ID); err != nil {
			r.log.Error("mark dead after envelope encode failed", zap.Error(err))
		}
		if r.dead != nil {
			r.dead.Add(ctx, 1)
		}
		return
	}

	msg := mq.Message{
		Topic: topic,
		Key:   m.MessageID,
		Tag:   m.Type,
		Body:  env,
	}
	if m.DeliverAt != nil {
		// 定时消息（order.timeout）：RocketMQ 5.x timer wheel 秒级定时，
		// 到点前对消费者不可见。relay 扫到即发，延迟由 broker 承担——
		// 这比"relay 自己等 15 分钟再投"省了扫描周期内的额外误差。
		msg.DeliverAt = *m.DeliverAt
	}

	callCtx, cancel := context.WithTimeout(ctx, deliverTimeout)
	err = r.publisher.Send(callCtx, msg)
	cancel()
	if err == nil {
		if err := sess.MarkSent(ctx, m.ID); err != nil {
			r.log.Error("mark sent failed", zap.Uint64("outbox_id", m.ID), zap.Error(err))
			span.SetStatus(codes.Error, "mark sent failed")
			return
		}
		if r.delivered != nil {
			r.delivered.Add(ctx, 1)
		}
		span.SetAttributes(attribute.Bool("outbox.delivered", true))
		return
	}

	dead, markErr := sess.MarkFailed(ctx, m.ID, err)
	if markErr != nil {
		r.log.Error("mark failed errored", zap.Uint64("outbox_id", m.ID), zap.Error(markErr))
		span.SetStatus(codes.Error, "mark failed errored")
		return
	}
	span.SetStatus(codes.Error, err.Error())

	if dead {
		// 死信：重试耗尽（broker 长时间不可用等）。MQ 是投递通道不是
		// 正确性依赖，消息仍在 outbox 表里标 DEAD，可人工重放。
		r.log.Error("OUTBOX DEAD: delivery exhausted, manual action required",
			zap.Uint64("outbox_id", m.ID),
			zap.String("message_id", m.MessageID),
			zap.String("type", m.Type),
			zap.Int("retries", m.RetryCount+1),
			zap.Error(err),
		)
		if r.dead != nil {
			r.dead.Add(ctx, 1)
		}
		return
	}
	r.log.Warn("outbox delivery failed, will retry with backoff",
		zap.Uint64("outbox_id", m.ID),
		zap.String("message_id", m.MessageID),
		zap.String("type", m.Type),
		zap.Int("retry_count", m.RetryCount+1),
		zap.Error(err),
	)
}
