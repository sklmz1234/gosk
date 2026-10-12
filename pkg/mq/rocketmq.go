// rocketmq.go：mq.go 接口的 RocketMQ 5.x 实现。
//
// 客户端是 rocketmq-clients/golang/v5（纯 Go，gRPC 协议），连的是
// Proxy（默认 8081）而不是 NameServer（9876）——5.x 客户端最常见的
// 连接错误。本地 broker 没开 ACL，Credentials 传 nil。
package mq

import (
	"context"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"go.uber.org/zap"
)

const (
	// receiveBatch 是单次 Receive 拉取的上限，与 relay 的 batch 同量级。
	receiveBatch int32 = 16
	// defaultInvisible：不 Ack 的消息对消费组"隐身"的时长，到期自动重投。
	// 客户端要求 > 20s，取 30s：业务失败（如下游 DB 抖动）后约半分钟重试，
	// 不会风暴也不会太迟。
	defaultInvisible = 30 * time.Second
	// receiveAwait：Receive 长轮询的等待时长，服务端无消息时挂 5s 返回空。
	receiveAwait = 5 * time.Second
	// receiveErrBackoff：Receive 本身失败（broker 抖动）后的休整间隔。
	receiveErrBackoff = 2 * time.Second
)

// NewPublisher 创建并启动生产者。topics 是该 producer 声明要发的 topic
//（5.x 客户端在握手时预绑定，broker 未开启 autoCreateTopic 时也能
// 正确路由）。broker 不可用时返回 error，调用方降级（relay 不启动，
// 消息滞留 outbox 表，进程重启后自愈——MQ 是通道不是正确性依赖）。
func NewPublisher(endpoint string, topics ...string) (Publisher, error) {
	p, err := rmq.NewProducer(&rmq.Config{Endpoint: endpoint}, rmq.WithTopics(topics...))
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &rocketPublisher{p: p}, nil
}

type rocketPublisher struct {
	p rmq.Producer
}

func (r *rocketPublisher) Send(ctx context.Context, m Message) error {
	msg := &rmq.Message{Topic: m.Topic, Body: m.Body}
	msg.SetKeys(m.Key)
	if m.Tag != "" {
		msg.SetTag(m.Tag)
	}
	if !m.DeliverAt.IsZero() {
		msg.SetDelayTimestamp(m.DeliverAt)
	}
	_, err := r.p.Send(ctx, msg)
	return err
}

func (r *rocketPublisher) Close() error {
	return r.p.GracefulStop()
}

// NewConsumer 创建并启动 SimpleConsumer（拉模式：Receive/Ack 显式
// 控制，与项目"显式优于隐式"的风格一致——PushConsumer 的重试语义
// 藏在客户端里，SimpleConsumer 的重投完全由 invisible 时长决定，可讲可测）。
// invisible <= 0 用默认值。
func NewConsumer(endpoint, group string, invisible time.Duration, topics ...string, ) (Consumer, error) {
	if invisible <= 0 {
		invisible = defaultInvisible
	}
	subs := make(map[string]*rmq.FilterExpression, len(topics))
	for _, t := range topics {
		subs[t] = rmq.SUB_ALL
	}
	sc, err := rmq.NewSimpleConsumer(&rmq.Config{
		Endpoint:      endpoint,
		ConsumerGroup: group,
	}, rmq.WithSimpleAwaitDuration(receiveAwait),
		rmq.WithSimpleSubscriptionExpressions(subs))
	if err != nil {
		return nil, err
	}
	if err := sc.Start(); err != nil {
		return nil, err
	}
	return &rocketConsumer{sc: sc, invisible: invisible}, nil
}

type rocketConsumer struct {
	sc        rmq.SimpleConsumer
	invisible time.Duration
}

// Run 是消费循环：长轮询拉取 → 逐条 handler → 成功 Ack / 失败不 Ack
//（invisible 到期重投）。ctx 取消即退出，由组合根的 shutdown 流程
// 触发。
func (c *rocketConsumer) Run(ctx context.Context, h Handler, ) error {
	log := zap.NewNop() // pkg 层不打日志：消费成败由 handler 侧（服务层）记录
	for {
		if ctx.Err() != nil {
			return nil
		}
		mvs, err := c.sc.Receive(ctx, receiveBatch, c.invisible)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// broker 抖动：退避后再拉，不退出进程。
			log.Warn("mq receive failed", zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(receiveErrBackoff):
			}
			continue
		}
		for _, mv := range mvs {
			d := Delivered{
				Topic:     mv.GetTopic(),
				MessageID: mv.GetMessageId(),
				Key:       firstString(mv.GetKeys()),
				Tag:       derefString(mv.GetTag()),
				Body:      mv.GetBody(),
			}
			if herr := h(ctx, d); herr != nil {
				// 不 Ack：invisible 到期自动重投。handler 返回 error 的
				// 语义见 mq.go 的 Handler 注释——只放瞬时失败进来。
				log.Warn("mq consume failed, will redeliver",
					zap.String("topic", d.Topic), zap.String("key", d.Key), zap.Error(herr))
				continue
			}
			if aerr := c.sc.Ack(ctx, mv); aerr != nil {
				// Ack 失败 = broker 没收到确认，到期重投，消费端幂等兜底。
				log.Warn("mq ack failed, message will redeliver",
					zap.String("topic", d.Topic), zap.String("key", d.Key), zap.Error(aerr))
			}
		}
	}
}

func (c *rocketConsumer) Close() error {
	return c.sc.GracefulStop()
}

func firstString(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	return ss[0]
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
