// order_timeout 消费者单元测试：mockery 的 MockRepository /
// MockOutboxRepository（订单现状与 CancelWithOutbox 的成败可控）。
//
// 测试重点是 Handle 的错误语义：什么情况 Ack（返回 nil）、什么情况
// 重投（返回 error）——这直接决定消息会不会无限循环。
package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go-ecom-admin/internal/order/model"
	"go-ecom-admin/internal/order/repository/mocks"
	"go-ecom-admin/pkg/mq"
)

func newTimeoutConsumer(t *testing.T) (*TimeoutConsumer, *mocks.MockRepository, *mocks.MockOutboxRepository) {
	t.Helper()
	repo := mocks.NewMockRepository(t)
	outbox := mocks.NewMockOutboxRepository(t)
	return NewTimeoutConsumer(repo, outbox, zaptest.NewLogger(t)), repo, outbox
}

// 订单仍在 PENDING：取消 + 写回补消息，消费成功。
func TestTimeoutConsumer_PendingOrder_CancelsWithOutbox(t *testing.T) {
	c, repo, outbox := newTimeoutConsumer(t)
	ctx := context.Background()

	o := &model.Order{ID: 42, Status: model.StatusPending, Items: []model.OrderItem{
		{ProductID: 7, Quantity: 3},
	}}
	repo.EXPECT().GetByID(ctx, uint64(42)).Return(o, nil)
	outbox.EXPECT().CancelWithOutbox(ctx, uint64(42), model.StatusPending, model.StatusCancelled,
		mock.MatchedBy(func(msgs []*model.OutboxMessage) bool {
			if len(msgs) != 1 {
				return false
			}
			var p model.StockRestorePayload
			return json.Unmarshal([]byte(msgs[0].Payload), &p) == nil &&
				p.ProductID == 7 && p.Quantity == 3 && p.OrderID == 42
		})).Return(nil)

	err := c.Handle(ctx, delivered(mq.TypeOrderTimeout, `{"order_id":42}`, "msg-t"))
	assert.NoError(t, err, "成功关单必须 Ack")
}

// 订单已支付（或已取消）：不动作，正常 Ack——不是错误。
func TestTimeoutConsumer_NotPending_SkipsAndAcks(t *testing.T) {
	c, repo, _ := newTimeoutConsumer(t)
	ctx := context.Background()

	repo.EXPECT().GetByID(ctx, uint64(42)).
		Return(&model.Order{ID: 42, Status: model.StatusPaid}, nil)

	err := c.Handle(ctx, delivered(mq.TypeOrderTimeout, `{"order_id":42}`, "msg-t"))
	assert.NoError(t, err, "已处理的订单必须 Ack，否则无限重投")
}

// 订单不存在：丢弃（Ack），重投也变不出订单。
func TestTimeoutConsumer_NotFound_AcksAndDiscards(t *testing.T) {
	c, repo, _ := newTimeoutConsumer(t)
	ctx := context.Background()

	repo.EXPECT().GetByID(ctx, uint64(42)).
		Return(nil, status.Error(codes.NotFound, "order not found"))

	err := c.Handle(ctx, delivered(mq.TypeOrderTimeout, `{"order_id":42}`, "msg-t"))
	assert.NoError(t, err, "NotFound 必须丢弃，不能重投")
}

// 用户在到点瞬间手动取消了（条件更新不命中）：不是错误，Ack。
func TestTimeoutConsumer_ConcurrentCancel_Acks(t *testing.T) {
	c, repo, outbox := newTimeoutConsumer(t)
	ctx := context.Background()

	repo.EXPECT().GetByID(ctx, uint64(42)).
		Return(&model.Order{ID: 42, Status: model.StatusPending, Items: nil}, nil)
	outbox.EXPECT().CancelWithOutbox(mock.Anything, uint64(42),
		model.StatusPending, model.StatusCancelled, mock.Anything).
		Return(status.Error(codes.FailedPrecondition, "order status changed concurrently"))

	err := c.Handle(ctx, delivered(mq.TypeOrderTimeout, `{"order_id":42}`, "msg-t"))
	assert.NoError(t, err, "并发取消的输家必须 Ack——赢家已经处理完")
}

// DB 抖动：返回 error 让 MQ 重投。
func TestTimeoutConsumer_TransientError_Redelivers(t *testing.T) {
	c, repo, _ := newTimeoutConsumer(t)
	ctx := context.Background()

	repo.EXPECT().GetByID(ctx, uint64(42)).
		Return(nil, status.Error(codes.Unavailable, "db down"))

	err := c.Handle(ctx, delivered(mq.TypeOrderTimeout, `{"order_id":42}`, "msg-t"))
	assert.Error(t, err, "瞬时失败必须重投")
}

// Envelope 损坏：永久性失败，Ack 丢弃（毒消息防线）。
func TestTimeoutConsumer_CorruptedEnvelope_AcksAndDiscards(t *testing.T) {
	c, _, _ := newTimeoutConsumer(t)

	err := c.Handle(context.Background(), mq.Delivered{
		Topic: mq.TopicOrderTimeout, MessageID: "bad", Body: []byte("{not json"),
	})
	assert.NoError(t, err, "解不开的消息重投也无解，必须丢弃")
}

// delivered 是组装 Delivered 的小工具：Envelope 走与 relay 相同的编码。
func delivered(msgType, payload, messageID string) mq.Delivered {
	env, _ := json.Marshal(mq.Envelope{
		MessageID: messageID,
		Type:      msgType,
		Payload:   json.RawMessage(payload),
	})
	return mq.Delivered{
		Topic:     "test",
		MessageID: messageID,
		Body:      env,
	}
}
