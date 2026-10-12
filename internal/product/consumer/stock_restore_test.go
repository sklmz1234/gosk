// stock_restore 消费者单元测试：mockery 的 MockRepository
// （RestoreStockIdempotent 的成败可控）。
//
// 测试重点是错误语义与幂等键的传递：Envelope.MessageID 必须原样
// 传给 RestoreStockIdempotent——它是 stock_restores 去重表的主键。
package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go-ecom-admin/internal/product/model"
	"go-ecom-admin/internal/product/repository/mocks"
	"go-ecom-admin/pkg/mq"
)

func newStockRestoreConsumer(t *testing.T) (*StockRestoreConsumer, *mocks.MockRepository) {
	t.Helper()
	repo := mocks.NewMockRepository(t)
	return NewStockRestoreConsumer(repo, zaptest.NewLogger(t)), repo
}

func stockDelivered(payload, messageID string) mq.Delivered {
	env, _ := json.Marshal(mq.Envelope{
		MessageID: messageID,
		Type:      mq.TypeStockRestore,
		Payload:   json.RawMessage(payload),
	})
	return mq.Delivered{Topic: "test", MessageID: messageID, Body: env}
}

// 正常回补：Envelope.MessageID 作为幂等键传给 repo。
func TestStockRestore_RestoresIdempotently(t *testing.T) {
	c, repo := newStockRestoreConsumer(t)
	ctx := context.Background()

	repo.EXPECT().RestoreStockIdempotent(ctx, "msg-1", uint64(7), int32(3)).
		Return(&model.Product{ID: 7}, nil)

	err := c.Handle(ctx, stockDelivered(`{"product_id":7,"quantity":3,"order_id":42}`, "msg-1"))
	assert.NoError(t, err)
}

// 商品已删：永久性失败，ERROR 日志 + Ack 丢弃，不重投。
func TestStockRestore_ProductDeleted_AcksAndDiscards(t *testing.T) {
	c, repo := newStockRestoreConsumer(t)
	ctx := context.Background()

	repo.EXPECT().RestoreStockIdempotent(ctx, "msg-1", uint64(7), int32(3)).
		Return(nil, status.Error(codes.NotFound, "product not found"))

	err := c.Handle(ctx, stockDelivered(`{"product_id":7,"quantity":3,"order_id":42}`, "msg-1"))
	assert.NoError(t, err, "商品已删是终态，重投无解，必须丢弃")
}

// DB 抖动：返回 error 让 MQ 重投（幂等表保证重投安全）。
func TestStockRestore_TransientError_Redelivers(t *testing.T) {
	c, repo := newStockRestoreConsumer(t)
	ctx := context.Background()

	repo.EXPECT().RestoreStockIdempotent(ctx, "msg-1", uint64(7), int32(3)).
		Return(nil, status.Error(codes.Unavailable, "db down"))

	err := c.Handle(ctx, stockDelivered(`{"product_id":7,"quantity":3,"order_id":42}`, "msg-1"))
	assert.Error(t, err, "瞬时失败必须重投")
}

// Envelope 损坏：毒消息，Ack 丢弃。
func TestStockRestore_CorruptedEnvelope_AcksAndDiscards(t *testing.T) {
	c, _ := newStockRestoreConsumer(t)

	err := c.Handle(context.Background(), mq.Delivered{
		Topic: mq.TopicStockRestore, MessageID: "bad", Body: []byte("{not json"),
	})
	assert.NoError(t, err, "解不开的消息必须丢弃，防无限重投")
}
