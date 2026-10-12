// relay 单元测试：真实 sqlite repo（状态机是真的）+ 手写的
// fakePublisher（投递成败可控）。
//
// 不 mock outbox repo 的理由：relay 测试的重点是"投递结果如何反映到
// 消息状态"（SENT/退避/DEAD），中间隔一层 mock 只能把被测逻辑复述一遍。
// publisher 用手写 fake 而不是 mockery：接口只有两个方法，手写还能顺带
// 记录 Send 的入参做断言（发到哪个 topic、Key 是什么、延迟多久）。
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"gorm.io/gorm"

	"go-ecom-admin/internal/order/model"
	"go-ecom-admin/internal/order/repository"
	"go-ecom-admin/pkg/mq"
)

// fakePublisher 记录每次 Send 的消息和可配置的失败行为。
type fakePublisher struct {
	mu       sync.Mutex
	sent     []mq.Message
	fail     error // 非 nil 时 Send 返回它
	block    bool  // 模拟"半死不活"：挂到 ctx 超时
	closeErr error
}

func (f *fakePublisher) Send(ctx context.Context, m mq.Message) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.fail != nil {
		return f.fail
	}
	f.mu.Lock()
	f.sent = append(f.sent, m)
	f.mu.Unlock()
	return nil
}

func (f *fakePublisher) Close() error { return f.closeErr }

func newRelay(t *testing.T) (*Relay, *gorm.DB, *fakePublisher) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	// sqlite :memory: 连接池锁单连接的理由见 order repository 测试注释。
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&model.OutboxMessage{}))

	pub := &fakePublisher{}
	relay := NewRelay(repository.NewOutboxRepository(db), pub, zaptest.NewLogger(t), time.Hour, 10)
	return relay, db, pub
}

func insertPending(t *testing.T, db *gorm.DB, messageID string) *model.OutboxMessage {
	t.Helper()
	payload, err := json.Marshal(model.StockRestorePayload{ProductID: 7, Quantity: 3, OrderID: 1001})
	require.NoError(t, err)
	m := &model.OutboxMessage{
		MessageID:   messageID,
		Type:        model.OutboxTypeStockRestore,
		Payload:     string(payload),
		Status:      model.OutboxStatusPending,
		NextRetryAt: time.Now(),
	}
	require.NoError(t, db.Create(m).Error)
	return m
}

func loadMessage(t *testing.T, db *gorm.DB, id uint64) model.OutboxMessage {
	t.Helper()
	var m model.OutboxMessage
	require.NoError(t, db.First(&m, id).Error)
	return m
}

// 投递成功：消息置 SENT；发出去的是带 Envelope 的 stock-restore 消息，
// Key=MessageID（全链路对账键）、Tag=type、无延迟。
func TestRelay_DeliverSuccess_MarksSent(t *testing.T) {
	relay, db, pub := newRelay(t)
	ctx := context.Background()
	m := insertPending(t, db, "msg-1")

	relay.tick(ctx)

	got := loadMessage(t, db, m.ID)
	assert.Equal(t, model.OutboxStatusSent, got.Status)
	require.NotNil(t, got.SentAt)

	require.Len(t, pub.sent, 1)
	sent := pub.sent[0]
	assert.Equal(t, mq.TopicStockRestore, sent.Topic)
	assert.Equal(t, "msg-1", sent.Key)
	assert.Equal(t, model.OutboxTypeStockRestore, sent.Tag)
	assert.True(t, sent.DeliverAt.IsZero(), "非定时消息不带延迟")

	var env mq.Envelope
	require.NoError(t, json.Unmarshal(sent.Body, &env))
	assert.Equal(t, "msg-1", env.MessageID)
	assert.Equal(t, model.OutboxTypeStockRestore, env.Type)
	assert.JSONEq(t, `{"product_id":7,"quantity":3,"order_id":1001}`, string(env.Payload))
}

// 定时消息（order.timeout）：relay 原样透传 DeliverAt 给 broker，
// 自己不等待——延迟语义由 RocketMQ timer wheel 承担。
func TestRelay_TimeoutMessage_PassesDeliverAtThrough(t *testing.T) {
	relay, db, pub := newRelay(t)
	ctx := context.Background()

	deliverAt := time.Now().Add(model.OrderTimeoutDelay)
	payload, err := json.Marshal(model.OrderTimeoutPayload{OrderID: 42})
	require.NoError(t, err)
	m := &model.OutboxMessage{
		MessageID:   "msg-t",
		Type:        model.OutboxTypeOrderTimeout,
		Payload:     string(payload),
		Status:      model.OutboxStatusPending,
		NextRetryAt: time.Now(),
		DeliverAt:   &deliverAt,
	}
	require.NoError(t, db.Create(m).Error)

	relay.tick(ctx)

	require.Len(t, pub.sent, 1)
	sent := pub.sent[0]
	assert.Equal(t, mq.TopicOrderTimeout, sent.Topic)
	assert.Equal(t, model.OutboxTypeOrderTimeout, sent.Tag)
	assert.WithinDuration(t, deliverAt, sent.DeliverAt, time.Second,
		"DeliverAt 必须原样透传，relay 不截断也不顺延")
}

// 投递失败：retry_count+1、指数退避推远 next_retry_at；退避窗口内
// 下一轮 tick 不会再碰它。
func TestRelay_DeliverFailure_BackoffAndDefer(t *testing.T) {
	relay, db, pub := newRelay(t)
	ctx := context.Background()
	m := insertPending(t, db, "msg-1")
	pub.fail = errors.New("broker down")

	before := time.Now()
	relay.tick(ctx)

	got := loadMessage(t, db, m.ID)
	assert.Equal(t, model.OutboxStatusPending, got.Status)
	assert.Equal(t, 1, got.RetryCount)
	assert.WithinDuration(t, before.Add(2*time.Second), got.NextRetryAt, 2*time.Second)

	// 退避未到期：第二轮 tick 空转（sent 不增加）。
	relay.tick(ctx)
	assert.Len(t, pub.sent, 0)
}

// 连续失败到上限：DEAD 死信 + ERROR 日志（zaptest 捕获），之后不再投递。
func TestRelay_DeadAfterExhaustedRetries(t *testing.T) {
	relay, db, pub := newRelay(t)
	ctx := context.Background()
	m := insertPending(t, db, "msg-1")
	pub.fail = errors.New("broker down")

	// 第 1~10 次投递全部失败（每轮 tick 后手动把退避拨到期，模拟时间流逝）。
	for i := 1; i <= repository.MaxOutboxRetry; i++ {
		relay.tick(ctx)
		require.NoError(t, db.Model(&model.OutboxMessage{}).
			Where("id = ?", m.ID).
			Update("next_retry_at", time.Now().Add(-time.Second)).Error)
	}

	got := loadMessage(t, db, m.ID)
	assert.Equal(t, model.OutboxStatusDead, got.Status, "重试耗尽必须死信")

	// 死信后 tick 空转——sent 不再增加。
	relay.tick(ctx)
	assert.Len(t, pub.sent, 0)
}

// 投递调用必须带独立超时：broker"半死不活"（永不主动返回）时，
// relay 的进程级 ctx 没有 deadline，调用会永远阻塞——整个 batch 卡住、
// session 行锁一直被持有，其他副本也被堵死。修复后期望：~3s 快速失败，
// 按普通失败走退避（retry_count+1），消息留在 PENDING 等下一轮。
func TestRelay_DeliverTimeout_FailsFastAndRetries(t *testing.T) {
	relay, db, pub := newRelay(t)
	ctx := context.Background()
	m := insertPending(t, db, "msg-1")
	pub.block = true

	start := time.Now()
	relay.tick(ctx)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, deliverTimeout+2*time.Second,
		"投递调用必须有独立超时，不能挂在进程级 ctx 上")
	got := loadMessage(t, db, m.ID)
	assert.Equal(t, model.OutboxStatusPending, got.Status)
	assert.Equal(t, 1, got.RetryCount, "超时按普通失败计一次退避")
}

// 消息体损坏：不重试直接死信（重试一万次也不会好）。
func TestRelay_UndecodablePayload_GoesDead(t *testing.T) {
	relay, db, _ := newRelay(t)
	ctx := context.Background()

	m := insertPending(t, db, "msg-bad")
	require.NoError(t, db.Model(&model.OutboxMessage{}).
		Where("id = ?", m.ID).
		Update("payload", "{not json").Error)

	relay.tick(ctx)

	got := loadMessage(t, db, m.ID)
	assert.Equal(t, model.OutboxStatusDead, got.Status)
	assert.Zero(t, got.RetryCount, "直接死信不计退避次数")
}

// 未知消息类型：没有对应 topic，重试无意义，直接死信。
func TestRelay_UnknownType_GoesDead(t *testing.T) {
	relay, db, pub := newRelay(t)
	ctx := context.Background()

	m := insertPending(t, db, "msg-unknown")
	require.NoError(t, db.Model(&model.OutboxMessage{}).
		Where("id = ?", m.ID).
		Update("type", "who.am.i").Error)

	relay.tick(ctx)

	got := loadMessage(t, db, m.ID)
	assert.Equal(t, model.OutboxStatusDead, got.Status)
	assert.Len(t, pub.sent, 0, "未知类型不能发到任何 topic")
}
