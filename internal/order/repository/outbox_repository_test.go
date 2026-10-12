// outbox_repository 单元测试：sqlite :memory: 跑真实 GORM 逻辑。

package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	apperrors "go-ecom-admin/pkg/errors"

	"go-ecom-admin/internal/order/model"
)

func newOutboxDB(t *testing.T) *outboxRepoHandle {
	t.Helper()
	db := newSQLiteDB(t)
	require.NoError(t, db.AutoMigrate(&model.OutboxMessage{}))
	return &outboxRepoHandle{repo: NewGormRepository(db).(*gormRepository), db: db}
}

// outboxRepoHandle 把 repo 和原始 db 一起带给测试（直接改库模拟退避到期、手动重置消息）。
type outboxRepoHandle struct {
	repo *gormRepository
	db   *gorm.DB
}

func pendingMessage(messageID string, nextRetry time.Time) *model.OutboxMessage {
	return &model.OutboxMessage{
		MessageID:   messageID,
		Type:        model.OutboxTypeStockRestore,
		Payload:     `{"product_id":1,"quantity":2,"order_id":10}`,
		Status:      model.OutboxStatusPending,
		NextRetryAt: nextRetry,
	}
}

func (h *outboxRepoHandle) insertMessage(t *testing.T, m *model.OutboxMessage) {
	t.Helper()
	require.NoError(t, h.db.Create(m).Error)
}

// CancelWithOutbox 的灵魂考点：状态迁移与消息"同生共死"——
// 迁移命中消息必落库；迁移不命中（并发取消）消息一条都不落。
func TestCancelWithOutbox_Atomicity(t *testing.T) {
	h := newOutboxDB(t)
	ctx := context.Background()

	order := &model.Order{UserID: 42, Status: model.StatusPending}
	require.NoError(t, h.repo.Create(ctx, order))

	msgs := []*model.OutboxMessage{
		pendingMessage("m-1", time.Now()),
		pendingMessage("m-2", time.Now()),
	}
	require.NoError(t, h.repo.CancelWithOutbox(ctx, order.ID, model.StatusPending, model.StatusCancelled, msgs))

	// 状态已迁移 + 消息都在（同一事务的两种写都生效）：
	// repo.Create 写入的 order.timeout 定时消息 + 取消写入的两条回补。
	o, err := h.repo.GetByID(ctx, order.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCancelled, o.Status)

	sess, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.Len(t, sess.Messages, 3)
	assert.Equal(t, model.OutboxTypeOrderTimeout, sess.Messages[0].Type, "Create 先写的超时消息排最前")
	require.NotNil(t, sess.Messages[0].DeliverAt, "下单即写入的定时消息必须带 DeliverAt")
	assert.Equal(t, "m-1", sess.Messages[1].MessageID, "按 id 升序 FIFO")
	assert.Equal(t, "m-2", sess.Messages[2].MessageID)
	// 显式及时关闭：单测的 sqlite 连接池锁成单连接（见 newSQLiteDB 注释），
	// session 持有事务期间其他 repo 调用会等连接——不留 defer 到函数尾。
	require.NoError(t, sess.Close())

	// 第二次取消：状态已不是 PENDING，FailedPrecondition 且不写消息。
	err = h.repo.CancelWithOutbox(ctx, order.ID, model.StatusPending, model.StatusCancelled,
		[]*model.OutboxMessage{pendingMessage("m-3", time.Now())})
	requireAppCode(t, err, apperrors.CodeFailedPrecondition)

	n, err := h.repo.CountPending(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n, "迁移失败的批次一条消息都不能落库（已有的 3 条不受影响）")
}

// Create 必须和支付超时消息"同生共死"：订单落库成功的同时写入
// order.timeout 定时消息（DeliverAt = CreatedAt + 15min）。
// 漏写 = 永远不会触发自动关单（静默漏关单）。
func TestCreate_WritesTimeoutMessage(t *testing.T) {
	h := newOutboxDB(t)
	ctx := context.Background()

	before := time.Now()
	order := &model.Order{UserID: 42, Status: model.StatusPending}
	require.NoError(t, h.repo.Create(ctx, order))

	var msgs []model.OutboxMessage
	require.NoError(t, h.db.Where("type = ?", model.OutboxTypeOrderTimeout).Find(&msgs).Error)
	require.Len(t, msgs, 1, "Create 恰好写一条超时消息")

	m := msgs[0]
	assert.Equal(t, model.OutboxStatusPending, m.Status)
	require.NotNil(t, m.DeliverAt)
	assert.WithinDuration(t, before.Add(model.OrderTimeoutDelay), *m.DeliverAt, time.Minute,
		"投递时刻 = 下单时刻 + 15 分钟")

	var p model.OrderTimeoutPayload
	require.NoError(t, json.Unmarshal([]byte(m.Payload), &p))
	assert.Equal(t, order.ID, p.OrderID)
}

// MarkFailed 的退避计算：每次失败 retry_count+1、next_retry_at 按指数推远。
func TestOutboxSession_MarkFailed_Backoff(t *testing.T) {
	h := newOutboxDB(t)
	ctx := context.Background()

	m := pendingMessage("m-1", time.Now().Add(-time.Minute))
	h.insertMessage(t, m)

	sess, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	require.NotNil(t, sess)

	before := time.Now()
	dead, err := sess.MarkFailed(ctx, m.ID, assert.AnError)
	require.NoError(t, err)
	assert.False(t, dead)
	require.NoError(t, sess.Close())

	// 重新读库验证：retry_count=1、next_retry_at ≈ now+2s（指数退避第一档）。
	var got model.OutboxMessage
	require.NoError(t, h.db.First(&got, m.ID).Error)
	assert.Equal(t, 1, got.RetryCount)
	assert.Equal(t, model.OutboxStatusPending, got.Status)
	assert.WithinDuration(t, before.Add(2*time.Second), got.NextRetryAt, 2*time.Second)

	// 退避中的消息不该被立刻抢到：next_retry_at 在未来。
	sess2, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	assert.Nil(t, sess2, "退避窗口内不可投递")
}

// retry 上限：连续失败到 MaxOutboxRetry 次进 DEAD，之后 Claim 不再捞出。
func TestOutboxSession_MarkFailed_DeadAfterMaxRetry(t *testing.T) {
	h := newOutboxDB(t)
	ctx := context.Background()

	m := pendingMessage("m-1", time.Now().Add(-time.Hour))
	h.insertMessage(t, m)

	// 跑满 MaxOutboxRetry-1 次失败（第 9 次），每次把 next_retry_at 拨回过去模拟退避到期。
	for i := 1; i < MaxOutboxRetry; i++ {
		sess, err := h.repo.ClaimPending(ctx, 10)
		require.NoError(t, err)
		require.NotNil(t, sess, "第 %d 轮应能抢到消息", i)

		dead, err := sess.MarkFailed(ctx, m.ID, assert.AnError)
		require.NoError(t, err)
		assert.False(t, dead, "第 %d 次失败还不该死信", i)
		require.NoError(t, sess.Close())

		// 手动把退避拨到期（模拟时间流逝，单测不等真实退避）。
		require.NoError(t, h.db.Model(&model.OutboxMessage{}).
			Where("id = ?", m.ID).
			Update("next_retry_at", time.Now().Add(-time.Second)).Error)
	}

	// 第 MaxOutboxRetry 次失败 → DEAD。
	sess, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	require.NotNil(t, sess)
	dead, err := sess.MarkFailed(ctx, m.ID, assert.AnError)
	require.NoError(t, err)
	assert.True(t, dead, "第 %d 次失败必须死信", MaxOutboxRetry)
	require.NoError(t, sess.Close())

	var got model.OutboxMessage
	require.NoError(t, h.db.First(&got, m.ID).Error)
	assert.Equal(t, model.OutboxStatusDead, got.Status)
	assert.Equal(t, MaxOutboxRetry, got.RetryCount)

	// 死信不再被 Claim——claim 只扫 PENDING。
	sess2, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	assert.Nil(t, sess2, "DEAD 消息不能再被投递")
}

// MarkSent 后消息出队（Claim 不再捞出），CountPending 归零。
func TestOutboxSession_MarkSent(t *testing.T) {
	h := newOutboxDB(t)
	ctx := context.Background()

	m := pendingMessage("m-1", time.Now())
	h.insertMessage(t, m)

	sess, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.NoError(t, sess.MarkSent(ctx, m.ID))
	require.NoError(t, sess.Close())

	var got model.OutboxMessage
	require.NoError(t, h.db.First(&got, m.ID).Error)
	assert.Equal(t, model.OutboxStatusSent, got.Status)
	require.NotNil(t, got.SentAt, "SENT 必须带投递时间戳")

	n, err := h.repo.CountPending(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)

	sess2, err := h.repo.ClaimPending(ctx, 10)
	require.NoError(t, err)
	assert.Nil(t, sess2)
}
