// 路由层测试：验证"哪些接口需要登录"这张表本身。
// 订单三个接口全部要求登录——未带 token 的请求必须在 JWT 中间件被 401 拦下，
// 永远到不了 handler（所以测试里 handler 的 svc 传 nil 也安全）。
package router_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"go-ecom-admin/internal/gateway/handler"
	"go-ecom-admin/internal/gateway/router"
)

func TestOrders_RequireAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := handler.New(nil, zap.NewNop())
	r := router.New(h, "test-secret", zap.NewNop(), nil, nil)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"下单", http.MethodPost, "/api/v1/orders", `{"items":[{"product_id":1,"quantity":1}]}`},
		{"查订单列表", http.MethodGet, "/api/v1/orders", ""},
		{"查订单详情", http.MethodGet, "/api/v1/orders/1", ""},
		{"取消订单", http.MethodPost, "/api/v1/orders/1/cancel", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name+"未带token返回401", func(t *testing.T) {
			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}
			req := httptest.NewRequest(tt.method, tt.path, body)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
		})
	}
}
