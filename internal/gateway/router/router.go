// Package router 负责把 URL 路径映射到 handler 方法，是唯一知道具体路由表的地方。
// main.go 只需要调用 router.New(...) 拿到一个 *gin.Engine，不需要关心路由细节。
package router

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"go-ecom-admin/internal/gateway/handler"
	"go-ecom-admin/internal/gateway/middleware"
)

// 限流参数
const (
	rateLimitPerSecond rate.Limit = 10
	rateLimitBurst                = 20
)

// New 需要 jwtSecret 才能组装出鉴权中间件——路由表是唯一决定"哪些接口
// 需要登录"的地方，所以中间件的接入点也放在这里，而不是让每个 handler
// 自己判断要不要校验 token。

func New(h *handler.Handler, jwtSecret string, log *zap.Logger, metricsHandler http.Handler, rdb *redis.Client) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	r.Use(otelgin.Middleware("api-gateway"))
	r.Use(middleware.AccessLog(log))
	// HTTP 指标（QPS/P99/错误率的数据源）：标签用路由模式防基数爆炸，
	// 细节见 httpmetrics.go 的包注释。
	r.Use(middleware.HTTPMetrics())
	// 限流是所有中间件里最外层的一道：比 JWT 验签更早执行才省钱，
	// 且未登录接口（/auth/login）也受它保护。
	// 有 Redis 用分布式版（多副本共享额度），没有则回落单机令牌桶。
	if rdb != nil {
		r.Use(middleware.RateLimitRedis(rdb, rateLimitPerSecond, rateLimitBurst))
	} else {
		r.Use(middleware.RateLimit(rateLimitPerSecond, rateLimitBurst))
	}

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	if metricsHandler != nil {
		// 复用网关的 HTTP 端口暴露指标（两个 gRPC 服务则走独立 :9464，

		r.GET("/metrics", gin.WrapH(metricsHandler))
	}

	auth := middleware.Auth(jwtSecret)

	api := r.Group("/api/v1")
	{
		// /auth 下的接口本身就是"证明你是谁"的入口，不能反过来要求已经登录。
		authGroup := api.Group("/auth")
		authGroup.POST("/register", h.Register)
		authGroup.POST("/login", h.Login)

		users := api.Group("/users")
		users.GET("/:id", h.GetUser)

		// 商品的"读"保持公开（游客可浏览），"写"（新增/改/删）需要登录——
		// 这是本阶段唯一的权限模型，还没有细到"哪个用户能改哪个商品"。
		products := api.Group("/products")
		products.GET("/:id", h.GetProduct)
		products.GET("", h.ListProducts)
		products.POST("", auth, h.CreateProduct)
		products.PUT("/:id", auth, h.UpdateProduct)
		products.DELETE("/:id", auth, h.DeleteProduct)

		// 订单（阶段 3）全部要求登录，没有公开读——订单是私密资源，
		// 连"浏览"都必须是自己的订单。
		orders := api.Group("/orders")
		orders.POST("", auth, h.CreateOrder)
		orders.GET("", auth, h.ListMyOrders)
		orders.GET("/:id", auth, h.GetOrder)
		orders.POST("/:id/cancel", auth, h.CancelOrder)
		// 模拟支付
		orders.POST("/:id/pay", auth, h.PayOrder)

		cart := api.Group("/cart")
		cart.POST("/items", auth, h.AddCartItem)
		cart.GET("", auth, h.ListCart)
		cart.PUT("/items/:productId", auth, h.UpdateCartItem)
		cart.DELETE("/items", auth, h.RemoveCartItems)

		addresses := api.Group("/addresses", auth)
		addresses.GET("/random", h.GetRandomAddress)
	}

	return r
}
