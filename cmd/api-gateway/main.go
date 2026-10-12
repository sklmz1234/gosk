// Command api-gateway 启动 REST API 网关：接收 HTTP 请求，转发给 user-service /
// product-service 的 gRPC 接口。
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"

	"github.com/redis/go-redis/v9"

	"go-ecom-admin/pkg/cache"
	"go-ecom-admin/pkg/config"
	"go-ecom-admin/pkg/logger"
	"go-ecom-admin/pkg/telemetry"

	"go-ecom-admin/internal/gateway/handler"
	"go-ecom-admin/internal/gateway/repository"
	"go-ecom-admin/internal/gateway/router"
	"go-ecom-admin/internal/gateway/service"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	log, err := logger.New(logger.Config{
		Level:       cfg.Log.Level,
		Encoding:    cfg.Log.Encoding,
		OutputPaths: cfg.Log.OutputPath,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 链路追踪
	var tracerProvider *sdktrace.TracerProvider
	var meterProvider *sdkmetric.MeterProvider
	var metricsHandler http.Handler
	if cfg.Telemetry.Enabled {
		tracerProvider, err = telemetry.Setup(ctx, telemetry.Config{
			ServiceName:  "api-gateway",
			OTLPEndpoint: cfg.Telemetry.OTLPEndpoint,
			SampleRatio:  cfg.Telemetry.SampleRatio,
		})
		if err != nil {
			log.Fatal("init telemetry", zap.Error(err))
		}

		// 全局 MeterProvider 一设，otelgrpc 客户端的 gRPC 指标也自动出现。
		meterProvider, metricsHandler, err = telemetry.SetupMetrics()
		if err != nil {
			log.Fatal("init metrics", zap.Error(err))
		}
		log.Info("telemetry enabled",
			zap.String("otlp_endpoint", cfg.Telemetry.OTLPEndpoint),
			zap.Float64("sample_ratio", cfg.Telemetry.SampleRatio))
	}

	// grpc.NewClient 是非阻塞的
	userClient, err := repository.NewUserClient(cfg.GRPCClient.UserServiceAddr, log)
	if err != nil {
		log.Fatal("failed to create user-service client", zap.Error(err))
	}
	productClient, err := repository.NewProductClient(cfg.GRPCClient.ProductServiceAddr, log)
	if err != nil {
		log.Fatal("failed to create product-service client", zap.Error(err))
	}
	orderClient, err := repository.NewOrderClient(cfg.GRPCClient.OrderServiceAddr, log)
	if err != nil {
		log.Fatal("failed to create order-service client", zap.Error(err))
	}

	svc := service.New(userClient, productClient, orderClient)
	h := handler.New(svc, log)

	// 分布式限流的 Redis（可选依赖）：连不上时传 nil，路由层回落单机
	// 令牌桶——与 product-service 的缓存降级同一原则，加速层不是正确性依赖。
	var rdb *redis.Client
	if client, err := cache.New(cache.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB}); err != nil {
		log.Warn("redis unavailable, falling back to standalone rate limiter", zap.Error(err))
	} else {
		defer client.Close()
		rdb = client
	}

	engine := router.New(h, cfg.JWT.Secret, log, metricsHandler, rdb)

	addr := fmt.Sprintf(":%d", cfg.Server.APIGateway.HTTPPort)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: engine,
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down api-gateway")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful shutdown failed", zap.Error(err))
		}
		// HTTP 停完再 flush trace
		if tracerProvider != nil {
			if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
				log.Error("trace provider shutdown", zap.Error(err))
			}
		}
		// pull 模式的指标没有待发缓冲，Shutdown 只是停掉收集协程、
		// 干净退出
		if meterProvider != nil {
			if err := meterProvider.Shutdown(shutdownCtx); err != nil {
				log.Error("meter provider shutdown", zap.Error(err))
			}
		}
	}()

	log.Info("api-gateway listening", zap.String("addr", addr))
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("http server stopped with error", zap.Error(err))
	}
}
