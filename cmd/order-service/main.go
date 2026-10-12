// Command order-service 启动 order 服务的 gRPC server。
// 结构与 cmd/user-service / cmd/product-service 完全对称，理由见那边的注释；
// 与 product-service 的唯一差异是没有 Redis 缓存层（订单是写多、按人隔离的
// 数据，读缓存命中率和收益都低，不引入）。
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormotel "gorm.io/plugin/opentelemetry/tracing"

	"go-ecom-admin/pkg/config"
	"go-ecom-admin/pkg/database"
	"go-ecom-admin/pkg/logger"
	"go-ecom-admin/pkg/telemetry"

	"go-ecom-admin/internal/order/consumer"
	"go-ecom-admin/internal/order/model"
	"go-ecom-admin/internal/order/outbox"
	"go-ecom-admin/internal/order/repository"
	"go-ecom-admin/internal/order/service"
	"go-ecom-admin/pkg/mq"
	orderpb "go-ecom-admin/proto/order"
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

	// ctx 提前到数据库连接之前创建：启动期重试若撞上 SIGTERM，
	// 重试循环要能立刻让位退出。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 链路追踪 + 指标
	var tracerProvider *sdktrace.TracerProvider
	var meterProvider *sdkmetric.MeterProvider
	var metricsHandler http.Handler
	if cfg.Telemetry.Enabled {
		tracerProvider, err = telemetry.Setup(ctx, telemetry.Config{
			ServiceName:  "order-service",
			OTLPEndpoint: cfg.Telemetry.OTLPEndpoint,
			SampleRatio:  cfg.Telemetry.SampleRatio,
		})
		if err != nil {
			log.Fatal("init telemetry", zap.Error(err))
		}
		meterProvider, metricsHandler, err = telemetry.SetupMetrics()
		if err != nil {
			log.Fatal("init metrics", zap.Error(err))
		}
	}

	// 带退避重试的连接
	db, err := database.ConnectWithRetry(ctx, func() (*gorm.DB, error) {
		return gorm.Open(mysql.Open(cfg.MySQL.DSN()), &gorm.Config{TranslateError: true})
	}, database.ConnectConfig{Log: log})
	if err != nil {
		log.Fatal("failed to connect to MySQL", zap.Error(err))
	}
	// SQL span 埋点，与其他服务一致。
	if err := db.Use(gormotel.NewPlugin(gormotel.WithoutQueryVariables())); err != nil {
		log.Fatal("attach gorm otel plugin", zap.Error(err))
	}
	// 带锁迁移，多副本并发建表竞态。
	if err := database.Migrate(db, 30*time.Second, &model.Order{}, &model.OrderItem{}, &model.OutboxMessage{}); err != nil {
		log.Fatal("auto migrate failed", zap.Error(err))
	}
	repo := repository.NewGormRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)

	// order-service 是 gRPC 客户端（调 product-service 扣库存）兼服务端。
	// 客户端侧必须带 otel stats handler，trace 才能接力到下游（见 product_client.go）。
	productClient, err := repository.NewProductClient(cfg.GRPCClient.ProductServiceAddr)
	if err != nil {
		log.Fatal("dial product-service", zap.String("addr", cfg.GRPCClient.ProductServiceAddr), zap.Error(err))
	}

	svc := service.New(repo, outboxRepo, productClient, log)

	// ── MQ 接线（Phase 1b + 2）────────────────────────────────────────
	// publisher 是 relay 的"船"：outbox 表里的消息（stock.restore 回补 +
	// order.timeout 定时关单）经它上 RocketMQ。声明要发的两个 topic。
	// broker 不可用时降级：relay 不启动，消息滞留 outbox 表，broker 恢复
	// 后重启进程自愈——MQ 是投递通道不是正确性依赖。
	var publisher mq.Publisher
	if cfg.RocketMQ.ProxyAddr != "" {
		publisher, err = mq.NewPublisher(cfg.RocketMQ.ProxyAddr, mq.TopicStockRestore, mq.TopicOrderTimeout)
		if err != nil {
			log.Warn("rocketmq publisher unavailable, outbox messages stay pending until restart",
				zap.String("endpoint", cfg.RocketMQ.ProxyAddr), zap.Error(err))
		}
	}

	if publisher != nil {
		// relay：本地消息表 → RocketMQ。
		relay := outbox.NewRelay(outboxRepo, publisher, log, 0, 0)
		go relay.Run(ctx)

		// 超时关单消费者：消费 order-timeout topic，对 15 分钟未支付的
		// 订单做系统取消（复用 CancelWithOutbox，回补消息走同一条
		// relay 链路）。broker 不可用时这里 Start 已经失败不了——
		// publisher 活着说明 proxy 可达；若消费组连接仍失败，降级为
		// 超时关单暂不可用，订单停在 PENDING 等人工，不影响其他功能。
		timeoutConsumer, cerr := mq.NewConsumer(cfg.RocketMQ.ProxyAddr, mq.GroupOrderTimeout, 0, mq.TopicOrderTimeout)
		if cerr != nil {
			log.Warn("rocketmq timeout consumer unavailable, order auto-cancel disabled",
				zap.String("endpoint", cfg.RocketMQ.ProxyAddr), zap.Error(cerr))
		} else {
			go func() {
				if err := timeoutConsumer.Run(ctx, consumer.NewTimeoutConsumer(repo, outboxRepo, log).Handle); err != nil {
					log.Error("order timeout consumer stopped", zap.Error(err))
				}
			}()
			defer timeoutConsumer.Close()
		}
		defer func() {
			if err := publisher.Close(); err != nil {
				log.Error("publisher close", zap.Error(err))
			}
		}()
	}

	// 服务端埋点。
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	orderpb.RegisterOrderServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	// 标准健康检查服务，供 K8s 原生 grpc 探针使用（同 user/product）。
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	addr := fmt.Sprintf(":%d", cfg.Server.OrderService.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen", zap.String("addr", addr), zap.Error(err))
	}

	// /metrics 运维端点：业务/运维端口分离（业务 9003，运维 9464）。
	var metricsSrv *http.Server
	if metricsHandler != nil {
		metricsSrv = telemetry.NewMetricsServer(metricsHandler)
		go func() {
			log.Info("metrics endpoint listening", zap.Int("port", telemetry.MetricsPort))
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("metrics server stopped with error", zap.Error(err))
			}
		}()
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down order-service")
		grpcServer.GracefulStop()
		if metricsSrv != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
				log.Error("metrics server shutdown", zap.Error(err))
			}
		}
		// 停服后 flush 未导出的 span / 指标
		if tracerProvider != nil {
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tracerProvider.Shutdown(flushCtx); err != nil {
				log.Error("trace provider shutdown", zap.Error(err))
			}
		}
		if meterProvider != nil {
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := meterProvider.Shutdown(flushCtx); err != nil {
				log.Error("meter provider shutdown", zap.Error(err))
			}
		}
	}()

	log.Info("order-service listening", zap.String("addr", addr))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("grpc server stopped with error", zap.Error(err))
	}
}
