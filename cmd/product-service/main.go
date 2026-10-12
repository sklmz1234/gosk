// Command product-service 启动 product 服务的 gRPC server。
// 结构与 cmd/user-service/main.go 完全对称，理由见那边的注释。
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

	"go-ecom-admin/pkg/cache"
	"go-ecom-admin/pkg/config"
	"go-ecom-admin/pkg/database"
	"go-ecom-admin/pkg/logger"
	"go-ecom-admin/pkg/telemetry"

	"go-ecom-admin/internal/product/consumer"
	"go-ecom-admin/internal/product/model"
	"go-ecom-admin/internal/product/repository"
	"go-ecom-admin/internal/product/service"
	"go-ecom-admin/pkg/mq"
	productpb "go-ecom-admin/proto/product"
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
			ServiceName:  "product-service",
			OTLPEndpoint: cfg.Telemetry.OTLPEndpoint,
			SampleRatio:  cfg.Telemetry.SampleRatio,
		})
		if err != nil {
			log.Fatal("init telemetry", zap.Error(err))
		}
		// 指标支柱
		meterProvider, metricsHandler, err = telemetry.SetupMetrics()
		if err != nil {
			log.Fatal("init metrics", zap.Error(err))
		}
	}

	db, err := database.ConnectWithRetry(ctx, func() (*gorm.DB, error) {
		return gorm.Open(mysql.Open(cfg.MySQL.DSN()), &gorm.Config{TranslateError: true})
	}, database.ConnectConfig{Log: log})
	if err != nil {
		log.Fatal("failed to connect to MySQL", zap.Error(err))
	}
	// SQL span 埋点
	if err := db.Use(gormotel.NewPlugin(gormotel.WithoutQueryVariables())); err != nil {
		log.Fatal("attach gorm otel plugin", zap.Error(err))
	}
	// 带锁迁移多副本并发建表竞态。

	if err := database.Migrate(db, 30*time.Second, &model.Product{}, &model.StockRestore{}, &model.CartItem{}); err != nil {
		log.Fatal("auto migrate failed", zap.Error(err))
	}
	repo := repository.NewGormRepository(db)

	// 搜索装饰器
	esSearcher, err := repository.NewESSearcher(cfg.Elasticsearch.Addr, cfg.Elasticsearch.Index, log)
	if err != nil {
		log.Warn("elasticsearch unavailable, product search falls back to MySQL LIKE", zap.Error(err))
	} else {
		repo = repository.NewSearchRepository(repo, esSearcher, log)
		log.Info("elasticsearch search enabled", zap.String("addr", cfg.Elasticsearch.Addr), zap.String("index", cfg.Elasticsearch.Index))
	}

	// 缓存装饰器Redis 可用就把 gorm 实现包进 cache-aside 装饰器

	rdb, err := cache.New(cache.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err != nil {
		log.Warn("redis unavailable, product cache disabled (falling back to direct MySQL)", zap.Error(err))
	} else {
		defer rdb.Close()
		bloom := cache.NewBloomFilter(rdb, "bloom:product", 100000, 0.01)
		repo = repository.NewCachedRepository(repo, rdb, repository.NewStockGate(rdb), bloom)
		log.Info("product cache enabled", zap.String("redis", cfg.Redis.Addr))

		// 后台预热布隆过滤器：bloom 的 key 一旦丢失（FlushDB/重启清库），
		// "一定不存在"的判定会把所有真实商品误拦 404——启动重建是
		// 最低成本的对账。预热失败只降级（穿透由空值缓存兜底），不阻塞启动。
		go preloadBloom(repo, bloom, log)
	}

	svc := service.New(repo, log)

	cartSvc := service.NewCartService(repo, repository.NewGormCartRepository(db), log)

	// ── MQ 接线（Phase 1b）：stock-restore 消费者 ────────────────────
	// 消费取消订单的回补消息，走 RestoreStockIdempotent 幂等加回库存。
	// repo 是含 cache/bloom/ES 装饰器的整条链：RestoreStockIdempotent
	// 会穿透装饰器直达 gorm 层（回补后缓存失效由 cachedRepository 处理）。
	// broker 不可用时降级：消费者不启动，消息滞留 broker（invisible
	// 到期不断重投），进程重启后从断点续消——不 Fatal。
	if cfg.RocketMQ.ProxyAddr != "" {
		stockConsumer, err := mq.NewConsumer(cfg.RocketMQ.ProxyAddr, mq.GroupStockRestore, 0, mq.TopicStockRestore)
		if err != nil {
			log.Warn("rocketmq stock-restore consumer unavailable, messages stay in broker until restart",
				zap.String("endpoint", cfg.RocketMQ.ProxyAddr), zap.Error(err))
		} else {
			go func() {
				if err := stockConsumer.Run(ctx, consumer.NewStockRestoreConsumer(repo, log).Handle); err != nil {
					log.Error("stock-restore consumer stopped", zap.Error(err))
				}
			}()
			defer stockConsumer.Close()
		}
	}

	// 服务端埋点：提取上游 trace 上下文 + 每轮 RPC 建服务端 span。
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	productpb.RegisterProductServiceServer(grpcServer, svc)
	productpb.RegisterCartServiceServer(grpcServer, cartSvc)
	reflection.Register(grpcServer)

	// 同 user-service：注册标准健康检查服务，供 K8s 原生 grpc 探针使用，
	// 详见 cmd/user-service/main.go 里的注释。
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	addr := fmt.Sprintf(":%d", cfg.Server.ProductService.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen", zap.String("addr", addr), zap.Error(err))
	}

	// /metrics 运维端点，理由见 cmd/user-service/main.go：业务/运维端口分离。
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
		log.Info("shutting down product-service")
		grpcServer.GracefulStop()
		if metricsSrv != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
				log.Error("metrics server shutdown", zap.Error(err))
			}
		}
		// 停服后 flush 未导出的 span，理由见 api-gateway main.go。
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

	log.Info("product-service listening", zap.String("addr", addr))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("grpc server stopped with error", zap.Error(err))
	}
}

// preloadBloom 后台把存量商品 id 全量灌进布隆过滤器（启动对账）。
// 失败只降级——穿透防护退回空值缓存兜底，不影响服务可用性。
func preloadBloom(repo repository.Repository, bloom *cache.BloomFilter, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	loaded := 0
	for page := 1; ; page++ {
		products, _, err := repo.List(ctx, page, 500)
		if err != nil {
			log.Warn("bloom preload failed, penetration protection degraded to null caching",
				zap.Error(err), zap.Int("loaded", loaded))
			return
		}
		for _, p := range products {
			if err := bloom.Add(ctx, p.ID); err != nil {
				log.Warn("bloom preload add failed, penetration protection degraded to null caching",
					zap.Error(err), zap.Int("loaded", loaded))
				return
			}
			loaded++
		}
		if len(products) < 500 {
			break
		}
	}
	log.Info("bloom filter preloaded", zap.Int("products", loaded))
}
