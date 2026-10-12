// Package config 集中管理应用配置。

package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config 是整个应用的配置根节点，字段与 configs/config.yaml 的顶层 key 一一对应。
type Config struct {
	App           AppConfig           `mapstructure:"app"`
	Log           LogConfig           `mapstructure:"log"`
	MySQL         MySQLConfig         `mapstructure:"mysql"`
	Redis         RedisConfig         `mapstructure:"redis"`
	Elasticsearch ElasticsearchConfig `mapstructure:"elasticsearch"`
	RocketMQ      RocketMQConfig      `mapstructure:"rocketmq"`
	JWT           JWTConfig           `mapstructure:"jwt"`
	Server        ServerConfig        `mapstructure:"server"`
	GRPCClient    GRPCClientConfig    `mapstructure:"grpc_client"`
	Telemetry     TelemetryConfig     `mapstructure:"telemetry"`
}

type AppConfig struct {
	Name string `mapstructure:"name"`
	Env  string `mapstructure:"env"`
}

// LogConfig 直接映射到 pkg/logger 的初始化参数，配置和日志两个包解耦，
// logger 包不需要知道 Viper 的存在。
type LogConfig struct {
	Level      string   `mapstructure:"level"`
	Encoding   string   `mapstructure:"encoding"`
	OutputPath []string `mapstructure:"output_paths"`
}

type MySQLConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	DBName       string `mapstructure:"db_name"`
	Charset      string `mapstructure:"charset"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
}

func (m MySQLConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local",
		m.User, m.Password, m.Host, m.Port, m.DBName, m.Charset)
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type ElasticsearchConfig struct {
	Addr  string `mapstructure:"addr"`
	Index string `mapstructure:"index"`
}

// RocketMQConfig 是 5.x gRPC 客户端的接入点。ProxyAddr 指 Proxy（默认
// 8081），不是 NameServer（9876）——这是 5.x 客户端最常见的连接错误。
type RocketMQConfig struct {
	ProxyAddr string `mapstructure:"proxy_addr"`
}

type JWTConfig struct {
	Secret      string `mapstructure:"secret"`
	ExpireHours int    `mapstructure:"expire_hours"`
}

type ServerConfig struct {
	APIGateway     HTTPServerConfig `mapstructure:"api_gateway"`
	UserService    GRPCServerConfig `mapstructure:"user_service"`
	ProductService GRPCServerConfig `mapstructure:"product_service"`
	OrderService   GRPCServerConfig `mapstructure:"order_service"`
}

type HTTPServerConfig struct {
	HTTPPort int `mapstructure:"http_port"`
}

type GRPCServerConfig struct {
	GRPCPort int `mapstructure:"grpc_port"`
}

type GRPCClientConfig struct {
	UserServiceAddr    string `mapstructure:"user_service_addr"`
	ProductServiceAddr string `mapstructure:"product_service_addr"`
	OrderServiceAddr   string `mapstructure:"order_service_addr"`
}

type TelemetryConfig struct {
	Enabled      bool    `mapstructure:"enabled"`
	OTLPEndpoint string  `mapstructure:"otlp_endpoint"`
	SampleRatio  float64 `mapstructure:"sample_ratio"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("config: read config file %q: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config: unmarshal config: %w", err)
	}

	return &cfg, nil
}
