package config

import (
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server ServerConfig `yaml:"server"`
	Kafka  KafkaConfig  `yaml:"kafka"`
}

type ServerConfig struct {
	Addr            string        `yaml:"addr"`             // ":5555"
	ReadTimeout     time.Duration `yaml:"read_timeout"`     // 60s
	WriteTimeout    time.Duration `yaml:"write_timeout"`    // 5s
	MaxConnections  int           `yaml:"max_connections"`  // 20000
	MetricsAddr     string        `yaml:"metrics_addr"`     // ":9090"
}

type KafkaConfig struct {
	Brokers      []string      `yaml:"brokers"`
	TopicRaw     string        `yaml:"topic_raw"`
	TopicPos     string        `yaml:"topic_positions"`
	TopicEvents  string        `yaml:"topic_events"`
	BatchSize    int           `yaml:"batch_size"`    // messages per batch
	BatchTimeout time.Duration `yaml:"batch_timeout"` // max wait before flush
	Async        bool          `yaml:"async"`
}

func Load(path string) (*Config, error) {
	cfg := defaults()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}
	applyEnv(cfg)
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:           ":5555",
			ReadTimeout:    90 * time.Second,
			WriteTimeout:   5 * time.Second,
			MaxConnections: 20000,
			MetricsAddr:    ":9090",
		},
		Kafka: KafkaConfig{
			Brokers:      []string{"localhost:9092"},
			TopicRaw:     "egts.raw",
			TopicPos:     "egts.positions",
			TopicEvents:  "egts.events",
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			Async:        true,
		},
	}
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("EGTS_ADDR"); v != "" {
		cfg.Server.Addr = v
	}
	if v := os.Getenv("EGTS_METRICS_ADDR"); v != "" {
		cfg.Server.MetricsAddr = v
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = []string{v}
	}
	if v := os.Getenv("KAFKA_TOPIC_RAW"); v != "" {
		cfg.Kafka.TopicRaw = v
	}
	if v := os.Getenv("EGTS_MAX_CONN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Server.MaxConnections = n
		}
	}
}
