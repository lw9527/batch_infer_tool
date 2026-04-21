package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// 基础路径配置
var (
	BASE_DIR         string
	BATCH_RESULT_DIR string
	CHUNK_DIR        string
	MERGED_DIR       string
	DB_PATH          string
	LOG_DIR          string
)

var (
	TEST_LINES      = -1    // -1 不进行测试，其他数字为测试行数
	LINES_PER_CHUNK = 50000 // 默认每个分块50000行，不能超过这个值
	MAX_RETRY_COUNT = 0     // 最大重试次数（默认0，实际值从文件表的max_retry字段读取）
)

// ModelConfig Model 配置结构
type ModelConfig struct {
	Domain         string                 `yaml:"domain"`
	MaxTokens      int                    `yaml:"max_tokens"`
	MessagesKey    string                 `yaml:"messages_key"`
	Password       string                 `yaml:"password"`
	APIBaseURL     string                 `yaml:"api_base_url"` // 本地并发推理用 Chat Completions 根 URL，空则使用默认 MaaS v2 地址
	Temperature    *float64               `yaml:"temperature"`
	TopP           *float64               `yaml:"top_p"`
	EnableThinking *bool                  `yaml:"enable_thinking"`
	ExtraBody      map[string]interface{} `yaml:"extra_body"`
	Tools          []any                  `yaml:"tools"`
	ToolChoice     any                    `yaml:"tool_choice"`
}

// LocalInferConfig 本地 JSONL 并发推理（与 image.py 行为对齐）的可选调参
type LocalInferConfig struct {
	BatchLoadSize         int     `yaml:"batch_load_size"`         // 每批读入队列的任务数，默认 1000
	InitialWorkers        int     `yaml:"initial_workers"`         // Prometheus 返回 0 时的初始并发，默认 60
	ConcurrencyRatio      float64 `yaml:"concurrency_ratio"`       // 动态并发 = floor(部署路数 * ratio)，默认 0.7
	PrometheusIntervalSec int     `yaml:"prometheus_interval_sec"` // 刷新路数间隔秒，默认 5
	BandwidthMonitor      *bool   `yaml:"bandwidth_monitor"`       // 是否监控本地上/下行带宽，默认 true
	BandwidthIntervalSec  int     `yaml:"bandwidth_interval_sec"`  // 带宽采样间隔秒，默认 10
	HTTPTimeoutSec        int     `yaml:"http_timeout_sec"`        // 单次请求超时秒，默认 7200
	SubmitIntervalMs      int     `yaml:"submit_interval_ms"`      // 提交间隔毫秒，默认 100
	MaxPoolCap            int     `yaml:"max_pool_cap"`            // 内部池上限，默认 2000
	FlushEvery            int     `yaml:"flush_every"`             // 累计多少条刷盘，默认 10
}

// Config 配置结构
type Config struct {
	Model            ModelConfig       `yaml:"model"`
	LocalInfer       *LocalInferConfig `yaml:"local_infer"`
	TestLines        *int              `yaml:"test_lines"`           // -1 不进行测试，其他数字为测试行数
	MaxRetryCount    *int              `yaml:"max_retry_count"`      // 最大重试次数（默认0，实际值从文件表的max_retry字段读取）
	LinesPerChunk    *int              `yaml:"lines_per_chunk"`      // 默认每个分块50000行，不能超过这个值
	MaxLogFileSizeMB *int              `yaml:"max_log_file_size_mb"` // 单个日志文件最大大小（单位MB），默认100MB
}

// model 配置变量（从 YAML 文件加载）
var (
	ModelConf      ModelConfig
	LocalInferConf LocalInferConfig
)

// LoadConfig 从 YAML 文件加载配置
func LoadConfig(configPath string) error {
	// 如果配置文件不存在，使用默认值
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		configPath = "./config.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("解析配置文件失败: %v", err)
	}

	// 设置配置值
	ModelConf = config.Model
	if config.LocalInfer != nil {
		LocalInferConf = *config.LocalInfer
	}
	applyLocalInferDefaults(&LocalInferConf)

	// 验证配置
	if ModelConf.Domain == "" {
		return fmt.Errorf("配置文件中 domain 不能为空")
	}
	if ModelConf.MessagesKey == "" {
		return fmt.Errorf("配置文件中 messages_key 不能为空")
	}
	if ModelConf.Password == "" {
		return fmt.Errorf("配置文件中 password 不能为空")
	}
	if ModelConf.MaxTokens == 0 {
		return fmt.Errorf("配置文件中 max_tokens 不能为0")
	}

	// 设置测试行数、最大重试次数、每块行数（如果配置文件中指定了）
	if config.TestLines != nil {
		TEST_LINES = *config.TestLines
	}
	if config.MaxRetryCount != nil {
		MAX_RETRY_COUNT = *config.MaxRetryCount
	}
	if config.LinesPerChunk != nil {
		if *config.LinesPerChunk > 50000 {
			return fmt.Errorf("配置文件中 lines_per_chunk 不能超过 50000")
		}
		LINES_PER_CHUNK = *config.LinesPerChunk
	}
	if config.MaxLogFileSizeMB != nil {
		if *config.MaxLogFileSizeMB <= 0 {
			return fmt.Errorf("配置文件中 max_log_file_size_mb 必须大于0")
		}
		MaxLogFileSize = int64(*config.MaxLogFileSizeMB) * 1024 * 1024
		logInfo("日志文件大小限制设置为: %dMB", *config.MaxLogFileSizeMB)
	}

	logInfo("配置文件加载成功: %s", configPath)
	return nil
}

func applyLocalInferDefaults(c *LocalInferConfig) {
	if c.BatchLoadSize <= 0 {
		c.BatchLoadSize = 1000
	}
	if c.InitialWorkers <= 0 {
		c.InitialWorkers = 60
	}
	if c.ConcurrencyRatio <= 0 {
		c.ConcurrencyRatio = 0.7
	}
	if c.PrometheusIntervalSec <= 0 {
		c.PrometheusIntervalSec = 5
	}
	if c.BandwidthIntervalSec <= 0 {
		c.BandwidthIntervalSec = 10
	}
	if c.BandwidthMonitor == nil {
		v := true
		c.BandwidthMonitor = &v
	}
	if c.HTTPTimeoutSec <= 0 {
		c.HTTPTimeoutSec = 7200
	}
	if c.SubmitIntervalMs < 0 {
		c.SubmitIntervalMs = 100
	}
	if c.MaxPoolCap <= 0 {
		c.MaxPoolCap = 2000
	}
	if c.FlushEvery <= 0 {
		c.FlushEvery = 10
	}
}

func init() {
	// 获取可执行文件所在目录
	exePath, err := os.Executable()
	if err != nil {
		// 如果获取失败，使用当前工作目录
		exePath, _ = os.Getwd()
	}
	BASE_DIR = filepath.Dir(exePath)

	// 设置各个目录路径
	BATCH_RESULT_DIR = filepath.Join(BASE_DIR, "batch_result")
	CHUNK_DIR = filepath.Join(BASE_DIR, "chunks")
	MERGED_DIR = filepath.Join(BASE_DIR, "merged")
	DB_PATH = filepath.Join(BASE_DIR, "file_status.db")
	LOG_DIR = filepath.Join(BASE_DIR, "log")

	// 创建必要的目录
	os.MkdirAll(BATCH_RESULT_DIR, 0755)
	os.MkdirAll(CHUNK_DIR, 0755)
	os.MkdirAll(MERGED_DIR, 0755)
	os.MkdirAll(LOG_DIR, 0755)
}
