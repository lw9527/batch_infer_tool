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
	Temperature    *float64               `yaml:"temperature"`
	TopP           *float64               `yaml:"top_p"`
	EnableThinking *bool                  `yaml:"enable_thinking"`
	ExtraBody      map[string]interface{} `yaml:"extra_body"`
}

// Config 配置结构
type Config struct {
	Model         ModelConfig `yaml:"model"`
	TestLines     *int        `yaml:"test_lines"`      // -1 不进行测试，其他数字为测试行数
	MaxRetryCount *int        `yaml:"max_retry_count"` // 最大重试次数（默认0，实际值从文件表的max_retry字段读取）
	LinesPerChunk *int        `yaml:"lines_per_chunk"` // 默认每个分块50000行，不能超过这个值
}

// model 配置变量（从 YAML 文件加载）
var (
	ModelConf ModelConfig
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

	logInfo("配置文件加载成功: %s", configPath)
	return nil
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
