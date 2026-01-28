package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// 全局 Logger 对象
var (
	infoLogger  *log.Logger
	errorLogger *log.Logger
	logFile     *os.File
)

// 【注意】这里删除了 const LOG_DIR = "logs"，因为它在 config.go 里已经定义了

// initLogger 初始化日志系统
func initLogger() {
	var err error

	// LOG_DIR 来自 config.go
	if _, err := os.Stat(LOG_DIR); os.IsNotExist(err) {
		_ = os.MkdirAll(LOG_DIR, 0755)
	}

	logFileName := filepath.Join(LOG_DIR, "app.log")

	if isDaemonInternal() {
		logFileName = filepath.Join(LOG_DIR, "daemon.log")
	}

	logFile, err = os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("无法打开日志文件:", err)
	}

	var writer io.Writer
	if isDaemonInternal() {
		writer = logFile
	} else {
		writer = io.MultiWriter(os.Stdout, logFile)
	}

	infoLogger = log.New(writer, "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger = log.New(writer, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
}

func isDaemonInternal() bool {
	for _, arg := range os.Args {
		if arg == "-daemon-internal" {
			return true
		}
	}
	return false
}

func syncLog() {
	if logFile != nil {
		_ = logFile.Sync()
	}
}

func logInfo(format string, v ...interface{}) {
	if infoLogger != nil {
		if len(v) == 0 {
			infoLogger.Print(format)
		} else {
			infoLogger.Printf(format, v...)
		}
		syncLog()
	}
}

func logError(format string, v ...interface{}) {
	if errorLogger != nil {
		errorLogger.Printf(format, v...)
		syncLog()
	}
}

// ==========================================
// TaskLogger 实现
// ==========================================

type TaskLogger struct {
	TaskID string
}

func NewTaskLogger(taskID string) *TaskLogger {
	return &TaskLogger{TaskID: taskID}
}

func (l *TaskLogger) Info(module string, format string, v ...interface{}) {
	if infoLogger == nil {
		return
	}
	msg := fmt.Sprintf(format, v...)
	prefix := fmt.Sprintf("[%s] [%s] - ", l.TaskID, module)
	_ = infoLogger.Output(2, prefix+msg)
	syncLog()
}

func (l *TaskLogger) Error(module string, format string, v ...interface{}) {
	if errorLogger == nil {
		return
	}
	msg := fmt.Sprintf(format, v...)
	prefix := fmt.Sprintf("[%s] [%s] - ", l.TaskID, module)
	_ = errorLogger.Output(2, prefix+msg)
	syncLog()
}

// LogTimeEstimate 计算并打印预估耗时
func (l *TaskLogger) LogTimeEstimate(totalLines int) {
	if infoLogger == nil {
		return
	}

	// 1. 计算秒数 (基于纯数学计算，不需要 time 包)
	estimatedSeconds := (float64(totalLines) / 100.0) * 36.0
	
	hours := int(estimatedSeconds / 3600)
	minutes := int((int(estimatedSeconds) % 3600) / 60)
	seconds := int(estimatedSeconds) % 60

	// 2. 格式化字符串
	var timeStr string
	if hours > 0 {
		timeStr = fmt.Sprintf("%d小时 %d分 %d秒", hours, minutes, seconds)
	} else if minutes > 0 {
		timeStr = fmt.Sprintf("%d分 %d秒", minutes, seconds)
	} else {
		timeStr = fmt.Sprintf("%d秒", seconds)
	}

	// 3. 输出
	msg := fmt.Sprintf("⏳ [预估耗时] 任务共 %d 行，预计耗时: %s (按每100行36秒估算)", totalLines, timeStr)
	
	fmt.Println(msg)
	l.Info("Estimate", msg)
}