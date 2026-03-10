package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	maxLogFiles = 10 // 最多保留10个日志文件
)

var (
	// MaxLogFileSize 单个日志文件最大大小，默认100MB，可通过配置文件 max_log_file_size_mb 设置（单位MB）
	MaxLogFileSize int64 = 100 * 1024 * 1024
)

var (
	infoLogger  *log.Logger
	errorLogger *log.Logger
	logWriter   *RotatingWriter
)

// RotatingWriter 支持日志轮转的 Writer
type RotatingWriter struct {
	mu          sync.Mutex
	file        *os.File
	filePath    string
	currentSize int64
}

// NewRotatingWriter 创建一个支持轮转的日志写入器
func NewRotatingWriter(filePath string) (*RotatingWriter, error) {
	w := &RotatingWriter{
		filePath: filePath,
	}
	if err := w.openFile(); err != nil {
		return nil, err
	}
	return w, nil
}

// openFile 打开（或创建）日志文件，并记录当前文件大小
func (w *RotatingWriter) openFile() error {
	f, err := os.OpenFile(w.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.currentSize = info.Size()
	return nil
}

// Write 实现 io.Writer 接口，写入前检查是否需要轮转
func (w *RotatingWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentSize+int64(len(p)) > MaxLogFileSize {
		if err := w.rotate(); err != nil {
			// 轮转失败时仍然尝试写入当前文件
			fmt.Fprintf(os.Stderr, "日志轮转失败: %v\n", err)
		}
	}

	n, err = w.file.Write(p)
	w.currentSize += int64(n)

	// 确保日志立即刷新到文件
	w.file.Sync()
	return n, err
}

// rotate 执行日志轮转
func (w *RotatingWriter) rotate() error {
	// 关闭当前文件
	if w.file != nil {
		w.file.Close()
	}

	// 删除超出数量限制的旧日志文件
	w.removeOldLogs()

	// 重命名现有的日志文件（从大到小依次重命名，避免覆盖）
	dir := filepath.Dir(w.filePath)
	base := filepath.Base(w.filePath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	for i := maxLogFiles - 2; i >= 1; i-- {
		oldPath := filepath.Join(dir, fmt.Sprintf("%s.%d%s", name, i, ext))
		newPath := filepath.Join(dir, fmt.Sprintf("%s.%d%s", name, i+1, ext))
		if _, err := os.Stat(oldPath); err == nil {
			os.Rename(oldPath, newPath)
		}
	}

	// 将当前日志文件重命名为 .1
	backupPath := filepath.Join(dir, fmt.Sprintf("%s.%d%s", name, 1, ext))
	os.Rename(w.filePath, backupPath)

	// 打开新的日志文件
	return w.openFile()
}

// removeOldLogs 删除超出数量限制的旧日志文件
func (w *RotatingWriter) removeOldLogs() {
	dir := filepath.Dir(w.filePath)
	base := filepath.Base(w.filePath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	// 查找所有匹配的轮转日志文件
	pattern := filepath.Join(dir, fmt.Sprintf("%s.*%s", name, ext))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}

	// 按文件名排序（编号大的在后面）
	sort.Strings(matches)

	// 如果轮转文件数量超过限制，删除最旧的（编号最大的）
	if len(matches) >= maxLogFiles-1 {
		for _, f := range matches[maxLogFiles-2:] {
			os.Remove(f)
		}
	}
}

// Close 关闭日志文件
func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func initLogger() {
	logFileName := filepath.Join(LOG_DIR, "app.log")

	// 检查是否是守护进程内部运行（通过环境变量或命令行参数判断）
	// 如果是守护进程，使用 daemon.log 而不是 app.log
	if isDaemonInternal() {
		logFileName = filepath.Join(LOG_DIR, "daemon.log")
	}

	var err error
	logWriter, err = NewRotatingWriter(logFileName)
	if err != nil {
		log.Fatalln("无法打开日志文件:", err)
	}

	// 如果是守护进程内部运行，只输出到文件（因为 stdout 已经被重定向到文件）
	// 否则同时输出到文件和控制台
	var writer io.Writer
	if isDaemonInternal() {
		writer = logWriter
	} else {
		writer = io.MultiWriter(os.Stdout, logWriter)
	}
	infoLogger = log.New(writer, "[INFO] ", log.LstdFlags)
	errorLogger = log.New(writer, "[ERROR] ", log.LstdFlags)
}

// isDaemonInternal 检查是否是守护进程内部运行
func isDaemonInternal() bool {
	// 检查命令行参数中是否包含 -daemon-internal
	for _, arg := range os.Args {
		if arg == "-daemon-internal" {
			return true
		}
	}
	return false
}

func logInfo(format string, v ...interface{}) {
	if infoLogger != nil {
		// 如果有参数，使用 Printf；如果没有参数，使用 Print 避免格式化问题
		if len(v) == 0 {
			infoLogger.Print(format)
		} else {
			infoLogger.Printf(format, v...)
		}
	}
}

func logError(format string, v ...interface{}) {
	if errorLogger != nil {
		errorLogger.Printf(format, v...)
	}
}
