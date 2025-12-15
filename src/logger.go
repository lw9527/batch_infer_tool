package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

var (
	infoLogger  *log.Logger
	errorLogger *log.Logger
	logFile     *os.File
)

func initLogger() {
	var err error
	logFileName := filepath.Join(LOG_DIR, "app.log")

	// 检查是否是守护进程内部运行（通过环境变量或命令行参数判断）
	// 如果是守护进程，使用 daemon.log 而不是 app.log
	if isDaemonInternal() {
		logFileName = filepath.Join(LOG_DIR, "daemon.log")
	}

	logFile, err = os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("无法打开日志文件:", err)
	}

	// 如果是守护进程内部运行，只输出到文件（因为 stdout 已经被重定向到文件）
	// 否则同时输出到文件和控制台
	var writer io.Writer
	if isDaemonInternal() {
		writer = logFile
	} else {
		writer = io.MultiWriter(os.Stdout, logFile)
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
		// 确保日志立即刷新到文件
		if logFile != nil {
			logFile.Sync()
		}
	}
}

func logError(format string, v ...interface{}) {
	if errorLogger != nil {
		errorLogger.Printf(format, v...)
		// 确保日志立即刷新到文件
		if logFile != nil {
			logFile.Sync()
		}
	}
}
