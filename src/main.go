package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

)

func main() {
	pipelineFile := flag.String("pipeline", "", "Run full pipeline")
	taskIDPtr := flag.String("task-id", "", "Specify Task ID")
	cancelID := flag.String("cancel", "", "Cancel a task")
	daemonMode := flag.Bool("daemon", false, "Start daemon")
	daemonInternal := flag.Bool("daemon-internal", false, "Internal flag")
	configPath := flag.String("config", "config.yaml", "Config path")
	flag.Parse()

	if err := LoadConfig(*configPath); err != nil {
		fmt.Printf("配置加载失败: %v\n", err)
	} else {
		isUIMode := *pipelineFile != ""
		initLoggerFiltered(isUIMode)
	}

	db := NewDBManager()

	if *cancelID != "" {
		if exists, _ := db.CheckTaskIDExists(*cancelID); !exists {
			fmt.Println("❌ 任务不存在")
			return
		}
		msg := "User canceled"
		db.UpdateFileStatus(*cancelID, FileStatusStopping, &msg)
		fmt.Printf("✅ 任务 [%s] 正在取消...\n", *cancelID)
		return
	}

	fm := NewFileManager(db)
	svc := NewBatchInferService(db, fm)

	if *daemonMode {
		svc.RunDaemon(false)
	} else if *daemonInternal {
		svc.RunDaemon(true)
	} else if *pipelineFile != "" {
		lines := LINES_PER_CHUNK
		svc.RunPipeline(*pipelineFile, &lines, *taskIDPtr)
	} else {
		fmt.Println("Usage: -pipeline <file> | -cancel <task_id>")
	}
}

func initLoggerFiltered(isUIMode bool) {
	if _, err := os.Stat(LOG_DIR); os.IsNotExist(err) { _ = os.MkdirAll(LOG_DIR, 0755) }
	logFileName := filepath.Join(LOG_DIR, "app.log")
	if isDaemonInternal() { logFileName = filepath.Join(LOG_DIR, "daemon.log") }
	logFile, _ = os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	var writer io.Writer
	if isDaemonInternal() {
		writer = logFile
	} else if isUIMode {
		writer = logFile
	} else {
		writer = io.MultiWriter(os.Stdout, logFile)
	}

	infoLogger = log.New(writer, "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger = log.New(writer, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
}

type BatchInferService struct {
	dbManager   *DBManager
	fileManager *FileManager
	progress    *ProgressDisplay
}

func NewBatchInferService(db *DBManager, fm *FileManager) *BatchInferService {
	// 初始化时，时间相关的字段为空，等待第一次 ShowStatus 时填充
	return &BatchInferService{db, fm, &ProgressDisplay{width: 25}}
}

func (bis *BatchInferService) RunPipeline(fileOrID string, lines *int, tid string) {
	var finalID string
	var err error
	if _, errStat := os.Stat(fileOrID); errStat == nil {
		finalID, err = bis.SplitFile(fileOrID, lines, tid)
		if err != nil {
			logError("启动失败: %v", err)
			return
		}
	} else {
		finalID = fileOrID
	}

	go func() { StartDaemon(bis.dbManager, bis.fileManager, true) }()
	bis.MonitorTask(finalID)
}

func (bis *BatchInferService) SplitFile(path string, lines *int, tid string) (string, error) {
	limit := LINES_PER_CHUNK
	if lines != nil && *lines > 0 { limit = *lines }
	info, err := bis.fileManager.SplitFile(path, filepath.Base(path), limit, tid)
	if err != nil { return "", err }
	return info.TaskID, nil
}

func (bis *BatchInferService) RunDaemon(internal bool) {
	StartDaemon(bis.dbManager, bis.fileManager, internal)
}

func (bis *BatchInferService) MonitorTask(tid string) {
	for {
		time.Sleep(1 * time.Second)
		f, err := bis.dbManager.GetFile(tid)
		if err != nil || f == nil {
			fmt.Printf("\rWaiting init %s...", tid)
			continue
		}
		bis.progress.ShowStatus(f, true)
		if f.Status == FileStatusProcessCompleted || f.Status == FileStatusFailed || f.Status == FileStatusCanceled {
			bis.progress.ShowStatus(f, false)
			break
		}
	}
}

// ProgressDisplay 升级版：支持动态预估时间
type ProgressDisplay struct {
	width         int
	sessionStart  time.Time // 本次监控开始的时间
	initialLines  int       // 本次监控开始时已完成的行数
	isInitialized bool      // 是否已初始化
}

func (p *ProgressDisplay) clear() {
	if runtime.GOOS == "windows" {
		fmt.Print("\033[H\033[2J")
	} else {
		fmt.Print("\033[H\033[2J")
	}
}

// formatDuration 辅助函数：将秒数转换为友好的时间字符串
func formatDuration(seconds float64) string {
	if seconds < 0 { return "计算中..." }
	if seconds > 86400 { return "> 24小时" } // 避免显示过大数值
	
	d := time.Duration(seconds) * time.Second
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		return fmt.Sprintf("%d小时 %d分 %d秒", h, m, s)
	} else if m > 0 {
		return fmt.Sprintf("%d分 %d秒", m, s)
	}
	return fmt.Sprintf("%d秒", s)
}

func (p *ProgressDisplay) ShowStatus(f *FileInfo, loop bool) {
	if loop { p.clear() }
	s := f.GetStatusSummary()
	t := s.Total

	// --- 动态时间计算逻辑 ---
	completed := t["complete_count"]
	
	if !p.isInitialized {
		// 第一次运行时，记录起点
		p.sessionStart = time.Now()
		p.initialLines = completed
		p.isInitialized = true
	}

	etaStr := "计算中..."
	speedStr := "0.0 行/秒"
	
	// 至少要有进度变化且运行超过1秒，才能计算准确速度
	processedInSession := completed - p.initialLines
	duration := time.Since(p.sessionStart).Seconds()

	if duration > 1.0 && processedInSession > 0 {
		speed := float64(processedInSession) / duration
		speedStr = fmt.Sprintf("%.1f 行/秒", speed)
		
		remainingLines := f.TotalLines - completed
		if remainingLines > 0 {
			etaSeconds := float64(remainingLines) / speed
			etaStr = formatDuration(etaSeconds)
		} else {
			etaStr = "即将完成"
		}
	} else if completed == f.TotalLines {
		etaStr = "已完成"
	}
	// -----------------------

	fmt.Printf("🚀 Task: %s | File: %s\n", f.TaskID, f.OriginalFilename)
	fmt.Printf("状态: %s | 轮次: Retry %d | 更新: %s\n", f.Status, f.Retry, time.Now().Format("15:04:05"))
	fmt.Println(strings.Repeat("-", 65))

	totalPer := 0.0
	if f.TotalLines > 0 {
		totalPer = float64(t["complete_count"]) / float64(f.TotalLines) * 100
	}
	
	// 显示总进度
	fmt.Printf("总进度  %s %6.2f%% (%d/%d 行)\n", p.bar(totalPer), totalPer, t["complete_count"], f.TotalLines)
	
	// 【新增】显示动态预估信息
	if f.Status == FileStatusProcessing || f.Status == FileStatusPending {
		fmt.Printf("⏱️ 预估: %s | ⚡ 速度: %s\n", etaStr, speedStr)
	}
	
	fmt.Println(strings.Repeat("-", 65))

	fmt.Printf("分块详细进度 (当前轮次 Retry %d):\n", f.Retry)
	displayLimit := 10
	count := 0
	
	for _, chunk := range f.Chunks {
		if chunk.Retry != f.Retry { continue }
		
		if count >= displayLimit {
			fmt.Printf("... 更多分块在后台运行\n")
			break
		}

		chunkPer := 0.0
		icon := "⏳"
		if chunk.BatchTaskInfo != nil && chunk.BatchTaskInfo.TotalCount > 0 {
			chunkPer = float64(chunk.BatchTaskInfo.CompletedCount) / float64(chunk.BatchTaskInfo.TotalCount) * 100
		}

		switch chunk.Status {
		case ChunkStatusSuccess:   icon = "✅"
		case ChunkStatusProcessing: icon = "🔄"
		case ChunkStatusFailed:     icon = "❌"
		case ChunkStatusUploaded:   icon = "☁️"
		case ChunkStatusPending:    icon = "⏳"
		case ChunkStatusUploadFailed: icon = "⚠️" // 增加这个图标方便辨认
		}

		fmt.Printf("块 %-2d %s %s %6.2f%%\n", chunk.ChunkIndex+1, icon, p.bar(chunkPer), chunkPer)
		count++
	}

	fmt.Println(strings.Repeat("-", 65))
	fmt.Printf("📦 待传:%d | ☁️ 已传:%d | 🔄 处理:%d | ✅ 完成:%d | ❌ 失败:%d\n",
		t["pending"], t["uploaded"], t["processing"], t["processed"], t["upload_failed"]+t["failed"])
}

func (p *ProgressDisplay) bar(percent float64) string {
	fill := int(percent / 100.0 * float64(p.width))
	if fill > p.width { fill = p.width }
	if fill < 0 { fill = 0 }
	return fmt.Sprintf("[%s%s]", strings.Repeat("█", fill), strings.Repeat(" ", p.width-fill))
}