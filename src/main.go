package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"golang.org/x/term"
)

func main() {
	pipelineFile := flag.String("pipeline", "", "Run full pipeline")
	taskIDPtr := flag.String("task-id", "", "Specify Task ID")
	cancelID := flag.String("cancel", "", "Cancel a task") // 【新增】
	daemonMode := flag.Bool("daemon", false, "Start daemon")
	daemonInternal := flag.Bool("daemon-internal", false, "Internal flag")
	configPath := flag.String("config", "config.yaml", "Config path")
	flag.Parse()

	if err := LoadConfig(*configPath); err != nil { fmt.Printf("配置加载失败: %v\n", err) } else { initLogger() }

	db := NewDBManager()

	// 【新增】处理取消
	if *cancelID != "" {
		if exists, _ := db.CheckTaskIDExists(*cancelID); !exists {
			fmt.Println("❌ 任务不存在")
			return
		}
		msg := "User canceled"
		db.UpdateFileStatus(*cancelID, FileStatusStopping, &msg)
		fmt.Printf("✅ 任务 [%s] 正在取消 (超时1分钟强制结束)...\n", *cancelID)
		return
	}

	fm := NewFileManager(db)
	svc := NewBatchInferService(db, fm)

	if *daemonMode { svc.RunDaemon(false)
	} else if *daemonInternal { svc.RunDaemon(true)
	} else if *pipelineFile != "" {
		lines := LINES_PER_CHUNK
		svc.RunPipeline(*pipelineFile, &lines, *taskIDPtr)
	} else {
		fmt.Println("Usage: -pipeline <file> | -cancel <task_id>")
	}
}

type BatchInferService struct {
	dbManager *DBManager; fileManager *FileManager; progress *ProgressDisplay
}
func NewBatchInferService(db *DBManager, fm *FileManager) *BatchInferService { return &BatchInferService{db, fm, &ProgressDisplay{}} }

func (bis *BatchInferService) RunPipeline(fileOrID string, lines *int, tid string) {
	var finalID string; var err error
	if _, errStat := os.Stat(fileOrID); errStat == nil {
		finalID, err = bis.SplitFile(fileOrID, lines, tid)
		if err != nil { logError("启动失败: %v", err); return }
	} else { finalID = fileOrID }

	go func() { StartDaemon(bis.dbManager, bis.fileManager, true) }()
	bis.MonitorTask(finalID)
}

func (bis *BatchInferService) SplitFile(path string, lines *int, tid string) (string, error) {
	limit := LINES_PER_CHUNK; if lines != nil && *lines > 0 { limit = *lines }
	info, err := bis.fileManager.SplitFile(path, filepath.Base(path), limit, tid)
	if err != nil { return "", err }
	return info.TaskID, nil
}

func (bis *BatchInferService) RunDaemon(internal bool) { StartDaemon(bis.dbManager, bis.fileManager, internal) }

func (bis *BatchInferService) MonitorTask(tid string) {
	for {
		time.Sleep(1 * time.Second)
		f, err := bis.dbManager.GetFile(tid)
		if err != nil || f == nil { fmt.Printf("\rWaiting init %s...", tid); continue }
		bis.progress.ShowStatus(f, true)
		if f.Status == FileStatusProcessCompleted || f.Status == FileStatusFailed || f.Status == FileStatusCanceled { bis.progress.ShowStatus(f, false); break }
	}
}

type ProgressDisplay struct{}
func (p *ProgressDisplay) clear() {
	cmd := exec.Command("clear"); if runtime.GOOS == "windows" { cmd = exec.Command("cmd", "/c", "cls") }
	cmd.Stdout = os.Stdout; cmd.Run()
}
func (p *ProgressDisplay) ShowStatus(f *FileInfo, loop bool) {
	if loop { p.clear() }
	s := f.GetStatusSummary(); t := s.Total
	estSec := (float64(f.TotalLines)/100.0)*36.0
	fmt.Printf("🚀 Task: %s | File: %s\n", f.TaskID, f.OriginalFilename)
	fmt.Println("------------------------------------------------")
	per := 0.0; if f.TotalLines > 0 { per = float64(t["complete_count"])/float64(f.TotalLines)*100 }
	fmt.Printf("进度: %s %.2f%% (%d/%d 行)\n\n", p.bar(int(per)), per, t["complete_count"], f.TotalLines)
	fmt.Printf("📦 待传:%d | ☁️ 已传:%d | 🔄 处理:%d | ✅ 完成:%d | ❌ 失败:%d\n", t["pending"], t["uploaded"], t["processing"], t["processed"], t["upload_failed"]+t["failed"])
	fmt.Printf("⏳ 预计耗时: %d秒\n", int(estSec))
	icon := "🟢"; if f.Status == FileStatusStopping { icon = "🛑" }; if f.Status == FileStatusCanceled { icon = "🚫" }
	fmt.Printf("\n状态: %s %s (重试: %d)\n", icon, f.Status, f.Retry)
	if f.ErrorMessage != "" { fmt.Printf("⚠️ %s\n", f.ErrorMessage) }
	fmt.Println("------------------------------------------------")
}
func (p *ProgressDisplay) bar(per int) string {
	fill := int(float64(per)/100.0*40); if fill>40{fill=40}; if fill<0{fill=0}
	return fmt.Sprintf("[%s%s]", strings.Repeat("█", fill), strings.Repeat("░", 40-fill))
}
func getTerminalWidth() int { w, _, _ := term.GetSize(int(os.Stdout.Fd())); if w <= 0 { return 80 }; return w }