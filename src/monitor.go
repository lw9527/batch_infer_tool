package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Monitor struct {
	dbManager *DBManager
	taskID    string
}

func NewMonitor(db *DBManager, id string) *Monitor {
	return &Monitor{dbManager: db, taskID: id}
}

func (m *Monitor) Start() {
	fmt.Printf("\n====== 任务监控面板: %s ======\n", m.taskID)
	fmt.Println("按 Ctrl+C 退出监控 (不会停止后台任务)")
	
	for {
		m.printStatus()
		time.Sleep(5 * time.Second)
	}
}

func (m *Monitor) printStatus() {
	file, err := m.dbManager.GetFile(m.taskID)
	if err != nil || file == nil {
		fmt.Println("错误：找不到任务记录")
		return
	}

	total := len(file.Chunks)
	if total == 0 {
		fmt.Println("任务初始化中...")
		return
	}

	uploaded := 0
	processing := 0
	processed := 0
	failed := 0

	for _, c := range file.Chunks {
		switch c.Status {
		case ChunkStatusUploaded:
			uploaded++
		case ChunkStatusProcessing:
			processing++
		case ChunkStatusProcessed:
			processed++
		case ChunkStatusUploadFailed: // [修正] 删除了不存在的 ChunkStatusFailed
			failed++
		}
	}

	// 清屏
	fmt.Print("\033[H\033[2J")
	fmt.Printf("\n====== Task: %s ======\n", m.taskID)
	fmt.Printf("Status: %s\n", file.Status)
	fmt.Printf("Updated: %s\n", time.Now().Format("15:04:05"))
	fmt.Println("------------------------------")
	
	m.drawBar("Uploaded  ", uploaded+processing+processed, total)
	m.drawBar("Processed ", processed, total)
	
	if failed > 0 {
		fmt.Printf("\n[!] Failed Chunks: %d (Will retry automatically)\n", failed)
	}
	
	if processed == total {
		fmt.Println("\n[✔] 任务全部完成！结果已准备好。")
		fm := NewFileManager(m.dbManager)
		// [修正] 使用 MergeBatchResults 并传入正确的参数
		_, err := fm.MergeBatchResults(m.taskID, file.Retry)
		if err == nil {
			finalPath := filepath.Join(MERGED_DIR, m.taskID, "output.jsonl")
			fmt.Printf("结果文件: %s\n", finalPath)
		}
		os.Exit(0)
	}
}

func (m *Monitor) drawBar(label string, current, total int) {
	width := 20
	percent := float64(current) / float64(total)
	filled := int(percent * float64(width))
	
	bar := strings.Repeat("#", filled) + strings.Repeat("-", width-filled)
	fmt.Printf("%s [%s] %.0f%%\n", label, bar, percent*100)
}