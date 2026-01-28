package main

import (
	"fmt"
	"time"
)

func StartDaemon(db *DBManager, fm *FileManager, isInternal bool) {
	if !isInternal { fmt.Println("守护进程已启动，正在扫描任务...") }
	cm := NewChunkManager(db, fm, NewBatchManager())
	bm := NewBatchManager()

	for {
		files, err := db.GetAllFiles()
		if err != nil { time.Sleep(5 * time.Second); continue }

		activeCount := 0
		for _, file := range files {
			if file.Status == FileStatusProcessCompleted || file.Status == FileStatusFailed || file.Status == FileStatusCanceled { continue }
			activeCount++
			processSingleTask(db, fm, cm, bm, file)
		}
		if activeCount == 0 && !isInternal { time.Sleep(5 * time.Second) } else { time.Sleep(2 * time.Second) }
	}
}

func processSingleTask(db *DBManager, fm *FileManager, cm *ChunkManager, bm *BatchManager, file *FileInfo) {
	taskID := file.TaskID
	
	// 【新增】5分钟强制超时逻辑
	if file.Status == FileStatusStopping {
		updateTime, _ := time.Parse(time.RFC3339, file.UpdatedTime)
		if time.Since(updateTime) > 60*time.Second {
			logInfo("任务 [%s] 取消超时(>60秒)，强制结束。", taskID)
			db.ForceCancelTask(taskID, "Force canceled due to timeout")
			return
		}
	}

	allChunksDone := true
	if file.Status == FileStatusPending || file.Status == FileStatusSplitCompleted {
		db.UpdateFileStatus(taskID, FileStatusProcessing, nil)
	}

	for _, chunk := range file.Chunks {
		if chunk.Retry != file.Retry { continue }

		switch chunk.Status {
		case ChunkStatusPending:
			allChunksDone = false
			// 【新增】stopping 状态不上传
			if file.Status == FileStatusStopping { db.UpdateChunkStatus(chunk.ChunkID, ChunkStatusCanceled, nil); continue }
			logInfo("[%s] 开始上传...", chunk.ChunkID)
			cm.UploadChunk(chunk.ChunkID, nil)

		case ChunkStatusUploaded:
			allChunksDone = false
			// 【新增】stopping 状态不创建任务
			if file.Status == FileStatusStopping { db.UpdateChunkStatus(chunk.ChunkID, ChunkStatusCanceled, nil); continue }
			logInfo("[%s] 创建Batch...", chunk.ChunkID)
			cm.ChunkStartProcess(chunk.ChunkID)

		case ChunkStatusProcessing:
			allChunksDone = false
			cm.CheckChunkProcess(chunk.ChunkID)

		case ChunkStatusSuccess, ChunkStatusFailed, ChunkStatusCanceled:
			// done
		}
	}

	if allChunksDone {
		if file.Status == FileStatusStopping {
			db.UpdateFileStatus(taskID, FileStatusCanceled, nil)
			logInfo("任务 [%s] 已取消。", taskID)
			return
		}
		logInfo("任务 [%s] 本轮结束，合并中...", taskID)
		res, err := fm.MergeBatchResults(taskID, file.Retry)
		if err != nil { logError("合并失败: %v", err); return }

		retry, _ := fm.RetryFailedRecords(taskID)
		if !retry { 
			fatal := 0; if v, ok := res["fatal_count"].(int); ok { fatal = v }
			logInfo("任务 [%s] 全部结束 (致命错误: %d)", taskID, fatal) 
		} else {
			logInfo("任务 [%s] 触发重试...", taskID)
		}
	}
}