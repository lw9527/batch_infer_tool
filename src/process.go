package main

import (
	"fmt"
	"time"
)

func StartDaemon(db *DBManager, fm *FileManager, isInternal bool) {
	if !isInternal {
		fmt.Println("守护进程已启动，正在扫描任务...")
	}
	cm := NewChunkManager(db, fm, NewBatchManager())
	bm := NewBatchManager()

	for {
		files, err := db.GetAllFiles()
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		activeCount := 0
		for _, file := range files {
			if file.Status == FileStatusProcessCompleted || file.Status == FileStatusFailed || file.Status == FileStatusCanceled {
				continue
			}
			activeCount++
			processSingleTask(db, fm, cm, bm, file)
		}
		if activeCount == 0 && !isInternal {
			time.Sleep(5 * time.Second)
		} else {
			time.Sleep(2 * time.Second)
		}
	}
}

func processSingleTask(db *DBManager, fm *FileManager, cm *ChunkManager, bm *BatchManager, file *FileInfo) {
	taskID := file.TaskID

	// 1. 超时取消逻辑 (60秒强制结束)
	if file.Status == FileStatusStopping {
		updateTime, _ := time.Parse(time.RFC3339, file.UpdatedTime)
		if time.Since(updateTime) > 60*time.Second {
			logInfo("任务 [%s] 取消超时(>60秒)，强制结束。", taskID)
			db.ForceCancelTask(taskID, "Force canceled due to timeout")
			return
		}
	}

	// 2. 初始化状态标志
	// 默认假设完成，但在遍历中发现任何异常状态都会将其置为 false
	allChunksDone := true
	hasZombieChunks := false 

	if file.Status == FileStatusPending || file.Status == FileStatusSplitCompleted {
		db.UpdateFileStatus(taskID, FileStatusProcessing, nil)
	}

	// 3. 遍历分块检查状态
	for _, chunk := range file.Chunks {
		
		// --- A. 检查版本滞后的"僵尸分块" ---
		if chunk.Retry != file.Retry {
			// 如果旧版本的块状态不是“最终态”，说明它掉队了，必须强制重置
			if chunk.Status == ChunkStatusPending || chunk.Status == ChunkStatusUploaded || 
			   chunk.Status == ChunkStatusProcessing || chunk.Status == ChunkStatusUploadFailed {
				
				logInfo("发现僵尸分块 [%s] (Retry %d < File %d)，标记为失败以触发重试...", chunk.ChunkID, chunk.Retry, file.Retry)
				
				// 强制标记为 Failed，以便 Merge 阶段能捕获它
				msg := "Zombie chunk reset"
				db.UpdateChunkStatus(chunk.ChunkID, ChunkStatusFailed, &msg)
				
				hasZombieChunks = true
				allChunksDone = false
			}
			// 对于已经 Success/Failed 的旧块，忽略之
			continue
		}

		// --- B. 处理当前版本的分块 ---
		switch chunk.Status {
		case ChunkStatusPending:
			allChunksDone = false
			if file.Status == FileStatusStopping {
				db.UpdateChunkStatus(chunk.ChunkID, ChunkStatusCanceled, nil)
				continue
			}
			logInfo("[%s] 开始上传...", chunk.ChunkID)
			cm.UploadChunk(chunk.ChunkID, nil)

		case ChunkStatusUploaded:
			allChunksDone = false
			if file.Status == FileStatusStopping {
				db.UpdateChunkStatus(chunk.ChunkID, ChunkStatusCanceled, nil)
				continue
			}
			logInfo("[%s] 创建Batch...", chunk.ChunkID)
			cm.ChunkStartProcess(chunk.ChunkID)

		case ChunkStatusProcessing:
			allChunksDone = false
			cm.CheckChunkProcess(chunk.ChunkID)

		// 【核心修复】上传失败必须视为"未完成"并立即重传
		case ChunkStatusUploadFailed:
			allChunksDone = false 
			logInfo("[%s] 检测到上传失败状态，正在尝试自动重传...", chunk.ChunkID)
			cm.UploadChunk(chunk.ChunkID, nil)

		// 只有以下状态才被视为本轮"Done"
		case ChunkStatusSuccess, ChunkStatusFailed, ChunkStatusCanceled:
			// do nothing, count as done
		}
	}

	// 4. 合并逻辑
	// 只有当所有块处理完毕，且没有僵尸块干扰时，才进行合并
	if allChunksDone && !hasZombieChunks {
		if file.Status == FileStatusStopping {
			db.UpdateFileStatus(taskID, FileStatusCanceled, nil)
			logInfo("任务 [%s] 已取消。", taskID)
			return
		}

		logInfo("任务 [%s] 本轮结束，合并中...", taskID)
		res, err := fm.MergeBatchResults(taskID, file.Retry)
		if err != nil {
			logError("合并失败: %v", err)
			return
		}

		retry, _ := fm.RetryFailedRecords(taskID)
		if !retry {
			fatal := 0
			if v, ok := res["fatal_count"].(int); ok {
				fatal = v
			}
			logInfo("任务 [%s] 全部结束 (致命错误: %d)", taskID, fatal)
		} else {
			logInfo("任务 [%s] 触发重试...", taskID)
		}
	}
}