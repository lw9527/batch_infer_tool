package main

type ChunkManager struct {
	dbManager    *DBManager
	fileManager  *FileManager
	batchManager *BatchManager
}

func NewChunkManager(db *DBManager, fm *FileManager, bm *BatchManager) *ChunkManager {
	return &ChunkManager{dbManager: db, fileManager: fm, batchManager: bm}
}

// UploadChunk 上传文件块
func (cm *ChunkManager) UploadChunk(id string, data []byte) bool {
	c, _ := cm.dbManager.GetChunk(id)
	if c == nil { return false }

	if c.UploadFileID == nil {
		// 如果 data 为 nil，尝试从文件读取
		uid, err := cm.batchManager.UploadFile(c.ChunkPath)
		if err != nil {
			em := err.Error()
			cm.dbManager.UpdateChunkStatus(id, ChunkStatusUploadFailed, &em)
			return false
		}
		
		if err := cm.dbManager.UpdateChunkUploadSuccess(id, uid); err != nil {
			logError("严重错误：保存上传状态失败 [%s]: %v", id, err)
			return false 
		}
		c.UploadFileID = &uid
	}

	// 自动进入下一阶段
	cm.dbManager.UpdateChunkStatus(id, ChunkStatusUploaded, nil)
	return true
}

// ChunkStartProcess 开始处理
func (cm *ChunkManager) ChunkStartProcess(id string) bool {
	c, _ := cm.dbManager.GetChunk(id)
	if c == nil { return false }

	if c.Status != ChunkStatusUploaded || c.UploadFileID == nil { 
		return false 
	}
	
	if c.BatchID != nil { return true }

	bid, err := cm.batchManager.CreateBatchTask(*c.UploadFileID)
	if err != nil { 
		logError("创建Batch任务失败 [%s]: %v", id, err)
		return false 
	}
	
	cm.dbManager.UpdateChunkBatchID(id, bid)
	cm.dbManager.UpdateChunkStatus(id, ChunkStatusProcessing, nil)
	return true
}

// CheckChunkProcess 检查结果
func (cm *ChunkManager) CheckChunkProcess(id string) bool {
	c, _ := cm.dbManager.GetChunk(id)
	if c == nil || c.BatchID == nil { return false }
	
	if c.Status == ChunkStatusProcessed || c.Status == ChunkStatusCanceled { return true }
	
	res, err := cm.batchManager.GetResult(*c.BatchID)
	if err != nil || res == nil { return false }
	
	cm.dbManager.UpdateChunkBatchTaskInfo(id, res)
	
	if res.IsFinished() {
		// 【修复点 1】OutputFileID 是 string，直接判空，不需要解引用
		if res.OutputFileID != "" {
			data, err := cm.batchManager.GetFileContent(res.OutputFileID)
			if err == nil {
				cm.fileManager.SaveFile(c.TaskID, c.ChunkID, string(data), false)
			} else {
				logError("下载结果失败 [%s]: %v", id, err)
			}
		}

		// 【修复点 2】ErrorFileID 仍然是 *string，需要判 nil 和解引用
		if res.ErrorFileID != nil && *res.ErrorFileID != "" {
			data, err := cm.batchManager.GetFileContent(*res.ErrorFileID)
			if err == nil {
				cm.fileManager.SaveFile(c.TaskID, c.ChunkID, string(data), true)
			}
		}
		

		newStatus := ChunkStatusSuccess
		if res.Status == BatchStatusFailed || res.Status == BatchStatusExpired || res.Status == BatchStatusCanceled {
			// 在 CheckChunkProcess 函数中，更新状态前增加一行日志
			logInfo("[调试] 块 %s 状态变更: 旧状态=%s, 新状态=%s", id, c.Status, newStatus)
			newStatus = ChunkStatusFailed
		}
		
		cm.dbManager.UpdateChunkStatus(id, newStatus, nil)
		return true
	}
	
	return false
}