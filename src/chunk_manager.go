package main

// ChunkManager Chunk管理器
type ChunkManager struct {
	dbManager   *DBManager
	fileManager *FileManager
	batchManager *BatchManager
}

// NewChunkManager 创建Chunk管理器
func NewChunkManager(dbManager *DBManager, fileManager *FileManager, batchManager *BatchManager) *ChunkManager {
	return &ChunkManager{
		dbManager:    dbManager,
		fileManager:  fileManager,
		batchManager: batchManager,
	}
}

// UploadChunk 上传文件块
func (cm *ChunkManager) UploadChunk(chunkID string, fileData []byte) bool {
	// 获取文件块信息
	chunk, err := cm.dbManager.GetChunk(chunkID)
	if err != nil || chunk == nil {
		logError("文件块不存在: %s", chunkID)
		return false
	}

	// 上传文件，完成后直接更新为已上传状态
	if chunk.UploadFileID == nil {
		uploadFileID, err := cm.batchManager.UploadFile(chunk.ChunkPath)
		if err != nil {
			logError("上传文件块失败: %v", err)
			errorMsg := err.Error()
			cm.dbManager.UpdateChunkStatus(chunkID, ChunkStatusUploadFailed, &errorMsg)
			return false
		}
		
		if err := cm.dbManager.UpdateChunkUploadFileID(chunkID, uploadFileID); err != nil {
			logError("更新upload_file_id失败: %v", err)
			return false
		}
		chunk.UploadFileID = &uploadFileID
	}

	if err := cm.dbManager.UpdateChunkStatus(chunkID, ChunkStatusUploaded, nil); err != nil {
		logError("更新chunk状态失败: %v", err)
		return false
	}

	return true
}

// ChunkStartProcess 标记文件块为处理中
func (cm *ChunkManager) ChunkStartProcess(chunkID string) bool {
	chunk, err := cm.dbManager.GetChunk(chunkID)
	if err != nil || chunk == nil {
		logError("文件块不存在: %s", chunkID)
		return false
	}

	if chunk.UploadFileID == nil || chunk.Status != ChunkStatusUploaded {
		logError("文件块上传文件id不存在或状态不为已上传: %s", chunkID)
		return false
	}

	if chunk.BatchID != nil {
		logInfo("文件块batch_id已存在: %s", chunkID)
		return true
	}

	batchID, err := cm.batchManager.CreateBatchTask(*chunk.UploadFileID)
	if err != nil {
		logError("创建batch任务失败: %v", err)
		return false
	}

	if err := cm.dbManager.UpdateChunkBatchID(chunkID, batchID); err != nil {
		logError("更新batch_id失败: %v", err)
		return false
	}

	if err := cm.dbManager.UpdateChunkStatus(chunkID, ChunkStatusProcessing, nil); err != nil {
		logError("更新chunk状态失败: %v", err)
		return false
	}

	return true
}

// CheckChunkProcess 标记文件块为已处理
func (cm *ChunkManager) CheckChunkProcess(chunkID string) bool {
	chunk, err := cm.dbManager.GetChunk(chunkID)
	if err != nil || chunk == nil {
		logError("文件块不存在: %s", chunkID)
		return false
	}

	if chunk.BatchID == nil {
		logError("文件块batch_id不存在: %s", chunkID)
		return false
	}

	if chunk.Status == ChunkStatusProcessed || chunk.Status == ChunkStatusCanceled {
		return true
	}

	result, err := cm.batchManager.GetResult(*chunk.BatchID)
	if err != nil {
		logError("获取batch结果失败: %v", err)
		return false
	}

	if result == nil {
		return false
	}

	if chunk.Status == ChunkStatusUploaded {
		cm.dbManager.UpdateChunkStatus(chunkID, ChunkStatusProcessing, nil)
	}

	if err := cm.dbManager.UpdateChunkBatchTaskInfo(chunkID, result); err != nil {
		logError("更新batch_task_info失败: %v", err)
		return false
	}

	if result.IsFinished() {
		if result.OutputFileID != "" {
			content, err := cm.batchManager.GetFileContent(result.OutputFileID)
			if err == nil {
				cm.fileManager.SaveFile(chunk.FileID, chunk.ChunkID, content, false)
			}
		}

		if result.ErrorFileID != nil && *result.ErrorFileID != "" {
			content, err := cm.batchManager.GetFileContent(*result.ErrorFileID)
			if err == nil {
				cm.fileManager.SaveFile(chunk.FileID, chunk.ChunkID, content, true)
			}
		}

		if err := cm.dbManager.UpdateChunkStatus(chunkID, ChunkStatusProcessed, nil); err != nil {
			logError("更新chunk状态失败: %v", err)
			return false
		}

		return true
	}

	return false
}

