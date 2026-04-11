package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// ChunkManager Chunk管理器
type ChunkManager struct {
	dbManager    *DBManager
	fileManager  *FileManager
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

// DownloadCanceledChunkResult 下载已取消chunk的结果（取消的batch任务可能已部分完成，需要下载已有结果）
func (cm *ChunkManager) DownloadCanceledChunkResult(chunkID string) bool {
	chunk, err := cm.dbManager.GetChunk(chunkID)
	if err != nil || chunk == nil {
		logError("文件块不存在: %s", chunkID)
		return false
	}

	// 先判断chunk是否为canceled状态
	if chunk.Status != ChunkStatusCanceled && chunk.Status != ChunkStatusProcessing {
		logInfo("chunk %s 状态为 %s，非canceled，跳过下载", chunkID, chunk.Status)
		return true
	}

	if chunk.BatchID == nil {
		// 没有batch任务，无需下载
		return true
	}

	// 优先使用本地已有的BatchTaskInfo，减少下游请求
	var result *BatchTaskInfo
	// if chunk.BatchTaskInfo != nil && chunk.BatchTaskInfo.Status == BatchStatusCanceled && (chunk.BatchTaskInfo.OutputFileID!="" || chunk.BatchTaskInfo.ErrorFileID != nil) {
	// 	// 本地BatchTaskInfo已经是canceled状态，无需再查询远端
	// 	result = chunk.BatchTaskInfo
	// 	logInfo("chunk %s 的BatchTaskInfo已为canceled，使用本地缓存", chunkID)
	// } else {
	// BatchTaskInfo不存在或状态不是canceled，需要查询远端获取最新状态
	result, err = cm.batchManager.GetResult(*chunk.BatchID)
	if err != nil {
		logError("获取已取消chunk的batch结果失败 %s: %v", chunkID, err)
		return false
	}

	if result == nil {
		logInfo("已取消chunk %s 的batch任务尚无结果", chunkID)
		return true
	}
	if result.CompletedCount > 0 && result.OutputFileID == "" {
		logInfo("CompletedCount %d OutputFileID 为空", result.CompletedCount)
		return false
	}
	// 更新本地batch任务信息
	if err := cm.dbManager.UpdateChunkBatchTaskInfo(chunkID, result); err != nil {
		logError("更新batch_task_info失败: %v", err)
		return false
	}
	// }

	// 下载已有的结果（无论batch是completed/canceled/failed，只要有输出就下载）
	// 先检查本地文件是否已存在，避免重复下载
	outputPath := filepath.Join(BATCH_RESULT_DIR, chunk.TaskID, "output", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
	errorPath := filepath.Join(BATCH_RESULT_DIR, chunk.TaskID, "error", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))

	if result.OutputFileID != "" {
		if _, statErr := os.Stat(outputPath); statErr == nil {
			logInfo("chunk %s 的输出结果文件已存在，跳过下载", chunkID)
		} else {
			content, err := cm.batchManager.GetFileContent(result.OutputFileID)
			if err == nil {
				err = cm.fileManager.SaveFile(chunk.TaskID, chunk.ChunkID, content, false)
				if err != nil {
					logError("保存结果失败: %v", err)
					return false
				} else {
					logInfo("已下载取消chunk %s 的输出结果", chunkID)
				}

			} else {
				logError("下载取消chunk %s 的输出结果失败: %v", chunkID, err)
				return false
			}
		}
	}

	if result.ErrorFileID != nil && *result.ErrorFileID != "" {
		if _, statErr := os.Stat(errorPath); statErr == nil {
			logInfo("chunk %s 的错误结果文件已存在，跳过下载", chunkID)
		} else {
			content, err := cm.batchManager.GetFileContent(*result.ErrorFileID)
			if err == nil {
				err = cm.fileManager.SaveFile(chunk.TaskID, chunk.ChunkID, content, true)
				if err != nil {
					logError("保存错误结果失败: %v", err)
					return false
				}
				logInfo("已下载取消chunk %s 的错误结果", chunkID)
			} else {
				logError("下载取消chunk %s 的错误结果失败: %v", chunkID, err)
				return false
			}
		}
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
				err = cm.fileManager.SaveFile(chunk.TaskID, chunk.ChunkID, content, false)
				if err != nil {
					logError("保存结果失败: %v", err)
					return false
				}
			} else {
				logError("下载结果失败: %v", err)
				return false
			}
		}

		if result.ErrorFileID != nil && *result.ErrorFileID != "" {
			content, err := cm.batchManager.GetFileContent(*result.ErrorFileID)
			if err == nil {
				err = cm.fileManager.SaveFile(chunk.TaskID, chunk.ChunkID, content, true)
				if err != nil {
					logError("保存结果失败: %v", err)
					return false
				}
			} else {
				logError("下载结果失败: %v", err)
				return false
			}
		}
		if chunk.Status == ChunkStatusProcessing {
			err = cm.dbManager.UpdateChunkStatus(chunkID, ChunkStatusProcessed, nil)
			if err != nil {
				logError("更新chunk状态失败: %v", err)
				return false
			}
		}
		return true
	}

	return false
}
