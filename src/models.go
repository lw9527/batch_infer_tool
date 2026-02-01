package main

import (
	"encoding/json"
)

// ChunkStatus 文件块状态
type ChunkStatus string

const (
	ChunkStatusPending      ChunkStatus = "pending"
	ChunkStatusUploaded     ChunkStatus = "uploaded"
	ChunkStatusProcessing   ChunkStatus = "processing"
	ChunkStatusProcessed    ChunkStatus = "processed"
	ChunkStatusUploadFailed ChunkStatus = "upload_failed"
	ChunkStatusCanceled     ChunkStatus = "canceled"
)

// FileStatus 文件状态
type FileStatus string

const (
	FileStatusSplitting        FileStatus = "splitting"
	FileStatusSplitCompleted   FileStatus = "split_completed"
	FileStatusProcessing       FileStatus = "processing"
	FileStatusProcessCompleted FileStatus = "process_completed"
	FileStatusCanceled         FileStatus = "canceled"
	FileStatusFailed           FileStatus = "failed"
)

// BatchStatus 批处理任务状态
type BatchStatus string

const (
	BatchStatusQueueing   BatchStatus = "queueing"
	BatchStatusFailed     BatchStatus = "failed"
	BatchStatusInProgress BatchStatus = "in_progress"
	BatchStatusFinalizing BatchStatus = "finalizing"
	BatchStatusCompleted  BatchStatus = "completed"
	BatchStatusExpired    BatchStatus = "expired"
	BatchStatusCanceled   BatchStatus = "canceled"
)

// FileChunk 文件块信息
type FileChunk struct {
	ChunkID        string         `json:"chunk_id"`
	TaskID         string         `json:"task_id"`
	ChunkIndex     int            `json:"chunk_index"`
	ChunkPath      string         `json:"chunk_path"`
	ChunkSize      int            `json:"chunk_size"`
	Status         ChunkStatus    `json:"status"`
	UploadFileID   *string        `json:"upload_file_id,omitempty"`
	BatchID        *string        `json:"batch_id,omitempty"`
	UploadTime     *string        `json:"upload_time,omitempty"`
	ProcessTime    *string        `json:"process_time,omitempty"`
	BatchStartTime *string        `json:"batch_start_time,omitempty"`
	ErrorMessage   *string        `json:"error_message,omitempty"`
	BatchTaskInfo  *BatchTaskInfo `json:"batch_task_info,omitempty"`
	Retry          int            `json:"retry"`
}

// FileInfo 文件信息
type FileInfo struct {
	TaskID           string       `json:"task_id"`
	OriginalFilename string       `json:"original_filename"`
	FilePath         string       `json:"file_path"`
	FileSize         int64        `json:"file_size"`
	TotalChunks      int          `json:"total_chunks"`
	Status           FileStatus   `json:"status"`
	CreatedTime      string       `json:"created_time"`
	UpdatedTime      string       `json:"updated_time"`
	TotalLines       int          `json:"total_lines"`
	Chunks           []*FileChunk `json:"chunks"`
	MergedPath       *string      `json:"merged_path,omitempty"`
	ErrorMessage     *string      `json:"error_message,omitempty"`
	Retry            int          `json:"retry"`
	MaxRetry         int          `json:"max_retry"` // 最大重试次数
}

// BatchTaskInfo 批处理任务信息
type BatchTaskInfo struct {
	BatchID        string      `json:"batch_id"`
	Status         BatchStatus `json:"status"`
	InputFileID    string      `json:"input_file_id"`
	OutputFileID   string      `json:"output_file_id"`
	TotalCount     int         `json:"total_count"`
	CompletedCount int         `json:"completed_count"`
	FailedCount    int         `json:"failed_count"`
	ErrorFileID    *string     `json:"error_file_id,omitempty"`
}

// IsFinished 判断任务是否结束
func (b *BatchTaskInfo) IsFinished() bool {
	return (b.Status == BatchStatusCompleted || b.Status == BatchStatusFailed ||
		b.Status == BatchStatusCanceled || b.Status == BatchStatusExpired) &&
		b.CompletedCount+b.FailedCount == b.TotalCount
}

// StatusSummary 状态摘要
type StatusSummary struct {
	TotalChunks      int                               `json:"total_chunks"`
	ByRetry          map[int]map[string]int            `json:"by_retry"`
	Total            map[string]int                    `json:"total"`
	ProcessingTrunks map[string]map[string]interface{} `json:"processing_trunks"`
}

// GetStatusSummary 获取状态摘要
func (f *FileInfo) GetStatusSummary() *StatusSummary {
	summary := &StatusSummary{
		TotalChunks:      f.TotalChunks,
		ByRetry:          make(map[int]map[string]int),
		Total:            make(map[string]int),
		ProcessingTrunks: make(map[string]map[string]interface{}),
	}

	// 初始化总计
	summary.Total["pending"] = 0
	summary.Total["uploaded"] = 0
	summary.Total["processing"] = 0
	summary.Total["processed"] = 0
	summary.Total["upload_failed"] = 0
	summary.Total["total_count"] = 0
	summary.Total["complete_count"] = 0
	summary.Total["failed_count"] = 0

	if len(f.Chunks) == 0 {
		return summary
	}

	for _, chunk := range f.Chunks {
		retry := chunk.Retry

		// 初始化该retry级别的统计
		if summary.ByRetry[retry] == nil {
			summary.ByRetry[retry] = make(map[string]int)
			summary.ByRetry[retry]["pending"] = 0
			summary.ByRetry[retry]["uploaded"] = 0
			summary.ByRetry[retry]["processing"] = 0
			summary.ByRetry[retry]["processed"] = 0
			summary.ByRetry[retry]["upload_failed"] = 0
		}

		// 按状态累加
		switch chunk.Status {
		case ChunkStatusPending:
			summary.ByRetry[retry]["pending"]++
			summary.Total["pending"]++
		case ChunkStatusUploaded:
			summary.ByRetry[retry]["uploaded"]++
			summary.Total["uploaded"]++
		case ChunkStatusProcessing:
			summary.ByRetry[retry]["processing"]++
			summary.Total["processing"]++
			if chunk.BatchTaskInfo != nil {
				if retry == 0 {
					summary.Total["total_count"] += chunk.BatchTaskInfo.TotalCount
					summary.Total["complete_count"] += chunk.BatchTaskInfo.CompletedCount
					summary.Total["failed_count"] += chunk.BatchTaskInfo.FailedCount
				} else {
					summary.Total["complete_count"] += chunk.BatchTaskInfo.CompletedCount
					summary.Total["failed_count"] -= chunk.BatchTaskInfo.CompletedCount
				}
				trunkInfo := map[string]interface{}{
					"total_count":    chunk.BatchTaskInfo.TotalCount,
					"complete_count": chunk.BatchTaskInfo.CompletedCount,
					"failed_count":   chunk.BatchTaskInfo.FailedCount,
				}
				logInfo("chunk batch start time:%s", chunk.BatchStartTime)
				// 添加 batch_start_time，转换为 int64 (Unix 时间戳)
				if chunk.BatchStartTime != nil && *chunk.BatchStartTime != "" {
					trunkInfo["batch_start_time"] = *chunk.BatchStartTime
				}
				summary.ProcessingTrunks[chunk.ChunkID] = trunkInfo
			}
		case ChunkStatusProcessed:
			summary.ByRetry[retry]["processed"]++
			summary.Total["processed"]++
			if chunk.BatchTaskInfo != nil {
				if retry == 0 {
					summary.Total["total_count"] += chunk.BatchTaskInfo.TotalCount
					summary.Total["complete_count"] += chunk.BatchTaskInfo.CompletedCount
					summary.Total["failed_count"] += chunk.BatchTaskInfo.FailedCount
				} else {
					summary.Total["complete_count"] += chunk.BatchTaskInfo.CompletedCount
					summary.Total["failed_count"] -= chunk.BatchTaskInfo.CompletedCount
				}
			}
		case ChunkStatusUploadFailed:
			summary.ByRetry[retry]["upload_failed"]++
			summary.Total["upload_failed"]++
		}
	}

	return summary
}

// ToJSON 转换为JSON字符串
func (b *BatchTaskInfo) ToJSON() (string, error) {
	data, err := json.Marshal(b)
	return string(data), err
}

// FromJSON 从JSON字符串解析
func (b *BatchTaskInfo) FromJSON(data string) error {
	return json.Unmarshal([]byte(data), b)
}
