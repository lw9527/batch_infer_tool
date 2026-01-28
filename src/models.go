package main

// 类型定义
type FileStatus string
type ChunkStatus string
type BatchStatus string

const (
	// --- FileStatus ---
	FileStatusPending          FileStatus = "pending"
	FileStatusSplitting        FileStatus = "splitting"
	FileStatusSplitCompleted   FileStatus = "split_completed"
	FileStatusProcessing       FileStatus = "processing"
	FileStatusStopping         FileStatus = "stopping"          // 【新增】正在取消中
	FileStatusProcessCompleted FileStatus = "process_completed"
	FileStatusCompleted        FileStatus = "process_completed" 
	FileStatusFailed           FileStatus = "failed"
	FileStatusCanceled         FileStatus = "canceled"
)

const (
	// --- ChunkStatus ---
	ChunkStatusPending      ChunkStatus = "pending"
	ChunkStatusUploading    ChunkStatus = "uploading"
	ChunkStatusUploaded     ChunkStatus = "uploaded"
	ChunkStatusUploadFailed ChunkStatus = "upload_failed"
	ChunkStatusProcessing   ChunkStatus = "processing"
	ChunkStatusProcessed    ChunkStatus = "processed"
	ChunkStatusSuccess      ChunkStatus = "processed" 
	ChunkStatusFailed       ChunkStatus = "failed"
	ChunkStatusCanceled     ChunkStatus = "canceled"
)

const (
	// --- BatchStatus ---
	BatchStatusValidating  BatchStatus = "validating"
	BatchStatusQueueing    BatchStatus = "queueing"
	BatchStatusInProgress  BatchStatus = "in_progress"
	BatchStatusFinalizing  BatchStatus = "finalizing"
	BatchStatusCompleted   BatchStatus = "completed"
	BatchStatusFailed      BatchStatus = "failed"
	BatchStatusExpired     BatchStatus = "expired"
	BatchStatusCanceled    BatchStatus = "cancelled"
)

// FileInfo 保持不变
type FileInfo struct {
	TaskID           string       `json:"task_id"`
	OriginalFilename string       `json:"original_filename"`
	FilePath         string       `json:"file_path"`
	FileSize         int64        `json:"file_size"`
	TotalLines       int          `json:"total_lines"`
	TotalChunks      int          `json:"total_chunks"`
	Status           FileStatus   `json:"status"`
	Retry            int          `json:"retry"`
	MaxRetry         int          `json:"max_retry"`
	CreatedTime      string       `json:"created_time"`
	UpdatedTime      string       `json:"updated_time"`
	Chunks           []*FileChunk `json:"chunks"`
	MergedPath       string       `json:"merged_path"`
	ErrorMessage     string       `json:"error_message"`
	// UI统计
	CompletedLines int; PendingCount int; ProcessingCount int; CompletedCount int; FailedCount int
}

// FileChunk 保持不变
type FileChunk struct {
	ChunkID       string         `json:"chunk_id"`
	TaskID        string         `json:"task_id"`
	ChunkIndex    int            `json:"chunk_index"`
	ChunkPath     string         `json:"chunk_path"`
	ChunkSize     int            `json:"chunk_size"`
	Status        ChunkStatus    `json:"status"`
	Retry         int            `json:"retry"`
	UpdatedTime   string         `json:"updated_time"`
	UploadFileID  *string        `json:"upload_file_id"`
	BatchID       *string        `json:"batch_id"`
	BatchTaskInfo *BatchTaskInfo `json:"batch_task_info"`
	UploadTime   string; ProcessTime  string; ErrorMessage string
}

// BatchTaskInfo 保持不变
type BatchTaskInfo struct {
	ID               string      `json:"id"`
	Object           string      `json:"object"`
	Endpoint         string      `json:"endpoint"`
	InputFileID      string      `json:"input_file_id"`
	CompletionWindow string      `json:"completion_window"`
	Status           BatchStatus `json:"status"`
	OutputFileID     string      `json:"output_file_id"` 
	ErrorFileID      *string     `json:"error_file_id"`  
	CreatedAt        int64       `json:"created_at"`
	InProgressAt     int64       `json:"in_progress_at"`
	ExpiresAt        int64       `json:"expires_at"`
	FinalizingAt     int64       `json:"finalizing_at"`
	CompletedAt      int64       `json:"completed_at"`
	FailedAt         int64       `json:"failed_at"`
	CancelledAt      int64       `json:"cancelled_at"`
	RequestCounts    struct { Total int; Completed int; Failed int } `json:"request_counts"`
	Metadata map[string]interface{} `json:"metadata"`
	BatchID string; TotalCount int; CompletedCount int; FailedCount int
}

func (b *BatchTaskInfo) IsFinished() bool {
	return b.Status == BatchStatusCompleted || b.Status == BatchStatusFailed || b.Status == BatchStatusExpired || b.Status == BatchStatusCanceled
}

// FileStatusSummary 保持不变
type FileStatusSummary struct {
	Total            map[string]int
	ByRetry          map[int]map[string]int
	ProcessingTrunks map[string]map[string]int
	TotalChunks      int
}

func (f *FileInfo) GetStatusSummary() *FileStatusSummary {
	summary := &FileStatusSummary{Total: make(map[string]int), ByRetry: make(map[int]map[string]int), ProcessingTrunks: make(map[string]map[string]int), TotalChunks: len(f.Chunks)}
	keys := []string{"pending", "uploaded", "processing", "processed", "upload_failed", "failed", "canceled", "complete_count", "failed_count"}
	for _, k := range keys { summary.Total[k] = 0 }

	for _, chunk := range f.Chunks {
		retry := chunk.Retry
		if _, ok := summary.ByRetry[retry]; !ok { summary.ByRetry[retry] = make(map[string]int) }
		statusKey := string(chunk.Status)
		if chunk.Status == ChunkStatusSuccess { statusKey = "processed" }
		summary.ByRetry[retry][statusKey]++
		summary.Total[statusKey]++
		if chunk.BatchTaskInfo != nil {
			summary.Total["complete_count"] += chunk.BatchTaskInfo.CompletedCount
			summary.Total["failed_count"] += chunk.BatchTaskInfo.FailedCount
			if chunk.Status == ChunkStatusProcessing {
				summary.ProcessingTrunks[chunk.ChunkID] = map[string]int{"total": chunk.BatchTaskInfo.TotalCount, "completed": chunk.BatchTaskInfo.CompletedCount, "failed": chunk.BatchTaskInfo.FailedCount}
			}
		}
	}
	return summary
}