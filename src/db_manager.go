package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DBManager 数据库管理器
type DBManager struct {
	dbPath string
	db     *sql.DB
	mu     sync.Mutex // 保护数据库连接的互斥锁
}

// NewDBManager 创建数据库管理器
func NewDBManager() *DBManager {
	db := &DBManager{dbPath: DB_PATH}
	db.initDatabase()
	return db
}

// getConnection 获取数据库连接（使用连接池）
func (db *DBManager) getConnection() (*sql.DB, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.db != nil {
		// 检查连接是否仍然有效
		if err := db.db.Ping(); err == nil {
			return db.db, nil
		}
		// 连接无效，关闭并重新创建
		db.db.Close()
		db.db = nil
	}

	// 创建新连接，启用 WAL 模式以提高并发性能
	// _timeout: 超时时间（毫秒），设置为 30 秒
	// _journal=WAL: 启用 Write-Ahead Logging 模式
	// _busy_timeout: 当数据库被锁定时等待的时间（毫秒），设置为 30 秒
	dsn := fmt.Sprintf("%s?_timeout=30000&_journal=WAL&_busy_timeout=30000", db.dbPath)
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	// 设置连接池参数
	// SQLite 支持多个读连接，但写操作需要串行化
	conn.SetMaxOpenConns(10)  // 允许最多 10 个并发连接
	conn.SetMaxIdleConns(5)   // 保持 5 个空闲连接
	conn.SetConnMaxLifetime(time.Hour) // 连接最大生存时间

	// 启用 WAL 模式
	_, err = conn.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("启用 WAL 模式失败: %v", err)
	}

	// 设置其他优化参数
	conn.Exec("PRAGMA synchronous=NORMAL") // 平衡性能和安全性
	conn.Exec("PRAGMA cache_size=-64000")   // 设置缓存大小为 64MB

	db.db = conn
	return db.db, nil
}

// isBusyError 检查是否是 SQLITE_BUSY 错误
func (db *DBManager) isBusyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "database is locked (5)")
}

// execWithRetry 执行 SQL 语句，带重试机制
func (db *DBManager) execWithRetry(query string, args ...interface{}) (sql.Result, error) {
	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		conn, err := db.getConnection()
		if err != nil {
			return nil, err
		}

		result, err := conn.Exec(query, args...)
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !db.isBusyError(err) {
			// 不是忙错误，直接返回
			return nil, err
		}

		// 是忙错误，等待后重试
		if i < maxRetries-1 {
			time.Sleep(retryDelay * time.Duration(i+1)) // 指数退避
		}
	}

	return nil, fmt.Errorf("执行 SQL 失败（重试 %d 次后）: %v", maxRetries, lastErr)
}

// queryWithRetry 执行查询，带重试机制
func (db *DBManager) queryWithRetry(query string, args ...interface{}) (*sql.Rows, error) {
	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		conn, err := db.getConnection()
		if err != nil {
			return nil, err
		}

		rows, err := conn.Query(query, args...)
		if err == nil {
			return rows, nil
		}

		lastErr = err
		if !db.isBusyError(err) {
			// 不是忙错误，直接返回
			return nil, err
		}

		// 是忙错误，等待后重试
		if i < maxRetries-1 {
			time.Sleep(retryDelay * time.Duration(i+1)) // 指数退避
		}
	}

	return nil, fmt.Errorf("查询 SQL 失败（重试 %d 次后）: %v", maxRetries, lastErr)
}

// initDatabase 初始化数据库
func (db *DBManager) initDatabase() {

	// 创建文件信息表（使用重试机制）
	_, err = db.execWithRetry(`
		CREATE TABLE IF NOT EXISTS files (
			file_id TEXT PRIMARY KEY,
			original_filename TEXT NOT NULL,
			file_path TEXT NOT NULL,
			file_size INTEGER NOT NULL,
			total_chunks INTEGER NOT NULL,
			total_lines INTEGER DEFAULT 0,
			status TEXT NOT NULL,
			created_time TEXT NOT NULL,
			updated_time TEXT NOT NULL,
			merged_path TEXT,
			error_message TEXT,
			retry INTEGER DEFAULT 0,
			max_retry INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		logError("创建files表失败: %v", err)
		return
	}

	// 创建文件块表（使用重试机制）
	_, err = db.execWithRetry(`
		CREATE TABLE IF NOT EXISTS chunks (
			chunk_id TEXT PRIMARY KEY,
			file_id TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			chunk_path TEXT NOT NULL,
			chunk_size INTEGER NOT NULL,
			status TEXT NOT NULL,
			upload_file_id TEXT,
			batch_id TEXT,
			upload_time TEXT,
			process_time TEXT,
			batch_start_time TEXT,
			error_message TEXT,
			batch_task_info TEXT,
			retry INTEGER DEFAULT 0,
			FOREIGN KEY (file_id) REFERENCES files (file_id)
		)
	`)
	if err != nil {
		logError("创建chunks表失败: %v", err)
		return
	}
}

// CreateFile 创建文件记录
func (db *DBManager) CreateFile(fileInfo *FileInfo) error {
	_, err := db.execWithRetry(`
		INSERT INTO files (
			file_id, original_filename, file_path, file_size,
			total_chunks, total_lines, status, created_time, updated_time,
			merged_path, error_message, retry, max_retry
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		fileInfo.TaskID,
		fileInfo.OriginalFilename,
		fileInfo.FilePath,
		fileInfo.FileSize,
		fileInfo.TotalChunks,
		fileInfo.TotalLines,
		string(fileInfo.Status),
		fileInfo.CreatedTime,
		fileInfo.UpdatedTime,
		fileInfo.MergedPath,
		fileInfo.ErrorMessage,
		fileInfo.Retry,
		fileInfo.MaxRetry,
	)
	return err
}

// GetFile 获取文件信息
func (db *DBManager) GetFile(fileID string) (*FileInfo, error) {
	conn, err := db.getConnection()
	if err != nil {
		return nil, err
	}

	var fileInfo FileInfo
	err = conn.QueryRow(`
		SELECT file_id, original_filename, file_path, file_size,
		       total_chunks, total_lines, status, created_time, updated_time,
		       merged_path, error_message, retry, max_retry
		FROM files WHERE file_id = ?
	`, fileID).Scan(
		&fileInfo.TaskID,
		&fileInfo.OriginalFilename,
		&fileInfo.FilePath,
		&fileInfo.FileSize,
		&fileInfo.TotalChunks,
		&fileInfo.TotalLines,
		&fileInfo.Status,
		&fileInfo.CreatedTime,
		&fileInfo.UpdatedTime,
		&fileInfo.MergedPath,
		&fileInfo.ErrorMessage,
		&fileInfo.Retry,
		&fileInfo.MaxRetry,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		// 如果是忙错误，重试一次
		if db.isBusyError(err) {
			time.Sleep(200 * time.Millisecond)
			return db.GetFile(fileID)
		}
		return nil, err
	}

	// 获取文件块
	rows, err := db.queryWithRetry(`
		SELECT chunk_id, file_id, chunk_index, chunk_path, chunk_size,
		       status, upload_file_id, batch_id, upload_time, process_time,
		       batch_start_time, error_message, batch_task_info, retry
		FROM chunks WHERE file_id = ? ORDER BY chunk_index
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fileInfo.Chunks = []*FileChunk{}
	for rows.Next() {
		var chunk FileChunk
		var uploadFileID, batchID, uploadTime, processTime, batchStartTime, errorMessage, batchTaskInfoJSON sql.NullString

		err := rows.Scan(
			&chunk.ChunkID,
			&chunk.TaskID,
			&chunk.ChunkIndex,
			&chunk.ChunkPath,
			&chunk.ChunkSize,
			&chunk.Status,
			&uploadFileID,
			&batchID,
			&uploadTime,
			&processTime,
			&batchStartTime,
			&errorMessage,
			&batchTaskInfoJSON,
			&chunk.Retry,
		)
		if err != nil {
			continue
		}

		if uploadFileID.Valid {
			chunk.UploadFileID = &uploadFileID.String
		}
		if batchID.Valid {
			chunk.BatchID = &batchID.String
		}
		if uploadTime.Valid {
			chunk.UploadTime = &uploadTime.String
		}
		if processTime.Valid {
			chunk.ProcessTime = &processTime.String
		}
		if batchStartTime.Valid {
			chunk.BatchStartTime = &batchStartTime.String
		}
		if errorMessage.Valid {
			chunk.ErrorMessage = &errorMessage.String
		}

		// 解析 batch_task_info
		if batchTaskInfoJSON.Valid && batchTaskInfoJSON.String != "" {
			var batchTaskInfo BatchTaskInfo
			if err := json.Unmarshal([]byte(batchTaskInfoJSON.String), &batchTaskInfo); err == nil {
				chunk.BatchTaskInfo = &batchTaskInfo
			}
		}

		fileInfo.Chunks = append(fileInfo.Chunks, &chunk)
	}

	return &fileInfo, nil
}

// GetAllFiles 获取所有文件信息
func (db *DBManager) GetAllFiles() ([]*FileInfo, error) {
	rows, err := db.queryWithRetry(`SELECT file_id FROM files ORDER BY created_time DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileIDs []string
	for rows.Next() {
		var fileID string
		if err := rows.Scan(&fileID); err == nil {
			fileIDs = append(fileIDs, fileID)
		}
	}

	var files []*FileInfo
	for _, fileID := range fileIDs {
		file, err := db.GetFile(fileID)
		if err == nil && file != nil {
			files = append(files, file)
		}
	}

	return files, nil
}

// GetFileByFilename 通过文件名查询文件信息（返回第一个匹配的文件）
func (db *DBManager) GetFileByFilename(filename string) (*FileInfo, error) {
	conn, err := db.getConnection()
	if err != nil {
		return nil, err
	}

	var fileID string
	err = conn.QueryRow(`
		SELECT file_id FROM files WHERE original_filename = ? ORDER BY created_time DESC LIMIT 1
	`, filename).Scan(&fileID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		// 如果是忙错误，重试一次
		if db.isBusyError(err) {
			time.Sleep(200 * time.Millisecond)
			return db.GetFileByFilename(filename)
		}
		return nil, err
	}

	return db.GetFile(fileID)
}

// UpdateFileStatus 更新文件状态
func (db *DBManager) UpdateFileStatus(fileID string, status FileStatus, errorMessage *string) error {
	_, err := db.execWithRetry(`
		UPDATE files 
		SET status = ?, updated_time = ?, error_message = ?
		WHERE file_id = ?
	`, string(status), time.Now().Format(time.RFC3339), errorMessage, fileID)
	return err
}

// UpdateFileMergedPath 更新合并后的文件路径
func (db *DBManager) UpdateFileMergedPath(fileID string, mergedPath string) error {
	_, err := db.execWithRetry(`
		UPDATE files 
		SET merged_path = ?, updated_time = ?
		WHERE file_id = ?
	`, mergedPath, time.Now().Format(time.RFC3339), fileID)
	return err
}

// UpdateFileRetry 更新文件重试次数
func (db *DBManager) UpdateFileRetry(fileID string, retry int) error {
	_, err := db.execWithRetry(`
		UPDATE files 
		SET retry = ?, updated_time = ?
		WHERE file_id = ?
	`, retry, time.Now().Format(time.RFC3339), fileID)
	return err
}

// UpdateFileTotalChunks 更新文件总块数
func (db *DBManager) UpdateFileTotalChunks(fileID string, totalChunks int) error {
	_, err := db.execWithRetry(`
		UPDATE files 
		SET total_chunks = ?, updated_time = ?
		WHERE file_id = ?
	`, totalChunks, time.Now().Format(time.RFC3339), fileID)
	return err
}

// UpdateFileTotalLines 更新文件总行数
func (db *DBManager) UpdateFileTotalLines(fileID string, totalLines int) error {
	_, err := db.execWithRetry(`
		UPDATE files 
		SET total_lines = ?, updated_time = ?
		WHERE file_id = ?
	`, totalLines, time.Now().Format(time.RFC3339), fileID)
	return err
}

// GetChunk 获取文件块
func (db *DBManager) GetChunk(chunkID string) (*FileChunk, error) {
	conn, err := db.getConnection()
	if err != nil {
		return nil, err
	}

	var chunk FileChunk
	var uploadFileID, batchID, uploadTime, processTime, batchStartTime, errorMessage, batchTaskInfoJSON sql.NullString

	err = conn.QueryRow(`
		SELECT chunk_id, file_id, chunk_index, chunk_path, chunk_size,
		       status, upload_file_id, batch_id, upload_time, process_time,
		       batch_start_time, error_message, batch_task_info, retry
		FROM chunks WHERE chunk_id = ?
	`, chunkID).Scan(
		&chunk.ChunkID,
		&chunk.TaskID,
		&chunk.ChunkIndex,
		&chunk.ChunkPath,
		&chunk.ChunkSize,
		&chunk.Status,
		&uploadFileID,
		&batchID,
		&uploadTime,
		&processTime,
		&batchStartTime,
		&errorMessage,
		&batchTaskInfoJSON,
		&chunk.Retry,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		// 如果是忙错误，重试一次
		if db.isBusyError(err) {
			time.Sleep(200 * time.Millisecond)
			return db.GetChunk(chunkID)
		}
		return nil, err
	}

	if uploadFileID.Valid {
		chunk.UploadFileID = &uploadFileID.String
	}
	if batchID.Valid {
		chunk.BatchID = &batchID.String
	}
	if uploadTime.Valid {
		chunk.UploadTime = &uploadTime.String
	}
	if processTime.Valid {
		chunk.ProcessTime = &processTime.String
	}
	if batchStartTime.Valid {
		chunk.BatchStartTime = &batchStartTime.String
	}
	if errorMessage.Valid {
		chunk.ErrorMessage = &errorMessage.String
	}

	// 解析 batch_task_info
	if batchTaskInfoJSON.Valid && batchTaskInfoJSON.String != "" {
		var batchTaskInfo BatchTaskInfo
		if err := json.Unmarshal([]byte(batchTaskInfoJSON.String), &batchTaskInfo); err == nil {
			chunk.BatchTaskInfo = &batchTaskInfo
		}
	}

	return &chunk, nil
}

// AddChunk 添加文件块
func (db *DBManager) AddChunk(chunk *FileChunk) error {
	var batchTaskInfoJSON sql.NullString
	if chunk.BatchTaskInfo != nil {
		data, err := json.Marshal(chunk.BatchTaskInfo)
		if err == nil {
			batchTaskInfoJSON = sql.NullString{String: string(data), Valid: true}
		}
	}

	_, err := db.execWithRetry(`
		INSERT INTO chunks (
			chunk_id, file_id, chunk_index, chunk_path,
			chunk_size, status, upload_file_id, batch_id, upload_time, process_time, batch_start_time, error_message, batch_task_info, retry
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		chunk.ChunkID,
		chunk.TaskID,
		chunk.ChunkIndex,
		chunk.ChunkPath,
		chunk.ChunkSize,
		string(chunk.Status),
		chunk.UploadFileID,
		chunk.BatchID,
		chunk.UploadTime,
		chunk.ProcessTime,
		chunk.BatchStartTime,
		chunk.ErrorMessage,
		batchTaskInfoJSON,
		chunk.Retry,
	)
	return err
}

// UpdateChunkStatus 更新文件块状态
func (db *DBManager) UpdateChunkStatus(chunkID string, status ChunkStatus, errorMessage *string) error {
	now := time.Now().Format(time.RFC3339)
	if status == ChunkStatusUploaded {
		_, err := db.execWithRetry(`
			UPDATE chunks 
			SET status = ?, upload_time = ?, error_message = ?
			WHERE chunk_id = ?
		`, string(status), now, errorMessage, chunkID)
		return err
	} else if status == ChunkStatusProcessed {
		_, err := db.execWithRetry(`
			UPDATE chunks 
			SET status = ?, process_time = ?, error_message = ?
			WHERE chunk_id = ?
		`, string(status), now, errorMessage, chunkID)
		return err
	} else {
		_, err := db.execWithRetry(`
			UPDATE chunks 
			SET status = ?, error_message = ?
			WHERE chunk_id = ?
		`, string(status), errorMessage, chunkID)
		return err
	}
}

// UpdateChunkUploadFileID 更新文件块上传文件id
func (db *DBManager) UpdateChunkUploadFileID(chunkID string, uploadFileID string) error {
	now := time.Now().Format(time.RFC3339)
	_, err := db.execWithRetry(`
		UPDATE chunks 
		SET upload_file_id = ?, upload_time = ?
		WHERE chunk_id = ?
	`, uploadFileID, now, chunkID)
	return err
}

// UpdateChunkBatchID 更新文件块batch任务id
func (db *DBManager) UpdateChunkBatchID(chunkID string, batchID string) error {
	_, err := db.execWithRetry(`
		UPDATE chunks 
		SET batch_id = ?, batch_start_time = ?
		WHERE chunk_id = ?
	`, batchID, time.Now().Format(time.DateTime), chunkID)
	return err
}

// UpdateChunkBatchStartTime 更新文件块batch任务开始时间
func (db *DBManager) UpdateChunkBatchStartTime(chunkID string, batchStartTime string) error {
	_, err := db.execWithRetry(`
		UPDATE chunks 
		SET batch_start_time = ?
		WHERE chunk_id = ?
	`, batchStartTime, chunkID)
	return err
}

// UpdateChunkBatchTaskInfo 更新文件块batch任务信息
func (db *DBManager) UpdateChunkBatchTaskInfo(chunkID string, batchTaskInfo *BatchTaskInfo) error {
	data, err := json.Marshal(batchTaskInfo)
	if err != nil {
		return err
	}

	_, err = db.execWithRetry(`
		UPDATE chunks 
		SET batch_task_info = ?
		WHERE chunk_id = ?
	`, string(data), chunkID)
	return err
}

// DeleteFile 删除文件记录
func (db *DBManager) DeleteFile(fileID string) error {
	// 先删除文件块
	_, err := db.execWithRetry(`DELETE FROM chunks WHERE file_id = ?`, fileID)
	if err != nil {
		return err
	}

	// 再删除文件
	_, err = db.execWithRetry(`DELETE FROM files WHERE file_id = ?`, fileID)
	return err
}

// GetPendingFiles 获取需要自动执行的文件列表（状态为split_completed或processing的文件）
func (db *DBManager) GetPendingFiles() ([]string, error) {
	rows, err := db.queryWithRetry(`
		SELECT file_id 
		FROM files 
		WHERE status IN (?, ?)
		ORDER BY created_time ASC
	`, string(FileStatusSplitCompleted), string(FileStatusProcessing))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileIDs []string
	for rows.Next() {
		var fileID string
		if err := rows.Scan(&fileID); err == nil {
			fileIDs = append(fileIDs, fileID)
		}
	}

	return fileIDs, nil
}
