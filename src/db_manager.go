package main

import (
	"database/sql"
	"encoding/json"
	"time"

	_ "modernc.org/sqlite"
)

// DBManager 数据库管理器
type DBManager struct {
	dbPath string
}

// NewDBManager 创建数据库管理器
func NewDBManager() *DBManager {
	db := &DBManager{dbPath: DB_PATH}
	db.initDatabase()
	return db
}

// getConnection 获取数据库连接
func (db *DBManager) getConnection() (*sql.DB, error) {
	conn, err := sql.Open("sqlite", db.dbPath+"?_timeout=10000")
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// initDatabase 初始化数据库
func (db *DBManager) initDatabase() {
	conn, err := db.getConnection()
	if err != nil {
		logError("初始化数据库失败: %v", err)
		return
	}
	defer conn.Close()

	// 设置连接池参数
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	// 创建文件信息表
	_, err = conn.Exec(`
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

	// 创建文件块表
	_, err = conn.Exec(`
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
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
		INSERT INTO files (
			file_id, original_filename, file_path, file_size,
			total_chunks, total_lines, status, created_time, updated_time,
			merged_path, error_message, retry, max_retry
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		fileInfo.FileID,
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
	defer conn.Close()

	var fileInfo FileInfo
	err = conn.QueryRow(`
		SELECT file_id, original_filename, file_path, file_size,
		       total_chunks, total_lines, status, created_time, updated_time,
		       merged_path, error_message, retry, max_retry
		FROM files WHERE file_id = ?
	`, fileID).Scan(
		&fileInfo.FileID,
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
		return nil, err
	}

	// 获取文件块
	rows, err := conn.Query(`
		SELECT chunk_id, file_id, chunk_index, chunk_path, chunk_size,
		       status, upload_file_id, batch_id, upload_time, process_time,
		       error_message, batch_task_info, retry
		FROM chunks WHERE file_id = ? ORDER BY chunk_index
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fileInfo.Chunks = []*FileChunk{}
	for rows.Next() {
		var chunk FileChunk
		var uploadFileID, batchID, uploadTime, processTime, errorMessage, batchTaskInfoJSON sql.NullString

		err := rows.Scan(
			&chunk.ChunkID,
			&chunk.FileID,
			&chunk.ChunkIndex,
			&chunk.ChunkPath,
			&chunk.ChunkSize,
			&chunk.Status,
			&uploadFileID,
			&batchID,
			&uploadTime,
			&processTime,
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
	conn, err := db.getConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query(`SELECT file_id FROM files ORDER BY created_time DESC`)
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
	defer conn.Close()

	var fileID string
	err = conn.QueryRow(`
		SELECT file_id FROM files WHERE original_filename = ? ORDER BY created_time DESC LIMIT 1
	`, filename).Scan(&fileID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return db.GetFile(fileID)
}

// UpdateFileStatus 更新文件状态
func (db *DBManager) UpdateFileStatus(fileID string, status FileStatus, errorMessage *string) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
		UPDATE files 
		SET status = ?, updated_time = ?, error_message = ?
		WHERE file_id = ?
	`, string(status), time.Now().Format(time.RFC3339), errorMessage, fileID)
	return err
}

// UpdateFileMergedPath 更新合并后的文件路径
func (db *DBManager) UpdateFileMergedPath(fileID string, mergedPath string) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
		UPDATE files 
		SET merged_path = ?, updated_time = ?
		WHERE file_id = ?
	`, mergedPath, time.Now().Format(time.RFC3339), fileID)
	return err
}

// UpdateFileRetry 更新文件重试次数
func (db *DBManager) UpdateFileRetry(fileID string, retry int) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
		UPDATE files 
		SET retry = ?, updated_time = ?
		WHERE file_id = ?
	`, retry, time.Now().Format(time.RFC3339), fileID)
	return err
}

// UpdateFileTotalChunks 更新文件总块数
func (db *DBManager) UpdateFileTotalChunks(fileID string, totalChunks int) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
		UPDATE files 
		SET total_chunks = ?, updated_time = ?
		WHERE file_id = ?
	`, totalChunks, time.Now().Format(time.RFC3339), fileID)
	return err
}

// UpdateFileTotalLines 更新文件总行数
func (db *DBManager) UpdateFileTotalLines(fileID string, totalLines int) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
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
	defer conn.Close()

	var chunk FileChunk
	var uploadFileID, batchID, uploadTime, processTime, errorMessage, batchTaskInfoJSON sql.NullString

	err = conn.QueryRow(`
		SELECT chunk_id, file_id, chunk_index, chunk_path, chunk_size,
		       status, upload_file_id, batch_id, upload_time, process_time,
		       error_message, batch_task_info, retry
		FROM chunks WHERE chunk_id = ?
	`, chunkID).Scan(
		&chunk.ChunkID,
		&chunk.FileID,
		&chunk.ChunkIndex,
		&chunk.ChunkPath,
		&chunk.ChunkSize,
		&chunk.Status,
		&uploadFileID,
		&batchID,
		&uploadTime,
		&processTime,
		&errorMessage,
		&batchTaskInfoJSON,
		&chunk.Retry,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
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
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	var batchTaskInfoJSON sql.NullString
	if chunk.BatchTaskInfo != nil {
		data, err := json.Marshal(chunk.BatchTaskInfo)
		if err == nil {
			batchTaskInfoJSON = sql.NullString{String: string(data), Valid: true}
		}
	}

	_, err = conn.Exec(`
		INSERT INTO chunks (
			chunk_id, file_id, chunk_index, chunk_path,
			chunk_size, status, upload_file_id, batch_id, upload_time, process_time, error_message, batch_task_info, retry
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		chunk.ChunkID,
		chunk.FileID,
		chunk.ChunkIndex,
		chunk.ChunkPath,
		chunk.ChunkSize,
		string(chunk.Status),
		chunk.UploadFileID,
		chunk.BatchID,
		chunk.UploadTime,
		chunk.ProcessTime,
		chunk.ErrorMessage,
		batchTaskInfoJSON,
		chunk.Retry,
	)
	return err
}

// UpdateChunkStatus 更新文件块状态
func (db *DBManager) UpdateChunkStatus(chunkID string, status ChunkStatus, errorMessage *string) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	now := time.Now().Format(time.RFC3339)
	if status == ChunkStatusUploaded {
		_, err = conn.Exec(`
			UPDATE chunks 
			SET status = ?, upload_time = ?, error_message = ?
			WHERE chunk_id = ?
		`, string(status), now, errorMessage, chunkID)
	} else if status == ChunkStatusProcessed {
		_, err = conn.Exec(`
			UPDATE chunks 
			SET status = ?, process_time = ?, error_message = ?
			WHERE chunk_id = ?
		`, string(status), now, errorMessage, chunkID)
	} else {
		_, err = conn.Exec(`
			UPDATE chunks 
			SET status = ?, error_message = ?
			WHERE chunk_id = ?
		`, string(status), errorMessage, chunkID)
	}
	return err
}

// UpdateChunkUploadFileID 更新文件块上传文件id
func (db *DBManager) UpdateChunkUploadFileID(chunkID string, uploadFileID string) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	now := time.Now().Format(time.RFC3339)
	_, err = conn.Exec(`
		UPDATE chunks 
		SET upload_file_id = ?, upload_time = ?
		WHERE chunk_id = ?
	`, uploadFileID, now, chunkID)
	return err
}

// UpdateChunkBatchID 更新文件块batch任务id
func (db *DBManager) UpdateChunkBatchID(chunkID string, batchID string) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(`
		UPDATE chunks 
		SET batch_id = ?
		WHERE chunk_id = ?
	`, batchID, chunkID)
	return err
}

// UpdateChunkBatchTaskInfo 更新文件块batch任务信息
func (db *DBManager) UpdateChunkBatchTaskInfo(chunkID string, batchTaskInfo *BatchTaskInfo) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	data, err := json.Marshal(batchTaskInfo)
	if err != nil {
		return err
	}

	_, err = conn.Exec(`
		UPDATE chunks 
		SET batch_task_info = ?
		WHERE chunk_id = ?
	`, string(data), chunkID)
	return err
}

// DeleteFile 删除文件记录
func (db *DBManager) DeleteFile(fileID string) error {
	conn, err := db.getConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	// 先删除文件块
	_, err = conn.Exec(`DELETE FROM chunks WHERE file_id = ?`, fileID)
	if err != nil {
		return err
	}

	// 再删除文件
	_, err = conn.Exec(`DELETE FROM files WHERE file_id = ?`, fileID)
	return err
}

// GetPendingFiles 获取需要自动执行的文件列表（状态为split_completed或processing的文件）
func (db *DBManager) GetPendingFiles() ([]string, error) {
	conn, err := db.getConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query(`
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
