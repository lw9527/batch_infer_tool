package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"time"
	_ "modernc.org/sqlite"
)

type DBManager struct {
	dbPath string
	mu     sync.RWMutex
	files  map[string]*FileInfo
}

func NewDBManager() *DBManager {
	db := &DBManager{dbPath: DB_PATH, files: make(map[string]*FileInfo)}
	if err := db.initDatabase(); err != nil { log.Fatalf("DB初始化失败: %v", err) }
	return db
}

func (db *DBManager) getConnection() (*sql.DB, error) { return sql.Open("sqlite", db.dbPath+"?_timeout=10000&_journal=WAL") }

func (db *DBManager) initDatabase() error {
	conn, err := db.getConnection(); if err != nil { return err }; defer conn.Close()
	conn.Exec(`CREATE TABLE IF NOT EXISTS files (task_id TEXT PRIMARY KEY, original_filename TEXT NOT NULL, file_path TEXT NOT NULL, file_size INTEGER NOT NULL, total_chunks INTEGER NOT NULL, total_lines INTEGER DEFAULT 0, status TEXT NOT NULL, created_time TEXT NOT NULL, updated_time TEXT NOT NULL, merged_path TEXT, error_message TEXT, retry INTEGER DEFAULT 0, max_retry INTEGER DEFAULT 0)`)
	conn.Exec(`CREATE TABLE IF NOT EXISTS chunks (chunk_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, chunk_index INTEGER NOT NULL, chunk_path TEXT NOT NULL, chunk_size INTEGER NOT NULL, status TEXT NOT NULL, upload_file_id TEXT, batch_id TEXT, upload_time TEXT, process_time TEXT, error_message TEXT, batch_task_info TEXT, retry INTEGER DEFAULT 0, updated_time TEXT, FOREIGN KEY (task_id) REFERENCES files (task_id))`)
	return nil
}

// CheckTaskIDExists 检查ID存在
func (db *DBManager) CheckTaskIDExists(taskID string) (bool, error) {
	conn, err := db.getConnection(); if err != nil { return false, err }; defer conn.Close()
	var count int
	err = conn.QueryRow("SELECT count(*) FROM files WHERE task_id = ?", taskID).Scan(&count)
	return count > 0, err
}

func (db *DBManager) GetPendingFiles() ([]*FileInfo, error) { return db.GetActiveFiles() }

func (db *DBManager) GetActiveFiles() ([]*FileInfo, error) {
	conn, err := db.getConnection(); if err != nil { return nil, err }; defer conn.Close()
	rows, err := conn.Query(`SELECT task_id FROM files WHERE status NOT IN (?, ?, ?) ORDER BY created_time DESC`, FileStatusProcessCompleted, FileStatusFailed, FileStatusCanceled)
	if err != nil { return nil, err }; defer rows.Close()
	var files []*FileInfo
	for rows.Next() {
		var id string; rows.Scan(&id)
		if f, _ := db.GetFile(id); f != nil { files = append(files, f) }
	}
	return files, nil
}

func (db *DBManager) CreateFile(f *FileInfo) error {
	db.mu.Lock(); defer db.mu.Unlock()
	conn, err := db.getConnection(); if err != nil { return err }; defer conn.Close()
	_, err = conn.Exec(`INSERT INTO files (task_id, original_filename, file_path, file_size, total_chunks, total_lines, status, created_time, updated_time, retry, max_retry) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, f.TaskID, f.OriginalFilename, f.FilePath, f.FileSize, f.TotalChunks, f.TotalLines, f.Status, f.CreatedTime, f.UpdatedTime, f.Retry, f.MaxRetry)
	if err == nil { db.files[f.TaskID] = f }
	return err
}

func (db *DBManager) AddChunk(c *FileChunk) error {
	db.mu.Lock(); defer db.mu.Unlock()
	conn, err := db.getConnection(); if err != nil { return err }; defer conn.Close()
	var uid, bid interface{}
	if c.UploadFileID != nil { uid = *c.UploadFileID }
	if c.BatchID != nil { bid = *c.BatchID }
	var infoJSON interface{}
	if c.BatchTaskInfo != nil { b, _ := json.Marshal(c.BatchTaskInfo); infoJSON = string(b) }
	_, err = conn.Exec(`INSERT INTO chunks (chunk_id, task_id, chunk_index, chunk_path, chunk_size, status, upload_file_id, batch_id, upload_time, process_time, error_message, batch_task_info, retry, updated_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, c.ChunkID, c.TaskID, c.ChunkIndex, c.ChunkPath, c.ChunkSize, c.Status, uid, bid, c.UploadTime, c.ProcessTime, c.ErrorMessage, infoJSON, c.Retry, time.Now().Format(time.RFC3339))
	return err
}

func (db *DBManager) GetFile(taskID string) (*FileInfo, error) {
	
	conn, err := db.getConnection(); if err != nil { return nil, err }; defer conn.Close()
	f := &FileInfo{}; var mergedPath, errMsg sql.NullString
	err = conn.QueryRow(`SELECT task_id, original_filename, file_path, file_size, total_chunks, total_lines, status, created_time, updated_time, retry, max_retry, merged_path, error_message FROM files WHERE task_id = ?`, taskID).Scan(&f.TaskID, &f.OriginalFilename, &f.FilePath, &f.FileSize, &f.TotalChunks, &f.TotalLines, &f.Status, &f.CreatedTime, &f.UpdatedTime, &f.Retry, &f.MaxRetry, &mergedPath, &errMsg)
	if err != nil { return nil, err }
	f.MergedPath = mergedPath.String; f.ErrorMessage = errMsg.String; f.Chunks = []*FileChunk{}
	rows, err := conn.Query(`SELECT chunk_id, chunk_index, chunk_path, chunk_size, status, upload_file_id, batch_id, upload_time, process_time, error_message, batch_task_info, retry FROM chunks WHERE task_id = ? ORDER BY chunk_index ASC`, taskID)
	if err != nil { return f, nil }; defer rows.Close()
	for rows.Next() {
		c := &FileChunk{TaskID: taskID}; var uid, bid, uTime, pTime, eMsg, bInfo sql.NullString
		rows.Scan(&c.ChunkID, &c.ChunkIndex, &c.ChunkPath, &c.ChunkSize, &c.Status, &uid, &bid, &uTime, &pTime, &eMsg, &bInfo, &c.Retry)
		if uid.Valid { s := uid.String; c.UploadFileID = &s }
		if bid.Valid { s := bid.String; c.BatchID = &s }
		c.UploadTime = uTime.String; c.ProcessTime = pTime.String; c.ErrorMessage = eMsg.String
		if bInfo.Valid && bInfo.String != "" { json.Unmarshal([]byte(bInfo.String), &c.BatchTaskInfo) }
		f.Chunks = append(f.Chunks, c)
	}
	db.mu.Lock(); db.files[taskID] = f; db.mu.Unlock()
	return f, nil
}

func (db *DBManager) GetAllFiles() ([]*FileInfo, error) {
	conn, err := db.getConnection(); if err != nil { return nil, err }; defer conn.Close()
	rows, err := conn.Query(`SELECT task_id FROM files ORDER BY created_time DESC`)
	if err != nil { return nil, err }; defer rows.Close()
	var files []*FileInfo
	for rows.Next() {
		var id string; rows.Scan(&id)
		if f, _ := db.GetFile(id); f != nil { files = append(files, f) }
	}
	return files, nil
}

func (db *DBManager) UpdateFileStatus(id string, status FileStatus, errInfo *string) error {
	conn, err := db.getConnection(); if err != nil { return err }; defer conn.Close()
	if errInfo != nil {
		_, err = conn.Exec(`UPDATE files SET status = ?, error_message = ?, updated_time = ? WHERE task_id = ?`, status, *errInfo, time.Now().Format(time.RFC3339), id)
	} else {
		_, err = conn.Exec(`UPDATE files SET status = ?, updated_time = ? WHERE task_id = ?`, status, time.Now().Format(time.RFC3339), id)
	}
	return err
}

func (db *DBManager) UpdateFileRetry(id string, retry int) error {
	conn, _ := db.getConnection(); defer conn.Close(); _, err := conn.Exec(`UPDATE files SET retry = ? WHERE task_id = ?`, retry, id); return err
}
func (db *DBManager) UpdateFileTotalChunks(id string, n int) error {
	conn, _ := db.getConnection(); defer conn.Close(); _, err := conn.Exec(`UPDATE files SET total_chunks = ? WHERE task_id = ?`, n, id); return err
}
func (db *DBManager) UpdateFileTotalLines(id string, n int) error {
	conn, _ := db.getConnection(); defer conn.Close(); _, err := conn.Exec(`UPDATE files SET total_lines = ? WHERE task_id = ?`, n, id); return err
}

func (db *DBManager) GetChunk(chunkID string) (*FileChunk, error) {
	conn, _ := db.getConnection(); defer conn.Close()
	c := &FileChunk{ChunkID: chunkID}; var uid, bid, bInfo sql.NullString
	err := conn.QueryRow(`SELECT task_id, chunk_index, chunk_path, status, upload_file_id, batch_id, batch_task_info, retry FROM chunks WHERE chunk_id = ?`, chunkID).Scan(&c.TaskID, &c.ChunkIndex, &c.ChunkPath, &c.Status, &uid, &bid, &bInfo, &c.Retry)
	if err != nil { return nil, err }
	if uid.Valid { s := uid.String; c.UploadFileID = &s }
	if bid.Valid { s := bid.String; c.BatchID = &s }
	if bInfo.Valid && bInfo.String != "" { json.Unmarshal([]byte(bInfo.String), &c.BatchTaskInfo) }
	return c, nil
}

func (db *DBManager) UpdateChunkStatus(id string, status ChunkStatus, msg *string) error {
	conn, _ := db.getConnection(); defer conn.Close()
	if msg != nil { _, err := conn.Exec(`UPDATE chunks SET status = ?, error_message = ? WHERE chunk_id = ?`, status, *msg, id); return err }
	_, err := conn.Exec(`UPDATE chunks SET status = ? WHERE chunk_id = ?`, status, id); return err
}
func (db *DBManager) UpdateChunkUploadSuccess(cid string, uid string) error {
	conn, _ := db.getConnection(); defer conn.Close(); _, err := conn.Exec(`UPDATE chunks SET status = 'uploaded', upload_file_id = ?, upload_time = ? WHERE chunk_id = ?`, uid, time.Now().Format(time.RFC3339), cid); return err
}
func (db *DBManager) UpdateChunkBatchID(cid string, bid string) error {
	conn, _ := db.getConnection(); defer conn.Close(); _, err := conn.Exec(`UPDATE chunks SET batch_id = ? WHERE chunk_id = ?`, bid, cid); return err
}
func (db *DBManager) UpdateChunkBatchTaskInfo(cid string, info *BatchTaskInfo) error {
	conn, _ := db.getConnection(); defer conn.Close(); data, _ := json.Marshal(info); _, err := conn.Exec(`UPDATE chunks SET batch_task_info = ?, process_time = ? WHERE chunk_id = ?`, string(data), time.Now().Format(time.RFC3339), cid); return err
}

// 【新增】ForceCancelTask 强制终止任务
func (db *DBManager) ForceCancelTask(taskID string, reason string) error {
	conn, err := db.getConnection(); if err != nil { return err }; defer conn.Close()
	// 1. 文件标记为 Canceled
	conn.Exec(`UPDATE files SET status = ?, error_message = ?, updated_time = ? WHERE task_id = ?`, FileStatusCanceled, reason, time.Now().Format(time.RFC3339), taskID)
	// 2. 将所有未完成分块强制 Canceled
	conn.Exec(`UPDATE chunks SET status = ? WHERE task_id = ? AND status NOT IN (?, ?)`, ChunkStatusCanceled, taskID, ChunkStatusSuccess, ChunkStatusFailed)
	return nil
}