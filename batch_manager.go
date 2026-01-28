package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// BatchManager 批处理管理器
type BatchManager struct {
	password string
	header   map[string]string
}

func NewBatchManager() *BatchManager {
	header := make(map[string]string)
	header["Authorization"] = "Bearer " + ModelConf.Password
	header["Content-Type"] = "application/json"
	return &BatchManager{password: ModelConf.Password, header: header}
}

// UploadFile 上传文件 (增加 300秒 超时)
func (bm *BatchManager) UploadFile(filePath string) (string, error) {
	url := "https://spark-api-open.xf-yun.com/v1/files"
	file, err := os.Open(filePath); if err != nil { return "", err }
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("purpose", "batch")
	part, _ := writer.CreateFormFile("file", filepath.Base(filePath))
	io.Copy(part, file)
	writer.Close()

	req, _ := http.NewRequest("POST", url, body)
	req.Header.Set("Authorization", "Bearer "+bm.password)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// 【修改点】超时防卡死
	client := &http.Client{Timeout: 300 * time.Second} 
	resp, err := client.Do(req)
	if err != nil { return "", err }
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 { return "", fmt.Errorf("上传失败 HTTP %d: %s", resp.StatusCode, string(respBody)) }

	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	if id, ok := result["id"].(string); ok { return id, nil }
	return "", fmt.Errorf("未返回文件ID")
}

// CreateBatchTask 创建任务 (增加 30秒 超时)
func (bm *BatchManager) CreateBatchTask(fileID string) (string, error) {
	url := "https://spark-api-open.xf-yun.com/v1/batches"
	data := map[string]interface{}{"input_file_id": fileID, "endpoint": "/v1/chat/completions", "completion_window": "24h", "metadata": map[string]string{"description": "batch_job"}}
	jsonData, _ := json.Marshal(data)
	
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	for k, v := range bm.header { req.Header.Set(k, v) }

	// 【修改点】超时防卡死
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return "", err }
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 { return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody)) }

	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	if id, ok := result["id"].(string); ok { return id, nil }
	return "", fmt.Errorf("未返回Batch ID")
}

// GetResult 查询结果 (增加 10秒 超时)
func (bm *BatchManager) GetResult(batchID string) (*BatchTaskInfo, error) {
	url := fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/batches/%s", batchID)
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range bm.header { req.Header.Set(k, v) }

	// 【修改点】超时防卡死
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return nil, err }
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 { return nil, fmt.Errorf("HTTP %d", resp.StatusCode) }

	var r map[string]interface{}
	json.Unmarshal(respBody, &r)
	
	info := &BatchTaskInfo{
		ID: batchID, 
		Status: BatchStatus(r["status"].(string)),
	}
	// 兼容处理
	if oid, ok := r["output_file_id"].(string); ok { info.OutputFileID = oid }
	if eid, ok := r["error_file_id"].(string); ok && eid != "" { info.ErrorFileID = &eid }

	if counts, ok := r["request_counts"].(map[string]interface{}); ok {
		if t, ok := counts["total"].(float64); ok { info.TotalCount = int(t) }
		if c, ok := counts["completed"].(float64); ok { info.CompletedCount = int(c) }
		if f, ok := counts["failed"].(float64); ok { info.FailedCount = int(f) }
		info.RequestCounts.Total = info.TotalCount; info.RequestCounts.Completed = info.CompletedCount; info.RequestCounts.Failed = info.FailedCount
	}
	return info, nil
}

// GetFileContent 下载 (增加 120秒 超时)
func (bm *BatchManager) GetFileContent(fileID string) ([]byte, error) {
	url := fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/files/%s/content", fileID)
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range bm.header { req.Header.Set(k, v) }

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return nil, err }
	defer resp.Body.Close()

	if resp.StatusCode != 200 { return nil, fmt.Errorf("HTTP %d", resp.StatusCode) }
	return io.ReadAll(resp.Body)
}