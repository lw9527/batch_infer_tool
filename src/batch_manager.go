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
	"time" // [新增] 用于超时控制
)

// BatchManager 批处理管理器
type BatchManager struct {
	password string
	header   map[string]string
}

// NewBatchManager 创建批处理管理器
func NewBatchManager() *BatchManager {
	header := make(map[string]string)
	// [修改] 使用全局配置 ModelConf
	header["Authorization"] = "Bearer " + ModelConf.Password 
	header["Content-Type"] = "application/json"

	return &BatchManager{
		password: ModelConf.Password,
		header:   header,
	}
}

// UploadFile 上传文件获取链接
func (bm *BatchManager) UploadFile(filePath string) (string, error) {
	url := "https://spark-api-open.xf-yun.com/v1/files"

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	writer.WriteField("purpose", "batch")

	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return "", err
	}

	_, err = io.Copy(part, file)
	if err != nil {
		return "", err
	}

	err = writer.Close()
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+bm.password)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// [修改] 增加 300秒 超时防止大文件上传卡死
	client := &http.Client{Timeout: 300 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	logInfo("上传文件结果：%s", string(respBody))

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", err
	}

	id, ok := result["id"].(string)
	if !ok {
		return "", fmt.Errorf("响应中缺少id字段")
	}

	return id, nil
}

// GetFiles 获取文件信息 (保留原版逻辑)
func (bm *BatchManager) GetFiles(fileID *string) (map[string]interface{}, error) {
	var url string
	if fileID == nil {
		url = "https://spark-api-open.xf-yun.com/v1/files"
	} else {
		url = fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/files/%s", *fileID)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range bm.header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 15 * time.Second} // [新增] 超时
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// GetFileContent 获取文件内容 (修改返回类型为 []byte 以兼容存储逻辑)
func (bm *BatchManager) GetFileContent(fileID string) ([]byte, error) {
	url := fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/files/%s/content", fileID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range bm.header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 120 * time.Second} // [新增] 超时
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// DeleteFile 删除文件 (保留原版逻辑)
func (bm *BatchManager) DeleteFile(fileID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/files/%s", fileID)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range bm.header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 10 * time.Second} // [新增] 超时
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// CreateBatchTask 创建任务并返回taskid
func (bm *BatchManager) CreateBatchTask(inputFileID string) (string, error) {
	url := "https://spark-api-open.xf-yun.com/v1/batches"

	body := map[string]interface{}{
		"input_file_id":     inputFileID,
		"endpoint":          "/v1/chat/completions",
		"completion_window": "24h",
		// [新增] 保留你之前添加的 metadata 逻辑方便识别
		"metadata":          map[string]string{"description": "batch_job"}, 
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	logInfo("创建batch 任务请求：%s", string(bodyJSON))

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyJSON))
	if err != nil {
		return "", err
	}

	for k, v := range bm.header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 30 * time.Second} // [新增] 超时
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	logInfo("创建batch 任务结果：%s", string(respBody))

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", err
	}

	id, ok := result["id"].(string)
	if !ok {
		return "", fmt.Errorf("响应中缺少id字段")
	}

	return id, nil
}

// CancelBatchTask 取消批量任务 (保留原版逻辑)
func (bm *BatchManager) CancelBatchTask(batchID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/batches/%s/cancel", batchID)

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range bm.header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 10 * time.Second} // [新增] 超时
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// QueryBatchTask 查询批量任务状态 (保留原版逻辑)
func (bm *BatchManager) QueryBatchTask(batchID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://spark-api-open.xf-yun.com/v1/batches/%s", batchID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range bm.header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 10 * time.Second} // [新增] 超时
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// GetResult 查询结果 (适配 models.go 的 BatchTaskInfo)
func (bm *BatchManager) GetResult(batchID string) (*BatchTaskInfo, error) {
	resp, err := bm.QueryBatchTask(batchID)
	if err != nil {
		return nil, err
	}

	status, ok := resp["status"].(string)
	if !ok {
		return nil, fmt.Errorf("响应中缺少status字段")
	}

	// 检查状态是否在有效范围内
	validStatuses := []string{
		string(BatchStatusCompleted),
		string(BatchStatusFailed),
		string(BatchStatusCanceled),
		string(BatchStatusExpired),
		string(BatchStatusInProgress),
		string(BatchStatusFinalizing),
		string(BatchStatusValidating), // [新增] 兼容你代码中的 Validating
	}

	valid := false
	for _, vs := range validStatuses {
		if status == vs {
			valid = true
			break
		}
	}

	if !valid {
		return nil, nil
	}

	outputFileID, _ := resp["output_file_id"].(string)
	errorFileID, _ := resp["error_file_id"].(string)

	batchTaskInfo := &BatchTaskInfo{
		BatchID:     batchID,
		ID:          batchID, // [新增] 适配你 models.go 中的 ID 字段
		Status:      BatchStatus(status),
		OutputFileID: outputFileID,
	}

	if errorFileID != "" {
		batchTaskInfo.ErrorFileID = &errorFileID
	}

	// [修改] 安全处理数字转换
	if requestCounts, ok := resp["request_counts"].(map[string]interface{}); ok {
		if t, ok := requestCounts["total"].(float64); ok { batchTaskInfo.TotalCount = int(t) }
		if c, ok := requestCounts["completed"].(float64); ok { batchTaskInfo.CompletedCount = int(c) }
		if f, ok := requestCounts["failed"].(float64); ok { batchTaskInfo.FailedCount = int(f) }
		
		// 同步到内嵌结构体以兼容 ProgressDisplay
		batchTaskInfo.RequestCounts.Total = batchTaskInfo.TotalCount
		batchTaskInfo.RequestCounts.Completed = batchTaskInfo.CompletedCount
		batchTaskInfo.RequestCounts.Failed = batchTaskInfo.FailedCount
	}

	logInfo("查询批量任务结果：%+v", batchTaskInfo)
	return batchTaskInfo, nil
}