package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type FileManager struct {
	dbManager *DBManager
}

func NewFileManager(db *DBManager) *FileManager {
	return &FileManager{dbManager: db}
}

func (fm *FileManager) generateTaskID(filename string) string {
	return fmt.Sprintf("task_%s_%s", time.Now().Format("20060102_150405"), uuid.New().String()[:8])
}

func (fm *FileManager) generateChunkID(taskID string, chunkIndex int, retry int) string {
	if retry > 0 {
		return fmt.Sprintf("%s_retry%d_chunk_%d", taskID, retry, chunkIndex)
	}
	return fmt.Sprintf("%s_chunk_%d", taskID, chunkIndex)
}

func (fm *FileManager) writeChunk(taskID string, chunkIndex int, originalFilename string,
	chunkDir string, currentChunkLines []string, fileInfo *FileInfo, retry int) error {
	chunkID := fm.generateChunkID(taskID, chunkIndex, retry)
	var chunkFilename string
	if retry > 0 {
		chunkFilename = fmt.Sprintf("retry%d_part%d.%s", retry, chunkIndex, originalFilename)
	} else {
		chunkFilename = fmt.Sprintf("part%d.%s", chunkIndex, originalFilename)
	}
	chunkPath := filepath.Join(chunkDir, chunkFilename)
	chunkData := strings.Join(currentChunkLines, "\n")
	os.WriteFile(chunkPath, []byte(chunkData), 0644)
	chunk := &FileChunk{
		ChunkID:    chunkID,
		TaskID:     taskID,
		ChunkIndex: chunkIndex,
		ChunkPath:  chunkPath,
		ChunkSize:  len([]byte(chunkData)),
		Status:     ChunkStatusPending,
		Retry:      retry,
	}
	return fm.dbManager.AddChunk(chunk)
}

// SplitFile: 包含错误拦截、记录、暂停展示统计以及【预估耗时日志】功能
func (fm *FileManager) SplitFile(filePath string, originalFilename string, linesPerChunk int, taskID string) (*FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("文件不存在")
	}
	defer file.Close()

	if taskID == "" {
		taskID = fm.generateTaskID(originalFilename)
	}
	exists, _ := fm.dbManager.CheckTaskIDExists(taskID)
	if exists {
		return nil, fmt.Errorf("task_id [%s] 已存在", taskID)
	}

	stat, _ := file.Stat()
	fileInfoObj := &FileInfo{
		TaskID:           taskID,
		OriginalFilename: originalFilename,
		FilePath:         filePath,
		FileSize:         stat.Size(),
		CreatedTime:      time.Now().Format(time.RFC3339),
		UpdatedTime:      time.Now().Format(time.RFC3339),
		Status:           FileStatusSplitting,
		Chunks:           []*FileChunk{},
		MaxRetry:         MAX_RETRY_COUNT,
	}
	fm.dbManager.CreateFile(fileInfoObj)

	chunkDir := filepath.Join(CHUNK_DIR, taskID)
	os.MkdirAll(chunkDir, 0755)

	// --- 准备错误记录文件 ---
	validationDir := filepath.Join(BATCH_RESULT_DIR, taskID, "validation")
	os.MkdirAll(validationDir, 0755)
	errorFilePath := filepath.Join(validationDir, "format_errors.jsonl")

	errorFile, err := os.Create(errorFilePath)
	var errorWriter *bufio.Writer
	if err == nil {
		errorWriter = bufio.NewWriter(errorFile)
		defer errorFile.Close()
		defer errorWriter.Flush()
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	chunkIndex := 0
	totalLines := 0
	scanLineIndex := 0
	errorCount := 0
	currentChunkLines := []string{}

	fmt.Printf("正在扫描文件: %s ...\n", originalFilename)

	for scanner.Scan() {
		scanLineIndex++
		rawLine := scanner.Text()
		line := strings.TrimSpace(rawLine)

		if line == "" {
			continue
		}

		var originJSON map[string]interface{}
		var parseError string = ""

		// 1. JSON 语法检查
		if err := json.Unmarshal([]byte(line), &originJSON); err != nil {
			parseError = "Invalid JSON syntax"
		} else {
			// 2. 业务字段检查
			if _, ok := originJSON[ModelConf.MessagesKey].([]interface{}); !ok {
				parseError = fmt.Sprintf("Missing or invalid field: %s", ModelConf.MessagesKey)
			}
		}

		// 3. 错误处理
		if parseError != "" {
			errorCount++
			if errorWriter != nil {
				errRecord := map[string]interface{}{
					"line_no": scanLineIndex,
					"error":   parseError,
					"content": line,
				}
				if errBytes, err := json.Marshal(errRecord); err == nil {
					errorWriter.WriteString(string(errBytes) + "\n")
				}
			}
			if errorCount <= 5 {
				fmt.Printf("⚠️  [第 %d 行] 格式错误: %s\n", scanLineIndex, parseError)
			}
			continue
		}

		// 4. 有效数据处理
		messages, _ := originJSON[ModelConf.MessagesKey].([]interface{})
		body := map[string]interface{}{"model": ModelConf.Domain, "messages": messages, "max_tokens": ModelConf.MaxTokens}
		if ModelConf.Temperature != nil {
			body["temperature"] = *ModelConf.Temperature
		}
		if ModelConf.TopP != nil {
			body["top_p"] = *ModelConf.TopP
		}

		newline := map[string]interface{}{
			"custom_id": fmt.Sprintf("%d", totalLines),
			"method":    "POST", "url": "/v1/chat/completions", "body": body,
		}
		newlineJSON, _ := json.Marshal(newline)
		currentChunkLines = append(currentChunkLines, string(newlineJSON))
		totalLines++

		if TEST_LINES > 0 && totalLines >= TEST_LINES {
			break
		}

		if len(currentChunkLines) >= linesPerChunk {
			fm.writeChunk(taskID, chunkIndex, originalFilename, chunkDir, currentChunkLines, fileInfoObj, fileInfoObj.Retry)
			chunkIndex++
			currentChunkLines = []string{}
		}
	}

	if len(currentChunkLines) > 0 {
		fm.writeChunk(taskID, chunkIndex, originalFilename, chunkDir, currentChunkLines, fileInfoObj, fileInfoObj.Retry)
		chunkIndex++
	}

	if errorWriter != nil {
		errorWriter.Flush()
	}

	// --- 5. 打印统计并调用日志记录时间 ---
	fmt.Println("----------------------------------------------------------------")
	
	// 计算并记录预估时间 (调用 logger.go 中的新方法)
	// 这会将 "⏳ [预估耗时]..." 写入 app.log 并显示在终端
	taskLogger := NewTaskLogger(taskID)
	taskLogger.LogTimeEstimate(totalLines) 

	fmt.Printf("📊 文件扫描统计 | 任务ID: %s\n", taskID)
	fmt.Printf("   - 扫描总行数: %d\n", scanLineIndex)
	fmt.Printf("   - ✅ 有效数据: %d\n", totalLines)
	fmt.Printf("   - ❌ 错误数据: %d\n", errorCount)

	if errorCount > 0 {
		absPath, _ := filepath.Abs(errorFilePath)
		fmt.Printf("\n📂 错误行已单独保存至:\n   %s\n", absPath)
		fmt.Println("\n⚠️  检测到格式错误，请阅读以上统计信息 (系统将在 5秒 后自动继续)...")
		time.Sleep(5 * time.Second)
	} else {
		fmt.Println("\n✅ 校验通过，准备开始处理任务 (2秒后开始)...")
		time.Sleep(2 * time.Second)
	}
	fmt.Println("----------------------------------------------------------------")

	fileInfoObj.TotalChunks = chunkIndex
	fileInfoObj.TotalLines = totalLines
	fm.dbManager.UpdateFileStatus(taskID, FileStatusSplitCompleted, nil)
	fm.dbManager.UpdateFileTotalChunks(taskID, chunkIndex)
	fm.dbManager.UpdateFileTotalLines(taskID, totalLines)
	return fileInfoObj, nil
}

func (fm *FileManager) SaveFile(taskID string, chunkID string, fileContent string, isError bool) error {
	chunk, _ := fm.dbManager.GetChunk(chunkID)
	if chunk == nil {
		return fmt.Errorf("chunk not found")
	}
	path := filepath.Join(BATCH_RESULT_DIR, taskID, "output")
	if isError {
		path = filepath.Join(BATCH_RESULT_DIR, taskID, "error")
	}
	os.MkdirAll(path, 0755)
	return os.WriteFile(filepath.Join(path, fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunkID)), []byte(fileContent), 0644)
}

// isFatalError: 判断是否为不可重试的致命错误
func (fm *FileManager) isFatalError(statusCode int, errCode string) (bool, string) {
	// 1. 状态码判断: 400-499 通常是请求问题 (除了 429 Rate Limit)
	if statusCode >= 400 && statusCode < 500 {
		if statusCode == 429 {
			return false, "Rate Limit (Retryable)"
		}
		return true, fmt.Sprintf("HTTP %d (Fatal)", statusCode)
	}

	// 2. 错误码判断 (OpenAI/DeepSeek/Spark 标准错误码)
	fatalCodes := map[string]bool{
		"context_length_exceeded":  true, // 上下文超长
		"invalid_request_error":    true, // 请求格式错误
		"invalid_api_key":          true, // Key 错误
		"unknown_url":              true, // URL 错误
		"string_above_128k_tokens": true, // 具体模型限制
		"model_not_found":          true, // 模型名错误
		"10003":                    true, // Spark: invalid role / 参数错误
		"invalid_role":             true, // 角色错误
		"INVALID_PAYLOAD":          true, // 自定义：payload 错误
	}

	if fatalCodes[errCode] {
		return true, fmt.Sprintf("Error Code: %s", errCode)
	}

	return false, "Retryable Error"
}

// MergeBatchResults: 包含对“假成功”数据（HTTP 101 但 code!=0）的识别
// 以及 CSV 报告生成
func (fm *FileManager) MergeBatchResults(taskID string, retry int) (map[string]interface{}, error) {
	fileInfo, err := fm.dbManager.GetFile(taskID)
	if err != nil || fileInfo == nil {
		return nil, fmt.Errorf("任务不存在")
	}

	var chunks []*FileChunk
	for _, c := range fileInfo.Chunks {
		if c.Retry == retry {
			chunks = append(chunks, c)
		}
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkIndex < chunks[j].ChunkIndex })

	allOutput := []string{}
	succeededIDs := make(map[string]bool)
	fatalErrorIDs := make(map[string]bool)

	type FailureRecord struct {
		CustomID   string
		StatusCode int
		ErrorCode  string
		ErrorMsg   string
		IsFatal    bool
	}
	failureReport := []FailureRecord{}

	mergedDir := filepath.Join(MERGED_DIR, taskID)
	os.MkdirAll(mergedDir, 0755)

	// --- 1. 读取 Output 和 Error 文件 ---
	for _, chunk := range chunks {
		// A. Output (成功记录)
		outputFile := filepath.Join(BATCH_RESULT_DIR, taskID, "output", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
		if f, err := os.Open(outputFile); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				allOutput = append(allOutput, line)
				var r map[string]interface{}
				if json.Unmarshal([]byte(line), &r) == nil {
					if cid, ok := r["custom_id"].(string); ok {
						succeededIDs[cid] = true
					}
				}
			}
			f.Close()
		}

		// B. Error (失败记录)
		errorFile := filepath.Join(BATCH_RESULT_DIR, taskID, "error", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
		if f, err := os.Open(errorFile); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				
				// 定义解析结构 (支持 Spark 等返回的 code 为 int 的情况)
				var r struct {
					CustomID string `json:"custom_id"`
					Response struct {
						StatusCode int `json:"status_code"`
						Body       struct {
							Code    interface{} `json:"code"` // 支持数字或字符串
							Message string      `json:"message"`
							Error   struct {
								Code    string `json:"code"`
								Message string `json:"message"`
							} `json:"error"`
						} `json:"body"`
					} `json:"response"`
				}

				if json.Unmarshal([]byte(line), &r) == nil && r.CustomID != "" {
					
					// ================== 核心逻辑修正 ==================
					
					// 1. 提取业务状态码 (Business Code)
					var bizCode int = 0
					if r.Response.Body.Code != nil {
						switch v := r.Response.Body.Code.(type) {
						case float64:
							bizCode = int(v)
						case int:
							bizCode = v
						}
					}

					// 2. 判断是否真的是成功
					// 如果 HTTP < 300 且 业务Code == 0，才是真成功，忽略它
					if r.Response.StatusCode > 0 && r.Response.StatusCode < 300 && bizCode == 0 {
						continue
					}

					// 3. 提取错误信息
					finalErrorCode := r.Response.Body.Error.Code
					finalErrorMsg := r.Response.Body.Error.Message
					
					// 兼容 Spark 错误格式 (body.code != 0 但 error 字段为空的情况)
					if finalErrorCode == "" && bizCode != 0 {
						finalErrorCode = fmt.Sprintf("%d", bizCode)
						finalErrorMsg = r.Response.Body.Message
					}

					// Case 2: 非标准错误 (StatusCode == 0，无 Response 字段)
					if r.Response.StatusCode == 0 {
						fatalErrorIDs[r.CustomID] = true
						failureReport = append(failureReport, FailureRecord{
							CustomID:   r.CustomID,
							StatusCode: 400,
							ErrorCode:  "INVALID_PAYLOAD",
							ErrorMsg:   "API直接拒收请求，Payload格式严重错误",
							IsFatal:    true,
						})
						continue
					}

					// Case 3: 真正的错误
					isFatal, _ := fm.isFatalError(r.Response.StatusCode, finalErrorCode)
					if isFatal {
						fatalErrorIDs[r.CustomID] = true
					}

					failureReport = append(failureReport, FailureRecord{
						CustomID:   r.CustomID,
						StatusCode: r.Response.StatusCode,
						ErrorCode:  finalErrorCode,
						ErrorMsg:   finalErrorMsg,
						IsFatal:    isFatal,
					})
				}
			}
			f.Close()
		}
	}

	// --- 2. 筛选需要重试的数据 ---
	needRetryLines := []string{}
	for _, chunk := range chunks {
		chunkInputFile := chunk.ChunkPath
		if f, err := os.Open(chunkInputFile); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				var r map[string]interface{}
				json.Unmarshal([]byte(line), &r)
				cid, _ := r["custom_id"].(string)

				if succeededIDs[cid] { continue }
				if fatalErrorIDs[cid] { continue } // 致命错误不重试
				
				needRetryLines = append(needRetryLines, line)
			}
			f.Close()
		}
	}

	// --- 3. 写入文件 ---
	os.WriteFile(filepath.Join(mergedDir, fmt.Sprintf("output_retry%d.jsonl", retry)), []byte(strings.Join(allOutput, "\n")+"\n"), 0644)
	os.WriteFile(filepath.Join(mergedDir, fmt.Sprintf("missing_records_retry%d.jsonl", retry)), []byte(strings.Join(needRetryLines, "\n")+"\n"), 0644)

	// 生成 CSV 失败分析报告
	if len(failureReport) > 0 {
		reportPath := filepath.Join(mergedDir, fmt.Sprintf("failure_analysis_retry%d.csv", retry))
		csvFile, _ := os.Create(reportPath)
		defer csvFile.Close()
		csvFile.WriteString("\xEF\xBB\xBF") 
		writer := csv.NewWriter(csvFile)
		writer.Write([]string{"CustomID", "状态", "HTTP状态码", "错误代码", "错误信息", "建议操作"})
		for _, rec := range failureReport {
			status := "需重试"
			action := "系统将自动重试"
			if rec.IsFatal {
				status = "已放弃"
				action = "请检查Prompt长度/格式/模型"
			}
			writer.Write([]string{
				rec.CustomID, status, fmt.Sprintf("%d", rec.StatusCode),
				rec.ErrorCode, rec.ErrorMsg, action,
			})
		}
		writer.Flush()
		fmt.Printf("\n📋 已生成失败分析报告: %s\n", reportPath)
	}

	// --- 4. 最终合并 ---
	if retry == fileInfo.MaxRetry || len(needRetryLines) == 0 {
		finalOut := []string{}
		for r := 0; r <= retry; r++ {
			if d, err := os.ReadFile(filepath.Join(mergedDir, fmt.Sprintf("output_retry%d.jsonl", r))); err == nil {
				finalOut = append(finalOut, string(d))
			}
		}
		os.WriteFile(filepath.Join(mergedDir, "output.jsonl"), []byte(strings.Join(finalOut, "")), 0644)
		fm.dbManager.UpdateFileStatus(taskID, FileStatusProcessCompleted, nil)
	}

	return map[string]interface{}{
		"missing_count": len(needRetryLines),
		"fatal_count":   len(fatalErrorIDs),
	}, nil
}

func (fm *FileManager) RetryFailedRecords(taskID string) (bool, error) {
	fileInfo, _ := fm.dbManager.GetFile(taskID)
	missingPath := filepath.Join(MERGED_DIR, taskID, fmt.Sprintf("missing_records_retry%d.jsonl", fileInfo.Retry))
	data, _ := os.ReadFile(missingPath)
	if len(data) == 0 {
		return true, nil
	}

	newRetry := fileInfo.Retry + 1
	fm.dbManager.UpdateFileRetry(taskID, newRetry)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	chunkDir := filepath.Join(CHUNK_DIR, taskID)
	chunkIndex, currentChunkLines := 0, []string{}

	for _, line := range lines {
		currentChunkLines = append(currentChunkLines, line)
		if len(currentChunkLines) >= LINES_PER_CHUNK {
			fm.writeChunk(taskID, chunkIndex, fileInfo.OriginalFilename, chunkDir, currentChunkLines, fileInfo, newRetry)
			chunkIndex++
			currentChunkLines = []string{}
		}
	}
	if len(currentChunkLines) > 0 {
		fm.writeChunk(taskID, chunkIndex, fileInfo.OriginalFilename, chunkDir, currentChunkLines, fileInfo, newRetry)
		chunkIndex++
	}
	fm.dbManager.UpdateFileTotalChunks(taskID, fileInfo.TotalChunks+chunkIndex)
	return false, nil
}