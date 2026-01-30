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

// SplitFile: 包含全量错误拦截、测试行数截断以及预估耗时功能
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
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	// 初始化循环变量
	chunkIndex := 0
	totalLines := 0
	scanLineIndex := 0
	errorCount := 0
	currentChunkLines := []string{}

	fmt.Printf("开始全量格式扫描: %s ...\n", originalFilename)

	for scanner.Scan() {
		scanLineIndex++
		rawLine := scanner.Text()
		line := strings.TrimSpace(rawLine)

		if line == "" {
			continue
		}

		var originJSON map[string]interface{}
		var parseError string = ""

		// 1. 语法与业务字段检查
		if err := json.Unmarshal([]byte(line), &originJSON); err != nil {
			parseError = "Invalid JSON syntax"
		} else {
			if _, ok := originJSON[ModelConf.MessagesKey].([]interface{}); !ok {
				parseError = fmt.Sprintf("Missing or invalid field: %s", ModelConf.MessagesKey)
			}
		}

		// 2. 发现错误：记录并统计
		if parseError != "" {
			errorCount++
			if errorWriter != nil {
				errRecord := map[string]interface{}{
					"line_no": scanLineIndex,
					"error":   parseError,
					"content": line,
				}
				errBytes, _ := json.Marshal(errRecord)
				errorWriter.WriteString(string(errBytes) + "\n")
			}
			if errorCount <= 5 {
				fmt.Printf("⚠️  [第 %d 行] 格式错误: %s\n", scanLineIndex, parseError)
			}
			continue
		}

		// 3. 处理有效数据
		messages, _ := originJSON[ModelConf.MessagesKey].([]interface{})
		body := map[string]interface{}{
			"model":      ModelConf.Domain,
			"messages":   messages,
			"max_tokens": ModelConf.MaxTokens,
		}
		
		newline := map[string]interface{}{
			"custom_id": fmt.Sprintf("%d", totalLines),
			"method":    "POST",
			"url":       "/v1/chat/completions",
			"body":      body,
		}
		newlineJSON, _ := json.Marshal(newline)
		currentChunkLines = append(currentChunkLines, string(newlineJSON))
		totalLines++

		// 4. 测试行数截断逻辑 (仅在有效数据处理后判断)
		if TEST_LINES > 0 && totalLines >= TEST_LINES {
			fmt.Printf("⚠️  [测试模式] 已达到测试行数限制: %d 行，停止后续扫描。\n", TEST_LINES)
			break
		}

		// 5. 写入分块
		if len(currentChunkLines) >= linesPerChunk {
			fm.writeChunk(taskID, chunkIndex, originalFilename, chunkDir, currentChunkLines, fileInfoObj, fileInfoObj.Retry)
			chunkIndex++
			currentChunkLines = []string{}
		}
	}

	if errorWriter != nil {
		errorWriter.Flush()
	}

	// 6. 扫描完成后统一拦截错误
	if errorCount > 0 {
		absPath, _ := filepath.Abs(errorFilePath)
		fmt.Println("----------------------------------------------------------------")
		fmt.Printf("❌ 扫描完成，发现数据异常！任务已强制拦截。\n")
		fmt.Printf("   - 扫描总行数: %d\n", scanLineIndex)
		fmt.Printf("   - 错误行总数: %d\n", errorCount)
		fmt.Printf("📂 完整错误清单请查看:\n   %s\n", absPath)
		fmt.Println("----------------------------------------------------------------")
		os.RemoveAll(chunkDir)
		return nil, fmt.Errorf("数据文件中存在 %d 处格式错误，请根据错误清单修改后重试", errorCount)
	}

	// 7. 处理剩余数据
	if len(currentChunkLines) > 0 {
		fm.writeChunk(taskID, chunkIndex, originalFilename, chunkDir, currentChunkLines, fileInfoObj, fileInfoObj.Retry)
		chunkIndex++
	}

	// 8. 统计日志
	taskLogger := NewTaskLogger(taskID)
	taskLogger.LogTimeEstimate(totalLines)

	fmt.Printf("📊 文件扫描统计 | 任务ID: %s\n", taskID)
	fmt.Printf("   - 扫描总行数: %d\n", scanLineIndex)
	fmt.Printf("   - ✅ 有效数据: %d\n", totalLines)
	fmt.Printf("   - ❌ 错误数据: %d\n", errorCount)
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

func (fm *FileManager) isFatalError(statusCode int, errCode string) (bool, string) {
	if statusCode >= 400 && statusCode < 500 {
		if statusCode == 429 {
			return false, "Rate Limit (Retryable)"
		}
		return true, fmt.Sprintf("HTTP %d (Fatal)", statusCode)
	}
	fatalCodes := map[string]bool{
		"context_length_exceeded": true, "invalid_request_error": true,
		"invalid_api_key": true, "unknown_url": true,
		"model_not_found": true, "10003": true, "invalid_role": true,
		"INVALID_PAYLOAD": true,
	}
	if fatalCodes[errCode] { return true, fmt.Sprintf("Error Code: %s", errCode) }
	return false, "Retryable Error"
}

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
		CustomID string; StatusCode int; ErrorCode string; ErrorMsg string; IsFatal bool
	}
	failureReport := []FailureRecord{}

	mergedDir := filepath.Join(MERGED_DIR, taskID)
	os.MkdirAll(mergedDir, 0755)

	for _, chunk := range chunks {
		outputFile := filepath.Join(BATCH_RESULT_DIR, taskID, "output", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
		if f, err := os.Open(outputFile); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				allOutput = append(allOutput, line)
				var r map[string]interface{}
				if json.Unmarshal([]byte(line), &r) == nil {
					if cid, ok := r["custom_id"].(string); ok { succeededIDs[cid] = true }
				}
			}
			f.Close()
		}

		errorFile := filepath.Join(BATCH_RESULT_DIR, taskID, "error", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
		if f, err := os.Open(errorFile); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				var r struct {
					CustomID string `json:"custom_id"`
					Response struct {
						StatusCode int `json:"status_code"`
						Body struct {
							Code interface{} `json:"code"`; Message string `json:"message"`
							Error struct { Code string `json:"code"`; Message string `json:"message"` } `json:"error"`
						} `json:"body"`
					} `json:"response"`
				}
				if json.Unmarshal([]byte(line), &r) == nil && r.CustomID != "" {
					var bizCode int = 0
					if r.Response.Body.Code != nil {
						if v, ok := r.Response.Body.Code.(float64); ok { bizCode = int(v) }
						if v, ok := r.Response.Body.Code.(int); ok { bizCode = v }
					}
					if r.Response.StatusCode > 0 && r.Response.StatusCode < 300 && bizCode == 0 { continue }
					
					fCode := r.Response.Body.Error.Code; fMsg := r.Response.Body.Error.Message
					if fCode == "" && bizCode != 0 { fCode = fmt.Sprintf("%d", bizCode); fMsg = r.Response.Body.Message }
					
					if r.Response.StatusCode == 0 {
						fCode = "INVALID_PAYLOAD"; fMsg = "API直接拒收请求"; r.Response.StatusCode = 400
					}

					isFatal, _ := fm.isFatalError(r.Response.StatusCode, fCode)
					if isFatal { fatalErrorIDs[r.CustomID] = true }
					failureReport = append(failureReport, FailureRecord{r.CustomID, r.Response.StatusCode, fCode, fMsg, isFatal})
				}
			}
			f.Close()
		}
	}

	needRetryLines := []string{}
	for _, chunk := range chunks {
		if f, err := os.Open(chunk.ChunkPath); err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				var r map[string]interface{}; json.Unmarshal([]byte(line), &r)
				cid, _ := r["custom_id"].(string)
				if succeededIDs[cid] || fatalErrorIDs[cid] { continue }
				needRetryLines = append(needRetryLines, line)
			}
			f.Close()
		}
	}

	os.WriteFile(filepath.Join(mergedDir, fmt.Sprintf("output_retry%d.jsonl", retry)), []byte(strings.Join(allOutput, "\n")+"\n"), 0644)
	os.WriteFile(filepath.Join(mergedDir, fmt.Sprintf("missing_records_retry%d.jsonl", retry)), []byte(strings.Join(needRetryLines, "\n")+"\n"), 0644)

	if len(failureReport) > 0 {
		reportPath := filepath.Join(mergedDir, fmt.Sprintf("failure_analysis_retry%d.csv", retry))
		csvFile, _ := os.Create(reportPath); defer csvFile.Close(); csvFile.WriteString("\xEF\xBB\xBF")
		writer := csv.NewWriter(csvFile)
		writer.Write([]string{"CustomID", "状态", "HTTP状态码", "错误代码", "错误信息", "建议操作"})
		for _, rec := range failureReport {
			status, action := "需重试", "系统将自动重试"
			if rec.IsFatal { status, action = "已放弃", "请检查Prompt长度/格式/模型" }
			writer.Write([]string{rec.CustomID, status, fmt.Sprintf("%d", rec.StatusCode), rec.ErrorCode, rec.ErrorMsg, action})
		}
		writer.Flush()
	}

	// 最终合并逻辑：如果达到了 MaxRetry 或者 没有需要重试的数据，则视为完成
	if retry >= fileInfo.MaxRetry || len(needRetryLines) == 0 {
		finalOut := []string{}
		for r := 0; r <= retry; r++ {
			if d, err := os.ReadFile(filepath.Join(mergedDir, fmt.Sprintf("output_retry%d.jsonl", r))); err == nil {
				finalOut = append(finalOut, string(d))
			}
		}
		os.WriteFile(filepath.Join(mergedDir, "output.jsonl"), []byte(strings.Join(finalOut, "")), 0644)
		fm.dbManager.UpdateFileStatus(taskID, FileStatusProcessCompleted, nil)
	}

	return map[string]interface{}{"missing_count": len(needRetryLines), "fatal_count": len(fatalErrorIDs)}, nil
}

// RetryFailedRecords: 【关键修复】严格遵守 MaxRetry，防止生成幽灵分块
func (fm *FileManager) RetryFailedRecords(taskID string) (bool, error) {
	fileInfo, _ := fm.dbManager.GetFile(taskID)

	// 【新增】严格检查：如果达到最大重试次数，直接停止，绝不生成新的分块
	if fileInfo.Retry >= fileInfo.MaxRetry {
		logInfo("任务 [%s] 已达最大重试次数 (%d)，停止生成重试分块。", taskID, fileInfo.MaxRetry)
		// 返回 true，告知上层任务已结束
		return true, nil 
	}

	missingPath := filepath.Join(MERGED_DIR, taskID, fmt.Sprintf("missing_records_retry%d.jsonl", fileInfo.Retry))
	data, _ := os.ReadFile(missingPath)
	if len(data) == 0 { return true, nil }

	newRetry := fileInfo.Retry + 1
	fm.dbManager.UpdateFileRetry(taskID, newRetry)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	chunkDir := filepath.Join(CHUNK_DIR, taskID)
	chunkIndex, currentChunkLines := 0, []string{}
	for _, line := range lines {
		currentChunkLines = append(currentChunkLines, line)
		if len(currentChunkLines) >= LINES_PER_CHUNK {
			fm.writeChunk(taskID, chunkIndex, fileInfo.OriginalFilename, chunkDir, currentChunkLines, fileInfo, newRetry)
			chunkIndex++; currentChunkLines = []string{}
		}
	}
	if len(currentChunkLines) > 0 {
		fm.writeChunk(taskID, chunkIndex, fileInfo.OriginalFilename, chunkDir, currentChunkLines, fileInfo, newRetry)
		chunkIndex++
	}
	fm.dbManager.UpdateFileTotalChunks(taskID, fileInfo.TotalChunks+chunkIndex)
	return false, nil
}