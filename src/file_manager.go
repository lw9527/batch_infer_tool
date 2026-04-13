package main

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const scannerMaxTokenSize = 100 * 1024 * 1024

type customIDOnlyRecord struct {
	CustomID string `json:"custom_id"`
}

type mergedOutputRecord struct {
	CustomID string          `json:"custom_id"`
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

type chunkInputRecord struct {
	CustomID string          `json:"custom_id"`
	Body     json.RawMessage `json:"body"`
}

type chunkMergeResult struct {
	index          int
	chunkID        string
	outputLines    []string
	errorLines     []string
	missingBodies  []json.RawMessage
	missingIDs     []string
	logEachMissing bool
	completedCount int
	totalCount     int
}

func parseCustomID(line []byte) (string, error) {
	var record customIDOnlyRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return "", err
	}
	if record.CustomID == "" {
		return "", errors.New("missing custom_id")
	}
	return record.CustomID, nil
}

func parseChunkInputLine(line []byte) (string, json.RawMessage, error) {
	var record chunkInputRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return "", nil, err
	}
	if record.CustomID == "" {
		return "", nil, errors.New("missing custom_id")
	}
	return record.CustomID, record.Body, nil
}

func (fm *FileManager) mergeSingleChunk(taskID string, chunk *FileChunk, index int) (*chunkMergeResult, error) {
	estimatedRecords := LINES_PER_CHUNK
	if estimatedRecords <= 0 {
		estimatedRecords = 1024
	}
	if chunk.BatchTaskInfo != nil && chunk.BatchTaskInfo.TotalCount > 0 {
		estimatedRecords = chunk.BatchTaskInfo.TotalCount
	}

	result := &chunkMergeResult{
		index:          index,
		chunkID:        chunk.ChunkID,
		outputLines:    make([]string, 0, estimatedRecords),
		errorLines:     make([]string, 0, estimatedRecords/8+1),
		missingBodies:  make([]json.RawMessage, 0, estimatedRecords/4+1),
		missingIDs:     make([]string, 0, estimatedRecords/4+1),
		logEachMissing: chunk.BatchTaskInfo != nil && chunk.BatchTaskInfo.CompletedCount != chunk.BatchTaskInfo.TotalCount,
	}
	if chunk.BatchTaskInfo != nil {
		result.completedCount = chunk.BatchTaskInfo.CompletedCount
		result.totalCount = chunk.BatchTaskInfo.TotalCount
	}

	chunkPath := chunk.ChunkPath
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		return result, nil
	}

	chunkCustomIDs := make(map[string]struct{}, estimatedRecords)
	chunkRawLines := make(map[string]json.RawMessage, estimatedRecords)
	chunkBodyByID := make(map[string]json.RawMessage, estimatedRecords)

	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxTokenSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lineBytes := []byte(line)
		customID, bodyRaw, err := parseChunkInputLine(lineBytes)
		if err != nil {
			logInfo("警告: 解析chunk记录失败: %v", err)
			continue
		}
		chunkCustomIDs[customID] = struct{}{}
		chunkRawLines[customID] = append([]byte(nil), lineBytes...)
		if len(bodyRaw) > 0 {
			chunkBodyByID[customID] = append([]byte(nil), bodyRaw...)
		}
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return nil, err
	}
	file.Close()

	outputCustomIDs := make(map[string]struct{}, estimatedRecords)
	outputFile := filepath.Join(BATCH_RESULT_DIR, taskID, "output", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
	if _, err := os.Stat(outputFile); err == nil {
		file, err := os.Open(outputFile)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxTokenSize)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			lineBytes := []byte(line)
			customID, err := parseCustomID(lineBytes)
			if err != nil {
				logInfo("警告: 解析output记录失败: %v", err)
				continue
			}
			outputCustomIDs[customID] = struct{}{}

			request := json.RawMessage([]byte("{}"))
			if raw, ok := chunkRawLines[customID]; ok {
				request = raw
			}
			newRecordJSON, err := json.Marshal(mergedOutputRecord{
				CustomID: customID,
				Request:  request,
				Response: json.RawMessage(lineBytes),
			})
			if err != nil {
				logInfo("警告: 序列化新记录失败: %v", err)
				continue
			}
			result.outputLines = append(result.outputLines, string(newRecordJSON))
		}
		if err := scanner.Err(); err != nil {
			file.Close()
			return nil, err
		}
		file.Close()
	}

	errorFile := filepath.Join(BATCH_RESULT_DIR, taskID, "error", fmt.Sprintf("retry%d_%s.jsonl", chunk.Retry, chunk.ChunkID))
	if _, err := os.Stat(errorFile); err == nil {
		file, err := os.Open(errorFile)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxTokenSize)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				result.errorLines = append(result.errorLines, line)
			}
		}
		if err := scanner.Err(); err != nil {
			file.Close()
			return nil, err
		}
		file.Close()
	}

	for customID := range chunkCustomIDs {
		if _, ok := outputCustomIDs[customID]; ok {
			continue
		}
		if body, ok := chunkBodyByID[customID]; ok && len(body) > 0 {
			result.missingBodies = append(result.missingBodies, body)
		} else {
			result.missingBodies = append(result.missingBodies, json.RawMessage([]byte("{}")))
		}
		result.missingIDs = append(result.missingIDs, customID)
	}

	return result, nil
}

// FileManager 文件管理器
type FileManager struct {
	dbManager *DBManager
}

// NewFileManager 创建文件管理器
func NewFileManager(dbManager *DBManager) *FileManager {
	return &FileManager{dbManager: dbManager}
}

// generatetaskID 生成文件ID
func (fm *FileManager) generateTaskID(filename string) string {
	timestamp := time.Now().Format(time.RFC3339)
	uniqueID := uuid.New().String()
	data := fmt.Sprintf("%s_%s_%s", filename, timestamp, uniqueID)
	hash := md5.Sum([]byte(data))
	return fmt.Sprintf("%x", hash)
}

// generateChunkID 生成文件块ID
func (fm *FileManager) generateChunkID(taskID string, chunkIndex int, retry int) string {
	if retry > 0 {
		return fmt.Sprintf("%s_retry%d_chunk_%d", taskID, retry, chunkIndex)
	}
	return fmt.Sprintf("%s_chunk_%d", taskID, chunkIndex)
}

// writeChunk 写入文件块
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

	// 写入块文件（保留换行符）
	chunkData := strings.Join(currentChunkLines, "\n")

	err := os.WriteFile(chunkPath, []byte(chunkData), 0644)
	if err != nil {
		return err
	}

	// 创建文件块信息
	chunk := &FileChunk{
		ChunkID:    chunkID,
		TaskID:     taskID,
		ChunkIndex: chunkIndex,
		ChunkPath:  chunkPath,
		ChunkSize:  len([]byte(chunkData)),
		Status:     ChunkStatusPending,
		Retry:      retry,
	}

	// 保存到数据库
	if err := fm.dbManager.AddChunk(chunk); err != nil {
		return err
	}

	fileInfo.Chunks = append(fileInfo.Chunks, chunk)
	return nil
}
func ValidateMessage(message interface{}) error {
	//将interface{}类型断言为map[string]interface{}（JSON对象解析后的标准类型）
	msgMap, ok := message.(map[string]interface{})
	if !ok {
		return errors.New("message格式错误：非有效的JSON对象类型")
	}

	//校验必选字段role - 存在性 + 字符串类型
	roleVal, hasRole := msgMap["role"]
	if !hasRole {
		return errors.New("message格式错误：缺少必选字段role")
	}
	if _, ok := roleVal.(string); !ok {
		return errors.New("message格式错误：role字段必须为字符串类型")
	}

	//校验content字段 - 存在性 + 字符串/数组类型（JSON数组解析后为[]interface{}）
	contentVal, hasContent := msgMap["content"]
	if !hasContent {
		return errors.New("message格式错误：缺少字段content")
	}
	// 判断content是否为字符串 或 数组（[]interface{}）
	switch contentVal.(type) {
	case string:
		// 字符串类型，合法
	case []interface{}:
		// 数组类型，合法（JSON数组解析后统一为[]interface{}）
	default:
		return errors.New("message格式错误：content字段必须为字符串或数组类型")
	}

	// 所有校验通过
	return nil
}

func CheckFileFormat(file *os.File, mc ModelConfig) error {
	if file == nil {
		return fmt.Errorf("file si nil")
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 100*1024*1024), 100*1024*1024)
	currentLine := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 解析JSON
		var originJSON map[string]interface{}
		if err := json.Unmarshal([]byte(line), &originJSON); err != nil {
			return fmt.Errorf("err in line %d: %s", currentLine, err.Error())
		}
		messages, ok := originJSON[mc.MessagesKey].([]interface{})
		if !ok {
			return fmt.Errorf("err in line %d: %s(%s) is wrong", currentLine, mc.MessagesKey, originJSON[mc.MessagesKey])
		}
		for _, message := range messages {
			if err := ValidateMessage(message); err != nil {
				return fmt.Errorf("err in line %d, ")
			}
		}

	}
	return nil

}

// SplitFile 分割文件（按行数）；modelCfg 会写入 FileInfo 供后续上传/批处理使用
func (fm *FileManager) SplitFile(filePath string, originalFilename string, taskID string, linesPerChunk int, modelCfg ModelConfig) (*FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("文件不存在: %s", filePath)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// 创建文件信息
	fileInfoObj := &FileInfo{
		TaskID:           taskID,
		OriginalFilename: originalFilename,
		FilePath:         filePath,
		FileSize:         fileSize,
		TotalChunks:      0,
		TotalLines:       0,
		Status:           FileStatusSplitting,
		CreatedTime:      time.Now().Format(time.RFC3339),
		UpdatedTime:      time.Now().Format(time.RFC3339),
		Chunks:           []*FileChunk{},
		Retry:            0,
		MaxRetry:         MAX_RETRY_COUNT, // 在分割文件时写入最大重试次数
		Model:            modelCfg,
	}

	// 保存文件信息到数据库
	if err := fm.dbManager.CreateFile(fileInfoObj); err != nil {
		return nil, err
	}

	// 创建文件块目录
	chunkDir := filepath.Join(CHUNK_DIR, taskID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	// 设置更大的缓冲区以支持超长行（默认64KB，设置为100MB）
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 100*1024*1024) // 最大100MB的缓冲区

	chunkIndex := 0
	currentChunkLines := []string{}
	totalLines := 0
	currentChunkSize := 0                 // 当前chunk的累计大小（字节数）
	const maxChunkSize = 99 * 1024 * 1024 // 100M = 104857600 字节 需要考虑header大小

	// 检查文件是否为空
	logInfo("文件大小: %d 字节", fileSize)
	if fileSize == 0 {
		logInfo("警告: 文件为空，无法分割")
		return fileInfoObj, nil
	}

	mc := fileInfoObj.Model
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 解析JSON
		var originJSON map[string]interface{}
		if err := json.Unmarshal([]byte(line), &originJSON); err != nil {
			logInfo("err in line %d: %s", chunkIndex, err.Error())
			continue
		}

		// 构建新行
		messages, ok := originJSON[mc.MessagesKey].([]interface{})
		if !ok {
			continue
		}
		body := map[string]interface{}{
			"model":      mc.Domain,
			"messages":   messages,
			"max_tokens": mc.MaxTokens,
		}
		if mc.Temperature != nil {
			body["temperature"] = *mc.Temperature
		}
		if mc.TopP != nil {
			body["top_p"] = *mc.TopP
		}
		if mc.EnableThinking != nil {
			body["enable_thinking"] = *mc.EnableThinking
		}
		if mc.ExtraBody != nil && len(mc.ExtraBody) > 0 {
			body["extra_body"] = mc.ExtraBody
		}
		if mc.Tools != nil && len(mc.Tools) > 0 {
			body["tools"] = mc.Tools
		}
		if mc.ToolChoice != nil {
			body["tool_choice"] = mc.ToolChoice
		}
		newline := map[string]interface{}{
			"custom_id": fmt.Sprintf("%d", lineCount),
			"method":    "POST",
			"url":       "/v1/chat/completions",
			"body":      body,
		}

		newlineJSON, err := json.Marshal(newline)
		if err != nil {
			continue
		}

		// 计算当前行的字节大小（包括换行符）
		lineSize := len(newlineJSON) + 1 // +1 是换行符 \n
		newChunkSize := currentChunkSize + lineSize
		newChunkLineCount := len(currentChunkLines) + 1

		// 如果当前行大小+以前的大小超过100M，或者行数达到限制，将以前的写入文件，本次继续累计
		// 哪个先到就按那个来
		if (newChunkSize > maxChunkSize || newChunkLineCount > linesPerChunk) && len(currentChunkLines) > 0 {
			if err := fm.writeChunk(taskID, chunkIndex, originalFilename, chunkDir, currentChunkLines, fileInfoObj, fileInfoObj.Retry); err != nil {
				errorMsg := err.Error()
				fm.dbManager.UpdateFileStatus(taskID, FileStatusFailed, &errorMsg)
				fileInfoObj.Status = FileStatusFailed
				fileInfoObj.ErrorMessage = &errorMsg
				return nil, err
			}
			chunkIndex++
			currentChunkLines = []string{}
			currentChunkSize = 0
		}

		currentChunkLines = append(currentChunkLines, string(newlineJSON))
		currentChunkSize += lineSize
		totalLines++

		if TEST_LINES > 0 && totalLines >= TEST_LINES {
			break
		}
	}

	// 检查是否有读取错误
	if err := scanner.Err(); err != nil {
		logError("读取文件时发生错误: %v", err)
		return nil, fmt.Errorf("读取文件错误: %v", err)
	}

	logInfo("文件读取完成，共读取 %d 行，有效处理 %d 行", lineCount, totalLines)

	// 处理剩余的行
	if len(currentChunkLines) > 0 {
		if err := fm.writeChunk(taskID, chunkIndex, originalFilename, chunkDir, currentChunkLines, fileInfoObj, fileInfoObj.Retry); err != nil {
			errorMsg := err.Error()
			fm.dbManager.UpdateFileStatus(taskID, FileStatusFailed, &errorMsg)
			fileInfoObj.Status = FileStatusFailed
			fileInfoObj.ErrorMessage = &errorMsg
			return nil, err
		}
		chunkIndex++
	}

	// 更新总块数和总行数
	fileInfoObj.TotalChunks = chunkIndex
	fileInfoObj.TotalLines = totalLines

	// 更新状态为分割完成
	fm.dbManager.UpdateFileStatus(taskID, FileStatusSplitCompleted, nil)
	fm.dbManager.UpdateFileTotalChunks(taskID, fileInfoObj.TotalChunks)
	fm.dbManager.UpdateFileTotalLines(taskID, fileInfoObj.TotalLines)
	fileInfoObj.Status = FileStatusSplitCompleted

	return fileInfoObj, nil
}

// SaveFile 保存文件
func (fm *FileManager) SaveFile(taskID string, chunkID string, fileContent string, isError bool) error {
	// 获取chunk信息以确定retry值
	chunk, err := fm.dbManager.GetChunk(chunkID)
	if err != nil {
		return err
	}
	if chunk == nil {
		return fmt.Errorf("chunk不存在: %s", chunkID)
	}

	retry := chunk.Retry

	var path string
	if isError {
		path = filepath.Join(BATCH_RESULT_DIR, taskID, "error")
	} else {
		path = filepath.Join(BATCH_RESULT_DIR, taskID, "output")
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	filePath := filepath.Join(path, fmt.Sprintf("retry%d_%s.jsonl", retry, chunkID))
	return os.WriteFile(filePath, []byte(fileContent), 0644)
}

// MergeBatchResults 合并chunk的output和error文件，并找出缺失的记录
func (fm *FileManager) MergeBatchResults(taskID string, retry int) (map[string]interface{}, error) {
	// 强制刷新数据库缓存，确保获取最新数据
	fm.dbManager.RefreshCache()
	fileInfo, err := fm.dbManager.GetFile(taskID)
	if err != nil || fileInfo == nil {
		return nil, fmt.Errorf("文件不存在: %s", taskID)
	}

	// 创建merged目录
	mergedDir := filepath.Join(MERGED_DIR, taskID)
	if err := os.MkdirAll(mergedDir, 0755); err != nil {
		return nil, err
	}

	// 筛选指定retry级别的chunks
	var chunks []*FileChunk
	for _, chunk := range fileInfo.Chunks {
		if chunk.Retry == retry {
			chunks = append(chunks, chunk)
		}
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("没有找到retry=%d的chunks", retry)
	}

	// 按chunk_index排序
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].ChunkIndex < chunks[j].ChunkIndex
	})

	outputCount := 0
	errorCount := 0
	missingCount := 0

	// 确定输出文件名（根据retry参数）
	outputMergedPath := filepath.Join(mergedDir, fmt.Sprintf("output_retry%d.jsonl", retry))
	errorMergedPath := filepath.Join(mergedDir, fmt.Sprintf("error_retry%d.jsonl", retry))
	missingRecordsPath := filepath.Join(mergedDir, fmt.Sprintf("missing_records_retry%d.jsonl", retry))

	// 创建输出文件（流式写入，避免巨量内存占用）
	outputMergedFile, err := os.Create(outputMergedPath)
	if err != nil {
		return nil, err
	}
	defer outputMergedFile.Close()
	outputWriter := bufio.NewWriterSize(outputMergedFile, 4*1024*1024)
	defer outputWriter.Flush()

	errorMergedFile, err := os.Create(errorMergedPath)
	if err != nil {
		return nil, err
	}
	defer errorMergedFile.Close()
	errorWriter := bufio.NewWriterSize(errorMergedFile, 4*1024*1024)
	defer errorWriter.Flush()

	missingMergedFile, err := os.Create(missingRecordsPath)
	if err != nil {
		return nil, err
	}
	defer missingMergedFile.Close()
	missingWriter := bufio.NewWriterSize(missingMergedFile, 4*1024*1024)
	defer missingWriter.Flush()

	workerCount := runtime.NumCPU()
	if workerCount < 2 {
		workerCount = 2
	}
	if workerCount > 16 {
		workerCount = 16
	}
	if workerCount > len(chunks) {
		workerCount = len(chunks)
	}
	if workerCount <= 0 {
		workerCount = 1
	}

	type chunkJob struct {
		index int
		chunk *FileChunk
	}

	jobs := make(chan chunkJob, workerCount*2)
	results := make(chan *chunkMergeResult, workerCount*2)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				res, err := fm.mergeSingleChunk(taskID, job.chunk, job.index)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if res == nil {
					continue
				}
				results <- res
			}
		}()
	}

	go func() {
		for idx, chunk := range chunks {
			jobs <- chunkJob{index: idx, chunk: chunk}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	pending := make(map[int]*chunkMergeResult, workerCount*2)
	nextIndex := 0
	flushResult := func(res *chunkMergeResult) error {
		for _, line := range res.outputLines {
			if _, err := outputWriter.WriteString(line); err != nil {
				return err
			}
			if err := outputWriter.WriteByte('\n'); err != nil {
				return err
			}
			outputCount++
		}
		for _, line := range res.errorLines {
			if _, err := errorWriter.WriteString(line); err != nil {
				return err
			}
			if err := errorWriter.WriteByte('\n'); err != nil {
				return err
			}
			errorCount++
		}
		if res.logEachMissing {
			for i, body := range res.missingBodies {
				if _, err := missingWriter.Write(body); err != nil {
					return err
				}
				if err := missingWriter.WriteByte('\n'); err != nil {
					return err
				}
				missingCount++
				logInfo("发现缺失记录: chunk_id=%s, custom_id=%s, completed=%d, total=%d",
					res.chunkID, res.missingIDs[i], res.completedCount, res.totalCount)
			}
		} else {
			for _, body := range res.missingBodies {
				if _, err := missingWriter.Write(body); err != nil {
					return err
				}
				if err := missingWriter.WriteByte('\n'); err != nil {
					return err
				}
				missingCount++
			}
			if len(res.missingBodies) > 0 {
				logInfo("警告: chunk_id=%s 虽然completed_count==total_count，但发现缺失记录: %d条", res.chunkID, len(res.missingBodies))
			}
		}
		return nil
	}

	for res := range results {
		pending[res.index] = res
		for {
			ready, ok := pending[nextIndex]
			if !ok {
				break
			}
			if err := flushResult(ready); err != nil {
				return nil, err
			}
			delete(pending, nextIndex)
			nextIndex++
		}
	}
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	if err := outputWriter.Flush(); err != nil {
		return nil, err
	}
	if err := errorWriter.Flush(); err != nil {
		return nil, err
	}
	if err := missingWriter.Flush(); err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"output_file":          outputMergedPath,
		"error_file":           errorMergedPath,
		"missing_records_file": missingRecordsPath,
		"missing_count":        missingCount,
	}

	// 使用文件表中的 max_retry 字段，而不是全局的 MAX_RETRY_COUNT
	maxRetry := fileInfo.MaxRetry
	// 只有当没有缺失记录，或者达到最大重试次数时，才完成处理
	// 注意：即使达到最大重试次数，如果还有缺失记录，也应该完成处理（不再重试）
	shouldComplete := missingCount == 0 || retry >= maxRetry

	if shouldComplete {
		// 如果达到最大重试次数但仍有缺失记录，记录警告
		if retry >= maxRetry && missingCount > 0 {
			logInfo("已达到最大重试次数 (%d)，仍有 %d 条缺失记录，停止重试", maxRetry, missingCount)
		}

		// 合并之前所有retry级别的output文件
		finalOutputPath := filepath.Join(mergedDir, "output.jsonl")
		finalFile, err := os.Create(finalOutputPath)
		if err != nil {
			return nil, err
		}
		finalWriter := bufio.NewWriterSize(finalFile, 4*1024*1024)
		finalCount := 0

		// 按retry级别从0到retry读取并合并
		for retryLevel := 0; retryLevel <= retry; retryLevel++ {
			retryOutputPath := filepath.Join(mergedDir, fmt.Sprintf("output_retry%d.jsonl", retryLevel))

			if _, err := os.Stat(retryOutputPath); err == nil {
				file, err := os.Open(retryOutputPath)
				if err == nil {
					scanner := bufio.NewScanner(file)
					scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxTokenSize)
					for scanner.Scan() {
						line := strings.TrimSpace(scanner.Text())
						if line != "" {
							if _, err := finalWriter.WriteString(line); err != nil {
								file.Close()
								finalFile.Close()
								return nil, err
							}
							if err := finalWriter.WriteByte('\n'); err != nil {
								file.Close()
								finalFile.Close()
								return nil, err
							}
							finalCount++
						}
					}
					if err := scanner.Err(); err != nil {
						logInfo("警告: 读取最终合并文件失败: %v", err)
					}
					file.Close()
				}
			}
		}
		if err := finalWriter.Flush(); err != nil {
			finalFile.Close()
			return nil, err
		}
		finalFile.Close()

		logInfo("最终合并完成: output=%d条", finalCount)
		logInfo("最终文件路径: output=%s", finalOutputPath)

		result["final_output_file"] = finalOutputPath

		fm.dbManager.UpdateFileStatus(taskID, FileStatusProcessCompleted, nil)
		fileInfo.Status = FileStatusProcessCompleted
	}

	logInfo("合并完成: output=%d条, error=%d条, 缺失=%d条", outputCount, errorCount, missingCount)

	return result, nil
}

// RetryFailedRecords 重试失败数据
func (fm *FileManager) RetryFailedRecords(taskID string) (bool, error) {
	fileInfo, err := fm.dbManager.GetFile(taskID)
	if err != nil || fileInfo == nil {
		return false, fmt.Errorf("文件不存在: %s", taskID)
	}

	if fileInfo.Status == FileStatusFailed || fileInfo.Status == FileStatusProcessCompleted ||
		fileInfo.Status == FileStatusCanceled || fileInfo.Status == FileStatusCanceling {
		logInfo("文件状态为 %s，跳过重试", fileInfo.Status)
		return true, nil
	}

	// 检查missing_records.jsonl是否存在且有数据（根据当前retry值选择文件）
	missingRecordsPath := filepath.Join(MERGED_DIR, taskID, fmt.Sprintf("missing_records_retry%d.jsonl", fileInfo.Retry))

	if _, err := os.Stat(missingRecordsPath); os.IsNotExist(err) {
		return false, fmt.Errorf("缺失记录文件不存在: %s", missingRecordsPath)
	}

	// 读取缺失记录
	missingRecords := []string{}
	file, err := os.Open(missingRecordsPath)
	if err == nil {
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 100*1024*1024), 100*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				missingRecords = append(missingRecords, line)
			}
		}
		file.Close()
	} else {
		return false, err
	}

	if len(missingRecords) == 0 {
		return true, nil
	}

	// 检查是否已达到最大重试次数
	if fileInfo.Retry >= fileInfo.MaxRetry {
		logInfo("已达到最大重试次数 (%d)，仍有 %d 条缺失记录，停止重试", fileInfo.MaxRetry, len(missingRecords))
		return true, nil
	}

	logInfo("发现 %d 条缺失记录，开始重试...", len(missingRecords))

	// 更新重试次数
	newRetry := fileInfo.Retry + 1
	if err := fm.dbManager.UpdateFileRetry(taskID, newRetry); err != nil {
		return false, err
	}
	fileInfo.Retry = newRetry

	// 创建文件块目录（使用原始目录）
	chunkDir := filepath.Join(CHUNK_DIR, taskID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return false, err
	}

	// 分割缺失记录（同时检查行数和文件大小，限制100M）
	chunkIndex := 0
	currentChunkLines := []string{}
	currentChunkSize := 0                 // 当前chunk的累计大小（字节数）
	const maxChunkSize = 99 * 1024 * 1024 // 100M = 104857600 字节 需要考虑header大小

	for _, recordLine := range missingRecords {
		// 计算当前行的字节大小（包括换行符）
		lineSize := len(recordLine) + 1 // +1 是换行符 \n
		newChunkSize := currentChunkSize + lineSize
		newChunkLineCount := len(currentChunkLines) + 1

		// 如果当前行大小+以前的大小超过100M，或者行数达到限制，将以前的写入文件，本次继续累计
		// 哪个先到就按那个来
		if (newChunkSize > maxChunkSize || newChunkLineCount > LINES_PER_CHUNK) && len(currentChunkLines) > 0 {
			if err := fm.writeChunk(taskID, chunkIndex, fileInfo.OriginalFilename, chunkDir, currentChunkLines, fileInfo, newRetry); err != nil {
				return false, err
			}
			chunkIndex++
			currentChunkLines = []string{}
			currentChunkSize = 0
		}

		currentChunkLines = append(currentChunkLines, recordLine)
		currentChunkSize += lineSize
	}

	// 处理剩余的行
	if len(currentChunkLines) > 0 {
		if err := fm.writeChunk(taskID, chunkIndex, fileInfo.OriginalFilename, chunkDir, currentChunkLines, fileInfo, newRetry); err != nil {
			return false, err
		}
		chunkIndex++
	}

	// 更新总块数（累加）
	fileInfo.TotalChunks += chunkIndex
	if err := fm.dbManager.UpdateFileTotalChunks(taskID, fileInfo.TotalChunks); err != nil {
		return false, err
	}

	logInfo("重试完成: 新增 %d 个chunk，当前重试次数: %d", chunkIndex, newRetry)

	return false, nil
}
