package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// BatchInferService 批量推理服务主类
type BatchInferService struct {
	dbManager       *DBManager
	fileManager     *FileManager
	batchManager    *BatchManager
	chunkManager    *ChunkManager
	progress        *ProgressDisplay
	processingFiles map[string]bool // 正在处理的文件集合
	processingMutex sync.Mutex      // 保护 processingFiles 的互斥锁
}

// NewBatchInferService 创建批量推理服务
func NewBatchInferService() *BatchInferService {
	dbManager := NewDBManager()
	fileManager := NewFileManager(dbManager)
	batchManager := NewBatchManager()
	chunkManager := NewChunkManager(dbManager, fileManager, batchManager)

	return &BatchInferService{
		dbManager:       dbManager,
		fileManager:     fileManager,
		batchManager:    batchManager,
		chunkManager:    chunkManager,
		progress:        NewProgressDisPlay(),
		processingFiles: make(map[string]bool),
	}
}

// ValidateFileExists 验证文件是否存在
func (bis *BatchInferService) ValidateFileExists(taskID string) (*FileInfo, error) {
	fileInfo, err := bis.dbManager.GetFile(taskID)
	if err != nil || fileInfo == nil {
		return nil, fmt.Errorf("文件不存在: %s", taskID)
	}
	return fileInfo, nil
}

// SplitFile 分割文件
func (bis *BatchInferService) SplitFile(filePath string, taskId string, linesPerChunk *int) (string, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return "", fmt.Errorf("文件不存在: %s", filePath)
	}

	lines := LINES_PER_CHUNK
	if linesPerChunk != nil {
		lines = *linesPerChunk
	}

	filename := filepath.Base(filePath)
	bis.progress.Update(fmt.Sprintf("开始分割文件: %s (每块行数: %d)", filename, lines))

	fileInfo, err := bis.fileManager.SplitFile(filePath, filename, taskId, lines)
	if err != nil {
		bis.progress.Update(fmt.Sprintf("✗ 文件分割失败: %v", err))
		return "", err
	}

	bis.progress.Update(fmt.Sprintf("✓ 文件分割完成: %s", fileInfo.TaskID))
	bis.progress.ShowStatus(fileInfo, true)
	return fileInfo.TaskID, nil
}

// UploadChunks 上传文件块（无并发限制）
func (bis *BatchInferService) UploadChunks(taskID string) int {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return 0
	}

	if len(fileInfo.Chunks) == 0 {
		return 0
	}

	// 获取待上传的chunk（只能上传PENDING和UPLOAD_FAILED状态）
	uploadableStates := map[ChunkStatus]bool{
		ChunkStatusPending:      true,
		ChunkStatusUploadFailed: true,
	}
	waitingUploadChunks := []*FileChunk{}
	for _, chunk := range fileInfo.Chunks {
		if uploadableStates[chunk.Status] {
			waitingUploadChunks = append(waitingUploadChunks, chunk)
		}
	}

	// 上传所有待上传的chunk（无并发限制）
	uploadedCount := 0
	for _, chunk := range waitingUploadChunks {
		if _, err := os.Stat(chunk.ChunkPath); err == nil {
			data, err := os.ReadFile(chunk.ChunkPath)
			if err == nil {
				if bis.chunkManager.UploadChunk(chunk.ChunkID, data) {
					uploadedCount++
				}
			} else {
				logError("读取chunk文件失败 %s: %v", chunk.ChunkID, err)
			}
		}
	}

	if uploadedCount > 0 && fileInfo.Status == FileStatusSplitCompleted {
		bis.dbManager.UpdateFileStatus(taskID, FileStatusProcessing, nil)
		fileInfo.Status = FileStatusProcessing
	}

	return uploadedCount
}

// StartChunkProcess 启动batch任务（无并发限制）
func (bis *BatchInferService) StartChunkProcess(taskID string) int {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return 0
	}

	// 获取已上传待处理的chunk（只能处理UPLOADED状态）
	uploadedChunks := []*FileChunk{}
	for _, chunk := range fileInfo.Chunks {
		if chunk.Status == ChunkStatusUploaded {
			uploadedChunks = append(uploadedChunks, chunk)
		}
	}

	// 处理所有已上传的chunk（无并发限制）
	processedCount := 0
	for _, chunk := range uploadedChunks {
		if bis.chunkManager.ChunkStartProcess(chunk.ChunkID) {
			processedCount++
		}
	}

	return processedCount
}

// CheckChunkProcess 检查batch任务
func (bis *BatchInferService) CheckChunkProcess(taskID string) int {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return 0
	}

	// 只检查正在处理的chunk
	processingChunks := []*FileChunk{}
	for _, chunk := range fileInfo.Chunks {
		if chunk.Status == ChunkStatusProcessing {
			processingChunks = append(processingChunks, chunk)
		}
	}

	completedCount := 0
	for _, chunk := range processingChunks {
		if bis.chunkManager.CheckChunkProcess(chunk.ChunkID) {
			completedCount++
		}
	}

	return completedCount
}

// UploadAndProcessLoop 循环执行上传、处理和检查
func (bis *BatchInferService) UploadAndProcessLoop(taskID string) {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return
	}

	totalChunks := len(fileInfo.Chunks)

	for {
		// 刷新文件信息
		fileInfo, err = bis.dbManager.GetFile(taskID)
		if err != nil || fileInfo == nil {
			break
		}

		// 统计各状态的数量
		pendingCount := 0
		uploadedCount := 0
		processingCount := 0
		processedCount := 0
		failedCount := 0

		for _, chunk := range fileInfo.Chunks {
			switch chunk.Status {
			case ChunkStatusPending:
				pendingCount++
			case ChunkStatusUploaded:
				uploadedCount++
			case ChunkStatusProcessing:
				processingCount++
			case ChunkStatusProcessed:
				processedCount++
			case ChunkStatusUploadFailed:
				failedCount++
			}
		}

		// 显示进度
		bis.progress.ShowStatus(fileInfo, true)

		// 检查是否全部完成
		if processedCount == totalChunks {
			bis.progress.Update("✓ 所有文件块处理结束")
			break
		}

		// 1. 检查正在处理的chunk状态
		if processingCount > 0 {
			completedCount := bis.CheckChunkProcess(taskID)
			if completedCount > 0 {
				processingCount -= completedCount
				processedCount += completedCount
			}
		}

		// 无并发限制，直接上传和处理所有可用的chunk
		if pendingCount+uploadedCount+failedCount > 0 {
			bis.UploadChunks(taskID)
			bis.StartChunkProcess(taskID)
		}

		// 等待一段时间后再次检查
		time.Sleep(10 * time.Second)
	}
}

// MergeFile 合并文件
func (bis *BatchInferService) MergeFile(taskID string) (map[string]interface{}, error) {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return nil, err
	}

	// 检查所有chunk是否都在结束状态
	finalStates := map[ChunkStatus]bool{
		ChunkStatusProcessed: true,
		ChunkStatusCanceled:  true,
	}

	nonFinalChunks := []*FileChunk{}
	for _, chunk := range fileInfo.Chunks {
		if !finalStates[chunk.Status] {
			nonFinalChunks = append(nonFinalChunks, chunk)
		}
	}

	if len(nonFinalChunks) > 0 {
		errorMsg := "无法合并：存在非结束状态的chunk。\n"
		statusMap := make(map[string][]string)
		for _, chunk := range nonFinalChunks {
			status := string(chunk.Status)
			statusMap[status] = append(statusMap[status], chunk.ChunkID)
		}

		for status, chunkIDs := range statusMap {
			errorMsg += fmt.Sprintf("  状态 %s: %d 个chunk\n", status, len(chunkIDs))
			if len(chunkIDs) <= 5 {
				errorMsg += fmt.Sprintf("    Chunk IDs: %s\n", strings.Join(chunkIDs, ", "))
			} else {
				errorMsg += fmt.Sprintf("    Chunk IDs: %s ... (共%d个)\n", strings.Join(chunkIDs[:5], ", "), len(chunkIDs))
			}
		}

		return nil, fmt.Errorf(errorMsg)
	}

	bis.progress.Update(fmt.Sprintf("开始合并文件: %s", taskID))

	mergedDict, err := bis.fileManager.MergeBatchResults(taskID, fileInfo.Retry)
	if err != nil {
		bis.progress.Update(fmt.Sprintf("✗ 文件合并失败: %v", err))
		return nil, err
	}

	bis.progress.Update(fmt.Sprintf("✓ 文件合并完成: %v", mergedDict))

	// 显示最终状态
	fileInfo, _ = bis.dbManager.GetFile(taskID)
	if fileInfo != nil {
		bis.progress.ShowStatus(fileInfo, true)
	}

	return mergedDict, nil
}

// Cancel 终止调度
func (bis *BatchInferService) Cancel(taskID string) {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return
	}

	// 将file_info的状态设置成canceled
	bis.dbManager.UpdateFileStatus(taskID, FileStatusCanceled, nil)
	bis.progress.Update(fmt.Sprintf("文件状态已设置为 CANCELED: %s", taskID))

	// 对processing的chunk调用cancelBatchTask
	for _, chunk := range fileInfo.Chunks {
		if chunk.Status == ChunkStatusProcessing && chunk.BatchID != nil {
			_, err := bis.batchManager.CancelBatchTask(*chunk.BatchID)
			if err == nil {
				logInfo("已取消batch任务: %s (chunk: %s)", *chunk.BatchID, chunk.ChunkID)
			} else {
				logError("取消batch任务失败 %s: %v", *chunk.BatchID, err)
			}
		}
	}

	// uploaded的chunk设置为CANCELED
	for _, chunk := range fileInfo.Chunks {
		if chunk.Status == ChunkStatusUploaded {
			bis.dbManager.UpdateChunkStatus(chunk.ChunkID, ChunkStatusCanceled, nil)
			logInfo("已设置chunk状态为CANCELED: %s", chunk.ChunkID)
		}
	}

	// 刷新并显示状态
	fileInfo, _ = bis.dbManager.GetFile(taskID)
	if fileInfo != nil {
		bis.progress.ShowStatus(fileInfo, true)
	}
	bis.progress.Update(fmt.Sprintf("✓ 文件调度已终止: %s", taskID))
}

// QueryStatus 查询并更新文件状态
func (bis *BatchInferService) QueryStatus(taskID string) {
	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		return
	}

	fileInfo, _ = bis.dbManager.GetFile(taskID)
	if fileInfo != nil {
		bis.progress.ShowStatus(fileInfo, true)
	}
}

// MonitorStatus 监控文件状态
// 如果 taskID 为空，显示所有分割完成且进行中的文件列表及进度信息
// 如果传入 taskID，查询单个文件进度
// 刷新间隔固定为 10 秒
func (bis *BatchInferService) MonitorStatus(taskID string) {
	if taskID == "" {
		// 显示所有进行中的文件
		bis.MonitorAllFiles()
	} else {
		// 监控单个文件
		bis.MonitorSingleFile(taskID)
	}
}

// MonitorSingleFile 监控单个文件状态（固定刷新间隔10秒）
func (bis *BatchInferService) MonitorSingleFile(taskID string) {
	fmt.Printf("开始监控文件状态: %s (刷新间隔: 10秒)\n", taskID)
	fmt.Println("按 Ctrl+C 停止监控\n")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fileInfo, err := bis.dbManager.GetFile(taskID)
			if err != nil || fileInfo == nil {
				fmt.Printf("文件不存在: %s\n", taskID)
				return
			}

			// 清屏并显示状态
			clearScreenFunc()
			fmt.Printf("=== 文件状态监控: %s ===\n\n", fileInfo.OriginalFilename)
			bis.progress.ShowStatus(fileInfo, true)

			// 检查是否完成
			if fileInfo.Status == FileStatusProcessCompleted {
				fmt.Println("\n✓ 所有流程已完成！")
				return
			} else if fileInfo.Status == FileStatusFailed {
				errorMsg := "未知错误"
				if fileInfo.ErrorMessage != nil {
					errorMsg = *fileInfo.ErrorMessage
				}
				fmt.Printf("\n✗ 流程失败: %s\n", errorMsg)
				return
			}
		}
	}
}

// MonitorAllFiles 监控所有进行中的文件（固定刷新间隔10秒）
func (bis *BatchInferService) MonitorAllFiles() {
	fmt.Printf("开始监控所有进行中的文件 (刷新间隔: 10秒)\n")
	fmt.Println("按 Ctrl+C 停止监控\n")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 获取所有进行中的文件（分割完成或处理中）
			taskIDs, err := bis.dbManager.GetPendingFiles()
			if err != nil {
				fmt.Printf("获取文件列表失败: %v\n", err)
				continue
			}

			// 清屏
			clearScreenFunc()
			fmt.Printf("=== 所有进行中的文件监控 ===\n\n")
			fmt.Printf("文件总数: %d\n\n", len(taskIDs))

			if len(taskIDs) == 0 {
				fmt.Println("当前没有进行中的文件")
				continue
			}

			// 显示每个文件的状态
			for _, taskID := range taskIDs {
				fileInfo, err := bis.dbManager.GetFile(taskID)
				if err != nil || fileInfo == nil {
					continue
				}

				bis.progress.ShowStatus(fileInfo, false)
				fmt.Println() // 空行分隔
			}

			// 检查是否所有文件都已完成
			allCompleted := true
			for _, taskID := range taskIDs {
				fileInfo, _ := bis.dbManager.GetFile(taskID)
				if fileInfo != nil && fileInfo.Status != FileStatusProcessCompleted && fileInfo.Status != FileStatusFailed && fileInfo.Status != FileStatusCanceled {
					allCompleted = false
					break
				}
			}

			if allCompleted && len(taskIDs) > 0 {
				fmt.Println("\n✓ 所有文件处理完成！")
				return
			}
		}
	}
}

// RunPipeline 运行完整流程
func (bis *BatchInferService) RunPipeline(filePath string, taskId string, linesPerChunk *int) {
	var taskID string

	fileInfo, err := bis.dbManager.GetFile(taskId)
	if err != nil {
		logError("执行错误:%s", err)
		os.Exit(1)
	}
	if fileInfo != nil {
		logInfo("文件 %s 已存在", taskId)
		return
	}

	if _, err := os.Stat(filePath); err == nil {
		// 是文件路径，执行分割
		logInfo("开始分割文件---------------------------")
		var err error
		taskID, err = bis.SplitFile(filePath, taskId, linesPerChunk)
		if err != nil {
			logError("流程执行失败: %v", err)
			os.Exit(1)
		}
	} else {
		logError("文件错误:%s", err)
		os.Exit(1)
	}

	for {
		time.Sleep(10 * time.Second)
		fileInfo, err := bis.dbManager.GetFile(taskID)
		if err != nil {
			continue
		}
		if fileInfo.Status == FileStatusProcessCompleted || fileInfo.Status == FileStatusFailed || fileInfo.Status == FileStatusCanceled {
			logInfo("文件 %s 已结束，状态为 %s", taskID, fileInfo.Status)
			break
		}
		bis.progress.ShowStatus(fileInfo, true)
	}

}

// ProcessFile 处理单个文件的完整流程（从上传到重试）
func (bis *BatchInferService) ProcessFile(taskID string) {
	// 检查是否正在处理，如果是则跳过
	bis.processingMutex.Lock()
	if bis.processingFiles[taskID] {
		bis.processingMutex.Unlock()
		logInfo("文件 %s 正在处理中，跳过重复执行", taskID)
		return
	}
	// 标记为正在处理
	bis.processingFiles[taskID] = true
	bis.processingMutex.Unlock()

	// 确保处理完成后清除标记
	defer func() {
		bis.processingMutex.Lock()
		delete(bis.processingFiles, taskID)
		bis.processingMutex.Unlock()
	}()

	logInfo("========== 开始处理文件: %s ==========", taskID)

	fileInfo, err := bis.ValidateFileExists(taskID)
	if err != nil {
		logError("文件验证失败 %s: %v", taskID, err)
		return
	}

	// 检查文件状态，如果已经完成或取消，跳过
	if fileInfo.Status == FileStatusProcessCompleted ||
		fileInfo.Status == FileStatusCanceled ||
		fileInfo.Status == FileStatusFailed {
		logInfo("文件 %s 状态为 %s，跳过处理", taskID, fileInfo.Status)
		return
	}

	isDone := false
	// 使用文件表中的 max_retry 字段，而不是全局的 MAX_RETRY_COUNT
	maxRetry := fileInfo.MaxRetry
	for i := fileInfo.Retry; i <= maxRetry; i++ {
		logInfo("[%s] 开始上传和处理文件块（循环执行）-------------------", taskID)
		bis.UploadAndProcessLoop(taskID)
		logInfo("[%s] 开始合并文件----------------------------", taskID)
		_, err := bis.MergeFile(taskID)
		if err != nil {
			logError("[%s] 合并文件失败: %v", taskID, err)
			break
		}
		logInfo("[%s] 检查失败数据---------------------------", taskID)
		done, err := bis.fileManager.RetryFailedRecords(taskID)
		if err != nil {
			logError("[%s] 重试失败记录失败: %v", taskID, err)
			break
		}
		isDone = done
		fileInfo, _ := bis.dbManager.GetFile(taskID)
		if fileInfo != nil {
			bis.progress.ShowStatus(fileInfo, true)
		}
		if isDone {
			logInfo("[%s] 完整流程执行结束！", taskID)
			break
		}
	}

	logInfo("========== 文件处理完成: %s ==========", taskID)
}

// runDaemonLoop 守护进程的主循环（在独立的goroutine中运行）
func (bis *BatchInferService) runDaemonLoop(ctx context.Context) {
	logInfo("========== 守护进程已启动（后台运行）==========")
	if runtime.GOOS == "windows" {
		logInfo("提示: 守护进程在后台运行，使用 taskkill /PID %d /F 命令停止", os.Getpid())
	} else {
		logInfo("提示: 守护进程在后台运行，使用 kill -TERM %d 或 kill %d 命令停止", os.Getpid(), os.Getpid())
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	// 立即执行一次
	bis.processPendingFiles()

	// 主循环：定期执行或响应context取消
	for {
		select {
		case <-ctx.Done():
			// 收到停止信号
			logInfo("守护进程收到停止信号，正在退出...")
			return
		case <-ticker.C:
			// 定期执行扫描
			bis.processPendingFiles()
		}
	}
}

// checkDaemonRunning 检查守护进程是否已经在运行
func (bis *BatchInferService) checkDaemonRunning() bool {
	lockFile := filepath.Join(BASE_DIR, ".daemon.lock")
	logInfo("检查锁文件: %s", lockFile)

	// 检查锁文件是否存在
	if _, err := os.Stat(lockFile); os.IsNotExist(err) {
		// 锁文件不存在，说明守护进程没有运行
		logInfo("锁文件不存在，守护进程未运行")
		return false
	}
	logInfo("锁文件存在，继续检查进程")

	// 锁文件存在，读取PID并检查进程是否存在
	data, readErr := os.ReadFile(lockFile)
	if readErr != nil {
		// 读取失败，删除无效的锁文件
		os.Remove(lockFile)
		return false
	}

	var pid int
	fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid)
	if pid <= 0 {
		// PID无效，删除无效的锁文件
		os.Remove(lockFile)
		return false
	}

	// 检查进程是否存在
	if runtime.GOOS == "windows" {
		// Windows: 使用tasklist检查
		// 注意：tasklist 在找不到进程时会返回 exit status 1，这是正常的
		logInfo("执行 tasklist 命令检查进程 %d", pid)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH", "/FO", "CSV")
		output, err := cmd.Output()

		// 检查输出内容，而不是错误状态
		// tasklist 找不到进程时会返回 exit status 1，但这是正常的
		outputStr := strings.TrimSpace(string(output))
		if outputStr != "" {
			logInfo("tasklist 输出: %s", outputStr)
			// 检查输出中是否包含PID（格式：进程名,PID,...）
			// CSV 格式输出，PID 在第二个字段
			if strings.Contains(outputStr, fmt.Sprintf(",%d,", pid)) ||
				strings.Contains(outputStr, fmt.Sprintf(",%d\n", pid)) ||
				strings.Contains(outputStr, fmt.Sprintf("\"%d\"", pid)) {
				logInfo("守护进程正在运行，PID: %d", pid)
				return true
			}
		}
		// 如果没有输出或输出中不包含PID，说明进程不存在
		if err != nil {
			logInfo("tasklist 命令执行结果: %v (进程可能不存在)", err)
		} else {
			logInfo("tasklist 未找到进程 %d", pid)
		}
	} else {
		// Unix: 使用kill -0检查
		process, err := os.FindProcess(pid)
		if err == nil {
			// 尝试发送信号0（不实际发送，只检查进程是否存在）
			err := process.Signal(syscall.Signal(0))
			if err == nil {
				logInfo("守护进程正在运行，PID: %d", pid)
				return true
			}
		}
	}

	// 进程不存在，删除旧的锁文件
	logInfo("锁文件存在但进程 %d 不存在，删除旧的锁文件", pid)
	os.Remove(lockFile)
	return false
}

// removeDaemonLock 删除守护进程锁文件
func (bis *BatchInferService) removeDaemonLock() {
	lockFile := filepath.Join(BASE_DIR, ".daemon.lock")
	os.Remove(lockFile)
}

// RunDaemon 启动常驻进程（使用exec.Command启动独立进程）
// 多次执行只启动一次，守护进程与主进程完全独立
func (bis *BatchInferService) RunDaemon() {
	// 检查守护进程是否已经在运行
	isRunning := bis.checkDaemonRunning()
	if isRunning {
		logInfo("守护进程已经在运行")
		return
	}
	logInfo("========== 正在启动守护进程 ==========")

	// 获取可执行文件路径
	exePath, err := os.Executable()
	if err != nil {
		logError("获取可执行文件路径失败: %v", err)
		return
	}

	// 构建命令参数（固定间隔60秒）
	args := []string{"-daemon-internal"}

	// 重定向输出到日志文件
	logFile := filepath.Join(LOG_DIR, "daemon.log")
	logFileHandle, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logError("打开日志文件失败: %v", err)
		return
	}

	// 创建命令
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		// Windows: 使用 cmd /c start 启动独立进程
		// 这样可以确保进程完全独立于父进程
		cmdArgs := []string{"/c", "start", "/B", exePath}
		cmdArgs = append(cmdArgs, args...)
		cmd = exec.Command("cmd", cmdArgs...)
		cmd.Stdout = logFileHandle
		cmd.Stderr = logFileHandle
	} else {
		// Unix: 直接启动
		cmd = exec.Command(exePath, args...)
		cmd.Stdout = logFileHandle
		cmd.Stderr = logFileHandle
		// 注意：exec.Command 启动的进程本身就是独立的
	}

	// 启动进程（不等待）
	err = cmd.Start()
	if err != nil {
		logError("启动守护进程失败: %v", err)
		logFileHandle.Close()
		return
	}

	// 在 Windows 上，使用 start 命令启动的进程，cmd.Process.Pid 可能是 cmd.exe 的 PID
	// 我们需要等待一下，让守护进程启动并创建锁文件，然后从锁文件中读取真正的 PID
	if runtime.GOOS == "windows" {
		// 等待守护进程启动并创建锁文件
		time.Sleep(1 * time.Second)
		lockFile := filepath.Join(BASE_DIR, ".daemon.lock")
		if data, err := os.ReadFile(lockFile); err == nil {
			var pid int
			if fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); pid > 0 {
				logInfo("守护进程已启动，进程ID: %d", pid)
			} else {
				logInfo("守护进程已启动（等待锁文件创建）")
			}
		} else {
			logInfo("守护进程已启动（等待锁文件创建）")
		}
	} else {
		logInfo("守护进程已启动，进程ID: %d", cmd.Process.Pid)
	}

	// logInfo("守护进程日志文件: %s", logFile)
	// logInfo("提示: 守护进程在后台独立运行，与当前进程无关")
	// logInfo("提示: 使用以下命令停止守护进程：")
	lockFile := filepath.Join(BASE_DIR, ".daemon.lock")
	if data, err := os.ReadFile(lockFile); err == nil {
		var pid int
		if fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); pid > 0 {
			if runtime.GOOS == "windows" {
				logInfo("  taskkill /PID %d /F", pid)
			} else {
				logInfo("  kill -TERM %d 或 kill %d", pid, pid)
			}
		}
	}

	// 关闭日志文件句柄
	// 注意：在 Windows 上使用 start 命令时，子进程会继承句柄，但父进程可以关闭
	// 在 Unix 上，子进程会复制文件描述符，父进程可以关闭
	logFileHandle.Close()

	// 立即返回，不等待子进程
	// 注意：不要调用cmd.Wait()，让子进程独立运行
}

// RunDaemonInternal 守护进程内部运行函数（由exec.Command启动的进程调用）
func (bis *BatchInferService) RunDaemonInternal() {
	logInfo("========== 守护进程内部运行 ==========")
	logInfo("进程ID: %d", os.Getpid())
	// logInfo("扫描间隔: 60 秒")

	// 在守护进程中创建锁文件
	lockFile := filepath.Join(BASE_DIR, ".daemon.lock")
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		logError("创建锁文件失败: %v", err)
		os.Exit(1)
	}
	fmt.Fprintf(file, "%d", os.Getpid())
	file.Close()
	// logInfo("已创建守护进程锁文件: %s (PID: %d)", lockFile, os.Getpid())

	// 确保退出时删除锁文件
	defer func() {
		logInfo("守护进程退出，删除锁文件")
		bis.removeDaemonLock()
	}()

	// 使用context.Background()创建独立的context
	ctx, cancel := context.WithCancel(context.Background())

	// 创建信号通道，用于接收终止信号（SIGTERM等）
	termChan := make(chan os.Signal, 1)

	// 只注册终止信号，不注册SIGINT（Ctrl+C）
	if runtime.GOOS == "windows" {
		signal.Notify(termChan, syscall.SIGTERM)
	} else {
		signal.Notify(termChan, syscall.SIGTERM, syscall.SIGQUIT)
	}

	// 在独立的goroutine中启动守护进程（固定间隔60秒）
	go bis.runDaemonLoop(ctx)

	// 等待终止信号
	sig := <-termChan
	logInfo("收到终止信号: %v，正在停止守护进程...", sig)
	cancel()
	time.Sleep(1 * time.Second) // 给守护进程一点时间退出
	os.Exit(0)
}

// processPendingFiles 处理所有待处理的文件
func (bis *BatchInferService) processPendingFiles() {
	logInfo("========== 开始扫描待处理文件 ==========")

	taskIDs, err := bis.dbManager.GetPendingFiles()
	if err != nil {
		logError("获取待处理文件列表失败: %v", err)
		return
	}

	if len(taskIDs) == 0 {
		logInfo("没有待处理的文件")
		return
	}

	logInfo("找到 %d 个待处理文件", len(taskIDs))

	// 并行处理每个文件
	var wg sync.WaitGroup
	for _, taskID := range taskIDs {
		wg.Add(1)
		go func(fid string) {
			defer wg.Done()
			bis.ProcessFile(fid)
		}(taskID)
	}

	// 等待所有文件处理完成
	wg.Wait()

	logInfo("========== 本次扫描处理完成 ==========")
}

// DeleteFile 删除文件
func (bis *BatchInferService) DeleteFile(taskID string) {
	if taskID != "" {
		bis.dbManager.DeleteFile(taskID)
		chunkDir := filepath.Join(CHUNK_DIR, taskID)
		os.RemoveAll(chunkDir)
	}
}

func main() {
	initLogger()
	logInfo("========== 程序启动 ==========")

	// var pipeline, split, upload, process, merge, taskId, cancel, monitor, deleteFile string
	var pipeline, taskId, cancel, monitor string
	var configPath string
	var monitorProvided bool // 标记是否提供了 -monitor 参数
	var daemonInternal bool

	flag.StringVar(&configPath, "config", "", "模型配置文件路径（YAML格式），如果不指定则使用默认配置./config.yaml")
	flag.StringVar(&pipeline, "pipeline", "", "数据文件路径,运行完整流程（分割->上传->处理->合并->重试->结束）")
	flag.StringVar(&taskId, "task-id", "", "pipeline 传参，task_id不能为空")
	flag.StringVar(&cancel, "cancel", "", "具体task_id取消调度")
	flag.StringVar(&monitor, "monitor", "", "监控文件状态，不传task_id则显示所有进行中的文件")

	flag.BoolVar(&daemonInternal, "daemon-internal", false, "内部标志：守护进程内部运行（不要手动使用）")

	// 自定义 Usage 函数，隐藏 daemon-internal 参数
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "用法: %s [参数]\n\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "可用参数:\n")
		flag.VisitAll(func(f *flag.Flag) {
			if f.Name != "daemon-internal" {
				// 计算缩进
				s := fmt.Sprintf("  -%s", f.Name)
				name, usage := flag.UnquoteUsage(f)
				if len(name) > 0 {
					s += " " + name
				}
				// 添加默认值说明
				if f.DefValue != "" {
					usage += fmt.Sprintf(" (默认: %s)", f.DefValue)
				}
				fmt.Fprintf(flag.CommandLine.Output(), "%s\n    %s\n", s, usage)
			}
		})
	}

	// 先解析一次以获取 configPath（如果提供了）
	flag.Parse()

	// 加载配置文件
	if configPath == "" {
		// 如果没有指定配置文件路径，使用默认路径
		configPath = filepath.Join(BASE_DIR, "config.yaml")
	}
	if err := LoadConfig(configPath); err != nil {
		logError("加载配置文件失败: %v", err)
		os.Exit(1)
	}

	// 检查是否提供了 -monitor 参数
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "monitor" {
			monitorProvided = true
		}
	})

	logInfo("========== 开始执行 ==========")
	service := NewBatchInferService()

	// 首先检查是否是守护进程内部运行
	if daemonInternal {
		service.RunDaemonInternal()
		return
	}
	go service.RunDaemon()
	switch {
	case pipeline != "":
		// 完整流程
		logInfo("进入 pipeline case，参数: %s", pipeline)
		if taskId == "" {
			logError("task-id 参数不能为空")
			os.Exit(1)
		}
		service.RunPipeline(pipeline, taskId, nil)
	case cancel != "":
		// 终止调度
		logInfo("进入 cancel case，参数: %s", cancel)
		service.Cancel(cancel)
	case monitorProvided:
		// 监控状态
		// 如果 monitor 为空字符串，显示所有进行中的文件；否则显示指定文件
		logInfo("进入 monitor case，参数: %s", monitor)
		service.MonitorStatus(monitor)
	default:
		flag.Usage()
		os.Exit(1)
	}
}
