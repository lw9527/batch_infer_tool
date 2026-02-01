package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"
)

// ProgressDisplay 进度显示类
type ProgressDisplay struct {
	width int
}

func NewProgressDisPlay() *ProgressDisplay {
	return &ProgressDisplay{
		width: getScreenWidth(),
	}
}
func getScreenWidth() int {
	terminalWidth := 120

	// 尝试获取终端宽度
	if runtime.GOOS == "windows" {
		width, _, err := term.GetSize(int(os.Stdout.Fd()))
		if err == nil && width > 0 {
			terminalWidth = width
		}
	} else {
		// Unix: 使用 stty 获取终端宽度
		cmd := exec.Command("stty", "size")
		cmd.Stdin = os.Stdin
		if output, err := cmd.Output(); err == nil {
			parts := strings.Fields(string(output))
			if len(parts) >= 2 {
				if w, err := strconv.Atoi(parts[1]); err == nil && w > 0 {
					terminalWidth = w
				}
			}
		}
	}
	return terminalWidth
}

// Update 更新显示
func (p *ProgressDisplay) Update(message string) {
	// 直接使用 log.Print 而不是 logInfo，避免格式化问题
	// 因为 message 可能包含 % 字符（如进度条中的百分比）
	if infoLogger != nil {
		infoLogger.Print(message)
		if logFile != nil {
			logFile.Sync()
		}
	}
}

// GetDisplayWidth 计算字符串在终端中的显示宽度（中文字符占2个字符宽度）
// 注意：Unicode 块字符（如 █、▌）在终端中通常只占1个字符宽度，不是2个
func (p *ProgressDisplay) GetDisplayWidth(text string) int {
	width := 0
	for _, char := range text {
		// Unicode 块字符范围：U+2580-U+259F，这些字符在终端中占1个字符宽度
		if char >= 0x2580 && char <= 0x259F {
			width += 1
		} else if char > 127 {
			// 其他非ASCII字符（如中文）占2个字符宽度
			width += 2
		} else {
			width += 1
		}
	}
	return width
}

// CalcProgress 计算进度条
func (p *ProgressDisplay) CalcProgress(prefix string, finishCount int, totalCount int, execTime *time.Time) string {

	// 计算进度百分比
	var progressPercent float64
	if totalCount > 0 {
		progressPercent = float64(finishCount) / float64(totalCount) * 100
	}

	// 左侧部分：百分比 + 竖线
	// 使用 %d 格式化整数百分比，%% 转义为单个 %
	percentInt := int(progressPercent)
	// 直接拼接字符串，避免格式化问题
	leftPart := fmt.Sprintf("%d", percentInt) + "%|"

	// 右侧部分：竖线 + 已完成数/总数
	rightPart := fmt.Sprintf("|%d/%d", finishCount, totalCount)

	remainTimePart := "|" + getRemainTime(execTime, float64(finishCount)/float64(totalCount))

	// 计算各部分显示宽度（统一使用显示宽度，不是字节数）
	prefixWidth := p.GetDisplayWidth(prefix)
	leftWidth := p.GetDisplayWidth(leftPart)
	rightWidth := p.GetDisplayWidth(rightPart)
	remainTimeWidth := p.GetDisplayWidth(remainTimePart)

	// 计算中间进度条的可用宽度
	progressWidth := p.width - leftWidth - rightWidth - prefixWidth - remainTimeWidth
	if progressWidth < 10 {
		progressWidth = 10
	}

	// 每个进度块 "█▌" 在终端中占2个字符宽度（每个字符占1个宽度）
	blockSize := p.GetDisplayWidth("█▌") // 应该是 2
	maxBlocks := progressWidth / blockSize

	// 计算已完成的块数量
	var filledBlocks int
	var partialProgress float64
	if totalCount > 0 {
		filledBlocks = int(float64(finishCount) / float64(totalCount) * float64(maxBlocks))
		partialProgress = float64(finishCount)/float64(totalCount)*float64(maxBlocks) - float64(filledBlocks)
	}

	// 构建进度条
	progressBar := strings.Repeat("█▌", filledBlocks)

	// 添加部分完成的块（如果有）
	if partialProgress > 0.2 {
		progressBar += "▌"
	}

	// 计算剩余的空格数（使用显示宽度，不是字节数）
	// 使用 GetDisplayWidth 准确计算进度条的显示宽度
	usedDisplayWidth := p.GetDisplayWidth(progressBar)
	remainingSpaces := progressWidth - usedDisplayWidth

	// 添加所有空格到后面
	if remainingSpaces > 0 {
		progressBar += strings.Repeat(" ", remainingSpaces)
	} else if remainingSpaces < 0 {
		// 如果超出，需要截断进度条
		// 由于进度条字符是Unicode，需要按显示宽度截断
		if progressWidth > 0 {
			// 逐步减少块数，直到宽度合适
			for p.GetDisplayWidth(progressBar) > progressWidth {
				if len(progressBar) >= 2 {
					progressBar = progressBar[:len(progressBar)-2] // 移除最后一个 "█▌"
				} else {
					break
				}
			}
			// 如果还有空间，尝试添加部分块
			currentWidth := p.GetDisplayWidth(progressBar)
			if currentWidth < progressWidth && partialProgress > 0.2 {
				// 检查是否可以添加 "▌"
				testBar := progressBar + "▌"
				if p.GetDisplayWidth(testBar) <= progressWidth {
					progressBar = testBar
				}
			}
			// 最后确保宽度正确
			currentWidth = p.GetDisplayWidth(progressBar)
			if currentWidth < progressWidth {
				progressBar += strings.Repeat(" ", progressWidth-currentWidth)
			}
		}
	}

	// 组合：prefix + 百分比|进度条|已完成/总数
	// 使用字符串拼接而不是 fmt.Sprintf，避免格式化问题
	logInfo("prefix: %d, leftPart: %d, progressBar: %d, rightPart: %d, remainTimePart: %d", prefixWidth, leftWidth, p.GetDisplayWidth(progressBar), rightWidth, remainTimeWidth)
	return prefix + leftPart + progressBar + rightPart + remainTimePart
}

func SecondsToHHMMSS(seconds int64) string {
	const (
		secPerHour = 3600
		secPerMin  = 60
	)
	hours := seconds / secPerHour     // 总小时数
	remainSec := seconds % secPerHour // 取小时后剩余秒数
	minutes := remainSec / secPerMin  // 剩余秒数拆分钟
	secs := remainSec % secPerMin     // 最终剩余秒数
	// %02d：格式化为两位十进制数，不足两位左侧补0
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, secs)
}
func getRemainTime(execTime *time.Time, completedRadio float64) string {
	if execTime == nil || completedRadio == 0 {
		return "        "
	}
	logInfo("getRemainTime :%s", execTime.Format(time.DateTime))
	if completedRadio >= 1 {
		return "00:00:01"
	}
	usedTime := time.Now().Sub(*execTime).Seconds()
	if usedTime < 0 {
		return "        "
	}
	reaminTime := int(usedTime/completedRadio - usedTime)
	return SecondsToHHMMSS(int64(reaminTime))

}

// ShowStatus 显示文件状态
func (p *ProgressDisplay) ShowStatus(fileInfo *FileInfo, clearScreen bool) {
	summary := fileInfo.GetStatusSummary()
	total := summary.Total

	statusMsg := fmt.Sprintf("\n 文件: %s | task_id: %s \n 总块数: %d | 待上传：%d | 已上传: %d | 处理中: %d | 已处理: %d | 上传失败: %d \n 总处理行数: %d | 已完成行数: %d | 失败行数: %d | 重试次数: %d次",
		fileInfo.OriginalFilename, fileInfo.TaskID, summary.TotalChunks,
		total["pending"], total["uploaded"], total["processing"], total["processed"], total["upload_failed"],
		fileInfo.TotalLines, total["complete_count"], total["failed_count"], fileInfo.Retry)

	// 计算进度条
	totalCount := fileInfo.TotalLines
	completeCount := total["complete_count"]
	statusMsg += "\n"
	statusMsg += p.CalcProgress("已成功占比：", completeCount, totalCount, nil)

	processingTrunks := summary.ProcessingTrunks

	// 迭代显示每个处理中块的进度
	if len(processingTrunks) > 0 {
		// 排序chunk_id以便显示
		chunkIDs := make([]string, 0, len(processingTrunks))
		for chunkID := range processingTrunks {
			chunkIDs = append(chunkIDs, chunkID)
		}
		sort.Strings(chunkIDs)

		for _, chunkID := range chunkIDs {
			trunkInfo := processingTrunks[chunkID]
			trunkTotal, _ := trunkInfo["total_count"].(int)
			trunkComplete, _ := trunkInfo["complete_count"].(int)
			trunkFailed, _ := trunkInfo["failed_count"].(int)
			// 显示块ID和进度条
			statusMsg += "\n"
			// 获取 batch_start_time 用于计算剩余时间
			var batchStartTime time.Time
			var err error
			if batchStartTimeVal, ok := trunkInfo["batch_start_time"].(string); ok {
				batchStartTime, err = time.ParseInLocation(time.DateTime, batchStartTimeVal, time.Local)
				if err != nil {
					logError("batch start time parse error:%s", err.Error())
				}
			}
			statusMsg += p.CalcProgress(fmt.Sprintf("%s 完成进度: ", chunkID), trunkComplete+trunkFailed, trunkTotal, &batchStartTime)
		}
	}

	// 根据参数决定是否清屏并显示状态
	if clearScreen {
		clearScreenFunc()
	}
	p.Update(statusMsg)
}

// ShowSimpleFileInfo 显示简化的文件信息（只显示文件名、file_id、创建时间、当前状态）
func (p *ProgressDisplay) ShowSimpleFileInfo(fileInfo *FileInfo) {
	statusMsg := fmt.Sprintf("文件名: %s | task_id: %s | 创建时间: %s | 当前状态: %s",
		fileInfo.OriginalFilename, fileInfo.TaskID, fileInfo.CreatedTime, fileInfo.Status)
	p.Update(statusMsg)
}

// clearScreenFunc 清屏
func clearScreenFunc() {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	} else {
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func (p *ProgressDisplay) bar(percent float64) string {
	fill := int(percent / 100.0 * float64(p.width))
	if fill > p.width {
		fill = p.width
	}
	if fill < 0 {
		fill = 0
	}
	return fmt.Sprintf("[%s%s]", strings.Repeat("█", fill), strings.Repeat(" ", p.width-fill))
}
