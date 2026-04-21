package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gnet "github.com/shirou/gopsutil/v3/net"
)

const defaultLocalChatAPIBase = "https://maas-api.cn-huabei-1.xf-yun.com/v2"

const defaultUserQuery = `请解答这道题目，回答需要包含分析和答案。回答格式如下：
【分析】：xxx
【解答】：xxx
【答案】：xxx（如果答案已经在【解答】中，可选择省略【答案】）`

var imageMimeByExt = map[string]string{
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".png":  "image/png",
	".gif":  "image/gif",
	".bmp":  "image/bmp",
	".webp": "image/webp",
	".tiff": "image/tiff",
	".tif":  "image/tiff",
}

// inferTask 单行 JSON 任务
type inferTask struct {
	idValue     interface{}
	messagesRaw []map[string]interface{}
	imgPath     string
	fullImgPath string
	inputFile   string
	lineNum     int
}

// localInferResult 与 image.py 输出字段对齐
type localInferResult struct {
	ID          interface{}     `json:"id"`
	Messages    interface{}     `json:"messages,omitempty"`
	ImgPath     string          `json:"img_path,omitempty"`
	ImagePath   string          `json:"image_path,omitempty"`
	StartTime   string          `json:"start_time,omitempty"`
	EndTime     string          `json:"end_time,omitempty"`
	RequestTime *float64        `json:"request_time,omitempty"`
	Success     bool            `json:"success"`
	Response    json.RawMessage `json:"response,omitempty"`
	Error       string          `json:"error,omitempty"`
}

func chatAPIBaseURL(mc ModelConfig) string {
	s := strings.TrimSpace(mc.APIBaseURL)
	if s != "" {
		return strings.TrimRight(s, "/")
	}
	return defaultLocalChatAPIBase
}

func parseCommaPaths(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func imageMimeType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if m, ok := imageMimeByExt[ext]; ok {
		return m
	}
	return "image/jpeg"
}

// RunLocalInfer 处理多个 JSONL 输入文件（每行一条 JSON），支持纯文本与图文多模态，并发度随 Prometheus 部署路数调整。
func RunLocalInfer(inputFiles []string) error {
	mc := ModelConf
	cfg := LocalInferConf
	httpClient := &http.Client{Timeout: time.Duration(cfg.HTTPTimeoutSec) * time.Second}

	initialWorkers := waitForServiceReady(mc.Domain, cfg)
	var maxWorkers int32 = int32(initialWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go monitorPrometheusForLocalInfer(ctx, mc.Domain, &maxWorkers, cfg)
	if cfg.BandwidthMonitor != nil && *cfg.BandwidthMonitor {
		go monitorLocalBandwidth(ctx, cfg)
	}

	for _, inputPath := range inputFiles {
		outPath := defaultResultPath(inputPath)
		logInfo("本地并发推理: %s -> %s", inputPath, outPath)
		if err := processOneJSONLFile(inputPath, outPath, mc, cfg, httpClient, &maxWorkers); err != nil {
			return fmt.Errorf("%s: %w", inputPath, err)
		}
	}
	return nil
}

func monitorLocalBandwidth(ctx context.Context, cfg LocalInferConfig) {
	interval := time.Duration(cfg.BandwidthIntervalSec) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}

	prevSent, prevRecv, err := readTotalIOCounters()
	if err != nil {
		logInfo("带宽监控初始化失败: %v", err)
		return
	}
	prevTime := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			sent, recv, err := readTotalIOCounters()
			if err != nil {
				logInfo("带宽监控采样失败: %v", err)
				continue
			}
			elapsed := now.Sub(prevTime).Seconds()
			if elapsed <= 0 {
				continue
			}
			upMbps := float64(sent-prevSent) * 8 / 1024 / 1024 / elapsed
			downMbps := float64(recv-prevRecv) * 8 / 1024 / 1024 / elapsed
			logInfo("带宽监控: 上行=%.2f Mbps 下行=%.2f Mbps", upMbps, downMbps)

			prevSent, prevRecv = sent, recv
			prevTime = now
		}
	}
}

func readTotalIOCounters() (uint64, uint64, error) {
	counters, err := gnet.IOCounters(true)
	if err != nil {
		return 0, 0, err
	}
	var totalSent, totalRecv uint64
	for _, c := range counters {
		// 排除回环与虚拟隧道，避免噪声；其余网卡累加统计
		name := strings.ToLower(c.Name)
		if strings.Contains(name, "loopback") || strings.HasPrefix(name, "lo") || strings.Contains(name, "isatap") || strings.Contains(name, "teredo") {
			continue
		}
		totalSent += c.BytesSent
		totalRecv += c.BytesRecv
	}
	return totalSent, totalRecv, nil
}

func calcWorkersFromTotal(total int, cfg LocalInferConfig) int {
	n := int(float64(total) * cfg.ConcurrencyRatio)
	if n < 1 {
		n = 1
	}
	if n > cfg.MaxPoolCap {
		n = cfg.MaxPoolCap
	}
	return n
}

func waitForServiceReady(domain string, cfg LocalInferConfig) int {
	interval := time.Duration(cfg.PrometheusIntervalSec) * time.Second
	for {
		total, used, nodeTotal := getServiceInfo(domain)
		if total > 0 {
			workers := calcWorkersFromTotal(total, cfg)
			logInfo("服务已ready: domain=%s total=%d used=%d node_total=%d 并发=%d", domain, total, used, nodeTotal, workers)
			return workers
		}
		logInfo("服务未ready: domain=%s total=%d used=%d node_total=%d，等待%ds后重试", domain, total, used, nodeTotal, cfg.PrometheusIntervalSec)
		time.Sleep(interval)
	}
}

func monitorPrometheusForLocalInfer(ctx context.Context, domain string, maxWorkers *int32, cfg LocalInferConfig) {
	apply := func() {
		total, _, _ := getServiceInfo(domain)
		if total > 0 {
			atomic.StoreInt32(maxWorkers, int32(calcWorkersFromTotal(total, cfg)))
		}
	}
	apply()
	t := time.NewTicker(time.Duration(cfg.PrometheusIntervalSec) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			apply()
		}
	}
}

func defaultResultPath(inputPath string) string {
	dir := filepath.Dir(inputPath)
	base := filepath.Base(inputPath)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	return filepath.Join(dir, fmt.Sprintf("%s_result%s", stem, ext))
}

// loadProcessedIDsFromJSONL 读取 JSONL（每行一个 JSON）
func loadProcessedIDsFromJSONL(outputPath string) (map[string]struct{}, error) {
	done := make(map[string]struct{})
	data, err := os.ReadFile(outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return done, nil
		}
		return nil, err
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal(line, &row); err != nil {
			continue
		}
		if ok, _ := row["success"].(bool); !ok {
			continue
		}
		var key string
		if id, ok := row["id"]; ok {
			key = fmt.Sprintf("%v", id)
		} else if id, ok := row["image_id"]; ok {
			key = fmt.Sprintf("%v", id)
		}
		if key != "" {
			done[key] = struct{}{}
		}
	}
	return done, nil
}

func parseInferLine(line []byte, inputFile string, lineNum int) (*inferTask, string) {
	var row map[string]interface{}
	if err := json.Unmarshal(line, &row); err != nil {
		return nil, ""
	}
	idVal, ok := row["id"]
	if !ok || idVal == nil {
		logInfo("警告: %s 第 %d 行缺少 id，跳过", inputFile, lineNum)
		return nil, ""
	}
	idStr := fmt.Sprintf("%v", idVal)
	messageKey := strings.TrimSpace(ModelConf.MessagesKey)
	if messageKey == "" {
		messageKey = "messages"
	}
	var messagesRaw []map[string]interface{}
	if raw, exists := row[messageKey]; exists && raw != nil {
		switch v := raw.(type) {
		case []interface{}:
			for _, item := range v {
				if msg, ok := item.(map[string]interface{}); ok {
					messagesRaw = append(messagesRaw, msg)
				}
			}
		}
	}
	if len(messagesRaw) == 0 {
		logInfo("警告: %s 第 %d 行缺少有效的 %s 数组，跳过", inputFile, lineNum, messageKey)
		return nil, idStr
	}
	imgPath, _ := row["img_path"].(string)
	imgPath = strings.TrimSpace(imgPath)

	inputDir := filepath.Dir(inputFile)
	var fullImg string
	if imgPath != "" {
		if filepath.IsAbs(imgPath) {
			fullImg = imgPath
		} else {
			fullImg = filepath.Join(inputDir, filepath.Clean(imgPath))
		}
		if _, err := os.Stat(fullImg); err != nil {
			logInfo("警告: 图片不存在 %s (id=%s)，跳过", fullImg, idStr)
			return nil, idStr
		}
	}

	return &inferTask{
		idValue:     idVal,
		messagesRaw: messagesRaw,
		imgPath:     imgPath,
		fullImgPath: fullImg,
		inputFile:   inputFile,
		lineNum:     lineNum,
	}, idStr
}

func buildMessages(mc ModelConfig, task *inferTask) ([]map[string]interface{}, error) {
	if strings.TrimSpace(task.imgPath) == "" {
		return task.messagesRaw, nil
	}

	b, err := os.ReadFile(task.fullImgPath)
	if err != nil {
		return nil, err
	}
	b64 := base64.StdEncoding.EncodeToString(b)
	mime := imageMimeType(task.fullImgPath)
	dataURL := fmt.Sprintf("data:%s;base64,%s", mime, b64)

	messages := make([]map[string]interface{}, len(task.messagesRaw))
	for i, msg := range task.messagesRaw {
		clone := make(map[string]interface{}, len(msg))
		for k, v := range msg {
			clone[k] = v
		}
		messages[i] = clone
	}
	imagePart := map[string]interface{}{
		"type": "image_url",
		"image_url": map[string]interface{}{
			"url": dataURL,
		},
	}
	messages = append(messages, map[string]interface{}{
		"role": "user",
		"content": []interface{}{
			imagePart,
		},
	})
	return messages, nil
}

func mergeChatRequestBody(body map[string]interface{}, mc ModelConfig) {
	if mc.ExtraBody != nil {
		for k, v := range mc.ExtraBody {
			body[k] = v
		}
	}
	if mc.EnableThinking != nil && *mc.EnableThinking {
		var ctk map[string]interface{}
		if raw, ok := body["chat_template_kwargs"]; ok {
			if m, ok := raw.(map[string]interface{}); ok {
				ctk = m
			}
		}
		if ctk == nil {
			ctk = make(map[string]interface{})
		}
		ctk["thinking"] = true
		body["chat_template_kwargs"] = ctk
	}
}

func postChatCompletions(ctx context.Context, client *http.Client, mc ModelConfig, messages []map[string]interface{}) (json.RawMessage, int, error) {
	base := chatAPIBaseURL(mc)
	u := base + "/chat/completions"

	body := map[string]interface{}{
		"model":      mc.Domain,
		"max_tokens": mc.MaxTokens,
		"stream":     false,
		"stream_options": map[string]interface{}{
			"include_usage": true,
		},
	}
	msgKey := mc.MessagesKey
	if msgKey == "" {
		msgKey = "messages"
	}
	body[msgKey] = messages

	if mc.Temperature != nil {
		body["temperature"] = *mc.Temperature
	} else {
		body["temperature"] = 1.0
	}
	if mc.TopP != nil {
		body["top_p"] = *mc.TopP
	}
	if len(mc.Tools) > 0 {
		body["tools"] = mc.Tools
	}
	if mc.ToolChoice != nil {
		body["tool_choice"] = mc.ToolChoice
	}
	mergeChatRequestBody(body, mc)

	raw, err := json.Marshal(body)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+mc.Password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return json.RawMessage(respBody), resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncateForLog(respBody))
	}
	return json.RawMessage(respBody), resp.StatusCode, nil
}

func truncateForLog(b []byte) string {
	s := string(b)
	if len(s) > 512 {
		return s[:512] + "..."
	}
	return s
}

func runOneInferTask(ctx context.Context, client *http.Client, mc ModelConfig, task *inferTask) localInferResult {
	nowStr := func() string { return time.Now().Format("2006-01-02 15:04:05") }
	res := localInferResult{
		ID:        task.idValue,
		Messages:  task.messagesRaw,
		ImgPath:   task.imgPath,
		ImagePath: task.fullImgPath,
		Success:   false,
	}
	start := time.Now()
	res.StartTime = nowStr()

	msgs, err := buildMessages(mc, task)
	if err != nil {
		res.EndTime = nowStr()
		res.Error = err.Error()
		return res
	}

	raw, _, err := postChatCompletions(ctx, client, mc, msgs)
	end := time.Now()
	res.EndTime = end.Format("2006-01-02 15:04:05")
	sec := end.Sub(start).Seconds()
	res.RequestTime = &sec

	if err != nil {
		res.Error = err.Error()
		if len(raw) > 0 {
			res.Response = raw
		}
		return res
	}
	res.Success = true
	res.Response = raw
	return res
}

func workerLimit(cfg LocalInferConfig, maxWorkers *int32) int32 {
	l := atomic.LoadInt32(maxWorkers)
	if l < 1 {
		l = 1
	}
	if int(l) > cfg.MaxPoolCap {
		l = int32(cfg.MaxPoolCap)
	}
	return l
}

type passStats struct {
	attempted int64
	succeeded int64
	failed    int64
}

func processOneJSONLFile(inputPath, outputPath string, mc ModelConfig, cfg LocalInferConfig, client *http.Client, maxWorkers *int32) error {
	maxRetry := MAX_RETRY_COUNT
	if maxRetry < 0 {
		maxRetry = 0
	}
	logInfo("文件重试策略: input=%s max_retry_count=%d（整文件轮次重试）", inputPath, maxRetry)

	for round := 0; round <= maxRetry; round++ {
		stats, err := runOneJSONLPass(inputPath, outputPath, mc, cfg, client, maxWorkers, round)
		if err != nil {
			return err
		}
		logInfo("文件轮次完成: input=%s round=%d attempted=%d success=%d failed=%d", inputPath, round, stats.attempted, stats.succeeded, stats.failed)
		if stats.failed == 0 {
			logInfo("文件处理结束: %s 无需继续重试", inputPath)
			return nil
		}
		if round == maxRetry {
			logInfo("文件处理结束: %s 已达到最大轮次，仍有失败=%d", inputPath, stats.failed)
			return nil
		}
	}
	return nil
}

func runOneJSONLPass(inputPath, outputPath string, mc ModelConfig, cfg LocalInferConfig, client *http.Client, maxWorkers *int32, round int) (passStats, error) {
	var stats passStats
	processed, err := loadProcessedIDsFromJSONL(outputPath)
	if err != nil {
		return stats, err
	}
	if len(processed) > 0 {
		logInfo("轮次%d: %s 已有成功记录 %d 条（本轮跳过）", round, outputPath, len(processed))
	}

	inFile, err := os.Open(inputPath)
	if err != nil {
		return stats, err
	}
	defer inFile.Close()

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return stats, err
	}
	defer outFile.Close()

	taskCh := make(chan *inferTask, cfg.BatchLoadSize*2)
	var wg sync.WaitGroup
	var writeMu sync.Mutex
	buf := make([]localInferResult, 0, cfg.FlushEvery)
	flushLocked := func() error {
		for _, r := range buf {
			line, err := json.Marshal(r)
			if err != nil {
				return err
			}
			if _, err := outFile.Write(append(line, '\n')); err != nil {
				return err
			}
		}
		buf = buf[:0]
		return nil
	}
	flush := func() error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return flushLocked()
	}

	var inFlight int32

	workerLoop := func() {
		defer wg.Done()
		for task := range taskCh {
			for atomic.LoadInt32(&inFlight) >= workerLimit(cfg, maxWorkers) {
				time.Sleep(5 * time.Millisecond)
			}
			atomic.AddInt32(&inFlight, 1)
			res := runOneInferTask(context.Background(), client, mc, task)
			atomic.AddInt32(&inFlight, -1)

			atomic.AddInt64(&stats.attempted, 1)
			if res.Success {
				atomic.AddInt64(&stats.succeeded, 1)
			} else {
				atomic.AddInt64(&stats.failed, 1)
			}

			writeMu.Lock()
			buf = append(buf, res)
			shouldFlush := len(buf) >= cfg.FlushEvery
			writeMu.Unlock()
			if shouldFlush {
				if err := flush(); err != nil {
					logError("写入结果失败: %v", err)
				}
			}
			n := atomic.LoadInt64(&stats.attempted)
			st := "✓"
			if !res.Success {
				st = "✗"
			}
			rt := ""
			if res.RequestTime != nil {
				rt = fmt.Sprintf("%.2f", *res.RequestTime)
			}
			logInfo("[%d] %s id=%v 耗时=%ss 当前并发上限=%d 在途=%d", n, st, res.ID, rt, workerLimit(cfg, maxWorkers), atomic.LoadInt32(&inFlight))
		}
	}

	nPool := cfg.MaxPoolCap
	if nPool < 1 {
		nPool = 1
	}
	wg.Add(nPool)
	for i := 0; i < nPool; i++ {
		go workerLoop()
	}

	scanner := bufio.NewScanner(inFile)
	bufSize := cfg.BatchLoadSize * 1024
	if bufSize < 64*1024 {
		bufSize = 64 * 1024
	}
	scanner.Buffer(make([]byte, bufSize), 10*1024*1024)

	lineNum := 0
	var lastSubmit time.Time
	minGap := time.Duration(cfg.SubmitIntervalMs) * time.Millisecond

	for scanner.Scan() {
		lineNum++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		task, idStr := parseInferLine(line, inputPath, lineNum)
		if task == nil {
			continue
		}
		if _, skip := processed[idStr]; skip {
			continue
		}
		if !lastSubmit.IsZero() && minGap > 0 {
			if d := time.Since(lastSubmit); d < minGap {
				time.Sleep(minGap - d)
			}
		}
		taskCh <- task
		lastSubmit = time.Now()
	}
	if err := scanner.Err(); err != nil {
		close(taskCh)
		wg.Wait()
		return stats, err
	}
	close(taskCh)
	wg.Wait()
	return stats, flush()
}
