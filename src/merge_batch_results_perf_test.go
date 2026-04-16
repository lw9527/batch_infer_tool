package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMergeBatchResultsPerformance(t *testing.T) {
	t.Helper()

	chunkCounts := parseIntListEnv("MERGE_PERF_CHUNK_COUNTS", []int{2000})
	recordsPerChunk := parseIntEnv("MERGE_PERF_RECORDS_PER_CHUNK", 5000)
	missingPerChunk := parseIntEnv("MERGE_PERF_MISSING_PER_CHUNK", 0)
	retry := parseIntEnv("MERGE_PERF_RETRY", 0)

	if recordsPerChunk <= 0 {
		t.Fatalf("MERGE_PERF_RECORDS_PER_CHUNK must be > 0, got %d", recordsPerChunk)
	}
	if missingPerChunk < 0 {
		t.Fatalf("MERGE_PERF_MISSING_PER_CHUNK must be >= 0, got %d", missingPerChunk)
	}
	if missingPerChunk >= recordsPerChunk {
		t.Fatalf("MERGE_PERF_MISSING_PER_CHUNK(%d) must be < recordsPerChunk(%d)", missingPerChunk, recordsPerChunk)
	}

	t.Logf("config: chunk_counts=%v records_per_chunk=%d missing_per_chunk=%d retry=%d",
		chunkCounts, recordsPerChunk, missingPerChunk, retry)

	for _, chunkCount := range chunkCounts {
		if chunkCount <= 0 {
			t.Fatalf("invalid chunk count: %d", chunkCount)
		}

		fm, taskID, expectedMissing, cleanup := buildMergePerfFixture(
			t,
			chunkCount,
			recordsPerChunk,
			missingPerChunk,
			retry,
		)
		defer cleanup()

		start := time.Now()
		result, err := fm.MergeBatchResults(taskID, retry)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("MergeBatchResults failed: %v", err)
		}

		gotMissing, ok := result["missing_count"].(int)
		if !ok {
			t.Fatalf("missing_count type assert failed, value=%v", result["missing_count"])
		}
		if gotMissing != expectedMissing {
			t.Fatalf("missing_count mismatch: got=%d want=%d", gotMissing, expectedMissing)
		}

		totalRecords := chunkCount * recordsPerChunk
		rps := float64(totalRecords) / elapsed.Seconds()
		t.Logf(
			"[scale chunks=%d total_records=%d] elapsed=%s throughput=%.2f records/s missing=%d output=%v error=%v",
			chunkCount,
			totalRecords,
			elapsed,
			rps,
			gotMissing,
			result["output_file"],
			result["error_file"],
		)
	}
}

func buildMergePerfFixture(
	t *testing.T,
	chunkCount int,
	recordsPerChunk int,
	missingPerChunk int,
	retry int,
) (*FileManager, string, int, func()) {
	t.Helper()

	root := "./tmp"
	resetGlobalPathsForTest(root)

	db := NewDBManager()
	fm := NewFileManager(db)

	taskID := fmt.Sprintf("perf_task_%d_%d", chunkCount, time.Now().UnixNano())
	now := time.Now().Format(time.RFC3339)

	fileInfo := &FileInfo{
		TaskID:           taskID,
		OriginalFilename: "perf_mock.jsonl",
		FilePath:         filepath.Join(root, "mock_input.jsonl"),
		FileSize:         int64(chunkCount * recordsPerChunk * 256),
		TotalChunks:      chunkCount,
		TotalLines:       chunkCount * recordsPerChunk,
		Status:           FileStatusProcessing,
		CreatedTime:      now,
		UpdatedTime:      now,
		Retry:            retry,
		MaxRetry:         retry + 1,
		Model: ModelConfig{
			Domain:      "perf-model",
			MaxTokens:   1024,
			MessagesKey: "messages",
			Password:    "perf-password",
		},
	}
	if err := db.CreateFile(fileInfo); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	outputDir := filepath.Join(BATCH_RESULT_DIR, taskID, "output")
	errorDir := filepath.Join(BATCH_RESULT_DIR, taskID, "error")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("mkdir output dir failed: %v", err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		t.Fatalf("mkdir error dir failed: %v", err)
	}
	chunkDir := filepath.Join(CHUNK_DIR, taskID)
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatalf("mkdir chunk dir failed: %v", err)
	}

	expectedMissing := chunkCount * missingPerChunk

	for i := 0; i < chunkCount; i++ {
		chunkID := fmt.Sprintf("%s_chunk_%d", taskID, i)
		chunkPath := filepath.Join(chunkDir, fmt.Sprintf("part%d.perf_mock.jsonl", i))

		chunkLines := make([]string, 0, recordsPerChunk)
		outputLines := make([]string, 0, recordsPerChunk-missingPerChunk)

		for j := 0; j < recordsPerChunk; j++ {
			customID := fmt.Sprintf("chunk%06d_record%06d", i, j)
			chunkRecord := map[string]interface{}{
				"custom_id": customID,
				"method":    "POST",
				"url":       "/v1/chat/completions",
				"body": map[string]interface{}{
					"model":    "perf-model",
					"messages": []map[string]string{{"role": "user", "content": "hello"}},
				},
			}
			chunkJSON, _ := json.Marshal(chunkRecord)
			chunkLines = append(chunkLines, string(chunkJSON))

			if j >= recordsPerChunk-missingPerChunk {
				continue
			}

			outputRecord := map[string]interface{}{
				"custom_id": customID,
				"id":        fmt.Sprintf("resp_%s", customID),
				"status":    "success",
			}
			outputJSON, _ := json.Marshal(outputRecord)
			outputLines = append(outputLines, string(outputJSON))
		}

		if err := os.WriteFile(chunkPath, []byte(strings.Join(chunkLines, "\n")), 0o644); err != nil {
			t.Fatalf("write chunk file failed: %v", err)
		}

		outputPath := filepath.Join(outputDir, fmt.Sprintf("retry%d_%s.jsonl", retry, chunkID))
		if err := os.WriteFile(outputPath, []byte(strings.Join(outputLines, "\n")), 0o644); err != nil {
			t.Fatalf("write output file failed: %v", err)
		}

		chunk := &FileChunk{
			ChunkID:    chunkID,
			TaskID:     taskID,
			ChunkIndex: i,
			ChunkPath:  chunkPath,
			ChunkSize:  len(strings.Join(chunkLines, "\n")),
			Status:     ChunkStatusProcessed,
			Retry:      retry,
			BatchTaskInfo: &BatchTaskInfo{
				BatchID:        fmt.Sprintf("batch_%d", i),
				Status:         BatchStatusCompleted,
				InputFileID:    fmt.Sprintf("input_%d", i),
				OutputFileID:   fmt.Sprintf("output_%d", i),
				TotalCount:     recordsPerChunk,
				CompletedCount: len(outputLines),
				FailedCount:    missingPerChunk,
			},
		}
		if err := db.AddChunk(chunk); err != nil {
			t.Fatalf("AddChunk failed: %v", err)
		}
	}

	cleanup := func() {
		_ = db.db.Close()
	}
	return fm, taskID, expectedMissing, cleanup
}

func resetGlobalPathsForTest(root string) {
	BASE_DIR = root
	BATCH_RESULT_DIR = filepath.Join(root, "batch_result")
	CHUNK_DIR = filepath.Join(root, "chunks")
	MERGED_DIR = filepath.Join(root, "merged")
	DB_PATH = filepath.Join(root, "file_status.db")
	LOG_DIR = filepath.Join(root, "log")
	_ = os.MkdirAll(BATCH_RESULT_DIR, 0o755)
	_ = os.MkdirAll(CHUNK_DIR, 0o755)
	_ = os.MkdirAll(MERGED_DIR, 0o755)
	_ = os.MkdirAll(LOG_DIR, 0o755)
}

func parseIntEnv(key string, defaultValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue
	}
	return v
}

func parseIntListEnv(key string, defaults []int) []int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaults
	}

	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil || v <= 0 {
			continue
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		return defaults
	}
	return values
}

// go test -vet=off -run TestMergeBatchResultsPerformance -v
