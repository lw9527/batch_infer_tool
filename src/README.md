# Batch Infer Tool (批量异步推理工具)

> **简介**：这是一个专为大规模数据处理设计的高性能离线推理工具。它支持将大规模数据文件自动切分、分批上传并进行异步处理。工具内置自动容错重试机制，并能将分散的处理结果自动合并为最终数据文件。

---

## 📚 目录 (Table of Contents)
1. [快速开始](#-快速开始-quick-start)
2. [数据输入规范](#-数据输入规范-data-specification)
3. [核心功能与多任务并行](#-核心功能与多任务并行)
4. [输出结果与合并逻辑](#-输出结果与合并逻辑-outputs)
5. [常见问题与排查](#-常见问题与排查-faq)

---

## 🚀 快速开始 (Quick Start)

### 第一步：准备环境
确保目录下包含以下文件：
* **程序文件**：`batch_infer_windows_arm64.exe` (Windows) 或 `batch_infer_linux_arm64` (Linux)
* **配置文件**：`config.yaml` (参考config.yaml.example)
* **数据文件**：如 `input.jsonl`

### 第二步：配置 Config.yaml
在同级目录新建 `config.yaml`。请注意以下参数对数据量的影响：

```yaml
model:
  domain: "test"      # 模型服务标识 (必填)
  password: "YOUR_PASSWORD"  # 授权密码 (必填)
  max_tokens: 16384          # 最大输出长度 (必填)
  messages_key: "messages"   # 数据集中的对话字段名
  temperature: 0.6
  enable_thinking: false
  extra_body: {
    "stop":[],
    "continue_final_message":false
  }

# 处理策略
test_lines: 300              # 【重要】测试行数限制。若设为 300，则仅处理输入文件的前 300 行，剩余数据将被忽略。正式生产环境请设为 0 以处理全量数据。
lines_per_chunk: 200         # 每个分块包含的数据行数。
max_retry_count: 3           # 失败后的最大重试次数。
```

---

## 📋 数据输入规范 (Data Specification)

为确保工具正常解析，您的输入文件（如 `input.jsonl`）必须符合以下标准：
* **文件格式**：必须为标准 `.jsonl` 格式，即每一行都是一个独立的 JSON 对象。
* **关键字段**：每行必须包含在 `config.yaml` 中指定的 `messages_key`（如 `messages`）。
* **输入示例**：
  `{"messages": [{"role": "user", "content": "你是谁？"}]}`

---

## ⚡ 核心功能与多任务并行

本工具支持多个任务同时运行，系统通过守护进程统一调度。

### 1. 启动任务 (`-pipeline`)
您可以连续启动多个任务，只要确保每个任务的 `task-id` 唯一：
```powershell
# 任务 A 启动
.\batch_infer_windows_arm64.exe -pipeline data1.jsonl -task-id "task_A"
# 任务 B 同时启动 (使用独立 ID)
.\batch_infer_windows_arm64.exe -pipeline data2.jsonl -task-id "task_B"
```
* **并行逻辑**：系统会检查 `.daemon.lock` 锁文件。若守护进程已在运行，新任务将自动加入调度队列并行处理。
* **隔离性**：不同 `task-id` 的任务在 `chunks/` 和 `merged/` 目录下拥有独立的存储空间，互不干扰。

### 2. 指定配置文件 (`-config`)
如果需要运行不同的模型配置，可以通过此参数指定不同文件：
```bash
.\batch_infer_windows_arm64.exe -pipeline input.jsonl -task-id "task_v2" -config "config_pro.yaml"
```

### 3. 实时监控 (`-monitor`)
```bash
# 监控指定任务的占比、进度及各分块状态
.\batch_infer_windows_arm64.exe -monitor "task_A"
```

### 4. 取消任务 (`-cancel`)
如果发现配置错误或需要中途停止，使用此命令终止调度。
```bash
.\batch_infer_windows_arm64.exe -cancel "task_v1"
```

---

## 📂 输出结果与合并逻辑 (Outputs)

任务完成后，工具会自动执行结果合并流程。结果存储在 `merged/[task_id]/` 目录下：

| 文件名 | 内容描述 | 合并逻辑细节 |
|:---|:---|:---|
| **output.jsonl** | ✅ 推理成功的数据 | 自动收集所有分块中状态为成功的回复，合并为一个完整文件。 |
| **error_retry0.jsonl** | ❌ 推理失败的数据 | 记录所有在达到最大重试次数后依然失败的请求及错误原因。 |
| **missing_records.jsonl** | ⚠️ 丢失/缺失数据 | 记录在原始文件中存在但在处理过程中未获取到任何返回的条目。 |

### 输出示例 (output.jsonl)
合并后的结果将保留您的 `custom_id` 并追加模型回复。成功结果示例如下：
```json
{"id":"batch_260202...","custom_id":"148","response":{"status_code":101,"body":{"choices":[{"message":{"role":"assistant","content":"我是通义千问...","reasoning_content":"用户问我是谁..."}}]}}}
{"id":"batch_260202...","custom_id":"108","response":{"status_code":101,"body":{"choices":[{"message":{"role":"assistant","content":"我是你的AI助手...","reasoning_content":"用户可能想了解我的身份..."}}]}}}
```

---

## ❓ 常见问题与排查 (FAQ)

**Q1: 为什么我的输出文件只有几百行，但原始文件有上万行？**
* **答**：请检查 `config.yaml` 中的 `test_lines` 参数。如果该值非 0，工具将截断数据仅处理设定的行数。

**Q2: 启动时报错 `task-id 参数不能为空`**
* **原因**：未传递任务标识。
* **解决**：命令后必须加上 `-task-id "你的任务名"`。

**Q3: 报错 `配置文件中 domain 不能为空` 或 `password 不能为空`**
* **原因**：`config.yaml` 缺失关键认证信息。
* **解决**：请检查配置文件，确保 `domain`, `password`, `max_tokens` 均已正确填写。

**Q4: 提示 `文件 [task_id] 已存在`**
* **原因**：当前目录下已经运行过同名的任务。
* **解决**：
    1. 更换一个新的 task-id（推荐）。
    2. 或者手动删除 `chunks/`, `merged/` 中对应的旧文件夹。

**Q5: 运行中断网了怎么办？(`dial tcp: ... no such host`)**
* **机制**：工具内置了常驻进程自动执行和重试机制。
* **解决**：无需手动干预。网络恢复后，程序会自动重试上传或拉取结果。如果程序已退出，使用相同的命令重新启动即可继续处理。

**Q6: 参数解析错误 `cannot unmarshal !!str into float64`**
* **原因**：`config.yaml` 格式错误。
* **解决**：检查 `temperature` 等数字字段，不要加引号（例如写成 `0.6` 而不是 `"0.6"`）。
