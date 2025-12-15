# Go 版本批量推理服务

这是 Python 版本的 Go 语言实现，提供更好的代码保护能力。

## 编译

### 安装依赖

```bash
cd src
go mod download
```

### 编译

```bash
# Windows
go build -ldflags="-s -w" -o batch_infer.exe

# Linux/macOS
go build -ldflags="-s -w" -o batch_infer
```

### 交叉编译

```bash
# Windows x64
set GOOS=windows
set GOARCH=amd64
go build -ldflags="-s -w" -o batch_infer_windows_amd64.exe

# Linux x64
set GOOS=linux
set GOARCH=amd64
go build -ldflags="-s -w" -o batch_infer_linux_amd64

# macOS x64
set GOOS=darwin
set GOARCH=amd64
go build -ldflags="-s -w" -o batch_infer_darwin_amd64

# macOS ARM64
set GOOS=darwin
set GOARCH=arm64
go build -ldflags="-s -w" -o batch_infer_darwin_arm64
```
