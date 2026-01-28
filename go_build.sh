#!/bin/bash
# Go 语言编译脚本（Linux/macOS）

echo "========================================"
echo "批量推理服务 - Go 编译脚本"
echo "========================================"
echo ""

# 检查 Go 是否安装
if ! command -v go &> /dev/null; then
    echo "[错误] 未找到 Go，请先安装 Go 1.16+"
    echo "下载地址: https://golang.org/dl/"
    exit 1
fi

echo "[1/3] 检查 Go 环境..."
go version
go env GOPATH
go env GOROOT

echo ""
echo "[2/3] 编译当前平台..."
mkdir -p dist

# 获取当前平台
CURRENT_OS=$(go env GOOS)
CURRENT_ARCH=$(go env GOARCH)

go build -ldflags="-s -w" -o "dist/batch_infer_${CURRENT_OS}_${CURRENT_ARCH}"
if [ $? -ne 0 ]; then
    echo "[错误] 编译失败"
    exit 1
fi

# 如果是 macOS，创建通用名称
if [ "$CURRENT_OS" = "darwin" ]; then
    cp "dist/batch_infer_${CURRENT_OS}_${CURRENT_ARCH}" dist/batch_infer
fi

# 如果是 Linux，创建通用名称
if [ "$CURRENT_OS" = "linux" ]; then
    cp "dist/batch_infer_${CURRENT_OS}_${CURRENT_ARCH}" dist/batch_infer
fi

echo ""
echo "[3/3] 交叉编译其他平台..."

echo "   - Windows x64..."
GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o dist/batch_infer_windows_amd64.exe

echo "   - Linux x64..."
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o dist/batch_infer_linux_amd64

echo "   - macOS x64..."
GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o dist/batch_infer_darwin_amd64

echo "   - macOS ARM64..."
GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o dist/batch_infer_darwin_arm64

echo ""
echo "========================================"
echo "编译完成！"
echo "========================================"
echo ""
echo "可执行文件位置:"
ls -lh dist/batch_infer* 2>/dev/null
echo ""
echo "测试运行:"
echo "  ./dist/batch_infer --help"
echo ""

