//go:build windows
// +build windows

package main

import "os/exec"

// setUnixProcessAttr Windows 版本：空实现
// Windows 上不需要设置 Setsid，因为已经使用 start /B 启动独立进程
func setUnixProcessAttr(cmd *exec.Cmd) {
	// Windows 上不需要设置 Setsid
}
