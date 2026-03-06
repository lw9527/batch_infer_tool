//go:build !windows
// +build !windows

package main

import (
	"os/exec"
	"syscall"
)

// setUnixProcessAttr 设置 Unix 系统的进程属性，使子进程独立于父进程
func setUnixProcessAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // 创建新的会话，脱离父进程组
	}
}
