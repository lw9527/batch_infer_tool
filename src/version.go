package main

import (
	"fmt"
	"runtime"
	"strings"
)

// 构建时可通过 -ldflags 注入，例如:
// go build -ldflags "-X main.version=1.2.3 -X main.commit=abc1234 -X main.buildTime=2026-04-21T12:00:00Z"
var (
	version   = "1.0.1"
	commit    = ""
	buildTime = ""
)

func formatVersion() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s", version)
	if commit != "" {
		fmt.Fprintf(&b, " commit=%s", commit)
	}
	if buildTime != "" {
		fmt.Fprintf(&b, " built=%s", buildTime)
	}
	fmt.Fprintf(&b, " go=%s", runtime.Version())
	return b.String()
}
