# 1. 强制杀死所有 Go 程序进程 (包括主程序和后台)
taskkill /F /IM batch_tool.exe /T 2>$null
taskkill /F /IM batch_infer.exe /T 2>$null

# 2. 删除守护进程锁文件 (解决"假死"和卡顿的关键)
Remove-Item .daemon.lock -Force -ErrorAction SilentlyContinue

# 3. 删除数据库文件 (清空所有历史任务，彻底重置)
Remove-Item file_status.db -Force -ErrorAction SilentlyContinue

# 4. 删除临时文件夹 (可选：如果你想连生成的碎片都删掉)
Remove-Item -Recurse -Force chunks, batch_result, merged -ErrorAction SilentlyContinue

Write-Host "✅ 环境已彻底清理！你可以重新开始测试了。" -ForegroundColor Green