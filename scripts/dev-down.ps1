#Requires -Version 5.1
<#
  gosk (go-ecom-admin) 一键停止脚本（dev-up 的反向操作）。

  默认：docker compose down —— 停掉并移除容器，数据卷保留（MySQL/Redis/ES 数据不丢）。
  前端 dev server 在各自窗口里，需要手动 Ctrl+C，或加 -KillFrontend 让脚本代劳。

  用法：
    双击          scripts\dev-down.bat
    PowerShell    powershell -ExecutionPolicy Bypass -File scripts\dev-down.ps1

  可选参数：
    -Volumes      等同于 make clean：连数据卷一起删（数据清零，慎用）
    -KillFrontend 顺手结束占用 5173/5174 的进程（vite dev server）
#>

[CmdletBinding()]
param(
    [switch]$Volumes,
    [switch]$KillFrontend
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot

function Write-Step { param([string]$Msg) Write-Host "`n==> $Msg" -ForegroundColor Cyan }
function Write-Good { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Green }
function Write-WarnMsg { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Yellow }

$script:DockerExe = 'docker'
if (-not (Get-Command 'docker' -ErrorAction SilentlyContinue)) {
    $fallback = "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
    if (Test-Path $fallback) { $script:DockerExe = $fallback }
}

if ($KillFrontend) {
    Write-Step '结束前端 dev server（5173 / 5174）'
    foreach ($port in @(5173, 5174)) {
        try {
            $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
            foreach ($c in $conns) {
                $p = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
                if ($p) {
                    Write-Host "    kill PID $($p.Id)  $($p.ProcessName)  (port $port)"
                    Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
                }
            }
        } catch {
            Write-WarnMsg "清理端口 $port 失败：$($_.Exception.Message)"
        }
    }
    Write-Good '端口已释放'
}

Write-Step 'docker compose down'
$args = @('compose', 'down')
if ($Volumes) { $args += '-v'; Write-WarnMsg '-Volumes：数据卷也会被删除，数据库数据清零！' }

Push-Location $Root
try {
    & $script:DockerExe @args
} finally { Pop-Location }

Write-Good '容器已停止移除（数据卷保留，下次 up 数据还在）'
Write-Host '  提示：前端窗口请自行 Ctrl+C，或下次用 dev-down.bat -KillFrontend' -ForegroundColor DarkGray
Write-Host ''
