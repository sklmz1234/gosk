#Requires -Version 5.1
<#
  gosk (go-ecom-admin) 一键开发环境启动器。

  Docker Desktop 由你自己启动（手动开，避免脚本抢跑导致整机卡顿），
  脚本负责其余全部终端自动化：
    1) 等 Docker engine 就绪（没开就提示你去开，脚本原地等待）
    2) 项目根目录 docker compose up -d
    3) 等网关 /healthz 通过（后端真正可服务）
    4) 新开两个终端窗口跑 frontend(5173) 和 shop(5174) 的 npm run dev

  用法：
    双击                 scripts\dev-up.bat
    PowerShell           powershell -ExecutionPolicy Bypass -File scripts\dev-up.ps1
    Git Bash             ./scripts/dev-up.sh

  可选参数：
    -Build        up 时加 --build（改了 Go 代码/Dockerfile 时用，首次会慢）
    -Seed        服务就绪后自动 docker compose run --rm seed 灌测试数据
    -SkipDocker  跳过 Docker 环节（容器已经跑着，只起两个前端）
    -NoFrontend  只起后端容器，不起前端
    -NoBrowser   最后不自动开浏览器
#>

[CmdletBinding()]
param(
    [switch]$Build,
    [switch]$Seed,
    [switch]$SkipDocker,
    [switch]$NoFrontend,
    [switch]$NoBrowser
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------- 路径常量
$Root      = Split-Path -Parent $PSScriptRoot          # 项目根目录（scripts 的上一级）
$AdminDir  = Join-Path $Root 'frontend'                # 管理台，vite 默认 5173
$ShopDir   = Join-Path $Root 'shop'                    # C 端商城，vite.config.js 里写死 5174
$HealthUrl = 'http://127.0.0.1:8080/healthz'           # 网关健康检查（internal/gateway/router）

# ---------------------------------------------------------------- 小工具函数
function Write-Step { param([string]$Msg) Write-Host "`n==> $Msg" -ForegroundColor Cyan }
function Write-Info { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor DarkGray }
function Write-Good { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Green }
function Write-WarnMsg { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Yellow }

# docker CLI：优先 PATH，找不到再试 Docker Desktop 自带的那一份。
$script:DockerExe = 'docker'
if (-not (Get-Command 'docker' -ErrorAction SilentlyContinue)) {
    foreach ($p in @(
        "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe",
        "${env:ProgramFiles(x86)}\Docker\Docker\resources\bin\docker.exe"
    )) {
        if (Test-Path $p) { $script:DockerExe = $p; break }
    }
}

function Invoke-Docker {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$CliArgs)
    & $script:DockerExe @CliArgs
    return $LASTEXITCODE
}

function Test-DockerDaemon {
    try {
        & $script:DockerExe info *>$null
        return ($LASTEXITCODE -eq 0)
    } catch { return $false }
}

<#
  等 Docker engine 就绪——但绝不主动启动 Docker Desktop（用户手动开，
  避免脚本抢跑拉起 WSL2/虚拟机导致整机卡顿）。就绪返回 $true，
  超时返回 $false。
#>
function Wait-DockerDaemon {
    param([int]$TimeoutSec = 300)

    if (Test-DockerDaemon) { Write-Good 'Docker engine 已就绪'; return $true }

    Write-WarnMsg 'Docker engine 未运行 —— 请你现在手动启动 Docker Desktop'
    Write-WarnMsg "（开始菜单 / 桌面图标，图标变绿即就绪。脚本每 5 秒探测一次，最多等 ${TimeoutSec}s）"
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $waited = 0
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        $waited += 5
        if (Test-DockerDaemon) { Write-Good "Docker engine 就绪（等待 ${waited}s）"; return $true }
        if ($waited % 30 -eq 0) { Write-Info "仍在等 Docker engine ... ${waited}s" }
    }
    Write-WarnMsg "等了 ${TimeoutSec}s Docker 还没就绪。等你启动好后重新跑一次脚本即可。"
    return $false
}

function Wait-Gateway {
    param([int]$TimeoutSec = 150)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $waited = 0
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $HealthUrl -UseBasicParsing -TimeoutSec 3
            if ($resp.StatusCode -eq 200) {
                Write-Good "网关健康（等待 ${waited}s）：$HealthUrl"
                return $true
            }
        } catch { <# 还没起来，继续轮询 #> }
        Start-Sleep -Seconds 3
        $waited += 3
        if ($waited % 15 -eq 0) { Write-Info "等待网关就绪 ... ${waited}s" }
    }
    Write-WarnMsg "网关 ${TimeoutSec}s 内没通过健康检查，继续跑，但页面可能暂时 500。"
    Write-WarnMsg '排查：docker compose ps / docker compose logs -f api-gateway'
    return $false
}

<#
  在独立窗口里起 vite dev server。
  优先 Windows Terminal（体验好、标签页），没有就用经典 cmd 窗口。
  命令里带 node_modules 判断：没装依赖自动 npm install 再 dev。
#>
function Start-DevServer {
    param(
        [string]$WorkDir,
        [string]$Label
    )
    if (-not (Test-Path $WorkDir)) { Write-WarnMsg "目录不存在，跳过：$WorkDir"; return }

    # 用 cmd 的 & 顺序执行（PS 5.1 源码里不能出现 &&，会被解析器当成语句分隔符报错）。
    $devCmd = '(if not exist node_modules npm install) & npm run dev'
    $cmdLine = '/k cd /d "' + $WorkDir + '" & ' + $devCmd

    $useWt = $null -ne (Get-Command 'wt.exe' -ErrorAction SilentlyContinue)
    try {
        if ($useWt) {
            Start-Process -FilePath 'wt.exe' -ArgumentList @('-d', $WorkDir, 'cmd', '/k', $devCmd) | Out-Null
            Write-Good "$Label 已在新 Windows Terminal 标签页启动（$WorkDir）"
        } else {
            Start-Process -FilePath 'cmd.exe' -ArgumentList $cmdLine | Out-Null
            Write-Good "$Label 已在新 cmd 窗口启动（$WorkDir）"
        }
    } catch {
        Write-WarnMsg "$Label 启动失败：$($_.Exception.Message)"
    }
}

# ================================================================ 主流程
Write-Host ''
Write-Host '  gosk 开发环境一键启动' -ForegroundColor Magenta
Write-Host "  项目根目录：$Root" -ForegroundColor DarkGray

# 0) .env 缺失就从模板复制一份（compose 里有默认值兜底，但显式存在更可控）
if (-not $SkipDocker) {
    $envFile = Join-Path $Root '.env'
    $envExample = Join-Path $Root '.env.example'
    if (-not (Test-Path $envFile) -and (Test-Path $envExample)) {
        Copy-Item $envExample $envFile
        Write-Good '.env 不存在，已从 .env.example 复制'
    }

    Write-Step '检查 Docker 环境（Docker Desktop 由你手动启动）'
    if (-not (Wait-DockerDaemon)) { exit 1 }

    Write-Step 'docker compose up -d'
    $composeArgs = @('compose', 'up', '-d')
    if ($Build) { $composeArgs += '--build' }
    Push-Location $Root
    try {
        $code = Invoke-Docker @composeArgs
        if ($code -ne 0) { throw "docker compose up 失败（exit=$code）" }
    } finally { Pop-Location }
    Write-Good '容器编排命令执行完成'

    Write-Step '等待网关健康检查'
    Wait-Gateway | Out-Null

    if ($Seed) {
        Write-Step '灌入测试数据（seed）'
        Push-Location $Root
        try { Invoke-Docker compose run --rm seed | Out-Null }
        finally { Pop-Location }
        Write-Good 'seed 执行完成（10 用户 + 20 商品，密码均为 123456）'
    }
} else {
    Write-WarnMsg '-SkipDocker：跳过 Docker 与 compose 环节'
}

if (-not $NoFrontend) {
    Write-Step '启动两个前端 dev server'
    Start-DevServer -WorkDir $AdminDir -Label '管理台(5173)'
    Start-DevServer -WorkDir $ShopDir  -Label 'C端商城(5174)'

    if (-not $NoBrowser) {
        Start-Sleep -Seconds 6   # 给 vite 冷启动留点时间
        Write-Step '打开浏览器'
        Start-Process 'http://localhost:5173' | Out-Null
        Start-Process 'http://localhost:5174' | Out-Null
        Write-Good '已打开 5173 / 5174'
    }
}

Write-Host ''
Write-Host '---------------------------------------------- 访问地址' -ForegroundColor Cyan
Write-Host '  管理台        http://localhost:5173'
Write-Host '  C 端商城      http://localhost:5174'
Write-Host '  网关 API      http://localhost:8080   (healthz)'
Write-Host '  Jaeger UI     http://localhost:16686'
Write-Host '  Grafana       http://localhost:3000   (admin/admin)'
Write-Host '  Prometheus    http://localhost:9090'
Write-Host '  RocketMQ 控制台 http://localhost:18080 (需 make mq-dashboard)'
Write-Host '----------------------------------------------'
Write-Host '  停止后端：scripts\dev-down.bat  （前端窗口里 Ctrl+C 即可）' -ForegroundColor DarkGray
Write-Host ''
