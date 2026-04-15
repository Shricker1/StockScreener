param(
    [switch]$SkipInstall
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

Write-Host "[1/3] 项目目录: $projectRoot"

if (-not $SkipInstall) {
    Write-Host "[2/3] 安装/更新依赖..."
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
} else {
    Write-Host "[2/3] 跳过依赖安装（-SkipInstall）"
}

Write-Host "[3/3] 开始打包 EXE..."
python -m PyInstaller --clean --noconfirm streamlit_app.spec

$exePath = Join-Path $projectRoot "dist\StockScreenerAI.exe"
if (Test-Path $exePath) {
    $rootExePath = Join-Path $projectRoot "StockScreenerAI.exe"
    Copy-Item $exePath $rootExePath -Force
    Write-Host ""
    Write-Host "打包完成: $exePath" -ForegroundColor Green
    Write-Host "已复制到项目根目录: $rootExePath" -ForegroundColor Green
    Write-Host "现在可双击根目录下的 EXE 启动项目。"
} else {
    Write-Host ""
    Write-Host "未找到输出 EXE，请检查打包日志。" -ForegroundColor Red
    exit 1
}
