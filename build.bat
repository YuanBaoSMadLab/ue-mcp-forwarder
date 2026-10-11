@echo off
rem 一键构建 ue-mcp-forwarder
cd /d "%~dp0"
set CARGO_INCREMENTAL=0
cargo build --release
if %errorlevel%==0 (
  copy /y "target\release\ue-mcp-forwarder.exe" "..\ue-mcp-forwarder.exe" >nul
  echo 构建完成: E:\offline_UEMcp\ue-mcp-forwarder.exe
) else (
  echo 构建失败，请检查 Rust 工具链
)
pause
