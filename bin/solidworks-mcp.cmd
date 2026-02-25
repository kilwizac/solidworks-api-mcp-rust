@echo off
setlocal
set SCRIPT_DIR=%~dp0
set RELEASE_BIN=%SCRIPT_DIR%..\target\release\sw-mcp-server.exe
set DEBUG_BIN=%SCRIPT_DIR%..\target\debug\sw-mcp-server.exe

if exist "%RELEASE_BIN%" (
  "%RELEASE_BIN%" %*
  exit /b %ERRORLEVEL%
)

if exist "%DEBUG_BIN%" (
  "%DEBUG_BIN%" %*
  exit /b %ERRORLEVEL%
)

echo sw-mcp-server binary not found.
echo Build first with: cargo build -p sw-mcp-server ^(or cargo build --release -p sw-mcp-server^)
exit /b 1