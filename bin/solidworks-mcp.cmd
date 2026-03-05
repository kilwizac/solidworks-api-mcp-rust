@echo off
setlocal
set SCRIPT_DIR=%~dp0
set RELEASE_BIN=%SCRIPT_DIR%..\target\release\sw-mcp-server.exe
set DEBUG_BIN=%SCRIPT_DIR%..\target\debug\sw-mcp-server.exe

if not "%SW_MCP_SERVER_BIN%"=="" (
  if exist "%SW_MCP_SERVER_BIN%" (
    "%SW_MCP_SERVER_BIN%" %*
    exit /b %ERRORLEVEL%
  )
)

if exist "%RELEASE_BIN%" (
  "%RELEASE_BIN%" %*
  exit /b %ERRORLEVEL%
)

if exist "%DEBUG_BIN%" (
  "%DEBUG_BIN%" %*
  exit /b %ERRORLEVEL%
)

where /q sw-mcp-server.exe
if not errorlevel 1 (
  sw-mcp-server.exe %*
  exit /b %ERRORLEVEL%
)

where /q sw-mcp-server
if not errorlevel 1 (
  sw-mcp-server %*
  exit /b %ERRORLEVEL%
)

echo sw-mcp-server binary not found.
echo Set SW_MCP_SERVER_BIN, build in this repo, or install with: cargo install --path crates/sw-mcp-server
exit /b 1