param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Args
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$releaseBin = Join-Path $scriptDir "..\target\release\sw-mcp-server.exe"
$debugBin = Join-Path $scriptDir "..\target\debug\sw-mcp-server.exe"

if ($env:SW_MCP_SERVER_BIN -and (Test-Path $env:SW_MCP_SERVER_BIN)) {
  & $env:SW_MCP_SERVER_BIN @Args
  exit $LASTEXITCODE
}

if (Test-Path $releaseBin) {
  & $releaseBin @Args
  exit $LASTEXITCODE
}

if (Test-Path $debugBin) {
  & $debugBin @Args
  exit $LASTEXITCODE
}

try {
  $command = Get-Command sw-mcp-server -ErrorAction Stop
  & $command.Source @Args
  exit $LASTEXITCODE
} catch {
}

Write-Error "sw-mcp-server binary not found. Set SW_MCP_SERVER_BIN, build in this repo, or install with: cargo install --path crates/sw-mcp-server."
exit 1