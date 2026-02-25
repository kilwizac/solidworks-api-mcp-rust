param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Args
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$releaseBin = Join-Path $scriptDir "..\target\release\sw-mcp-server.exe"
$debugBin = Join-Path $scriptDir "..\target\debug\sw-mcp-server.exe"

if (Test-Path $releaseBin) {
  & $releaseBin @Args
  exit $LASTEXITCODE
}

if (Test-Path $debugBin) {
  & $debugBin @Args
  exit $LASTEXITCODE
}

Write-Error "sw-mcp-server binary not found. Build first with: cargo build -p sw-mcp-server (or --release)."
exit 1