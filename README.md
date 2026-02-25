# SolidWorks API MCP Server (Rust)

Rust MCP server for fast search and deterministic fetch over the SolidWorks API JSON corpus.

## Runtime Surface

The server exposes two MCP tools:

- `solidworks_query`
- `solidworks_fetch`

Legacy tool names have been removed.

## Prerequisites

- Rust stable (`cargo`, `rustc`)

## Quick Start

1. Build the JSON-only binary index:

```bash
cargo run -p sw-indexer -- build --input solidworks-api
```

2. Start the MCP server:

```bash
cargo run -p sw-mcp-server
```

By default the server reads data from `<repo>/solidworks-api`.
Override with `SW_API_DATA_ROOT`:

```powershell
$env:SW_API_DATA_ROOT = "C:\path\to\solidworks-api"
cargo run -p sw-mcp-server
```

## Validate Index Artifact

```bash
cargo run -p sw-indexer -- validate --index solidworks-api/index-v2.swidx
```

## Claude Desktop MCP Configuration

```json
{
  "mcpServers": {
    "solidworks-api": {
      "command": "C:\\path\\to\\sw-mcp-server.exe"
    }
  }
}
```

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

## Project Structure

```text
solidworks-api-mcp/
|- crates/
|  |- sw-core/
|  |- sw-indexer/
|  `- sw-mcp-server/
|- solidworks-api/
|- bin/
|- Cargo.toml
|- rust-toolchain.toml
`- README.md
```
