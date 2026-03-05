# SolidWorks API MCP Server (Rust)

Rust MCP server and artifact toolchain for fast, targeted SolidWorks API documentation search and deterministic fetch.

## Runtime Surface

The server exposes two MCP tools:

- `solidworks_query`
- `solidworks_fetch`

Legacy tool names have been removed.

## What Changed

The current artifact format is optimized for plug-and-play installs:

- Build once into `index-v3.swidx`
- Run with either a full corpus root or only the artifact path
- Fetch payloads are embedded in the artifact, so runtime lookups do not need raw JSON files
- Named feature profiles let downstream software target smaller slices of the SolidWorks docs

## Built-In Profiles

`solidworks_query` accepts an optional `profiles` array. Current built-in profiles:

- `assemblies`
- `drawings`
- `sketching`
- `features`
- `documents_file_io`
- `macros_addins`
- `constants_reference`

These are logical overlays on the full corpus, not separate corpora. They improve relevance first and also reduce search work for targeted consumers such as add-ins.

## Prerequisites

- Rust stable (`cargo`, `rustc`)

## Quick Start

1. Build the artifact:

```bash
cargo run -p sw-indexer -- build --input solidworks-api
```

This writes:

- `solidworks-api/index-v3.swidx`
- `solidworks-api/index-v3.meta.json`

2. Start the MCP server with a corpus root:

```bash
cargo run -p sw-mcp-server -- --data-root solidworks-api
```

3. Or start the MCP server with only the built artifact:

```bash
cargo run -p sw-mcp-server -- --index solidworks-api/index-v3.swidx
```

## Install And Launch

Install the server binary from the workspace:

```bash
cargo install --path crates/sw-mcp-server
```

The helper launchers in `bin/` now prefer:

1. `SW_MCP_SERVER_BIN` if you set it
2. Repo-local `target/release` or `target/debug`
3. An installed `sw-mcp-server` on `PATH`

## Runtime Options

`sw-mcp-server` supports:

- `--data-root <path>`: root directory containing the SolidWorks corpus
- `--index <path>`: explicit artifact path for artifact-only installs
- `--default-profile <name>`: default profile to apply when a query does not specify `profiles`

Environment variables:

- `SW_API_DATA_ROOT`
- `SW_MCP_INDEX`
- `SW_MCP_DEFAULT_PROFILES`
- `SW_MCP_SERVER_BIN` for the launcher scripts

Example:

```powershell
$env:SW_MCP_INDEX = "C:\path\to\index-v3.swidx"
$env:SW_MCP_DEFAULT_PROFILES = "assemblies,features"
sw-mcp-server
```

## Querying Targeted Sections

Example `solidworks_query` arguments for an add-in that mostly touches assemblies and features:

```json
{
  "query": "add component mate",
  "profiles": ["assemblies", "features"],
  "types": ["method"],
  "limit": 10
}
```

Example exact symbol lookup:

```json
{
  "query": "IModelDoc2.Save3",
  "profiles": ["documents_file_io"]
}
```

## Validate Artifact

```bash
cargo run -p sw-indexer -- validate --index solidworks-api/index-v3.swidx
```

Validation now checks more than just deserialization. It verifies search rows, exact-match tables, profile metadata, and embedded payload coverage.

## Claude Desktop MCP Configuration

Using a corpus root:

```json
{
  "mcpServers": {
    "solidworks-api": {
      "command": "C:\\path\\to\\sw-mcp-server.exe",
      "args": ["--data-root", "C:\\path\\to\\solidworks-api"],
      "env": {
        "SW_MCP_DEFAULT_PROFILES": "assemblies,features"
      }
    }
  }
}
```

Using only an artifact:

```json
{
  "mcpServers": {
    "solidworks-api": {
      "command": "C:\\path\\to\\sw-mcp-server.exe",
      "args": ["--index", "C:\\path\\to\\index-v3.swidx"]
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
