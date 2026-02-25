use anyhow::{Context, Result};
use std::fs;
use std::io::{self, BufRead, Write};

fn main() -> Result<()> {
    let root = sw_mcp_server::default_data_root();
    if !root.exists() || !root.is_dir() {
        eprintln!("Data root not found: {}", root.display());
        std::process::exit(1);
    }

    let index_path = root.join("index-v2.swidx");
    if !index_path.exists() {
        eprintln!(
            "Index artifact not found: {}\nRun: cargo run -p sw-indexer -- build --input \"{}\"",
            index_path.display(),
            root.display()
        );
        std::process::exit(1);
    }

    let _ = fs::metadata(&index_path)
        .with_context(|| format!("failed to access index artifact: {}", index_path.display()))?;

    let store = sw_mcp_server::load_store_from_root(&root)?;
    let server = sw_mcp_server::MCPServer::new(store);

    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => continue,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(response) = server.handle_line(trimmed) {
            let serialized = serde_json::to_string(&response)
                .context("failed to serialize JSON-RPC response")?;
            writeln!(stdout, "{}", serialized)?;
            stdout.flush()?;
        }
    }

    Ok(())
}
