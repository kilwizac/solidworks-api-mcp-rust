use anyhow::{Context, Result};
use clap::Parser;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "sw-mcp-server", about = "SolidWorks API MCP server")]
struct Cli {
    #[arg(long, env = "SW_API_DATA_ROOT")]
    data_root: Option<PathBuf>,

    #[arg(long, env = "SW_MCP_INDEX")]
    index: Option<PathBuf>,

    #[arg(
        long = "default-profile",
        env = "SW_MCP_DEFAULT_PROFILES",
        value_delimiter = ','
    )]
    default_profiles: Vec<String>,
}

fn normalize_path(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let root = cli.data_root.map(normalize_path).or_else(|| {
        let candidate = sw_mcp_server::default_data_root();
        candidate.is_dir().then_some(candidate)
    });

    if let Some(root) = root.as_ref() {
        if !root.exists() || !root.is_dir() {
            eprintln!("Data root not found: {}", root.display());
            std::process::exit(1);
        }
    }

    let index_path = cli.index.map(normalize_path).or_else(|| {
        root.as_ref()
            .map(|root| sw_mcp_server::default_index_path(root))
    });

    let Some(index_path) = index_path else {
        eprintln!(
            "No index artifact was provided.\nUse --index <path> for an artifact-only install, or --data-root <path> to locate {} automatically.",
            sw_core::INDEX_ARTIFACT_NAME
        );
        std::process::exit(1);
    };

    if !index_path.exists() {
        if let Some(root) = root.as_ref() {
            eprintln!(
                "Index artifact not found: {}\nRun: cargo run -p sw-indexer -- build --input \"{}\" --output \"{}\"",
                index_path.display(),
                root.display(),
                index_path.display()
            );
        } else {
            eprintln!(
                "Index artifact not found: {}\nProvide a valid --index path or rebuild the artifact with sw-indexer.",
                index_path.display()
            );
        }
        std::process::exit(1);
    }

    let _ = fs::metadata(&index_path)
        .with_context(|| format!("failed to access index artifact: {}", index_path.display()))?;

    let store = sw_mcp_server::load_store(&index_path, root.as_deref())?;
    let server = sw_mcp_server::MCPServer::with_default_profiles(store, cli.default_profiles)?;

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
