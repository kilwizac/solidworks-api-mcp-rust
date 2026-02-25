use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use sw_core::{
    compute_corpus_fingerprint, extract_example_mapping, read_index_artifact, read_json,
    require_directory, write_index_artifact, BuiltIndex, DocsetStats, RootIndex, SearchDocument,
    SearchIndex, INDEX_SCHEMA_VERSION,
};

#[derive(Debug, Parser)]
#[command(name = "sw-indexer", about = "Build and validate SolidWorks MCP binary index artifacts")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Build {
        #[arg(long, default_value = "solidworks-api")]
        input: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        meta: Option<PathBuf>,
    },
    Validate {
        #[arg(long)]
        index: PathBuf,
    },
}

fn default_output(input: &Path) -> PathBuf {
    input.join("index-v2.swidx")
}

fn default_meta(input: &Path) -> PathBuf {
    input.join("index-v2.meta.json")
}

fn load_search(path: &Path) -> Result<SearchIndex> {
    read_json(path).with_context(|| format!("failed to load search index: {}", path.display()))
}

fn load_root(path: &Path) -> Result<RootIndex> {
    read_json(path).with_context(|| format!("failed to load root index: {}", path.display()))
}

fn collect_progguide_titles(search: &SearchIndex) -> BTreeMap<String, SearchDocument> {
    let mut titles = BTreeMap::new();
    for doc in search.documents.as_deref().unwrap_or(&[]) {
        if doc.docset.as_deref() != Some("progguide") {
            continue;
        }
        if let Some(title) = doc.title.as_deref() {
            if !title.is_empty() {
                titles.insert(title.to_string(), doc.clone());
            }
        }
    }
    titles
}

fn collect_examples_map(input: &Path) -> Result<BTreeMap<String, Vec<String>>> {
    let mapping_path = input
        .join("json")
        .join("sldworksapi")
        .join("patterns")
        .join("examples-to-members.json");

    if !mapping_path.exists() {
        return Ok(BTreeMap::new());
    }

    let value: Value = read_json(&mapping_path)
        .with_context(|| format!("failed to load examples mapping: {}", mapping_path.display()))?;

    let lines = value
        .get("body")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(extract_example_mapping(&lines))
}

fn collect_docset_stats(root: &RootIndex, search: &SearchIndex) -> BTreeMap<String, DocsetStats> {
    let mut stats: BTreeMap<String, DocsetStats> = BTreeMap::new();

    if let Some(docsets) = root.docsets.as_ref() {
        for (docset_name, docset) in docsets {
            let entry = stats.entry(docset_name.clone()).or_default();
            entry.interface_count = docset
                .interfaces
                .as_ref()
                .map(|interfaces| interfaces.len() as u64)
                .unwrap_or_default();
            entry.enum_count = docset
                .enums
                .as_ref()
                .map(|enums| enums.len() as u64)
                .unwrap_or_default();
        }
    }

    for doc in search.documents.as_deref().unwrap_or(&[]) {
        let docset = doc.docset.clone().unwrap_or_default();
        let entry = stats.entry(docset).or_default();
        entry.doc_count += 1;
    }

    stats
}

fn build(input: PathBuf, output: Option<PathBuf>, meta: Option<PathBuf>) -> Result<()> {
    require_directory(&input, "input corpus")?;
    require_directory(&input.join("json"), "input corpus json directory")?;

    let output = output.unwrap_or_else(|| default_output(&input));
    let meta = meta.unwrap_or_else(|| default_meta(&input));

    let root = load_root(&input.join("json").join("_index.json"))?;
    let search = load_search(&input.join("json").join("_search_index.json"))?;

    let examples_map = collect_examples_map(&input)?;
    let progguide_titles = collect_progguide_titles(&search);
    let docset_stats = collect_docset_stats(&root, &search);
    let fingerprint = compute_corpus_fingerprint(&input)?;

    let built = BuiltIndex::new(
        fingerprint.clone(),
        docset_stats,
        root,
        search,
        examples_map,
        progguide_titles,
    );

    write_index_artifact(&output, &built)?;

    if let Some(parent) = meta.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create metadata directory: {}", parent.display()))?;
    }

    let output_text = output.to_string_lossy().to_string();
    let metadata = json!({
        "schema_version": INDEX_SCHEMA_VERSION,
        "generated_at": built.generated_at,
        "corpus_fingerprint": fingerprint,
        "artifact": output_text,
        "docsets": built.docset_stats,
    });
    fs::write(&meta, serde_json::to_vec_pretty(&metadata).context("failed to serialize metadata")?)
        .with_context(|| format!("failed to write metadata file: {}", meta.display()))?;

    println!("Index written: {}", output.display());
    println!("Metadata written: {}", meta.display());
    Ok(())
}

fn validate(index_path: PathBuf) -> Result<()> {
    let index = read_index_artifact(&index_path)?;

    let doc_count = index
        .search_index
        .documents
        .as_ref()
        .map(|entries| entries.len())
        .unwrap_or(0);

    if doc_count == 0 {
        anyhow::bail!("index appears incomplete (docs: {})", doc_count);
    }

    println!("Index OK: {}", index_path.display());
    println!("Schema: {}", index.schema_version);
    println!("Generated: {}", index.generated_at);
    println!("Fingerprint: {}", index.corpus_fingerprint);
    println!("Docs: {}", doc_count);
    println!("Examples map keys: {}", index.examples_map.len());
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Build {
            input,
            output,
            meta,
        } => build(input, output, meta),
        Command::Validate { index } => validate(index),
    }
}
