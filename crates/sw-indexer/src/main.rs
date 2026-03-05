use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use sw_core::{
    build_search_assets, compute_corpus_fingerprint, default_profile_catalog,
    extract_example_mapping, load_referenced_payloads, read_index_artifact, read_json,
    require_directory, validate_built_index, write_index_artifact, BuiltIndex, DocsetStats,
    RootIndex, SearchDocument, SearchIndex, INDEX_ARTIFACT_NAME, INDEX_METADATA_NAME,
    INDEX_SCHEMA_VERSION,
};

#[derive(Debug, Parser)]
#[command(
    name = "sw-indexer",
    about = "Build and validate SolidWorks MCP binary index artifacts"
)]
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
    input.join(INDEX_ARTIFACT_NAME)
}

fn default_meta(input: &Path) -> PathBuf {
    input.join(INDEX_METADATA_NAME)
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

    let value: Value = read_json(&mapping_path).with_context(|| {
        format!(
            "failed to load examples mapping: {}",
            mapping_path.display()
        )
    })?;

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

fn profile_metadata(index: &BuiltIndex) -> BTreeMap<String, Value> {
    index
        .profiles
        .iter()
        .map(|(name, profile)| {
            let doc_count = index
                .profile_stats
                .get(name)
                .map(|entry| entry.doc_count)
                .unwrap_or_default();
            (
                name.clone(),
                json!({
                    "description": profile.description,
                    "doc_count": doc_count,
                    "docsets": profile.docsets,
                    "categories_any": profile.categories_any,
                    "categories_all": profile.categories_all,
                    "interfaces": profile.interfaces,
                    "types": profile.types,
                }),
            )
        })
        .collect()
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
    let profiles = default_profile_catalog();
    let search_assets = build_search_assets(&search, &profiles);
    let doc_payloads = load_referenced_payloads(&input, &root)?;

    let built = BuiltIndex::new(
        fingerprint.clone(),
        docset_stats,
        root,
        search,
        examples_map,
        progguide_titles,
        profiles,
        search_assets,
        doc_payloads,
    );
    let summary = validate_built_index(&built)?;

    write_index_artifact(&output, &built)?;

    if let Some(parent) = meta.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create metadata directory: {}", parent.display())
        })?;
    }

    let output_text = output.to_string_lossy().to_string();
    let metadata = json!({
        "schema_version": INDEX_SCHEMA_VERSION,
        "generated_at": built.generated_at,
        "corpus_fingerprint": fingerprint,
        "artifact": output_text,
        "docsets": built.docset_stats,
        "profiles": profile_metadata(&built),
        "payloads": summary.payload_count,
        "exact_keys": summary.exact_key_count,
    });
    fs::write(
        &meta,
        serde_json::to_vec_pretty(&metadata).context("failed to serialize metadata")?,
    )
    .with_context(|| format!("failed to write metadata file: {}", meta.display()))?;

    println!("Index written: {}", output.display());
    println!("Metadata written: {}", meta.display());
    Ok(())
}

fn validate(index_path: PathBuf) -> Result<()> {
    let index = read_index_artifact(&index_path)?;
    let summary = validate_built_index(&index)?;

    println!("Index OK: {}", index_path.display());
    println!("Schema: {}", index.schema_version);
    println!("Generated: {}", index.generated_at);
    println!("Fingerprint: {}", index.corpus_fingerprint);
    println!("Docs: {}", summary.doc_count);
    println!("Profiles: {}", summary.profile_count);
    println!("Embedded payloads: {}", summary.payload_count);
    println!("Exact keys: {}", summary.exact_key_count);
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

#[cfg(test)]
mod tests {
    use super::*;
    use sw_core::INDEX_ARTIFACT_NAME;
    use tempfile::tempdir;

    fn write_json(path: &Path, value: &Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    }

    fn create_test_corpus(root: &Path) {
        write_json(
            &root.join("json/_index.json"),
            &json!({
                "docsets": {
                    "sldworksapi": {
                        "interfaces": {
                            "IModelDoc2": {
                                "file": "json/sldworksapi/interfaces/IModelDoc2/_interface.json",
                                "members": {
                                    "Save3": "json/sldworksapi/interfaces/IModelDoc2/Save3.json"
                                }
                            }
                        }
                    },
                    "swconst": {
                        "enums": {
                            "swAddMateError_e": "json/swconst/enums/swAddMateError_e.json"
                        }
                    }
                }
            }),
        );

        write_json(
            &root.join("json/_search_index.json"),
            &json!({
                "documents": [
                    {
                        "id": "IModelDoc2.Save3",
                        "path": "json/sldworksapi/interfaces/IModelDoc2/Save3.json",
                        "type": "method",
                        "interface": "IModelDoc2",
                        "title": "Save3",
                        "summary": "Saves the current document.",
                        "docset": "sldworksapi",
                        "keywords": ["save", "document", "imodeldoc2"],
                        "categories": ["documents", "file-io"]
                    },
                    {
                        "id": "swAddMateError_e",
                        "path": "json/swconst/enums/swAddMateError_e.json",
                        "type": "enum",
                        "title": "swAddMateError_e",
                        "summary": "Status after adding or editing a mate.",
                        "docset": "swconst",
                        "keywords": ["mate", "error"],
                        "categories": ["constants"]
                    },
                    {
                        "id": "addin-best-practices",
                        "path": "json/progguide/patterns/addin-best-practices.json",
                        "type": "pattern",
                        "title": "Add-in Best Practices",
                        "summary": "Overview for add-ins.",
                        "docset": "progguide",
                        "keywords": ["addin", "application"],
                        "categories": ["documents"]
                    }
                ]
            }),
        );

        write_json(
            &root.join("json/sldworksapi/interfaces/IModelDoc2/_interface.json"),
            &json!({
                "title": "IModelDoc2",
                "description": "Document interface."
            }),
        );
        write_json(
            &root.join("json/sldworksapi/interfaces/IModelDoc2/Save3.json"),
            &json!({
                "title": "IModelDoc2.Save3",
                "description": "Saves the current document.",
                "examples": [{ "title": "Save File (C#)" }]
            }),
        );
        write_json(
            &root.join("json/swconst/enums/swAddMateError_e.json"),
            &json!({
                "title": "swAddMateError_e",
                "values": [{ "member": "swAddMateError_NoError", "value": "1" }]
            }),
        );
        write_json(
            &root.join("json/progguide/patterns/addin-best-practices.json"),
            &json!({
                "title": "Add-in Best Practices",
                "body": []
            }),
        );
        write_json(
            &root.join("json/sldworksapi/patterns/examples-to-members.json"),
            &json!({
                "body": [
                    "## Add-in Best Practices",
                    "- `IModelDoc2.Save3`"
                ]
            }),
        );
    }

    #[test]
    fn build_and_validate_round_trip() {
        let temp = tempdir().unwrap();
        create_test_corpus(temp.path());

        build(temp.path().to_path_buf(), None, None).unwrap();
        let index_path = temp.path().join(INDEX_ARTIFACT_NAME);

        validate(index_path.clone()).unwrap();

        let index = read_index_artifact(&index_path).unwrap();
        assert!(index
            .doc_payloads
            .contains_key("json/sldworksapi/interfaces/IModelDoc2/Save3.json"));
        assert!(index.profiles.contains_key("documents_file_io"));
        assert_eq!(
            index
                .profile_stats
                .get("documents_file_io")
                .map(|entry| entry.doc_count),
            Some(1)
        );
    }
}
