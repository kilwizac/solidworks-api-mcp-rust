use anyhow::{anyhow, bail, Context, Result};
use lru::LruCache;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub const INDEX_SCHEMA_VERSION: u32 = 2;
const INDEX_MAGIC: &[u8; 8] = b"SWIDXV2\0";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchDocument {
    pub title: Option<String>,
    pub summary: Option<String>,
    pub keywords: Option<Vec<String>>,
    pub categories: Option<Vec<String>>,
    #[serde(rename = "interface")]
    pub interface_name: Option<String>,
    #[serde(rename = "type")]
    pub doc_type: Option<String>,
    pub docset: Option<String>,
    pub href: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl SearchDocument {
    pub fn title_str(&self) -> &str {
        self.title.as_deref().unwrap_or("")
    }

    pub fn summary_str(&self) -> &str {
        self.summary.as_deref().unwrap_or("")
    }

    pub fn interface_str(&self) -> &str {
        self.interface_name.as_deref().unwrap_or("")
    }

    pub fn doc_type_str(&self) -> &str {
        self.doc_type.as_deref().unwrap_or("")
    }

    pub fn docset_str(&self) -> &str {
        self.docset.as_deref().unwrap_or("")
    }

    pub fn keywords_slice(&self) -> &[String] {
        self.keywords.as_deref().unwrap_or(&[])
    }

    pub fn categories_slice(&self) -> &[String] {
        self.categories.as_deref().unwrap_or(&[])
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchIndex {
    pub documents: Option<Vec<SearchDocument>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InterfaceIndexEntry {
    pub file: Option<String>,
    pub members: Option<BTreeMap<String, String>>,
    pub member_count: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocsetIndex {
    pub interfaces: Option<BTreeMap<String, InterfaceIndexEntry>>,
    pub enums: Option<BTreeMap<String, String>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RootIndex {
    pub docsets: Option<BTreeMap<String, DocsetIndex>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub score: i32,
    pub doc: SearchDocument,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocsetStats {
    pub doc_count: u64,
    pub interface_count: u64,
    pub enum_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuiltIndex {
    pub schema_version: u32,
    pub generated_at: String,
    pub corpus_fingerprint: String,
    pub docset_stats: BTreeMap<String, DocsetStats>,
    pub root_index: RootIndex,
    pub search_index: SearchIndex,
    pub examples_map: BTreeMap<String, Vec<String>>,
    pub progguide_titles: BTreeMap<String, SearchDocument>,
}

impl BuiltIndex {
    pub fn new(
        corpus_fingerprint: String,
        docset_stats: BTreeMap<String, DocsetStats>,
        root_index: RootIndex,
        search_index: SearchIndex,
        examples_map: BTreeMap<String, Vec<String>>,
        progguide_titles: BTreeMap<String, SearchDocument>,
    ) -> Self {
        Self {
            schema_version: INDEX_SCHEMA_VERSION,
            generated_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
            corpus_fingerprint,
            docset_stats,
            root_index,
            search_index,
            examples_map,
            progguide_titles,
        }
    }
}

fn non_zero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("cache size must be > 0")
}

#[derive(Clone)]
struct PreparedDoc {
    doc: SearchDocument,
    docset: String,
    doc_type: String,
    interface_name: String,
    categories: HashSet<String>,
    hay_title: String,
    hay_summary: String,
    hay_keywords: String,
    hay_categories: String,
    hay_interface: String,
    hay_type: String,
}

#[derive(Clone)]
struct BaseScoredDoc {
    score: i32,
    doc: SearchDocument,
    doc_type: String,
    interface_name: String,
    categories: HashSet<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SearchOptions {
    pub docset: Option<String>,
    pub doc_type: Option<String>,
    pub interface_name: Option<String>,
    pub categories: HashSet<String>,
    pub limit: Option<usize>,
}

pub struct DataStore {
    root: PathBuf,
    index: BuiltIndex,
    prepared_docs: Vec<PreparedDoc>,
    search_base_cache: Mutex<LruCache<String, Vec<BaseScoredDoc>>>,
    search_filtered_cache: Mutex<LruCache<String, Vec<SearchResult>>>,
    member_path_cache: Mutex<LruCache<String, Option<PathBuf>>>,
    interface_path_cache: Mutex<LruCache<String, Option<PathBuf>>>,
    enum_path_cache: Mutex<LruCache<String, Option<PathBuf>>>,
    json_cache: Mutex<LruCache<PathBuf, Value>>,
}

impl DataStore {
    pub fn new(root: impl Into<PathBuf>, index: BuiltIndex) -> Self {
        let prepared_docs = prepare_docs(index.search_index.documents.as_deref().unwrap_or(&[]));

        Self {
            root: root.into(),
            index,
            prepared_docs,
            search_base_cache: Mutex::new(LruCache::new(non_zero(256))),
            search_filtered_cache: Mutex::new(LruCache::new(non_zero(1024))),
            member_path_cache: Mutex::new(LruCache::new(non_zero(4096))),
            interface_path_cache: Mutex::new(LruCache::new(non_zero(4096))),
            enum_path_cache: Mutex::new(LruCache::new(non_zero(4096))),
            json_cache: Mutex::new(LruCache::new(non_zero(256))),
        }
    }

    pub fn index(&self) -> &RootIndex {
        &self.index.root_index
    }

    pub fn examples_map(&self) -> &BTreeMap<String, Vec<String>> {
        &self.index.examples_map
    }

    pub fn progguide_titles(&self) -> &BTreeMap<String, SearchDocument> {
        &self.index.progguide_titles
    }

    fn docset_index(&self, docset: &str) -> Option<&DocsetIndex> {
        self.index
            .root_index
            .docsets
            .as_ref()
            .and_then(|docsets| docsets.get(docset))
    }

    fn full_doc_path(&self, relative_path: Option<&str>) -> Option<PathBuf> {
        let relative_path = relative_path?;
        if relative_path.is_empty() {
            return None;
        }
        Some(self.root.join(relative_path))
    }

    pub fn resolve_member_path(
        &self,
        interface_name: Option<&str>,
        member_name: Option<&str>,
        docset: &str,
    ) -> Option<PathBuf> {
        let cache_key = format!(
            "{}|{}|{}",
            docset,
            interface_name.unwrap_or_default(),
            member_name.unwrap_or_default()
        );

        if let Ok(mut cache) = self.member_path_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                return cached.clone();
            }
        }

        let resolved = self
            .docset_index(docset)
            .and_then(|entry| entry.interfaces.as_ref())
            .and_then(|interfaces| interfaces.get(interface_name.unwrap_or_default()))
            .and_then(|interface_entry| interface_entry.members.as_ref())
            .and_then(|members| members.get(member_name.unwrap_or_default()))
            .and_then(|relative| self.full_doc_path(Some(relative)));

        if let Ok(mut cache) = self.member_path_cache.lock() {
            cache.put(cache_key, resolved.clone());
        }

        resolved
    }

    pub fn resolve_interface_path(&self, interface_name: Option<&str>, docset: &str) -> Option<PathBuf> {
        let cache_key = format!("{}|{}", docset, interface_name.unwrap_or_default());

        if let Ok(mut cache) = self.interface_path_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                return cached.clone();
            }
        }

        let resolved = self
            .docset_index(docset)
            .and_then(|entry| entry.interfaces.as_ref())
            .and_then(|interfaces| interfaces.get(interface_name.unwrap_or_default()))
            .and_then(|interface_entry| interface_entry.file.as_deref())
            .and_then(|relative| self.full_doc_path(Some(relative)));

        if let Ok(mut cache) = self.interface_path_cache.lock() {
            cache.put(cache_key, resolved.clone());
        }

        resolved
    }

    pub fn resolve_enum_path(&self, enum_name: Option<&str>, docset: &str) -> Option<PathBuf> {
        let cache_key = format!("{}|{}", docset, enum_name.unwrap_or_default());

        if let Ok(mut cache) = self.enum_path_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                return cached.clone();
            }
        }

        let resolved = self
            .docset_index(docset)
            .and_then(|entry| entry.enums.as_ref())
            .and_then(|enums| enums.get(enum_name.unwrap_or_default()))
            .and_then(|relative| self.full_doc_path(Some(relative)));

        if let Ok(mut cache) = self.enum_path_cache.lock() {
            cache.put(cache_key, resolved.clone());
        }

        resolved
    }

    fn normalize_query(query: &str) -> String {
        query
            .to_ascii_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn search_base_cached(&self, query_key: &str, docset: Option<&str>) -> Vec<BaseScoredDoc> {
        let cache_key = format!("{}|{}", docset.unwrap_or_default(), query_key);
        if let Ok(mut cache) = self.search_base_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                return cached.clone();
            }
        }

        let tokens = tokenize(Some(query_key));
        if tokens.is_empty() {
            if let Ok(mut cache) = self.search_base_cache.lock() {
                cache.put(cache_key, Vec::new());
            }
            return Vec::new();
        }

        let mut scored = Vec::new();
        for item in &self.prepared_docs {
            if let Some(docset) = docset {
                if item.docset != docset {
                    continue;
                }
            }

            let mut score = 0;
            for token in &tokens {
                if item.hay_title.contains(token) {
                    score += 4;
                }
                if item.hay_keywords.contains(token) {
                    score += 3;
                }
                if item.hay_interface.contains(token) {
                    score += 2;
                }
                if item.hay_summary.contains(token) {
                    score += 1;
                }
                if item.hay_categories.contains(token) {
                    score += 1;
                }
                if item.hay_type.contains(token) {
                    score += 1;
                }
            }

            if score > 0 {
                scored.push(BaseScoredDoc {
                    score,
                    doc: item.doc.clone(),
                    doc_type: item.doc_type.clone(),
                    interface_name: item.interface_name.clone(),
                    categories: item.categories.clone(),
                });
            }
        }

        scored.sort_by(|left, right| right.score.cmp(&left.score));

        if let Ok(mut cache) = self.search_base_cache.lock() {
            cache.put(cache_key, scored.clone());
        }

        scored
    }

    fn search_filtered_cached(&self, query_key: &str, options: &SearchOptions) -> Vec<SearchResult> {
        let mut category_list = options.categories.iter().cloned().collect::<Vec<_>>();
        category_list.sort();

        let cache_key = format!(
            "{}|{}|{}|{}|{}",
            options.docset.as_deref().unwrap_or_default(),
            options.doc_type.as_deref().unwrap_or_default(),
            options.interface_name.as_deref().unwrap_or_default(),
            category_list.join(","),
            query_key,
        );

        if let Ok(mut cache) = self.search_filtered_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                return cached.clone();
            }
        }

        let base = self.search_base_cached(query_key, options.docset.as_deref());
        let mut filtered = Vec::new();
        for item in base {
            if let Some(doc_type) = options.doc_type.as_deref() {
                if item.doc_type != doc_type {
                    continue;
                }
            }
            if let Some(interface_name) = options.interface_name.as_deref() {
                if item.interface_name != interface_name {
                    continue;
                }
            }
            if !options.categories.is_empty()
                && !options.categories.iter().all(|entry| item.categories.contains(entry))
            {
                continue;
            }

            filtered.push(SearchResult {
                score: item.score,
                doc: item.doc,
            });
        }

        if let Ok(mut cache) = self.search_filtered_cache.lock() {
            cache.put(cache_key, filtered.clone());
        }

        filtered
    }

    pub fn search_api_scored(&self, query: &str, options: &SearchOptions) -> Vec<SearchResult> {
        let query_key = Self::normalize_query(query);
        if query_key.is_empty() {
            return Vec::new();
        }

        let mut results = self.search_filtered_cached(&query_key, options);
        match options.limit {
            Some(0) => Vec::new(),
            Some(limit) => {
                if results.len() > limit {
                    results.truncate(limit);
                }
                results
            }
            None => results,
        }
    }

    pub fn read_json_file(&self, path: &Path) -> Result<Value> {
        let normalized = normalize_path(path);

        if let Ok(mut cache) = self.json_cache.lock() {
            if let Some(cached) = cache.get(&normalized) {
                return Ok(cached.clone());
            }
        }

        let data = fs::read_to_string(&normalized)
            .with_context(|| format!("failed to read JSON file: {}", normalized.display()))?;
        let parsed: Value = serde_json::from_str(&data)
            .with_context(|| format!("failed to parse JSON file: {}", normalized.display()))?;

        if let Ok(mut cache) = self.json_cache.lock() {
            cache.put(normalized, parsed.clone());
        }

        Ok(parsed)
    }
}

fn prepare_docs(docs: &[SearchDocument]) -> Vec<PreparedDoc> {
    docs.iter()
        .map(|doc| {
            let categories = doc.categories_slice().iter().cloned().collect::<HashSet<_>>();
            PreparedDoc {
                doc: doc.clone(),
                docset: doc.docset_str().to_string(),
                doc_type: doc.doc_type_str().to_string(),
                interface_name: doc.interface_str().to_string(),
                categories,
                hay_title: doc.title_str().to_ascii_lowercase(),
                hay_summary: doc.summary_str().to_ascii_lowercase(),
                hay_keywords: doc
                    .keywords_slice()
                    .iter()
                    .map(|entry| entry.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" "),
                hay_categories: doc
                    .categories_slice()
                    .iter()
                    .map(|entry| entry.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" "),
                hay_interface: doc.interface_str().to_ascii_lowercase(),
                hay_type: doc.doc_type_str().to_ascii_lowercase(),
            }
        })
        .collect()
}

fn normalize_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

pub fn tokenize(text: Option<&str>) -> Vec<String> {
    let text = match text {
        Some(value) if !value.is_empty() => value,
        _ => return Vec::new(),
    };

    token_regex()
        .find_iter(&text.to_ascii_lowercase())
        .map(|entry| entry.as_str().to_string())
        .collect()
}

pub fn score_doc<'a, I>(doc: &SearchDocument, tokens: I) -> i32
where
    I: IntoIterator<Item = &'a str>,
{
    let hay_title = doc.title_str().to_ascii_lowercase();
    let hay_summary = doc.summary_str().to_ascii_lowercase();
    let hay_keywords = doc
        .keywords_slice()
        .iter()
        .map(|entry| entry.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    let hay_categories = doc
        .categories_slice()
        .iter()
        .map(|entry| entry.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    let hay_interface = doc.interface_str().to_ascii_lowercase();
    let hay_type = doc.doc_type_str().to_ascii_lowercase();

    let mut score = 0;
    for token in tokens {
        if hay_title.contains(token) {
            score += 4;
        }
        if hay_keywords.contains(token) {
            score += 3;
        }
        if hay_interface.contains(token) {
            score += 2;
        }
        if hay_summary.contains(token) {
            score += 1;
        }
        if hay_categories.contains(token) {
            score += 1;
        }
        if hay_type.contains(token) {
            score += 1;
        }
    }
    score
}

pub fn parse_limit(value: Option<&Value>, default_value: Option<usize>) -> Option<usize> {
    let Some(value) = value else {
        return default_value;
    };

    if value.is_null() {
        return default_value;
    }

    let parsed = match value {
        Value::Number(number) => number.as_i64(),
        Value::String(raw) if !raw.trim().is_empty() => raw.trim().parse::<i64>().ok(),
        _ => None,
    };

    let Some(parsed) = parsed else {
        return default_value;
    };

    if parsed < 0 {
        return Some(0);
    }

    usize::try_from(parsed).ok().or(default_value)
}

pub fn read_json<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read JSON file: {}", path.display()))?;
    serde_json::from_str(&data)
        .with_context(|| format!("failed to parse JSON file: {}", path.display()))
}

pub fn write_index_artifact(path: &Path, index: &BuiltIndex) -> Result<()> {
    let payload = bincode::serde::encode_to_vec(index, bincode::config::standard())
        .context("failed to serialize index")?;
    let compressed = zstd::stream::encode_all(Cursor::new(&payload), 10)
        .context("failed to compress index payload")?;

    let mut output = Vec::with_capacity(INDEX_MAGIC.len() + 12 + compressed.len());
    output.extend_from_slice(INDEX_MAGIC);
    output.extend_from_slice(&INDEX_SCHEMA_VERSION.to_le_bytes());
    output.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    output.extend_from_slice(&compressed);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create output directory for index artifact: {}", parent.display())
        })?;
    }

    fs::write(path, output)
        .with_context(|| format!("failed to write index artifact: {}", path.display()))
}

pub fn read_index_artifact(path: &Path) -> Result<BuiltIndex> {
    let bytes = fs::read(path).with_context(|| format!("failed to read index artifact: {}", path.display()))?;
    if bytes.len() < INDEX_MAGIC.len() + 12 {
        bail!("index artifact is too small: {}", path.display());
    }

    let (magic, rest) = bytes.split_at(INDEX_MAGIC.len());
    if magic != INDEX_MAGIC {
        bail!("index artifact magic mismatch: {}", path.display());
    }

    let (schema_bytes, rest) = rest.split_at(4);
    let schema_version = u32::from_le_bytes(schema_bytes.try_into().unwrap_or([0, 0, 0, 0]));
    if schema_version != INDEX_SCHEMA_VERSION {
        bail!(
            "unsupported index schema version {} (expected {})",
            schema_version,
            INDEX_SCHEMA_VERSION
        );
    }

    let (length_bytes, compressed) = rest.split_at(8);
    let expected_len = u64::from_le_bytes(length_bytes.try_into().unwrap_or([0; 8])) as usize;

    let payload = zstd::stream::decode_all(Cursor::new(compressed)).context("failed to decompress index payload")?;
    if payload.len() != expected_len {
        bail!(
            "index payload length mismatch: expected {}, got {}",
            expected_len,
            payload.len()
        );
    }

    let (index, _): (BuiltIndex, usize) =
        bincode::serde::decode_from_slice(&payload, bincode::config::standard())
            .context("failed to deserialize index payload")?;

    if index.schema_version != INDEX_SCHEMA_VERSION {
        bail!(
            "index schema inside payload is {}, expected {}",
            index.schema_version,
            INDEX_SCHEMA_VERSION
        );
    }

    Ok(index)
}

pub fn compute_corpus_fingerprint(root: &Path) -> Result<String> {
    let candidate_files = [
        root.join("json").join("_index.json"),
        root.join("json").join("_search_index.json"),
        root.join("json")
            .join("sldworksapi")
            .join("patterns")
            .join("examples-to-members.json"),
    ];

    let mut hasher = Sha256::new();
    for candidate in candidate_files {
        if !candidate.exists() {
            continue;
        }
        let metadata = fs::metadata(&candidate)
            .with_context(|| format!("failed to read metadata: {}", candidate.display()))?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|value| value.as_secs())
            .unwrap_or_default();

        hasher.update(candidate.to_string_lossy().as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        hasher.update(modified.to_le_bytes());

        let content = fs::read(&candidate)
            .with_context(|| format!("failed to read file for hashing: {}", candidate.display()))?;
        hasher.update(content);
    }

    Ok(hex::encode(hasher.finalize()))
}

pub fn resolve_data_root(env_value: Option<&str>) -> PathBuf {
    if let Some(raw) = env_value {
        if !raw.trim().is_empty() {
            return PathBuf::from(raw).canonicalize().unwrap_or_else(|_| PathBuf::from(raw));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        let candidate = cwd.join("solidworks-api");
        if candidate.is_dir() {
            return candidate.canonicalize().unwrap_or(candidate);
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            let candidate = parent.join("..").join("solidworks-api");
            if candidate.is_dir() {
                return candidate.canonicalize().unwrap_or(candidate);
            }
        }
    }

    PathBuf::from("solidworks-api")
}

pub fn parse_string(value: Option<&Value>) -> Option<Cow<'_, str>> {
    value.and_then(Value::as_str).map(Cow::Borrowed)
}

pub fn token_regex() -> &'static Regex {
    static TOKEN_RE: OnceLock<Regex> = OnceLock::new();
    TOKEN_RE.get_or_init(|| Regex::new(r"[a-z0-9]+").expect("token regex should compile"))
}

pub fn example_member_regex() -> &'static Regex {
    static EXAMPLE_RE: OnceLock<Regex> = OnceLock::new();
    EXAMPLE_RE.get_or_init(|| Regex::new(r"^- `([^`]+)`").expect("example regex should compile"))
}

pub fn extract_example_mapping(body_lines: &[String]) -> BTreeMap<String, Vec<String>> {
    let mut mapping = BTreeMap::new();
    let mut current_title: Option<String> = None;

    for line in body_lines {
        if let Some(stripped) = line.strip_prefix("## ") {
            current_title = Some(stripped.trim().to_string());
            continue;
        }

        let Some(title) = current_title.as_ref() else {
            continue;
        };

        if let Some(captures) = example_member_regex().captures(line) {
            if let Some(member) = captures.get(1).map(|entry| entry.as_str().trim()) {
                if !member.is_empty() {
                    mapping
                        .entry(member.to_string())
                        .or_insert_with(Vec::new)
                        .push(title.clone());
                }
            }
        }
    }

    mapping
}

pub fn as_object(value: Option<&Value>) -> Map<String, Value> {
    value
        .and_then(|entry| entry.as_object().cloned())
        .unwrap_or_default()
}

pub fn parse_categories(value: Option<&Value>) -> HashSet<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|entries| entries.iter())
        .filter_map(|entry| entry.as_str().map(str::to_string))
        .collect()
}

pub fn require_directory(path: &Path, label: &str) -> Result<()> {
    if !path.exists() {
        return Err(anyhow!("{} does not exist: {}", label, path.display()));
    }
    if !path.is_dir() {
        return Err(anyhow!("{} is not a directory: {}", label, path.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basic_words() {
        assert_eq!(tokenize(Some("Hello World")), vec!["hello", "world"]);
    }

    #[test]
    fn tokenize_splits_special_characters() {
        assert_eq!(tokenize(Some("get_feature-data!")), vec!["get", "feature", "data"]);
    }

    #[test]
    fn tokenize_empty_for_nullish() {
        assert!(tokenize(None).is_empty());
        assert!(tokenize(Some("\n\t")).is_empty());
    }

    #[test]
    fn score_doc_matches_weighted_fields() {
        let doc = SearchDocument {
            title: Some("feature".to_string()),
            summary: Some("feature info".to_string()),
            keywords: Some(vec!["feature".to_string()]),
            ..SearchDocument::default()
        };

        let score = score_doc(&doc, ["feature"].iter().copied());
        assert_eq!(score, 8);
    }

    #[test]
    fn parse_limit_string_and_negative() {
        let positive = Value::String("2".to_string());
        let negative = Value::from(-5);

        assert_eq!(parse_limit(Some(&positive), Some(20)), Some(2));
        assert_eq!(parse_limit(Some(&negative), Some(20)), Some(0));
    }

    #[test]
    fn extract_examples_map() {
        let lines = vec![
            "## Example One".to_string(),
            "- `ISldWorks.OpenDoc`".to_string(),
            "## Example Two".to_string(),
            "- `IFoo.Bar`".to_string(),
        ];

        let mapping = extract_example_mapping(&lines);
        assert_eq!(
            mapping.get("ISldWorks.OpenDoc"),
            Some(&vec!["Example One".to_string()])
        );
        assert_eq!(mapping.get("IFoo.Bar"), Some(&vec!["Example Two".to_string()]));
    }
}
