use anyhow::{anyhow, bail, Context, Result};
use lru::LruCache;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub const INDEX_SCHEMA_VERSION: u32 = 3;
pub const INDEX_ARTIFACT_NAME: &str = "index-v3.swidx";
pub const INDEX_METADATA_NAME: &str = "index-v3.meta.json";
const INDEX_MAGIC: &[u8; 8] = b"SWIDXV3\0";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchDocument {
    pub id: Option<String>,
    pub path: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub keywords: Option<Vec<String>>,
    pub categories: Option<Vec<String>>,
    #[serde(rename = "interface")]
    pub interface_name: Option<String>,
    #[serde(rename = "type")]
    pub doc_type: Option<String>,
    pub docset: Option<String>,
    pub parameters: Option<Vec<String>>,
    pub returns: Option<String>,
    pub href: Option<String>,
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct InterfaceIndexEntry {
    pub file: Option<String>,
    pub members: Option<BTreeMap<String, String>>,
    pub member_count: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocsetIndex {
    pub interfaces: Option<BTreeMap<String, InterfaceIndexEntry>>,
    pub enums: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RootIndex {
    pub docsets: Option<BTreeMap<String, DocsetIndex>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub doc_id: usize,
    pub score: i32,
    pub doc: SearchDocument,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocsetStats {
    pub doc_count: u64,
    pub interface_count: u64,
    pub enum_count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProfileDefinition {
    pub description: String,
    pub docsets: Vec<String>,
    pub categories_any: Vec<String>,
    pub categories_all: Vec<String>,
    pub interfaces: Vec<String>,
    pub types: Vec<String>,
    pub title_terms: Vec<String>,
    pub keyword_terms: Vec<String>,
}

impl ProfileDefinition {
    pub fn matches(&self, doc: &SearchDocument) -> bool {
        let docset = doc.docset_str().to_ascii_lowercase();
        let doc_type = doc.doc_type_str().to_ascii_lowercase();
        let interface_name = doc.interface_str().to_ascii_lowercase();
        let title = doc.title_str().to_ascii_lowercase();
        let keywords = doc
            .keywords_slice()
            .iter()
            .map(|entry| entry.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let categories = doc
            .categories_slice()
            .iter()
            .map(|entry| entry.to_ascii_lowercase())
            .collect::<Vec<_>>();

        if !self.docsets.is_empty() && !self.docsets.iter().any(|entry| entry == &docset) {
            return false;
        }
        if !self.types.is_empty() && !self.types.iter().any(|entry| entry == &doc_type) {
            return false;
        }
        if !self.interfaces.is_empty()
            && !self.interfaces.iter().any(|entry| entry == &interface_name)
        {
            return false;
        }
        if !self.categories_all.is_empty()
            && !self
                .categories_all
                .iter()
                .all(|entry| categories.iter().any(|candidate| candidate == entry))
        {
            return false;
        }
        if !self.categories_any.is_empty()
            && !self
                .categories_any
                .iter()
                .any(|entry| categories.iter().any(|candidate| candidate == entry))
        {
            return false;
        }
        if !self.title_terms.is_empty()
            && !self.title_terms.iter().any(|entry| title.contains(entry))
        {
            return false;
        }
        if !self.keyword_terms.is_empty()
            && !self
                .keyword_terms
                .iter()
                .any(|entry| keywords.iter().any(|candidate| candidate.contains(entry)))
        {
            return false;
        }

        true
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProfileStats {
    pub doc_count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PreparedSearchRow {
    pub doc_id: usize,
    pub docset: String,
    pub doc_type: String,
    pub interface_name: String,
    pub categories: Vec<String>,
    pub hay_title: String,
    pub hay_summary: String,
    pub hay_keywords: String,
    pub hay_categories: String,
    pub hay_interface: String,
    pub hay_type: String,
    pub exact_keys: Vec<String>,
    pub profiles: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SearchBuildAssets {
    pub prepared_search_rows: Vec<PreparedSearchRow>,
    pub exact_lookup: BTreeMap<String, Vec<usize>>,
    pub profile_doc_ids: BTreeMap<String, Vec<usize>>,
    pub profile_stats: BTreeMap<String, ProfileStats>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuiltIndex {
    pub schema_version: u32,
    pub generated_at: String,
    pub corpus_fingerprint: String,
    pub docset_stats: BTreeMap<String, DocsetStats>,
    pub profile_stats: BTreeMap<String, ProfileStats>,
    pub profiles: BTreeMap<String, ProfileDefinition>,
    pub root_index: RootIndex,
    pub search_index: SearchIndex,
    pub prepared_search_rows: Vec<PreparedSearchRow>,
    pub exact_lookup: BTreeMap<String, Vec<usize>>,
    pub profile_doc_ids: BTreeMap<String, Vec<usize>>,
    pub examples_map: BTreeMap<String, Vec<String>>,
    pub progguide_titles: BTreeMap<String, SearchDocument>,
    pub doc_payloads: BTreeMap<String, String>,
}

impl BuiltIndex {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        corpus_fingerprint: String,
        docset_stats: BTreeMap<String, DocsetStats>,
        root_index: RootIndex,
        search_index: SearchIndex,
        examples_map: BTreeMap<String, Vec<String>>,
        progguide_titles: BTreeMap<String, SearchDocument>,
        profiles: BTreeMap<String, ProfileDefinition>,
        search_assets: SearchBuildAssets,
        doc_payloads: BTreeMap<String, String>,
    ) -> Self {
        Self {
            schema_version: INDEX_SCHEMA_VERSION,
            generated_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
            corpus_fingerprint,
            docset_stats,
            profile_stats: search_assets.profile_stats,
            profiles,
            root_index,
            search_index,
            prepared_search_rows: search_assets.prepared_search_rows,
            exact_lookup: search_assets.exact_lookup,
            profile_doc_ids: search_assets.profile_doc_ids,
            examples_map,
            progguide_titles,
            doc_payloads,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SearchOptions {
    pub docset: Option<String>,
    pub doc_type: Option<String>,
    pub interface_name: Option<String>,
    pub categories: HashSet<String>,
    pub profiles: HashSet<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct IndexValidationSummary {
    pub doc_count: usize,
    pub payload_count: usize,
    pub exact_key_count: usize,
    pub profile_count: usize,
}

#[derive(Clone)]
struct BaseScoredDoc {
    doc_id: usize,
    score: i32,
    doc_type: String,
    interface_name: String,
    categories: Vec<String>,
}

pub struct DataStore {
    root: Option<PathBuf>,
    index: BuiltIndex,
    search_base_cache: Mutex<LruCache<String, Vec<BaseScoredDoc>>>,
    search_filtered_cache: Mutex<LruCache<String, Vec<SearchResult>>>,
    json_cache: Mutex<LruCache<PathBuf, Value>>,
}

impl DataStore {
    pub fn new(root: Option<PathBuf>, index: BuiltIndex) -> Self {
        Self {
            root,
            index,
            search_base_cache: Mutex::new(LruCache::new(non_zero(256))),
            search_filtered_cache: Mutex::new(LruCache::new(non_zero(1024))),
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

    pub fn profiles(&self) -> &BTreeMap<String, ProfileDefinition> {
        &self.index.profiles
    }

    pub fn profile_stats(&self) -> &BTreeMap<String, ProfileStats> {
        &self.index.profile_stats
    }

    pub fn doc_profiles(&self, doc_id: usize) -> Vec<String> {
        self.search_row(doc_id)
            .map(|entry| entry.profiles.clone())
            .unwrap_or_default()
    }

    pub fn search_document(&self, doc_id: usize) -> Option<&SearchDocument> {
        self.index
            .search_index
            .documents
            .as_ref()
            .and_then(|documents| documents.get(doc_id))
    }

    fn search_row(&self, doc_id: usize) -> Option<&PreparedSearchRow> {
        self.index.prepared_search_rows.get(doc_id)
    }

    fn docset_index(&self, docset: &str) -> Option<&DocsetIndex> {
        self.index
            .root_index
            .docsets
            .as_ref()
            .and_then(|docsets| lookup_case_insensitive(docsets, docset).map(|(_, entry)| entry))
    }

    fn full_doc_path(&self, payload_ref: &str) -> Option<PathBuf> {
        self.root.as_ref().map(|root| root.join(payload_ref))
    }

    pub fn resolve_member_ref(
        &self,
        interface_name: Option<&str>,
        member_name: Option<&str>,
        docset: &str,
    ) -> Option<String> {
        let interface_name = interface_name?;
        let member_name = member_name?;
        let interfaces = self.docset_index(docset)?.interfaces.as_ref()?;
        let (_, interface_entry) = lookup_case_insensitive(interfaces, interface_name)?;
        let members = interface_entry.members.as_ref()?;
        let (_, payload_ref) = lookup_case_insensitive(members, member_name)?;
        Some(payload_ref.clone())
    }

    pub fn resolve_interface_ref(
        &self,
        interface_name: Option<&str>,
        docset: &str,
    ) -> Option<String> {
        let interface_name = interface_name?;
        let interfaces = self.docset_index(docset)?.interfaces.as_ref()?;
        let (_, interface_entry) = lookup_case_insensitive(interfaces, interface_name)?;
        interface_entry.file.clone()
    }

    pub fn resolve_enum_ref(&self, enum_name: Option<&str>, docset: &str) -> Option<String> {
        let enum_name = enum_name?;
        let enums = self.docset_index(docset)?.enums.as_ref()?;
        let (_, payload_ref) = lookup_case_insensitive(enums, enum_name)?;
        Some(payload_ref.clone())
    }

    pub fn resolve_member_path(
        &self,
        interface_name: Option<&str>,
        member_name: Option<&str>,
        docset: &str,
    ) -> Option<PathBuf> {
        self.resolve_member_ref(interface_name, member_name, docset)
            .and_then(|payload_ref| self.full_doc_path(&payload_ref))
    }

    pub fn resolve_interface_path(
        &self,
        interface_name: Option<&str>,
        docset: &str,
    ) -> Option<PathBuf> {
        self.resolve_interface_ref(interface_name, docset)
            .and_then(|payload_ref| self.full_doc_path(&payload_ref))
    }

    pub fn resolve_enum_path(&self, enum_name: Option<&str>, docset: &str) -> Option<PathBuf> {
        self.resolve_enum_ref(enum_name, docset)
            .and_then(|payload_ref| self.full_doc_path(&payload_ref))
    }

    pub fn fetch_payload(&self, payload_ref: &str) -> Result<Value> {
        if let Some(payload) = self.index.doc_payloads.get(payload_ref) {
            return serde_json::from_str(payload)
                .with_context(|| format!("failed to parse embedded payload: {payload_ref}"));
        }

        let Some(path) = self.full_doc_path(payload_ref) else {
            bail!(
                "payload not available in artifact and no data root is configured: {payload_ref}"
            );
        };

        self.read_json_file(&path)
    }

    fn normalize_query(query: &str) -> String {
        query
            .to_ascii_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn profile_candidates(&self, profiles: &HashSet<String>) -> Option<BTreeSet<usize>> {
        if profiles.is_empty() {
            return None;
        }

        let mut doc_ids = BTreeSet::new();
        for profile in profiles {
            if let Some(ids) = self.index.profile_doc_ids.get(profile) {
                doc_ids.extend(ids.iter().copied());
            }
        }
        Some(doc_ids)
    }

    fn search_base_cached(&self, query_key: &str, options: &SearchOptions) -> Vec<BaseScoredDoc> {
        let cache_key = format!(
            "{}|{}|{}",
            options.docset.as_deref().unwrap_or_default(),
            serialize_string_set(&options.profiles),
            query_key
        );
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

        let candidate_ids = self.profile_candidates(&options.profiles);
        let candidate_iter: Box<dyn Iterator<Item = usize>> = match candidate_ids {
            Some(doc_ids) => Box::new(doc_ids.into_iter()),
            None => Box::new(0..self.index.prepared_search_rows.len()),
        };

        let mut scored = Vec::new();
        for doc_id in candidate_iter {
            let Some(item) = self.search_row(doc_id) else {
                continue;
            };
            if let Some(docset) = options.docset.as_deref() {
                if item.docset != docset {
                    continue;
                }
            }

            let score = score_prepared_row(item, tokens.iter().map(String::as_str));
            if score > 0 {
                scored.push(BaseScoredDoc {
                    doc_id: item.doc_id,
                    score,
                    doc_type: item.doc_type.clone(),
                    interface_name: item.interface_name.clone(),
                    categories: item.categories.clone(),
                });
            }
        }

        scored.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then(left.doc_id.cmp(&right.doc_id))
        });

        if let Ok(mut cache) = self.search_base_cache.lock() {
            cache.put(cache_key, scored.clone());
        }

        scored
    }

    fn search_filtered_cached(
        &self,
        query_key: &str,
        options: &SearchOptions,
    ) -> Vec<SearchResult> {
        let mut category_list = options.categories.iter().cloned().collect::<Vec<_>>();
        category_list.sort();

        let cache_key = format!(
            "{}|{}|{}|{}|{}|{}",
            options.docset.as_deref().unwrap_or_default(),
            options.doc_type.as_deref().unwrap_or_default(),
            options.interface_name.as_deref().unwrap_or_default(),
            category_list.join(","),
            serialize_string_set(&options.profiles),
            query_key,
        );

        if let Ok(mut cache) = self.search_filtered_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                return cached.clone();
            }
        }

        let base = self.search_base_cached(query_key, options);
        let mut filtered = Vec::new();
        for item in base {
            if let Some(doc_type) = options.doc_type.as_deref() {
                if item.doc_type != doc_type {
                    continue;
                }
            }
            if let Some(interface_name) = options.interface_name.as_deref() {
                if !item.interface_name.eq_ignore_ascii_case(interface_name) {
                    continue;
                }
            }
            if !options.categories.is_empty()
                && !options
                    .categories
                    .iter()
                    .all(|entry| item.categories.iter().any(|candidate| candidate == entry))
            {
                continue;
            }

            let Some(doc) = self.search_document(item.doc_id) else {
                continue;
            };
            filtered.push(SearchResult {
                doc_id: item.doc_id,
                score: item.score,
                doc: doc.clone(),
            });
        }

        if let Ok(mut cache) = self.search_filtered_cache.lock() {
            cache.put(cache_key, filtered.clone());
        }

        filtered
    }

    fn exact_search(&self, query: &str, options: &SearchOptions) -> Vec<SearchResult> {
        let mut doc_ids = BTreeSet::new();
        for key in exact_query_keys(query) {
            if let Some(ids) = self.index.exact_lookup.get(&key) {
                doc_ids.extend(ids.iter().copied());
            }
        }

        let mut results = Vec::new();
        for doc_id in doc_ids {
            let Some(row) = self.search_row(doc_id) else {
                continue;
            };
            if !matches_row_filters(row, options) {
                continue;
            }
            let Some(doc) = self.search_document(doc_id) else {
                continue;
            };

            results.push(SearchResult {
                doc_id,
                score: 10_000,
                doc: doc.clone(),
            });
        }

        results
    }

    pub fn search_api_scored(&self, query: &str, options: &SearchOptions) -> Vec<SearchResult> {
        let query_key = Self::normalize_query(query);
        if query_key.is_empty() {
            return Vec::new();
        }

        let exact_results = self.exact_search(query, options);
        if !exact_results.is_empty() && looks_like_exact_symbol(query) {
            return apply_limit(exact_results, options.limit);
        }

        let fuzzy_results = self.search_filtered_cached(&query_key, options);
        if exact_results.is_empty() {
            return apply_limit(fuzzy_results, options.limit);
        }

        let mut merged = Vec::new();
        let mut seen_doc_ids = HashSet::new();

        for entry in exact_results.into_iter().chain(fuzzy_results.into_iter()) {
            if seen_doc_ids.insert(entry.doc_id) {
                merged.push(entry);
            }
        }

        apply_limit(merged, options.limit)
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

fn non_zero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("cache size must be > 0")
}

fn normalize_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn apply_limit(mut results: Vec<SearchResult>, limit: Option<usize>) -> Vec<SearchResult> {
    match limit {
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

fn serialize_string_set(values: &HashSet<String>) -> String {
    let mut ordered = values.iter().cloned().collect::<Vec<_>>();
    ordered.sort();
    ordered.join(",")
}

fn score_fields<'a, I>(
    tokens: I,
    hay_title: &str,
    hay_summary: &str,
    hay_keywords: &str,
    hay_categories: &str,
    hay_interface: &str,
    hay_type: &str,
) -> i32
where
    I: IntoIterator<Item = &'a str>,
{
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

fn score_prepared_row<'a, I>(row: &PreparedSearchRow, tokens: I) -> i32
where
    I: IntoIterator<Item = &'a str>,
{
    score_fields(
        tokens,
        &row.hay_title,
        &row.hay_summary,
        &row.hay_keywords,
        &row.hay_categories,
        &row.hay_interface,
        &row.hay_type,
    )
}

fn normalize_exact_key(value: &str) -> String {
    value.trim().replace("::", ".").to_ascii_lowercase()
}

fn exact_query_keys(query: &str) -> Vec<String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let mut keys = BTreeSet::new();
    keys.insert(normalize_exact_key(trimmed));
    if trimmed.contains("::") {
        keys.insert(normalize_exact_key(&trimmed.replace("::", ".")));
    }
    keys.into_iter().collect()
}

fn doc_exact_keys(doc: &SearchDocument) -> Vec<String> {
    let mut keys = BTreeSet::new();
    if let Some(id) = doc.id.as_deref() {
        keys.insert(normalize_exact_key(id));
    }

    let title = doc.title_str().trim();
    let interface_name = doc.interface_str().trim();
    if !title.is_empty() {
        keys.insert(normalize_exact_key(title));
    }
    if !interface_name.is_empty() {
        keys.insert(normalize_exact_key(interface_name));
        if !title.is_empty() {
            keys.insert(normalize_exact_key(&format!("{interface_name}.{title}")));
        }
    }

    keys.into_iter().collect()
}

fn looks_like_exact_symbol(query: &str) -> bool {
    let trimmed = query.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return false;
    }

    trimmed.contains('.')
        || trimmed.contains("::")
        || trimmed.ends_with("_e")
        || (trimmed.starts_with('I')
            && trimmed
                .chars()
                .nth(1)
                .map(|entry| entry.is_ascii_uppercase())
                .unwrap_or(false))
}

fn matches_row_filters(row: &PreparedSearchRow, options: &SearchOptions) -> bool {
    if let Some(docset) = options.docset.as_deref() {
        if row.docset != docset {
            return false;
        }
    }
    if let Some(doc_type) = options.doc_type.as_deref() {
        if row.doc_type != doc_type {
            return false;
        }
    }
    if let Some(interface_name) = options.interface_name.as_deref() {
        if !row.interface_name.eq_ignore_ascii_case(interface_name) {
            return false;
        }
    }
    if !options.categories.is_empty()
        && !options
            .categories
            .iter()
            .all(|entry| row.categories.iter().any(|candidate| candidate == entry))
    {
        return false;
    }
    if !options.profiles.is_empty()
        && !row
            .profiles
            .iter()
            .any(|profile| options.profiles.contains(profile))
    {
        return false;
    }

    true
}

fn lookup_case_insensitive<'a, T>(
    values: &'a BTreeMap<String, T>,
    key: &str,
) -> Option<(&'a String, &'a T)> {
    values.get_key_value(key).or_else(|| {
        values
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
    })
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|entry| (*entry).to_string()).collect()
}

pub fn default_profile_catalog() -> BTreeMap<String, ProfileDefinition> {
    BTreeMap::from([
        (
            "assemblies".to_string(),
            ProfileDefinition {
                description: "Assembly-focused interfaces, members, and related search results."
                    .to_string(),
                docsets: strings(&["sldworksapi"]),
                categories_any: strings(&["assemblies"]),
                ..ProfileDefinition::default()
            },
        ),
        (
            "drawings".to_string(),
            ProfileDefinition {
                description: "Drawing documents, annotations, sheets, and related APIs."
                    .to_string(),
                docsets: strings(&["sldworksapi"]),
                categories_any: strings(&["drawings"]),
                ..ProfileDefinition::default()
            },
        ),
        (
            "sketching".to_string(),
            ProfileDefinition {
                description: "Sketch creation and sketch editing APIs.".to_string(),
                docsets: strings(&["sldworksapi"]),
                categories_any: strings(&["sketches"]),
                ..ProfileDefinition::default()
            },
        ),
        (
            "features".to_string(),
            ProfileDefinition {
                description: "Feature creation, definition, and feature-data APIs.".to_string(),
                docsets: strings(&["sldworksapi"]),
                categories_any: strings(&["features"]),
                ..ProfileDefinition::default()
            },
        ),
        (
            "documents_file_io".to_string(),
            ProfileDefinition {
                description: "Document lifecycle and file import/export operations.".to_string(),
                docsets: strings(&["sldworksapi"]),
                categories_any: strings(&["file-io"]),
                ..ProfileDefinition::default()
            },
        ),
        (
            "macros_addins".to_string(),
            ProfileDefinition {
                description: "Guides and examples for macros, add-ins, and standalone automation."
                    .to_string(),
                docsets: strings(&["progguide"]),
                types: strings(&["pattern"]),
                title_terms: strings(&["macro", "add-in", "addin"]),
                keyword_terms: strings(&["macro", "addin", "standalone", "application"]),
                ..ProfileDefinition::default()
            },
        ),
        (
            "constants_reference".to_string(),
            ProfileDefinition {
                description: "SolidWorks constant enums and reference values.".to_string(),
                docsets: strings(&["swconst"]),
                types: strings(&["enum"]),
                ..ProfileDefinition::default()
            },
        ),
    ])
}

pub fn build_search_assets(
    search: &SearchIndex,
    profiles: &BTreeMap<String, ProfileDefinition>,
) -> SearchBuildAssets {
    let documents = search.documents.as_deref().unwrap_or(&[]);
    let mut prepared_search_rows = Vec::with_capacity(documents.len());
    let mut exact_lookup: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut profile_doc_ids = profiles
        .keys()
        .cloned()
        .map(|name| (name, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let mut profile_stats = profiles
        .keys()
        .cloned()
        .map(|name| (name, ProfileStats::default()))
        .collect::<BTreeMap<_, _>>();

    for (doc_id, doc) in documents.iter().enumerate() {
        let categories = doc
            .categories_slice()
            .iter()
            .map(|entry| entry.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let matched_profiles = profiles
            .iter()
            .filter_map(|(name, profile)| profile.matches(doc).then_some(name.clone()))
            .collect::<Vec<_>>();
        let exact_keys = doc_exact_keys(doc);

        for exact_key in &exact_keys {
            exact_lookup
                .entry(exact_key.clone())
                .or_default()
                .push(doc_id);
        }
        for profile_name in &matched_profiles {
            profile_doc_ids
                .entry(profile_name.clone())
                .or_default()
                .push(doc_id);
            if let Some(stats) = profile_stats.get_mut(profile_name) {
                stats.doc_count += 1;
            }
        }

        prepared_search_rows.push(PreparedSearchRow {
            doc_id,
            docset: doc.docset_str().to_ascii_lowercase(),
            doc_type: doc.doc_type_str().to_ascii_lowercase(),
            interface_name: doc.interface_str().to_string(),
            hay_title: doc.title_str().to_ascii_lowercase(),
            hay_summary: doc.summary_str().to_ascii_lowercase(),
            hay_keywords: doc
                .keywords_slice()
                .iter()
                .map(|entry| entry.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(" "),
            hay_categories: categories.join(" "),
            hay_interface: doc.interface_str().to_ascii_lowercase(),
            hay_type: doc.doc_type_str().to_ascii_lowercase(),
            categories,
            exact_keys,
            profiles: matched_profiles,
        });
    }

    for entries in exact_lookup.values_mut() {
        entries.sort_unstable();
        entries.dedup();
    }
    for entries in profile_doc_ids.values_mut() {
        entries.sort_unstable();
        entries.dedup();
    }

    SearchBuildAssets {
        prepared_search_rows,
        exact_lookup,
        profile_doc_ids,
        profile_stats,
    }
}

pub fn referenced_payload_refs(root_index: &RootIndex) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();

    if let Some(docsets) = root_index.docsets.as_ref() {
        for docset in docsets.values() {
            if let Some(interfaces) = docset.interfaces.as_ref() {
                for interface in interfaces.values() {
                    if let Some(file) = interface.file.as_deref() {
                        refs.insert(file.to_string());
                    }
                    if let Some(members) = interface.members.as_ref() {
                        refs.extend(members.values().cloned());
                    }
                }
            }
            if let Some(enums) = docset.enums.as_ref() {
                refs.extend(enums.values().cloned());
            }
        }
    }

    refs
}

pub fn load_referenced_payloads(
    root: &Path,
    root_index: &RootIndex,
) -> Result<BTreeMap<String, String>> {
    let mut payloads = BTreeMap::new();
    for payload_ref in referenced_payload_refs(root_index) {
        let path = root.join(&payload_ref);
        let payload = fs::read_to_string(&path)
            .with_context(|| format!("failed to load fetch payload: {}", path.display()))?;
        payloads.insert(payload_ref, payload);
    }

    Ok(payloads)
}

pub fn validate_built_index(index: &BuiltIndex) -> Result<IndexValidationSummary> {
    let documents = index.search_index.documents.as_deref().unwrap_or(&[]);
    if documents.is_empty() {
        bail!("index appears incomplete (docs: 0)");
    }

    if index.prepared_search_rows.len() != documents.len() {
        bail!(
            "prepared search rows mismatch: expected {}, got {}",
            documents.len(),
            index.prepared_search_rows.len()
        );
    }

    let mut seen_doc_ids = HashSet::new();
    for row in &index.prepared_search_rows {
        if row.doc_id >= documents.len() {
            bail!(
                "prepared search row points to missing doc id {}",
                row.doc_id
            );
        }
        if !seen_doc_ids.insert(row.doc_id) {
            bail!("duplicate prepared search row for doc id {}", row.doc_id);
        }
    }

    for (exact_key, doc_ids) in &index.exact_lookup {
        if exact_key.trim().is_empty() {
            bail!("exact lookup contains an empty key");
        }
        for doc_id in doc_ids {
            if *doc_id >= documents.len() {
                bail!("exact lookup points to missing doc id {}", doc_id);
            }
        }
    }

    for (profile_name, doc_ids) in &index.profile_doc_ids {
        if !index.profiles.contains_key(profile_name) {
            bail!("profile doc ids contain unknown profile {profile_name}");
        }
        for doc_id in doc_ids {
            if *doc_id >= documents.len() {
                bail!("profile doc ids point to missing doc id {}", doc_id);
            }
        }
        let expected = doc_ids.len() as u64;
        let actual = index
            .profile_stats
            .get(profile_name)
            .map(|entry| entry.doc_count)
            .unwrap_or_default();
        if expected != actual {
            bail!("profile stats mismatch for {profile_name}: expected {expected}, got {actual}");
        }
    }

    for payload_ref in referenced_payload_refs(&index.root_index) {
        if !index.doc_payloads.contains_key(&payload_ref) {
            bail!("missing embedded payload for {}", payload_ref);
        }
    }

    for titles in index.examples_map.values() {
        for title in titles {
            if !index.progguide_titles.contains_key(title) {
                bail!("examples map references missing progguide title {}", title);
            }
        }
    }

    Ok(IndexValidationSummary {
        doc_count: documents.len(),
        payload_count: index.doc_payloads.len(),
        exact_key_count: index.exact_lookup.len(),
        profile_count: index.profiles.len(),
    })
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

    score_fields(
        tokens,
        &hay_title,
        &hay_summary,
        &hay_keywords,
        &hay_categories,
        &hay_interface,
        &hay_type,
    )
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
            format!(
                "failed to create output directory for index artifact: {}",
                parent.display()
            )
        })?;
    }

    fs::write(path, output)
        .with_context(|| format!("failed to write index artifact: {}", path.display()))
}

pub fn read_index_artifact(path: &Path) -> Result<BuiltIndex> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read index artifact: {}", path.display()))?;
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

    let payload = zstd::stream::decode_all(Cursor::new(compressed))
        .context("failed to decompress index payload")?;
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
            return PathBuf::from(raw)
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from(raw));
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
        .filter_map(|entry| entry.as_str().map(|value| value.to_ascii_lowercase()))
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

    fn search_doc(
        id: &str,
        title: &str,
        interface_name: &str,
        doc_type: &str,
        docset: &str,
        categories: &[&str],
        keywords: &[&str],
    ) -> SearchDocument {
        SearchDocument {
            id: Some(id.to_string()),
            path: Some(format!("json/{id}.json")),
            title: Some(title.to_string()),
            summary: Some(format!("{title} summary")),
            keywords: Some(strings(keywords)),
            categories: Some(strings(categories)),
            interface_name: Some(interface_name.to_string()),
            doc_type: Some(doc_type.to_string()),
            docset: Some(docset.to_string()),
            ..SearchDocument::default()
        }
    }

    fn test_index() -> BuiltIndex {
        let search_index = SearchIndex {
            documents: Some(vec![
                search_doc(
                    "IAssemblyDoc.AddComponent",
                    "AddComponent",
                    "IAssemblyDoc",
                    "method",
                    "sldworksapi",
                    &["assemblies"],
                    &["assembly", "component"],
                ),
                search_doc(
                    "IModelDoc2.Save3",
                    "Save3",
                    "IModelDoc2",
                    "method",
                    "sldworksapi",
                    &["documents", "file-io"],
                    &["save", "document"],
                ),
                SearchDocument {
                    title: Some("Macro Best Practices".to_string()),
                    summary: Some("macro setup".to_string()),
                    keywords: Some(strings(&["macro", "addin"])),
                    categories: Some(strings(&["documents"])),
                    interface_name: Some(String::new()),
                    doc_type: Some("pattern".to_string()),
                    docset: Some("progguide".to_string()),
                    ..SearchDocument::default()
                },
                SearchDocument {
                    title: Some("swAddMateError_e".to_string()),
                    summary: Some("Mate error enum".to_string()),
                    keywords: Some(strings(&["mate", "error"])),
                    categories: Some(strings(&["constants"])),
                    interface_name: Some(String::new()),
                    doc_type: Some("enum".to_string()),
                    docset: Some("swconst".to_string()),
                    ..SearchDocument::default()
                },
            ]),
        };

        let root_index: RootIndex = serde_json::from_value(serde_json::json!({
            "docsets": {
                "sldworksapi": {
                    "interfaces": {
                        "IAssemblyDoc": {
                            "file": "json/sldworksapi/interfaces/IAssemblyDoc/_interface.json",
                            "members": {
                                "AddComponent": "json/sldworksapi/interfaces/IAssemblyDoc/AddComponent.json"
                            }
                        },
                        "IModelDoc2": {
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
        }))
        .unwrap();

        let profiles = default_profile_catalog();
        let search_assets = build_search_assets(&search_index, &profiles);

        BuiltIndex::new(
            "fingerprint".to_string(),
            BTreeMap::new(),
            root_index,
            search_index,
            BTreeMap::new(),
            BTreeMap::new(),
            profiles,
            search_assets,
            BTreeMap::from([
                (
                    "json/sldworksapi/interfaces/IAssemblyDoc/AddComponent.json".to_string(),
                    serde_json::json!({ "related": ["IAssemblyDoc.AddComponent2"] }).to_string(),
                ),
                (
                    "json/sldworksapi/interfaces/IAssemblyDoc/_interface.json".to_string(),
                    serde_json::json!({ "title": "IAssemblyDoc" }).to_string(),
                ),
                (
                    "json/sldworksapi/interfaces/IModelDoc2/Save3.json".to_string(),
                    serde_json::json!({ "examples": [{ "title": "Save File (C#)" }] }).to_string(),
                ),
                (
                    "json/swconst/enums/swAddMateError_e.json".to_string(),
                    serde_json::json!({ "values": [] }).to_string(),
                ),
            ]),
        )
    }

    #[test]
    fn tokenize_basic_words() {
        assert_eq!(tokenize(Some("Hello World")), vec!["hello", "world"]);
    }

    #[test]
    fn tokenize_splits_special_characters() {
        assert_eq!(
            tokenize(Some("get_feature-data!")),
            vec!["get", "feature", "data"]
        );
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
        assert_eq!(
            mapping.get("IFoo.Bar"),
            Some(&vec!["Example Two".to_string()])
        );
    }

    #[test]
    fn default_profiles_match_expected_documents() {
        let search_index = SearchIndex {
            documents: Some(vec![
                search_doc(
                    "IAssemblyDoc.AddComponent",
                    "AddComponent",
                    "IAssemblyDoc",
                    "method",
                    "sldworksapi",
                    &["assemblies"],
                    &["assembly"],
                ),
                SearchDocument {
                    title: Some("Macro Best Practices".to_string()),
                    summary: Some("macro setup".to_string()),
                    keywords: Some(strings(&["macro", "addin"])),
                    categories: Some(strings(&["documents"])),
                    interface_name: Some(String::new()),
                    doc_type: Some("pattern".to_string()),
                    docset: Some("progguide".to_string()),
                    ..SearchDocument::default()
                },
            ]),
        };

        let profiles = default_profile_catalog();
        let assets = build_search_assets(&search_index, &profiles);

        assert_eq!(
            assets
                .profile_stats
                .get("assemblies")
                .map(|entry| entry.doc_count),
            Some(1)
        );
        assert_eq!(
            assets
                .profile_stats
                .get("macros_addins")
                .map(|entry| entry.doc_count),
            Some(1)
        );
    }

    #[test]
    fn search_api_scored_prefers_exact_symbol() {
        let store = DataStore::new(None, test_index());
        let results = store.search_api_scored("IModelDoc2.Save3", &SearchOptions::default());

        assert_eq!(
            results.first().and_then(|entry| entry.doc.title.as_deref()),
            Some("Save3")
        );
        assert!(results.first().map(|entry| entry.score).unwrap_or_default() >= 10_000);
    }

    #[test]
    fn validate_built_index_detects_missing_payloads() {
        let mut index = test_index();
        index.doc_payloads.clear();

        let error = validate_built_index(&index).unwrap_err().to_string();
        assert!(error.contains("missing embedded payload"));
    }
}
