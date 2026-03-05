use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use sw_core::{
    as_object, parse_limit, read_index_artifact, resolve_data_root, DataStore, SearchOptions,
};

const JSONRPC_VERSION: &str = "2.0";
const PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "solidworks-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const SERVER_NOT_INITIALIZED: i64 = -32002;

fn as_args_object(value: Option<&Value>) -> Map<String, Value> {
    as_object(value)
}

fn normalize_non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
}

fn normalize_lowercase_vec(values: Option<Vec<String>>) -> Vec<String> {
    values
        .unwrap_or_default()
        .into_iter()
        .map(|entry| entry.trim().to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn error_response(id: Option<Value>, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

fn tool_response(result: Value, is_error: bool) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string()),
            }
        ],
        "isError": is_error,
    })
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
struct RawQueryArgs {
    query: Option<String>,
    intent: Option<String>,
    docsets: Option<Vec<String>>,
    types: Option<Vec<String>>,
    #[serde(rename = "interface")]
    interface_name: Option<String>,
    member: Option<String>,
    categories: Option<Vec<String>>,
    profiles: Option<Vec<String>>,
    limit: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
struct RawFetchArgs {
    kind: Option<String>,
    docset: Option<String>,
    #[serde(rename = "interface")]
    interface_name: Option<String>,
    member: Option<String>,
    #[serde(rename = "enum")]
    enum_name: Option<String>,
}

#[derive(Debug)]
struct SearchRequest {
    query: String,
    docsets: Vec<String>,
    types: HashSet<String>,
    interface_name: Option<String>,
    categories: HashSet<String>,
    profiles: HashSet<String>,
    limit: Option<usize>,
}

#[derive(Debug)]
struct GuideExamplesRequest {
    query: String,
    profiles: HashSet<String>,
    limit: Option<usize>,
}

#[derive(Debug)]
struct MemberExamplesRequest {
    interface_name: String,
    member_name: String,
    docset: String,
    limit: Option<usize>,
}

#[derive(Debug)]
struct RelatedRequest {
    interface_name: String,
    member_name: String,
    docset: String,
    limit: Option<usize>,
}

#[derive(Debug)]
enum QueryToolRequest {
    Search(SearchRequest),
    GuideExamples(GuideExamplesRequest),
    MemberExamples(MemberExamplesRequest),
    Related(RelatedRequest),
}

#[derive(Debug)]
enum FetchToolRequest {
    Member {
        docset: String,
        interface_name: String,
        member_name: String,
    },
    Interface {
        docset: String,
        interface_name: String,
    },
    Enum {
        docset: String,
        enum_name: String,
    },
}

#[derive(Debug)]
enum ToolCallError {
    InvalidParams(String),
    Execution(String),
}

impl QueryToolRequest {
    fn parse(
        args: &Map<String, Value>,
        known_profiles: &HashSet<String>,
        default_profiles: &HashSet<String>,
    ) -> std::result::Result<Self, ToolCallError> {
        let raw: RawQueryArgs =
            serde_json::from_value(Value::Object(args.clone())).map_err(|error| {
                ToolCallError::InvalidParams(format!("invalid query arguments: {error}"))
            })?;

        let intent = normalize_non_empty(raw.intent).unwrap_or_else(|| "search".to_string());
        let docsets = normalize_lowercase_vec(raw.docsets);
        let categories = normalize_lowercase_vec(raw.categories)
            .into_iter()
            .collect::<HashSet<_>>();
        let profiles = parse_profiles(raw.profiles, known_profiles, default_profiles)?;
        let limit = parse_limit(raw.limit.as_ref(), Some(20));

        match intent.as_str() {
            "search" => Ok(QueryToolRequest::Search(SearchRequest {
                query: require_arg(raw.query, "query is required for search")?,
                docsets,
                types: normalize_lowercase_vec(raw.types).into_iter().collect(),
                interface_name: normalize_non_empty(raw.interface_name),
                categories,
                profiles,
                limit,
            })),
            "examples" => {
                let interface_name = normalize_non_empty(raw.interface_name);
                let member_name = normalize_non_empty(raw.member);
                if interface_name.is_some() || member_name.is_some() {
                    Ok(QueryToolRequest::MemberExamples(MemberExamplesRequest {
                        interface_name: require_arg(
                            interface_name,
                            "interface is required for member examples",
                        )?,
                        member_name: require_arg(
                            member_name,
                            "member is required for member examples",
                        )?,
                        docset: docsets
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "sldworksapi".to_string()),
                        limit,
                    }))
                } else {
                    Ok(QueryToolRequest::GuideExamples(GuideExamplesRequest {
                        query: require_arg(raw.query, "query is required for guide examples")?,
                        profiles,
                        limit,
                    }))
                }
            }
            "related" => Ok(QueryToolRequest::Related(RelatedRequest {
                interface_name: require_arg(
                    raw.interface_name,
                    "interface is required for related lookups",
                )?,
                member_name: require_arg(raw.member, "member is required for related lookups")?,
                docset: docsets
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "sldworksapi".to_string()),
                limit,
            })),
            _ => Err(ToolCallError::InvalidParams(format!(
                "unsupported intent '{}'",
                intent
            ))),
        }
    }
}

impl FetchToolRequest {
    fn parse(args: &Map<String, Value>) -> std::result::Result<Self, ToolCallError> {
        let raw: RawFetchArgs =
            serde_json::from_value(Value::Object(args.clone())).map_err(|error| {
                ToolCallError::InvalidParams(format!("invalid fetch arguments: {error}"))
            })?;
        let kind = require_arg(raw.kind, "kind is required")?;

        match kind.as_str() {
            "member" => Ok(FetchToolRequest::Member {
                docset: normalize_non_empty(raw.docset)
                    .unwrap_or_else(|| "sldworksapi".to_string())
                    .to_ascii_lowercase(),
                interface_name: require_arg(
                    raw.interface_name,
                    "interface is required for member fetches",
                )?,
                member_name: require_arg(raw.member, "member is required for member fetches")?,
            }),
            "interface" => Ok(FetchToolRequest::Interface {
                docset: normalize_non_empty(raw.docset)
                    .unwrap_or_else(|| "sldworksapi".to_string())
                    .to_ascii_lowercase(),
                interface_name: require_arg(
                    raw.interface_name,
                    "interface is required for interface fetches",
                )?,
            }),
            "enum" => Ok(FetchToolRequest::Enum {
                docset: normalize_non_empty(raw.docset)
                    .unwrap_or_else(|| "swconst".to_string())
                    .to_ascii_lowercase(),
                enum_name: require_arg(raw.enum_name, "enum is required for enum fetches")?,
            }),
            _ => Err(ToolCallError::InvalidParams(format!(
                "unsupported kind '{}'",
                kind
            ))),
        }
    }
}

fn require_arg(value: Option<String>, message: &str) -> std::result::Result<String, ToolCallError> {
    normalize_non_empty(value).ok_or_else(|| ToolCallError::InvalidParams(message.to_string()))
}

fn parse_profiles(
    raw_profiles: Option<Vec<String>>,
    known_profiles: &HashSet<String>,
    default_profiles: &HashSet<String>,
) -> std::result::Result<HashSet<String>, ToolCallError> {
    let requested = normalize_lowercase_vec(raw_profiles)
        .into_iter()
        .collect::<HashSet<_>>();
    let profiles = if requested.is_empty() {
        default_profiles.clone()
    } else {
        requested
    };

    let unknown = profiles
        .iter()
        .filter(|profile| !known_profiles.contains(*profile))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(ToolCallError::InvalidParams(format!(
            "unknown profiles: {}",
            unknown.join(", ")
        )));
    }

    Ok(profiles)
}

#[derive(Clone)]
pub struct MCPServer {
    store: Arc<DataStore>,
    initialized: Arc<AtomicBool>,
    default_profiles: HashSet<String>,
}

impl MCPServer {
    pub fn new(store: DataStore) -> Self {
        Self {
            store: Arc::new(store),
            initialized: Arc::new(AtomicBool::new(false)),
            default_profiles: HashSet::new(),
        }
    }

    pub fn with_default_profiles(
        store: DataStore,
        default_profiles: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        let server = Self::new(store);
        let known_profiles = server
            .store
            .profiles()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let normalized = default_profiles
            .into_iter()
            .map(|entry| entry.trim().to_ascii_lowercase())
            .filter(|entry| !entry.is_empty())
            .collect::<HashSet<_>>();

        let unknown = normalized
            .iter()
            .filter(|profile| !known_profiles.contains(*profile))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(anyhow!("unknown default profiles: {}", unknown.join(", ")));
        }

        Ok(Self {
            default_profiles: normalized,
            ..server
        })
    }

    fn initialize_result(&self) -> Value {
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": { "listChanged": false }
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION,
            }
        })
    }

    fn query_input_schema(&self) -> Value {
        let profile_names = self.store.profiles().keys().cloned().collect::<Vec<_>>();

        json!({
            "oneOf": [
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "query": { "type": "string" },
                        "intent": { "type": "string", "enum": ["search"] },
                        "docsets": { "type": "array", "items": { "type": "string" } },
                        "types": { "type": "array", "items": { "type": "string" } },
                        "interface": { "type": "string" },
                        "categories": { "type": "array", "items": { "type": "string" } },
                        "profiles": { "type": "array", "items": { "type": "string", "enum": profile_names.clone() } },
                        "limit": { "type": ["integer", "string"] }
                    },
                    "required": ["query"]
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "intent": { "const": "examples" },
                        "query": { "type": "string" },
                        "profiles": { "type": "array", "items": { "type": "string", "enum": profile_names.clone() } },
                        "limit": { "type": ["integer", "string"] }
                    },
                    "required": ["intent", "query"]
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "intent": { "const": "examples" },
                        "docsets": { "type": "array", "items": { "type": "string" } },
                        "interface": { "type": "string" },
                        "member": { "type": "string" },
                        "limit": { "type": ["integer", "string"] }
                    },
                    "required": ["intent", "interface", "member"]
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "intent": { "const": "related" },
                        "docsets": { "type": "array", "items": { "type": "string" } },
                        "interface": { "type": "string" },
                        "member": { "type": "string" },
                        "limit": { "type": ["integer", "string"] }
                    },
                    "required": ["intent", "interface", "member"]
                }
            ]
        })
    }

    fn fetch_input_schema(&self) -> Value {
        json!({
            "oneOf": [
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "kind": { "const": "member" },
                        "docset": { "type": "string" },
                        "interface": { "type": "string" },
                        "member": { "type": "string" }
                    },
                    "required": ["kind", "interface", "member"]
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "kind": { "const": "interface" },
                        "docset": { "type": "string" },
                        "interface": { "type": "string" }
                    },
                    "required": ["kind", "interface"]
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "kind": { "const": "enum" },
                        "docset": { "type": "string" },
                        "enum": { "type": "string" }
                    },
                    "required": ["kind", "enum"]
                }
            ]
        })
    }

    fn tools_list_result(&self) -> Value {
        json!({
            "tools": [
                {
                    "name": "solidworks_query",
                    "description": "Consolidated search/discovery interface for SolidWorks API docs.",
                    "inputSchema": self.query_input_schema()
                },
                {
                    "name": "solidworks_fetch",
                    "description": "Deterministic fetch of interface/member/enum docs.",
                    "inputSchema": self.fetch_input_schema()
                }
            ]
        })
    }

    fn doc_to_query_row(
        &self,
        score: i32,
        doc: &sw_core::SearchDocument,
        doc_id: Option<usize>,
    ) -> Value {
        let profiles = doc_id
            .map(|entry| self.store.doc_profiles(entry))
            .unwrap_or_default();
        json!({
            "score": score,
            "id": doc.id,
            "title": doc.title,
            "summary": doc.summary,
            "docset": doc.docset,
            "type": doc.doc_type,
            "interface": doc.interface_name,
            "path": doc.path,
            "payload_ref": doc.path,
            "profiles": profiles,
            "doc": doc,
        })
    }

    fn search_to_query_row(&self, result: &sw_core::SearchResult) -> Value {
        self.doc_to_query_row(result.score, &result.doc, Some(result.doc_id))
    }

    fn tool_query(&self, args: &Map<String, Value>) -> std::result::Result<Value, ToolCallError> {
        let known_profiles = self
            .store
            .profiles()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let request = QueryToolRequest::parse(args, &known_profiles, &self.default_profiles)?;

        match request {
            QueryToolRequest::Search(request) => {
                let SearchRequest {
                    query,
                    docsets,
                    types,
                    interface_name,
                    categories,
                    profiles,
                    limit,
                } = request;

                let (docset_filter, post_filter_docsets) = if docsets.len() == 1 {
                    (Some(docsets[0].clone()), HashSet::new())
                } else {
                    (None, docsets.into_iter().collect::<HashSet<_>>())
                };

                let options = SearchOptions {
                    docset: docset_filter,
                    doc_type: None,
                    interface_name,
                    categories,
                    profiles,
                    limit: None,
                };

                let mut results = self.store.search_api_scored(&query, &options);

                if !post_filter_docsets.is_empty() {
                    results.retain(|entry| {
                        post_filter_docsets.contains(&entry.doc.docset_str().to_ascii_lowercase())
                    });
                }

                if !types.is_empty() {
                    results.retain(|entry| {
                        types.contains(&entry.doc.doc_type_str().to_ascii_lowercase())
                    });
                }

                if let Some(limit) = limit {
                    results.truncate(limit);
                }

                Ok(json!({
                    "intent": "search",
                    "results": results.iter().map(|entry| self.search_to_query_row(entry)).collect::<Vec<_>>(),
                }))
            }
            QueryToolRequest::GuideExamples(request) => {
                let GuideExamplesRequest {
                    query,
                    profiles,
                    limit,
                } = request;
                let options = SearchOptions {
                    docset: Some("progguide".to_string()),
                    profiles,
                    limit,
                    ..SearchOptions::default()
                };
                let results = self.store.search_api_scored(&query, &options);

                Ok(json!({
                    "intent": "examples",
                    "results": results.iter().map(|entry| self.search_to_query_row(entry)).collect::<Vec<_>>(),
                }))
            }
            QueryToolRequest::MemberExamples(request) => {
                let MemberExamplesRequest {
                    interface_name,
                    member_name,
                    docset,
                    limit,
                } = request;
                let payload_ref = self
                    .store
                    .resolve_member_ref(Some(&interface_name), Some(&member_name), &docset)
                    .ok_or_else(|| ToolCallError::Execution("Not found".to_string()))?;
                let data = self
                    .store
                    .fetch_payload(&payload_ref)
                    .map_err(|error| ToolCallError::Execution(error.to_string()))?;
                let examples = data
                    .get("examples")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let key = format!("{}.{}", interface_name, member_name);
                let titles = self
                    .store
                    .examples_map()
                    .get(&key)
                    .cloned()
                    .unwrap_or_default();

                let mut related = titles
                    .iter()
                    .filter_map(|title| self.store.progguide_titles().get(title))
                    .map(|doc| self.doc_to_query_row(0, doc, None))
                    .collect::<Vec<_>>();
                if let Some(limit) = limit {
                    related.truncate(limit);
                }

                Ok(json!({
                    "intent": "examples",
                    "docset": docset,
                    "interface": interface_name,
                    "member": member_name,
                    "examples": examples,
                    "results": related,
                }))
            }
            QueryToolRequest::Related(request) => {
                let RelatedRequest {
                    interface_name,
                    member_name,
                    docset,
                    limit,
                } = request;
                let payload_ref = self
                    .store
                    .resolve_member_ref(Some(&interface_name), Some(&member_name), &docset)
                    .ok_or_else(|| ToolCallError::Execution("Not found".to_string()))?;
                let data = self
                    .store
                    .fetch_payload(&payload_ref)
                    .map_err(|error| ToolCallError::Execution(error.to_string()))?;
                let mut related = data
                    .get("related")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if let Some(limit) = limit {
                    related.truncate(limit);
                }

                Ok(json!({
                    "intent": "related",
                    "docset": docset,
                    "interface": interface_name,
                    "member": member_name,
                    "results": related,
                }))
            }
        }
    }

    fn tool_fetch(&self, args: &Map<String, Value>) -> std::result::Result<Value, ToolCallError> {
        let request = FetchToolRequest::parse(args)?;

        let (kind, docset, interface_name, member_name, enum_name, payload_ref) = match request {
            FetchToolRequest::Member {
                docset,
                interface_name,
                member_name,
            } => {
                let payload_ref = self
                    .store
                    .resolve_member_ref(Some(&interface_name), Some(&member_name), &docset)
                    .ok_or_else(|| ToolCallError::Execution("Not found".to_string()))?;
                (
                    "member",
                    docset,
                    Some(interface_name),
                    Some(member_name),
                    None,
                    payload_ref,
                )
            }
            FetchToolRequest::Interface {
                docset,
                interface_name,
            } => {
                let payload_ref = self
                    .store
                    .resolve_interface_ref(Some(&interface_name), &docset)
                    .ok_or_else(|| ToolCallError::Execution("Not found".to_string()))?;
                (
                    "interface",
                    docset,
                    Some(interface_name),
                    None,
                    None,
                    payload_ref,
                )
            }
            FetchToolRequest::Enum { docset, enum_name } => {
                let payload_ref = self
                    .store
                    .resolve_enum_ref(Some(&enum_name), &docset)
                    .ok_or_else(|| ToolCallError::Execution("Not found".to_string()))?;
                ("enum", docset, None, None, Some(enum_name), payload_ref)
            }
        };

        let data = self
            .store
            .fetch_payload(&payload_ref)
            .map_err(|error| ToolCallError::Execution(error.to_string()))?;

        Ok(json!({
            "kind": kind,
            "docset": docset,
            "interface": interface_name,
            "member": member_name,
            "enum": enum_name,
            "path": payload_ref,
            "payload_ref": payload_ref,
            "data": data,
        }))
    }

    pub(crate) fn call_tool(
        &self,
        tool_name: &str,
        args: &Map<String, Value>,
    ) -> Option<std::result::Result<Value, ToolCallError>> {
        Some(match tool_name {
            "solidworks_query" => self.tool_query(args),
            "solidworks_fetch" => self.tool_fetch(args),
            _ => return None,
        })
    }

    pub fn handle_line(&self, line: &str) -> Option<Value> {
        let parsed_value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => return Some(error_response(None, PARSE_ERROR, "Parse error")),
        };
        let parsed: JsonRpcRequest = match serde_json::from_value(parsed_value) {
            Ok(value) => value,
            Err(_) => return Some(error_response(None, INVALID_REQUEST, "Invalid Request")),
        };
        let request_id = parsed.id.clone();

        if let Some(jsonrpc) = parsed.jsonrpc.as_deref() {
            if jsonrpc != JSONRPC_VERSION {
                return Some(error_response(
                    request_id,
                    INVALID_REQUEST,
                    "Invalid Request",
                ));
            }
        }

        let Some(method) = parsed.method else {
            return Some(error_response(
                request_id,
                INVALID_REQUEST,
                "Invalid Request",
            ));
        };

        match method.as_str() {
            "initialize" => {
                self.initialized.store(true, Ordering::SeqCst);
                Some(json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": request_id,
                    "result": self.initialize_result(),
                }))
            }
            "notifications/initialized" => {
                self.initialized.store(true, Ordering::SeqCst);
                None
            }
            "tools/list" => {
                if !self.initialized.load(Ordering::SeqCst) {
                    return Some(error_response(
                        request_id,
                        SERVER_NOT_INITIALIZED,
                        "Server not initialized",
                    ));
                }
                Some(json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": request_id,
                    "result": self.tools_list_result(),
                }))
            }
            "tools/call" => {
                if !self.initialized.load(Ordering::SeqCst) {
                    return Some(error_response(
                        request_id,
                        SERVER_NOT_INITIALIZED,
                        "Server not initialized",
                    ));
                }

                let params = as_args_object(parsed.params.as_ref());
                let Some(tool_name) = params.get("name").and_then(Value::as_str) else {
                    return Some(error_response(
                        request_id,
                        INVALID_PARAMS,
                        "tools/call requires a tool name",
                    ));
                };
                let args = as_args_object(params.get("arguments"));
                let Some(result) = self.call_tool(tool_name, &args) else {
                    return Some(error_response(request_id, METHOD_NOT_FOUND, "Unknown tool"));
                };

                match result {
                    Ok(payload) => Some(json!({
                        "jsonrpc": JSONRPC_VERSION,
                        "id": request_id,
                        "result": tool_response(payload, false),
                    })),
                    Err(ToolCallError::InvalidParams(message)) => {
                        Some(error_response(request_id, INVALID_PARAMS, &message))
                    }
                    Err(ToolCallError::Execution(message)) => Some(json!({
                        "jsonrpc": JSONRPC_VERSION,
                        "id": request_id,
                        "result": tool_response(json!({ "error": message }), true),
                    })),
                }
            }
            _ => {
                if request_id.is_some() {
                    Some(error_response(
                        request_id,
                        METHOD_NOT_FOUND,
                        "Method not found",
                    ))
                } else {
                    None
                }
            }
        }
    }
}

pub fn load_store(index_path: &Path, root: Option<&Path>) -> Result<DataStore> {
    let index = read_index_artifact(index_path)?;
    Ok(DataStore::new(root.map(Path::to_path_buf), index))
}

pub fn load_store_from_root(root: &Path) -> Result<DataStore> {
    load_store(&root.join(sw_core::INDEX_ARTIFACT_NAME), Some(root))
}

pub fn default_data_root() -> PathBuf {
    resolve_data_root(std::env::var("SW_API_DATA_ROOT").ok().as_deref())
}

pub fn default_index_path(root: &Path) -> PathBuf {
    root.join(sw_core::INDEX_ARTIFACT_NAME)
}

pub fn available_profile_names(store: &DataStore) -> Vec<String> {
    store.profiles().keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use sw_core::{
        build_search_assets, default_profile_catalog, BuiltIndex, DocsetStats, RootIndex,
        SearchDocument, SearchIndex,
    };
    use tempfile::tempdir;

    fn test_index() -> BuiltIndex {
        let docs = vec![
            SearchDocument {
                id: Some("IFeatureManager.TargetMatch".to_string()),
                path: Some(
                    "json/sldworksapi/interfaces/IFeatureManager/TargetMatch.json".to_string(),
                ),
                title: Some("Target Match".to_string()),
                summary: Some("Feature creation details".to_string()),
                keywords: Some(vec!["feature".to_string(), "create".to_string()]),
                categories: Some(vec!["features".to_string(), "assemblies".to_string()]),
                interface_name: Some("IFeatureManager".to_string()),
                doc_type: Some("method".to_string()),
                docset: Some("sldworksapi".to_string()),
                ..SearchDocument::default()
            },
            SearchDocument {
                id: Some("IFeatureManager.WrongType".to_string()),
                path: Some(
                    "json/sldworksapi/interfaces/IFeatureManager/WrongType.json".to_string(),
                ),
                title: Some("Wrong Type".to_string()),
                summary: Some("Feature creation details".to_string()),
                keywords: Some(vec!["feature".to_string(), "create".to_string()]),
                categories: Some(vec!["features".to_string()]),
                interface_name: Some("IFeatureManager".to_string()),
                doc_type: Some("property".to_string()),
                docset: Some("sldworksapi".to_string()),
                ..SearchDocument::default()
            },
            SearchDocument {
                title: Some("Macro Guide".to_string()),
                summary: Some("macro setup".to_string()),
                keywords: Some(vec!["macro".to_string(), "addin".to_string()]),
                categories: Some(vec!["documents".to_string()]),
                interface_name: Some("".to_string()),
                doc_type: Some("pattern".to_string()),
                docset: Some("progguide".to_string()),
                ..SearchDocument::default()
            },
            SearchDocument {
                id: Some("IModelDoc2.Save3".to_string()),
                path: Some("json/sldworksapi/interfaces/IModelDoc2/Save3.json".to_string()),
                title: Some("Save3".to_string()),
                summary: Some("Saves the current document.".to_string()),
                keywords: Some(vec!["save".to_string(), "document".to_string()]),
                categories: Some(vec!["documents".to_string(), "file-io".to_string()]),
                interface_name: Some("IModelDoc2".to_string()),
                doc_type: Some("method".to_string()),
                docset: Some("sldworksapi".to_string()),
                ..SearchDocument::default()
            },
        ];

        let root_index = json!({
            "docsets": {
                "sldworksapi": {
                    "interfaces": {
                        "IFoo": {
                            "members": {
                                "Bar": "json/sldworksapi/interfaces/IFoo/members/Bar.json"
                            }
                        },
                        "IModelDoc2": {
                            "members": {
                                "Save3": "json/sldworksapi/interfaces/IModelDoc2/Save3.json"
                            }
                        }
                    },
                    "enums": {
                        "swThing_e": "json/swconst/enums/swThing_e.json"
                    }
                },
                "swconst": {
                    "enums": {
                        "swThing_e": "json/swconst/enums/swThing_e.json"
                    }
                }
            }
        });

        let root_index: RootIndex = serde_json::from_value(root_index).unwrap();
        let search_index = SearchIndex {
            documents: Some(docs),
        };
        let profiles = default_profile_catalog();
        let search_assets = build_search_assets(&search_index, &profiles);

        let mut stats = BTreeMap::new();
        stats.insert("sldworksapi".to_string(), DocsetStats::default());

        BuiltIndex::new(
            "test".to_string(),
            stats,
            root_index,
            search_index,
            BTreeMap::from([(String::from("IFoo.Bar"), vec![String::from("Macro Guide")])]),
            BTreeMap::from([(
                String::from("Macro Guide"),
                SearchDocument {
                    title: Some("Macro Guide".to_string()),
                    docset: Some("progguide".to_string()),
                    ..SearchDocument::default()
                },
            )]),
            profiles,
            search_assets,
            BTreeMap::from([
                (
                    "json/sldworksapi/interfaces/IFoo/members/Bar.json".to_string(),
                    json!({
                        "related": ["A", "B", "C"],
                        "examples": [{ "title": "Primary" }]
                    })
                    .to_string(),
                ),
                (
                    "json/sldworksapi/interfaces/IModelDoc2/Save3.json".to_string(),
                    json!({
                        "examples": [{ "title": "Save File (C#)" }]
                    })
                    .to_string(),
                ),
                (
                    "json/swconst/enums/swThing_e.json".to_string(),
                    json!({
                        "values": [{ "member": "A", "value": "1", "description": "Alpha" }]
                    })
                    .to_string(),
                ),
            ]),
        )
    }

    #[test]
    fn query_filters_combine() {
        let store = DataStore::new(None, test_index());
        let server = MCPServer::new(store);
        server.initialized.store(true, Ordering::SeqCst);

        let payload = server
            .tool_query(&Map::from_iter([
                (
                    "query".to_string(),
                    Value::String("create feature".to_string()),
                ),
                (
                    "docsets".to_string(),
                    Value::Array(vec![Value::String("sldworksapi".to_string())]),
                ),
                (
                    "types".to_string(),
                    Value::Array(vec![Value::String("method".to_string())]),
                ),
                (
                    "interface".to_string(),
                    Value::String("IFeatureManager".to_string()),
                ),
                (
                    "categories".to_string(),
                    Value::Array(vec![Value::String("features".to_string())]),
                ),
            ]))
            .unwrap();

        let results = payload
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["doc"]["title"].as_str(), Some("Target Match"));
    }

    #[test]
    fn related_limit_parses_string() {
        let store = DataStore::new(None, test_index());
        let server = MCPServer::new(store);
        server.initialized.store(true, Ordering::SeqCst);

        let payload = server
            .tool_query(&Map::from_iter([
                ("intent".to_string(), Value::String("related".to_string())),
                ("interface".to_string(), Value::String("IFoo".to_string())),
                ("member".to_string(), Value::String("Bar".to_string())),
                ("limit".to_string(), Value::String("2".to_string())),
            ]))
            .unwrap();

        let related = payload
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        assert_eq!(related.len(), 2);
    }

    #[test]
    fn default_profiles_are_applied_to_queries() {
        let store = DataStore::new(None, test_index());
        let server =
            MCPServer::with_default_profiles(store, ["documents_file_io".to_string()]).unwrap();
        server.initialized.store(true, Ordering::SeqCst);

        let payload = server
            .tool_query(&Map::from_iter([(
                "query".to_string(),
                Value::String("save".to_string()),
            )]))
            .unwrap();

        let results = payload["results"].as_array().cloned().unwrap_or_default();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["doc"]["title"].as_str(), Some("Save3"));
    }

    #[test]
    fn handle_line_reports_parse_errors() {
        let store = DataStore::new(None, test_index());
        let server = MCPServer::new(store);

        let response = server.handle_line("{bad json").unwrap();
        assert_eq!(response["error"]["code"].as_i64(), Some(PARSE_ERROR));
    }

    #[test]
    fn handle_line_marks_tool_execution_errors() {
        let store = DataStore::new(None, test_index());
        let server = MCPServer::new(store);

        server.handle_line(r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#);
        let response = server
            .handle_line(
                r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"solidworks_fetch","arguments":{"kind":"member","interface":"IFoo","member":"Missing"}}}"#,
            )
            .unwrap();

        assert_eq!(response["result"]["isError"].as_bool(), Some(true));
    }

    #[test]
    fn tool_dispatch_only_modern() {
        let store = DataStore::new(None, test_index());
        let server = MCPServer::new(store);

        assert!(server.call_tool("solidworks_query", &Map::new()).is_some());
        assert!(server.call_tool("solidworks_fetch", &Map::new()).is_some());
        assert!(server
            .call_tool("solidworks_lookup_method", &Map::new())
            .is_none());
    }

    #[test]
    fn load_store_works_without_data_root_when_payloads_are_embedded() {
        let temp = tempdir().unwrap();
        let index_path = temp.path().join(sw_core::INDEX_ARTIFACT_NAME);
        sw_core::write_index_artifact(&index_path, &test_index()).unwrap();

        let store = load_store(&index_path, None).unwrap();
        let payload = store
            .fetch_payload("json/sldworksapi/interfaces/IModelDoc2/Save3.json")
            .unwrap();
        assert!(payload.get("examples").is_some());
    }
}
