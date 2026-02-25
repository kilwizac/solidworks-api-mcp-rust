use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use sw_core::{
    as_object, parse_categories, parse_limit, parse_string, read_index_artifact, resolve_data_root,
    DataStore, SearchOptions,
};

const PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "solidworks-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

fn as_args_object(value: Option<&Value>) -> Map<String, Value> {
    as_object(value)
}

fn get_string(args: &Map<String, Value>, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|entry| !entry.is_empty())
}

fn get_string_array(args: &Map<String, Value>, key: &str) -> Vec<String> {
    args.get(key)
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Clone)]
pub struct MCPServer {
    store: std::sync::Arc<DataStore>,
}

impl MCPServer {
    pub fn new(store: DataStore) -> Self {
        Self {
            store: std::sync::Arc::new(store),
        }
    }

    fn initialize_result() -> Value {
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

    fn tools_list_result() -> Value {
        json!({
            "tools": [
                {
                    "name": "solidworks_query",
                    "description": "Consolidated search/discovery interface for SolidWorks API docs.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string" },
                            "intent": { "type": "string", "enum": ["search", "examples", "related"] },
                            "docsets": { "type": "array", "items": { "type": "string" } },
                            "types": { "type": "array", "items": { "type": "string" } },
                            "interface": { "type": "string" },
                            "member": { "type": "string" },
                            "categories": { "type": "array", "items": { "type": "string" } },
                            "limit": { "type": "integer" }
                        },
                        "required": ["query"]
                    }
                },
                {
                    "name": "solidworks_fetch",
                    "description": "Deterministic fetch of interface/member/enum docs.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "kind": { "type": "string", "enum": ["member", "interface", "enum"] },
                            "docset": { "type": "string" },
                            "interface": { "type": "string" },
                            "member": { "type": "string" },
                            "enum": { "type": "string" }
                        },
                        "required": ["kind"]
                    }
                }
            ]
        })
    }

    fn search_to_query_row(score: i32, doc: &sw_core::SearchDocument) -> Value {
        json!({
            "score": score,
            "title": doc.title,
            "summary": doc.summary,
            "docset": doc.docset,
            "type": doc.doc_type,
            "interface": doc.interface_name,
            "path": doc.extra.get("path"),
            "payload_ref": doc.href,
            "doc": doc,
        })
    }

    fn tool_query(&self, args: &Map<String, Value>) -> Value {
        let query = get_string(args, "query").unwrap_or_default();
        let intent = get_string(args, "intent").unwrap_or_else(|| "search".to_string());
        let docsets = get_string_array(args, "docsets");
        let types = get_string_array(args, "types").into_iter().collect::<HashSet<_>>();
        let interface_name = get_string(args, "interface");
        let member_name = get_string(args, "member");
        let categories = parse_categories(args.get("categories"));
        let limit = parse_limit(args.get("limit"), Some(20));

        match intent.as_str() {
            "related" => {
                let docset = docsets
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "sldworksapi".to_string());
                let resolved =
                    self.store
                        .resolve_member_path(interface_name.as_deref(), member_name.as_deref(), &docset);

                let Some(path) = resolved else {
                    return json!({ "error": "Not found" });
                };

                let data = match self.store.read_json_file(&path) {
                    Ok(value) => value,
                    Err(error) => return json!({ "error": error.to_string() }),
                };

                let mut related = data
                    .get("related")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();

                if let Some(limit) = limit {
                    related.truncate(limit);
                }

                return json!({
                    "intent": "related",
                    "interface": interface_name,
                    "member": member_name,
                    "results": related,
                });
            }
            "examples" => {
                if let (Some(interface_name), Some(member_name)) =
                    (interface_name.as_deref(), member_name.as_deref())
                {
                    let resolved = self.store.resolve_member_path(
                        Some(interface_name),
                        Some(member_name),
                        "sldworksapi",
                    );

                    let Some(path) = resolved else {
                        return json!({ "error": "Not found" });
                    };

                    let data = match self.store.read_json_file(&path) {
                        Ok(value) => value,
                        Err(error) => return json!({ "error": error.to_string() }),
                    };

                    let examples = data
                        .get("examples")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();

                    let key = format!("{}.{}", interface_name, member_name);
                    let titles = self.store.examples_map().get(&key).cloned().unwrap_or_default();

                    let mut related = Vec::new();
                    for title in titles {
                        if let Some(doc) = self.store.progguide_titles().get(&title) {
                            related.push(Self::search_to_query_row(0, doc));
                        }
                    }

                    if let Some(limit) = limit {
                        related.truncate(limit);
                    }

                    return json!({
                        "intent": "examples",
                        "interface": interface_name,
                        "member": member_name,
                        "examples": examples,
                        "results": related,
                    });
                }

                let options = SearchOptions {
                    docset: Some("progguide".to_string()),
                    limit,
                    ..SearchOptions::default()
                };
                let results = self.store.search_api_scored(&query, &options);
                let rows = results
                    .iter()
                    .map(|entry| Self::search_to_query_row(entry.score, &entry.doc))
                    .collect::<Vec<_>>();

                return json!({
                    "intent": "examples",
                    "results": rows,
                });
            }
            _ => {}
        }

        let (docset_filter, post_filter_docsets) = if docsets.len() == 1 {
            (Some(docsets[0].clone()), HashSet::new())
        } else {
            (None, docsets.into_iter().collect::<HashSet<_>>())
        };

        let options = SearchOptions {
            docset: docset_filter,
            categories,
            interface_name,
            limit: None,
            ..SearchOptions::default()
        };

        let mut results = self.store.search_api_scored(&query, &options);

        if !post_filter_docsets.is_empty() {
            results.retain(|entry| {
                post_filter_docsets.contains(entry.doc.docset.as_deref().unwrap_or_default())
            });
        }

        if !types.is_empty() {
            results.retain(|entry| types.contains(entry.doc.doc_type.as_deref().unwrap_or_default()));
        }

        if let Some(limit) = limit {
            results.truncate(limit);
        }

        let rows = results
            .iter()
            .map(|entry| Self::search_to_query_row(entry.score, &entry.doc))
            .collect::<Vec<_>>();

        json!({
            "intent": "search",
            "results": rows,
        })
    }

    fn tool_fetch(&self, args: &Map<String, Value>) -> Value {
        let Some(kind) = get_string(args, "kind") else {
            return json!({ "error": "kind is required" });
        };

        let (resolved, docset, interface_name, member_name, enum_name) = match kind.as_str() {
            "member" => {
                let docset = get_string(args, "docset").unwrap_or_else(|| "sldworksapi".to_string());
                let interface_name = get_string(args, "interface");
                let member_name = get_string(args, "member");
                let resolved = self.store.resolve_member_path(
                    interface_name.as_deref(),
                    member_name.as_deref(),
                    &docset,
                );
                (resolved, docset, interface_name, member_name, None)
            }
            "interface" => {
                let docset = get_string(args, "docset").unwrap_or_else(|| "sldworksapi".to_string());
                let interface_name = get_string(args, "interface");
                let resolved = self
                    .store
                    .resolve_interface_path(interface_name.as_deref(), &docset);
                (resolved, docset, interface_name, None, None)
            }
            "enum" => {
                let docset = get_string(args, "docset").unwrap_or_else(|| "swconst".to_string());
                let enum_name = get_string(args, "enum");
                let resolved = self.store.resolve_enum_path(enum_name.as_deref(), &docset);
                (resolved, docset, None, None, enum_name)
            }
            _ => return json!({ "error": "unsupported kind" }),
        };

        let Some(path) = resolved else {
            return json!({ "error": "Not found" });
        };

        let path_text = path.to_string_lossy().to_string();
        match self.store.read_json_file(&path) {
            Ok(data) => json!({
                "kind": kind,
                "docset": docset,
                "interface": interface_name,
                "member": member_name,
                "enum": enum_name,
                "path": path_text,
                "data": data,
            }),
            Err(error) => json!({ "error": error.to_string() }),
        }
    }

    pub(crate) fn call_tool(&self, tool_name: &str, args: &Map<String, Value>) -> Option<Value> {
        Some(match tool_name {
            "solidworks_query" => self.tool_query(args),
            "solidworks_fetch" => self.tool_fetch(args),
            _ => return None,
        })
    }

    pub fn handle_line(&self, line: &str) -> Option<Value> {
        let parsed: JsonRpcRequest = serde_json::from_str(line).ok()?;
        let request_id = parsed.id.clone();
        let method = parsed.method.unwrap_or_default();

        match method.as_str() {
            "initialize" => Some(
                json!({ "jsonrpc": "2.0", "id": request_id, "result": Self::initialize_result() }),
            ),
            "tools/list" => Some(
                json!({ "jsonrpc": "2.0", "id": request_id, "result": Self::tools_list_result() }),
            ),
            "tools/call" => {
                let params = as_args_object(parsed.params.as_ref());
                let Some(tool_name) = parse_string(params.get("name")).map(|entry| entry.into_owned())
                else {
                    return Some(json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": { "code": -32601, "message": "Unknown tool" }
                    }));
                };

                let args = as_args_object(params.get("arguments"));
                let Some(result) = self.call_tool(&tool_name, &args) else {
                    return Some(json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": { "code": -32601, "message": "Unknown tool" }
                    }));
                };

                Some(json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string())
                            }
                        ]
                    }
                }))
            }
            "initialized" => None,
            _ => {
                if request_id.is_some() {
                    Some(json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": { "code": -32601, "message": "Method not found" }
                    }))
                } else {
                    None
                }
            }
        }
    }
}

pub fn load_store_from_root(root: &Path) -> Result<DataStore> {
    let index_path = root.join("index-v2.swidx");
    let index = read_index_artifact(&index_path)?;
    Ok(DataStore::new(root.to_path_buf(), index))
}

pub fn default_data_root() -> PathBuf {
    resolve_data_root(std::env::var("SW_API_DATA_ROOT").ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use sw_core::{BuiltIndex, DocsetStats, RootIndex, SearchDocument, SearchIndex};
    use tempfile::tempdir;

    fn test_index(root: &Path) -> BuiltIndex {
        let docs = vec![
            SearchDocument {
                title: Some("Target Match".to_string()),
                summary: Some("Feature creation details".to_string()),
                keywords: Some(vec!["feature".to_string(), "create".to_string()]),
                categories: Some(vec!["api".to_string(), "automation".to_string()]),
                interface_name: Some("IFeatureManager".to_string()),
                doc_type: Some("method".to_string()),
                docset: Some("sldworksapi".to_string()),
                ..SearchDocument::default()
            },
            SearchDocument {
                title: Some("Wrong Type".to_string()),
                summary: Some("Feature creation details".to_string()),
                keywords: Some(vec!["feature".to_string(), "create".to_string()]),
                categories: Some(vec!["api".to_string(), "automation".to_string()]),
                interface_name: Some("IFeatureManager".to_string()),
                doc_type: Some("property".to_string()),
                docset: Some("sldworksapi".to_string()),
                ..SearchDocument::default()
            },
            SearchDocument {
                title: Some("Macro Guide".to_string()),
                summary: Some("macro setup".to_string()),
                keywords: Some(vec!["macro".to_string()]),
                categories: Some(vec!["guide".to_string()]),
                interface_name: Some("".to_string()),
                doc_type: Some("guide".to_string()),
                docset: Some("progguide".to_string()),
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
            ..SearchIndex::default()
        };

        let mut stats = BTreeMap::new();
        stats.insert("sldworksapi".to_string(), DocsetStats::default());

        fs::create_dir_all(root.join("json/sldworksapi/interfaces/IFoo/members")).unwrap();
        fs::create_dir_all(root.join("json/swconst/enums")).unwrap();

        fs::write(
            root.join("json/sldworksapi/interfaces/IFoo/members/Bar.json"),
            serde_json::to_vec(&json!({
                "related": ["A", "B", "C"],
                "examples": [
                    { "title": "Primary" }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        fs::write(
            root.join("json/swconst/enums/swThing_e.json"),
            serde_json::to_vec(&json!({
                "values": [
                    { "member": "A", "value": "1", "description": "Alpha" }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

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
        )
    }

    #[test]
    fn query_filters_combine() {
        let temp = tempdir().unwrap();
        let store = DataStore::new(temp.path().to_path_buf(), test_index(temp.path()));
        let server = MCPServer::new(store);

        let payload = server.tool_query(&Map::from_iter([
            ("query".to_string(), Value::String("create feature".to_string())),
            (
                "docsets".to_string(),
                Value::Array(vec![Value::String("sldworksapi".to_string())]),
            ),
            (
                "types".to_string(),
                Value::Array(vec![Value::String("method".to_string())]),
            ),
            ("interface".to_string(), Value::String("IFeatureManager".to_string())),
            (
                "categories".to_string(),
                Value::Array(vec![
                    Value::String("api".to_string()),
                    Value::String("automation".to_string()),
                ]),
            ),
        ]));

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
        let temp = tempdir().unwrap();
        let store = DataStore::new(temp.path().to_path_buf(), test_index(temp.path()));
        let server = MCPServer::new(store);

        let payload = server.tool_query(&Map::from_iter([
            ("intent".to_string(), Value::String("related".to_string())),
            ("interface".to_string(), Value::String("IFoo".to_string())),
            ("member".to_string(), Value::String("Bar".to_string())),
            ("limit".to_string(), Value::String("2".to_string())),
        ]));

        let related = payload
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        assert_eq!(related.len(), 2);
    }

    #[test]
    fn tool_dispatch_only_modern() {
        let temp = tempdir().unwrap();
        let store = DataStore::new(temp.path().to_path_buf(), test_index(temp.path()));
        let server = MCPServer::new(store);

        assert!(server.call_tool("solidworks_query", &Map::new()).is_some());
        assert!(server.call_tool("solidworks_fetch", &Map::new()).is_some());
        assert!(server.call_tool("solidworks_lookup_method", &Map::new()).is_none());
    }
}
