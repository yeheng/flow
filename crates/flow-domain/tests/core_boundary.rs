//! Public domain contracts also run with --no-default-features.
use flow_domain::Definition;
use serde_json::json;
use std::collections::BTreeSet;

fn definition() -> Definition {
    serde_json::from_value(json!({
        "semantics": 2,
        "nodes": [
            {"id":"start", "type":"start", "params":{}},
            {"id":"extension", "type":"example_transform", "params":{}},
            {"id":"end", "type":"end", "params":{}}
        ],
        "edges": [
            {"from":"start", "to":"extension"},
            {"from":"extension", "to":"end"}
        ]
    }))
    .unwrap()
}

#[test]
fn host_can_extend_node_vocabulary_without_changing_domain() {
    let graph = definition();
    assert!(graph.validate().is_err());
    let extensions = BTreeSet::from(["example_transform".to_owned()]);
    graph.validate_with_dynamic(&extensions).unwrap();
}

#[test]
fn extensions_cannot_bypass_graph_integrity() {
    let mut value = serde_json::to_value(definition()).unwrap();
    value["edges"][0]["to"] = json!("missing");
    let graph: Definition = serde_json::from_value(value).unwrap();
    let extensions = BTreeSet::from(["example_transform".to_owned()]);
    assert!(graph.validate_with_dynamic(&extensions).is_err());
}

#[test]
fn declaring_a_builtin_as_an_extension_cannot_override_its_parameter_rules() {
    let mut value = serde_json::to_value(definition()).unwrap();
    value["nodes"][1]["type"] = json!("script");
    let graph: Definition = serde_json::from_value(value).unwrap();
    let invalid = graph.validate().expect_err("script requires code");
    let extensions = BTreeSet::from(["script".to_owned()]);
    assert_eq!(graph.validate_with_dynamic(&extensions), Err(invalid));
}
