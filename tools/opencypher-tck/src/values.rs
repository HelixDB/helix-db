//! Independent TCK value parser and comparator. No production expression evaluator
//! is used to construct expected results or parameter values.
use crate::corpus::Result;
use serde_json::Value as Json;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Node {
        labels: Vec<String>,
        properties: BTreeMap<String, Value>,
    },
    Relationship {
        kind: String,
        properties: BTreeMap<String, Value>,
    },
    Path {
        nodes: Vec<Value>,
        relationships: Vec<(bool, Value)>,
    },
}

pub fn parse(text: &str) -> Result<Value> {
    let mut parser = Parser {
        text,
        position: 0,
        depth: 0,
    };
    let value = parser.value()?;
    parser.space();
    if parser.position != text.len() {
        return Err(format!("trailing TCK value: {text}").into());
    }
    Ok(value)
}

struct Parser<'a> {
    text: &'a str,
    position: usize,
    depth: usize,
}
impl Parser<'_> {
    fn space(&mut self) {
        while self.text[self.position..].starts_with(char::is_whitespace) {
            self.position += self.text[self.position..]
                .chars()
                .next()
                .expect("nonempty whitespace")
                .len_utf8();
        }
    }
    fn eat(&mut self, text: &str) -> bool {
        self.space();
        if self.text[self.position..].starts_with(text) {
            self.position += text.len();
            true
        } else {
            false
        }
    }
    fn require(&mut self, text: &str) -> Result<()> {
        if self.eat(text) {
            Ok(())
        } else {
            Err(format!("expected {text} at {} in {}", self.position, self.text).into())
        }
    }
    fn name(&mut self) -> Result<String> {
        self.space();
        if self.text[self.position..].starts_with(['\'', '"', '`']) {
            return self.string();
        }
        let start = self.position;
        while let Some(c) = self.text[self.position..].chars().next() {
            if c.is_alphanumeric() || c == '_' || c == '$' {
                self.position += c.len_utf8();
            } else {
                break;
            }
        }
        if self.position == start {
            return Err("expected identifier in TCK value".into());
        }
        Ok(self.text[start..self.position].into())
    }
    fn string(&mut self) -> Result<String> {
        self.space();
        let quote = self.text[self.position..]
            .chars()
            .next()
            .ok_or("missing quote")?;
        self.position += quote.len_utf8();
        let mut value = String::new();
        loop {
            let c = self.text[self.position..]
                .chars()
                .next()
                .ok_or("unterminated TCK string")?;
            self.position += c.len_utf8();
            if c == quote {
                break;
            }
            if c != '\\' {
                value.push(c);
                continue;
            }
            let escaped = self.text[self.position..]
                .chars()
                .next()
                .ok_or("unterminated escape")?;
            self.position += escaped.len_utf8();
            value.push(match escaped {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'b' => '\u{8}',
                'f' => '\u{c}',
                'u' => {
                    let end = self.position + 4;
                    let hex = self
                        .text
                        .get(self.position..end)
                        .ok_or("truncated unicode escape")?;
                    self.position = end;
                    char::from_u32(u32::from_str_radix(hex, 16)?).ok_or("invalid unicode scalar")?
                }
                c => c,
            });
        }
        Ok(value)
    }
    fn map(&mut self) -> Result<BTreeMap<String, Value>> {
        self.require("{")?;
        let mut values = BTreeMap::new();
        if self.eat("}") {
            return Ok(values);
        }
        loop {
            let name = self.name()?;
            self.require(":")?;
            if values.insert(name, self.value()?).is_some() {
                return Err("duplicate expected map key".into());
            }
            if self.eat("}") {
                break;
            }
            self.require(",")?;
        }
        Ok(values)
    }
    fn value(&mut self) -> Result<Value> {
        self.depth += 1;
        if self.depth > 128 {
            return Err("TCK value nesting limit exceeded".into());
        }
        self.space();
        let value = if self.eat("(") {
            let mut labels = Vec::new();
            while self.eat(":") {
                labels.push(self.name()?);
            }
            self.space();
            let properties = if self.text[self.position..].starts_with('{') {
                self.map()?
            } else {
                BTreeMap::new()
            };
            self.require(")")?;
            labels.sort();
            Value::Node { labels, properties }
        } else if self.eat("[") {
            if self.eat(":") {
                let kind = self.name()?;
                self.space();
                let properties = if self.text[self.position..].starts_with('{') {
                    self.map()?
                } else {
                    BTreeMap::new()
                };
                self.require("]")?;
                Value::Relationship { kind, properties }
            } else {
                let mut values = Vec::new();
                if !self.eat("]") {
                    loop {
                        values.push(self.value()?);
                        if self.eat("]") {
                            break;
                        }
                        self.require(",")?;
                    }
                }
                Value::List(values)
            }
        } else if self.eat("<") {
            let mut nodes = vec![self.value()?];
            let mut relationships = Vec::new();
            while !self.eat(">") {
                let incoming = self.eat("<-");
                if !incoming {
                    self.require("-")?;
                }
                let rel = self.value()?;
                self.require(if incoming { "-" } else { "->" })?;
                relationships.push((incoming, rel));
                nodes.push(self.value()?);
            }
            if nodes.iter().any(|node| !matches!(node, Value::Node { .. }))
                || relationships
                    .iter()
                    .any(|(_, rel)| !matches!(rel, Value::Relationship { .. }))
            {
                return Err("path must alternate graph nodes and relationships".into());
            }
            Value::Path {
                nodes,
                relationships,
            }
        } else if self.text[self.position..].starts_with('{') {
            Value::Map(self.map()?)
        } else if self.text[self.position..].starts_with(['\'', '"']) {
            Value::String(self.string()?)
        } else {
            let start = self.position;
            while let Some(c) = self.text[self.position..].chars().next() {
                if c.is_whitespace() || ",]}):>".contains(c) {
                    break;
                }
                self.position += c.len_utf8();
            }
            let token = &self.text[start..self.position];
            match token {
                "null" => Value::Null,
                "true" => Value::Boolean(true),
                "false" => Value::Boolean(false),
                "NaN" => Value::Float(f64::NAN),
                "Inf" => Value::Float(f64::INFINITY),
                "-Inf" => Value::Float(f64::NEG_INFINITY),
                _ => match token.parse::<i64>() {
                    Ok(i) => Value::Integer(i),
                    Err(_) => Value::Float(token.parse()?),
                },
            }
        };
        self.depth -= 1;
        Ok(value)
    }
}

pub fn wire(value: &Json) -> Result<Value> {
    Ok(match value {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Boolean(*b),
        Json::Number(n) => match n.as_i64() {
            Some(i) => Value::Integer(i),
            None => Value::Float(n.as_f64().ok_or("invalid JSON number")?),
        },
        Json::String(s) => Value::String(s.clone()),
        Json::Array(xs) => Value::List(xs.iter().map(wire).collect::<Result<_>>()?),
        Json::Object(map) => {
            let properties = |value: &Json| -> Result<BTreeMap<String, Value>> {
                let Value::Map(map) = wire(value)? else {
                    return Err("graph properties are not a map".into());
                };
                Ok(map)
            };
            match map.get("$type").and_then(Json::as_str) {
                Some("integer") => Value::Integer(
                    value["value"]
                        .as_str()
                        .ok_or("invalid integer envelope")?
                        .parse()?,
                ),
                Some("float") => Value::Float(match value["value"].as_str() {
                    Some("NaN") => f64::NAN,
                    Some("Infinity") => f64::INFINITY,
                    Some("-Infinity") => f64::NEG_INFINITY,
                    _ => return Err("invalid float envelope".into()),
                }),
                Some("map") => {
                    let map = value["value"].as_object().ok_or("invalid map envelope")?;
                    Value::Map(
                        map.iter()
                            .map(|(k, v)| Ok((k.clone(), wire(v)?)))
                            .collect::<Result<_>>()?,
                    )
                }
                Some("node") => {
                    graph_id(value, "id")?;
                    let mut labels = value["labels"]
                        .as_array()
                        .ok_or("invalid labels")?
                        .iter()
                        .map(|v| v.as_str().map(str::to_owned).ok_or("invalid label".into()))
                        .collect::<Result<Vec<_>>>()?;
                    labels.sort();
                    Value::Node {
                        labels,
                        properties: properties(&value["properties"])?,
                    }
                }
                Some("relationship") => {
                    for key in ["id", "start", "end"] {
                        graph_id(value, key)?;
                    }
                    Value::Relationship {
                        kind: value["type"]
                            .as_str()
                            .filter(|s| !s.is_empty())
                            .ok_or("invalid relationship type")?
                            .into(),
                        properties: properties(&value["properties"])?,
                    }
                }
                Some("path") => {
                    let nodes = value["nodes"].as_array().ok_or("invalid path nodes")?;
                    let relationships = value["relationships"]
                        .as_array()
                        .ok_or("invalid path relationships")?;
                    if nodes.len() != relationships.len().saturating_add(1)
                        || nodes.iter().any(|node| node["$type"] != "node")
                        || relationships
                            .iter()
                            .any(|rel| rel["$type"] != "relationship")
                    {
                        return Err("invalid path shape".into());
                    }
                    let relationships = relationships
                        .iter()
                        .enumerate()
                        .map(|(i, r)| {
                            let incoming = r["start"] != nodes[i]["id"];
                            let (from, to) = if incoming {
                                (&nodes[i + 1]["id"], &nodes[i]["id"])
                            } else {
                                (&nodes[i]["id"], &nodes[i + 1]["id"])
                            };
                            if &r["start"] != from || &r["end"] != to {
                                return Err(
                                    "path relationship does not connect adjacent nodes".into()
                                );
                            }
                            Ok((incoming, wire(r)?))
                        })
                        .collect::<Result<_>>()?;
                    Value::Path {
                        nodes: nodes.iter().map(wire).collect::<Result<_>>()?,
                        relationships,
                    }
                }
                Some(_) => return Err("unknown lossless envelope".into()),
                None => Value::Map(
                    map.iter()
                        .map(|(k, v)| Ok((k.clone(), wire(v)?)))
                        .collect::<Result<_>>()?,
                ),
            }
        }
    })
}

pub fn equal(a: &Value, b: &Value, unordered_lists: bool) -> bool {
    match (a, b) {
        (Value::Float(a), Value::Float(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Value::Integer(a), Value::Float(b)) | (Value::Float(b), Value::Integer(a)) => {
            b.is_finite()
                && b.fract() == 0.0
                && *b >= i64::MIN as f64
                && *b < -(i64::MIN as f64)
                && *a == *b as i64
        }
        (Value::List(a), Value::List(b)) => {
            rows_equal(a, b, unordered_lists, |a, b| equal(a, b, unordered_lists))
        }
        (Value::Map(a), Value::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, a)| b.get(k).is_some_and(|b| equal(a, b, unordered_lists)))
        }
        (
            Value::Node {
                labels: a,
                properties: ap,
            },
            Value::Node {
                labels: b,
                properties: bp,
            },
        ) => {
            a == b
                && equal(
                    &Value::Map(ap.clone()),
                    &Value::Map(bp.clone()),
                    unordered_lists,
                )
        }
        (
            Value::Relationship {
                kind: a,
                properties: ap,
            },
            Value::Relationship {
                kind: b,
                properties: bp,
            },
        ) => {
            a == b
                && equal(
                    &Value::Map(ap.clone()),
                    &Value::Map(bp.clone()),
                    unordered_lists,
                )
        }
        (
            Value::Path {
                nodes: a,
                relationships: ar,
            },
            Value::Path {
                nodes: b,
                relationships: br,
            },
        ) => {
            rows_equal(a, b, false, |a, b| equal(a, b, unordered_lists))
                && rows_equal(ar, br, false, |(ad, a), (bd, b)| {
                    ad == bd && equal(a, b, unordered_lists)
                })
        }
        _ => a == b,
    }
}

/// Unordered comparison is a multiset comparison: each actual row is consumed once.
pub fn rows_equal<T>(
    expected: &[T],
    actual: &[T],
    unordered: bool,
    equal: impl Fn(&T, &T) -> bool,
) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    if !unordered {
        return expected.iter().zip(actual).all(|(a, b)| equal(a, b));
    }
    let mut used = vec![false; actual.len()];
    expected.iter().all(|a| {
        let Some(i) = actual
            .iter()
            .enumerate()
            .position(|(i, b)| !used[i] && equal(a, b))
        else {
            return false;
        };
        used[i] = true;
        true
    })
}

impl Value {
    pub fn parameter(self) -> Result<helix_ast::query::QueryValue> {
        use helix_ast::query::QueryValue as Q;
        Ok(match self {
            Self::Null => Q::Null,
            Self::Boolean(b) => Q::Bool(b),
            Self::Integer(i) => Q::I64(i),
            Self::Float(f) => Q::F64(f),
            Self::String(s) => Q::String(s),
            Self::List(xs) => Q::Array(xs.into_iter().map(Self::parameter).collect::<Result<_>>()?),
            Self::Map(xs) => Q::Object(
                xs.into_iter()
                    .map(|(k, v)| Ok((k, v.parameter()?)))
                    .collect::<Result<_>>()?,
            ),
            Self::Node { .. } | Self::Relationship { .. } | Self::Path { .. } => {
                return Err("graph elements cannot be TCK parameters".into())
            }
        })
    }
}

#[cfg(test)]
#[path = "tests/values.rs"]
mod tests;

fn graph_id(value: &Json, key: &str) -> Result<u64> {
    let text = value[key]
        .as_str()
        .ok_or("graph ID must be a decimal string")?;
    let id: u64 = text.parse()?;
    if id.to_string() != text {
        return Err("graph ID is not a canonical unsigned integer".into());
    }
    Ok(id)
}
