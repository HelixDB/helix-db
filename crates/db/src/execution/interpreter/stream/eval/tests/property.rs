use super::*;

#[tokio::test]
async fn row_property_reads_id_stored_properties_and_missing_values() {
    let db = test_support::open_db("stream-eval-row-property").await;
    let id = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("ada".to_string())),
            ("age", PropertyValue::I64(37)),
            (
                "metadata",
                PropertyValue::object([
                    ("externalID", PropertyValue::from("ada-ext")),
                    ("score", PropertyValue::I64(9)),
                ]),
            ),
            ("metadata.externalID", PropertyValue::from("exact-ext")),
        ],
    )
    .await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = current_node(id);

    assert_eq!(
        ctx.row_property(&row, &name("$id")).await.unwrap(),
        Some(DbPropertyValue::I64(id as i64))
    );
    assert_eq!(
        ctx.row_property(&row, &name("name")).await.unwrap(),
        Some(DbPropertyValue::String("ada".to_string()))
    );
    assert_eq!(
        ctx.row_property(&row, &name("metadata.score"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(9))
    );
    assert_eq!(
        ctx.row_property(&row, &name("metadata.externalID"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("exact-ext".to_string()))
    );
    assert_eq!(
        ctx.row_property(&row, &name("metadata.")).await.unwrap(),
        None
    );
    assert_eq!(ctx.row_property(&row, &name(".score")).await.unwrap(), None);
    assert_eq!(
        ctx.row_property(&row, &name("age.value")).await.unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&row, &name("missing")).await.unwrap(),
        None
    );
    let mut resolver = RowValueResolver::new(&ctx);
    for last_use in [false, true] {
        assert_eq!(
            resolver
                .row_properties(&ExecutionRow::empty(), last_use)
                .await
                .unwrap(),
            Vec::new()
        );
        assert_eq!(
            resolver
                .row_properties(&current_node(u64::MAX), last_use)
                .await
                .unwrap(),
            Vec::new()
        );
    }
    // Earlier uses copy the cached record, the last use moves it out, and a
    // use after that reads the record again.
    resolver.prefetch([&ElementRef::Node(id)]).await.unwrap();
    let copied = resolver.row_properties(&row, false).await.unwrap();
    assert!(!copied.is_empty());
    assert_eq!(resolver.row_properties(&row, true).await.unwrap(), copied);
    assert_eq!(resolver.row_properties(&row, true).await.unwrap(), copied);
}

#[tokio::test]
async fn row_property_reads_edge_properties_and_empty_current_id() {
    let db = test_support::open_db("stream-eval-row-edge-property").await;
    let from = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("Alice".to_string())),
            ("kind", PropertyValue::String("source".to_string())),
            (
                "metadata",
                PropertyValue::object([("score", PropertyValue::I64(9))]),
            ),
            ("metadata.externalID", PropertyValue::from("exact-source")),
            ("$id", PropertyValue::from("stored-id-must-not-win")),
        ],
    )
    .await;
    let to = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("Bob".to_string())),
            ("kind", PropertyValue::String("target".to_string())),
        ],
    )
    .await;
    let edge = test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "Follows",
        vec![("since", PropertyValue::I64(2024))],
    )
    .await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("since"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(2024))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(from as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(to as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.$id"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(from as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to.$id"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(to as i64))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.name"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("Alice".to_string()))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to.kind"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("target".to_string()))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.metadata.score"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(9))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from.metadata.externalID"))
            .await
            .unwrap(),
        Some(DbPropertyValue::String("exact-source".to_string()))
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$to.missing"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(edge), &name("$from."))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_node(from), &name("$from"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&ExecutionRow::empty(), &name("$id"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(u64::MAX), &name("$from"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(u64::MAX), &name("$from.name"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ctx.row_property(&current_edge(u64::MAX), &name("$to.$id"))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn row_resolver_does_not_cache_property_decode_errors() {
    let db = test_support::open_db("stream-eval-row-property-corruption").await;
    let id = 11;
    let key = crate::encoding::keys::DataKey::Data {
        scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(id),
        ),
    }
    .to_bytes();
    db.inner_db()
        .put(key, bytes::Bytes::from_static(b"corrupt"))
        .await
        .expect("corrupt property blob writes");
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = current_node(id);
    let property = name("value");
    let mut resolver = RowValueResolver::new(&ctx);

    for _ in 0..2 {
        resolver
            .row_property(&row, &property)
            .await
            .expect_err("each corrupt property lookup must return the decode error");
    }

    assert_eq!(
        ctx.projection_read_snapshot(),
        crate::execution::interpreter::runtime_context::ProjectionReadSnapshot {
            property_gets: 2,
            property_decodes: 2,
            endpoint_gets: 0,
        }
    );
}

#[tokio::test]
async fn endpoint_property_lookup_propagates_corrupt_node_properties() {
    let db = test_support::open_db("stream-eval-endpoint-property-corruption").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let edge = test_support::add_edge(&db, from, to, "LINK").await;
    let key = crate::encoding::keys::DataKey::Data {
        scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(from),
        ),
    }
    .to_bytes();
    db.inner_db()
        .put(key, bytes::Bytes::from_static(b"corrupt"))
        .await
        .expect("corrupt endpoint property blob writes");
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert!(matches!(
        ctx.row_property(&current_edge(edge), &name("$from.name"))
            .await,
        Err(HelixDbError::Encoding(_))
    ));
}

/// Error text with the validator's absolute buffer addresses masked, so
/// errors from different copies of the same bytes compare equal.
fn masked_error(error: &str) -> String {
    let mut masked = String::with_capacity(error.len());
    let mut rest = error;
    while let Some(start) = rest.find("0x") {
        masked.push_str(&rest[..start]);
        masked.push_str("0x?");
        rest = rest[start + 2..].trim_start_matches(|c: char| c.is_ascii_hexdigit());
    }
    masked.push_str(rest);
    masked
}

/// Stored rows covering every value variant, duplicate names, dotted names,
/// empty rows and large payloads.
fn stored_shapes() -> Vec<Vec<Property>> {
    use std::collections::BTreeMap;
    let object = |entries: Vec<(&str, DbPropertyValue)>| {
        DbPropertyValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect::<BTreeMap<_, _>>(),
        )
    };
    vec![
        Vec::new(),
        vec![
            Property::string("$label", "User"),
            Property::new("null", DbPropertyValue::Null),
            Property::bool("bool", true),
            Property::i64("i64", i64::MIN),
            Property::datetime_millis("datetime", -1),
            Property::f64("f64", f64::NAN),
            Property::f64("negative_zero", -0.0),
            Property::new("f32", DbPropertyValue::F32(f64::from(f32::MAX))),
            Property::string("string", "héllo \u{1F600}"),
            Property::string("empty_string", ""),
            Property::bytes("bytes", vec![0, 255, 7]),
            Property::bytes("empty_bytes", Vec::new()),
            Property::i64_array("i64_array", vec![i64::MAX, -1]),
            Property::f64_array("f64_array", vec![f64::INFINITY, f64::NAN]),
            Property::f32_array("f32_array", Vec::new()),
            Property::string_array("string_array", vec![String::new(), "b".into()]),
            Property::new(
                "array",
                DbPropertyValue::Array(vec![
                    DbPropertyValue::I64(1),
                    object(vec![("inner", DbPropertyValue::Bool(false))]),
                ]),
            ),
            Property::new(
                "meta",
                object(vec![
                    ("score", DbPropertyValue::I64(9)),
                    (
                        "deep",
                        object(vec![("leaf", DbPropertyValue::String("x".into()))]),
                    ),
                    ("", DbPropertyValue::I64(0)),
                ]),
            ),
            Property::i64("meta.exact", 4),
        ],
        vec![
            Property::string("dup", "first"),
            Property::string("$label", "Other"),
            Property::string("dup", "second"),
            Property::new("meta", DbPropertyValue::I64(1)),
            Property::string("$label", "User"),
            Property::new("meta", object(vec![("score", DbPropertyValue::I64(2))])),
        ],
        vec![
            Property::string("$label", "Document"),
            Property::f32_array(
                "embedding",
                (0..1536).map(|value| value as f32 * 0.25).collect(),
            ),
            Property::string("body", "word ".repeat(4096)),
            Property::bytes("blob", vec![3; 70_000]),
        ],
    ]
}

/// The value the full-row decoder yields for `path`: an exact name first,
/// then a dotted walk through nested objects.
fn decoded_value(properties: &[Property], path: &str) -> Option<DbPropertyValue> {
    if let Some(property) = properties.iter().find(|property| property.name == path) {
        return Some(property.value.clone());
    }
    let mut segments = path.split('.');
    let first = segments.next().filter(|_| path.contains('.'))?;
    let mut value = properties
        .iter()
        .find(|property| !first.is_empty() && property.name == first)?
        .value
        .clone();
    for segment in segments {
        let DbPropertyValue::Object(values) = value else {
            return None;
        };
        value = values.get(segment).filter(|_| !segment.is_empty())?.clone();
    }
    Some(value)
}

fn same_values(left: &Option<DbPropertyValue>, right: &Option<DbPropertyValue>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.same_v1_representation(right),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn node_property_key(id: u64) -> bytes::Bytes {
    crate::encoding::keys::DataKey::Data {
        scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(id),
        ),
    }
    .to_bytes()
}

#[tokio::test]
async fn resolver_reads_every_stored_shape_exactly_as_the_full_row_decoder() {
    let db = test_support::open_db("stream-eval-resolver-decoder-oracle").await;
    let shapes = stored_shapes();
    for (id, properties) in (1_u64..).zip(&shapes) {
        db.inner_db()
            .put(
                node_property_key(id),
                crate::encoding::property::encode_properties(properties),
            )
            .await
            .unwrap();
    }
    let paths = [
        "$label",
        "null",
        "bool",
        "i64",
        "datetime",
        "f64",
        "negative_zero",
        "f32",
        "string",
        "empty_string",
        "bytes",
        "empty_bytes",
        "i64_array",
        "f64_array",
        "f32_array",
        "string_array",
        "array",
        "array.inner",
        "meta",
        "meta.score",
        "meta.deep",
        "meta.deep.leaf",
        "meta.deep.missing",
        "meta.",
        "meta..x",
        ".meta",
        "meta.exact",
        "meta.score.more",
        "dup",
        "embedding",
        "body",
        "blob",
        "missing",
        "missing.path",
    ];
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let mut batch = RowValueResolver::new(&ctx);
    let elements = (1..=shapes.len() as u64)
        .map(ElementRef::Node)
        .collect::<Vec<_>>();
    batch.prefetch(&elements).await.unwrap();
    for (id, properties) in (1_u64..).zip(&shapes) {
        let stored = db
            .inner_db()
            .get(node_property_key(id))
            .await
            .unwrap()
            .unwrap();
        let decoded = crate::encoding::property::decode_properties(&stored).unwrap();
        assert_eq!(decoded.len(), properties.len());
        assert!(decoded
            .iter()
            .zip(properties)
            .all(|(left, right)| left.same_v1_representation(right)));
        let row = current_node(id);
        for path in paths {
            let expected = decoded_value(&decoded, path);
            let alone = ctx.row_property(&row, &name(path)).await.unwrap();
            let batched = batch.row_property(&row, &name(path)).await.unwrap();
            assert!(same_values(&alone, &expected), "{id} {path}: {alone:?}");
            assert!(same_values(&batched, &expected), "{id} {path}: {batched:?}");
        }
        for last_use in [false, true] {
            let all = batch.row_properties(&row, last_use).await.unwrap();
            assert_eq!(all.len(), decoded.len());
            assert!(all
                .iter()
                .zip(&decoded)
                .all(|(left, right)| left.same_v1_representation(right)));
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn resolver_rejects_corrupt_rows_lazily_with_the_decoder_error() {
    let db = test_support::open_db("stream-eval-resolver-corruption-oracle").await;
    let valid = crate::encoding::property::encode_properties(&[
        Property::string("$label", "User"),
        Property::string("name", "ada"),
        Property::f32_array("embedding", vec![1.0; 64]),
    ]);
    let mut corrupt = vec![
        bytes::Bytes::from_static(b"corrupt"),
        bytes::Bytes::from_static(&[0]),
        valid.slice(..valid.len() - 1),
        valid.slice(1..),
    ];
    // Single-byte flips across the row: some still validate, most must not.
    for position in (0..valid.len()).step_by(7) {
        let mut flipped = valid.to_vec();
        flipped[position] ^= 0xA5;
        corrupt.push(bytes::Bytes::from(flipped));
    }
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let mut rejected = 0;
    for (id, bytes) in (100_u64..).zip(&corrupt) {
        db.inner_db()
            .put(node_property_key(id), bytes.clone())
            .await
            .unwrap();
        let row = current_node(id);
        let expected = crate::encoding::property::decode_properties(bytes);
        // `$id` never reads the record, so corruption cannot surface.
        assert_eq!(
            ctx.row_property(&row, &name("$id")).await.unwrap(),
            Some(DbPropertyValue::I64(id as i64))
        );
        for path in ["name", "embedding", "missing", "name.x"] {
            let actual = ctx.row_property(&row, &name(path)).await;
            match (&expected, actual) {
                (Ok(decoded), Ok(actual)) => {
                    assert!(same_values(&actual, &decoded_value(decoded, path)));
                }
                (
                    Err(crate::encoding::error::EncodingError::Rkyv(expected)),
                    Err(HelixDbError::Encoding(crate::encoding::error::EncodingError::Rkyv(
                        actual,
                    ))),
                ) => assert_eq!(masked_error(&actual), masked_error(expected)),
                (expected, actual) => panic!("{id} {path}: {expected:?} vs {actual:?}"),
            }
        }
        let mut resolver = RowValueResolver::new(&ctx);
        match (&expected, resolver.prefetch([&ElementRef::Node(id)]).await) {
            (Ok(_), Ok(())) => {}
            (Err(_), Err(HelixDbError::Encoding(_))) => rejected += 1,
            (expected, actual) => panic!("{id}: {expected:?} vs {actual:?}"),
        }
    }
    assert!(rejected >= 4, "most corruptions must be rejected");
    // A batch fails as a whole before any row reads its own record.
    db.inner_db()
        .put(node_property_key(1), valid.clone())
        .await
        .unwrap();
    let mut resolver = RowValueResolver::new(&ctx);
    assert!(resolver
        .prefetch([&ElementRef::Node(1), &ElementRef::Node(100)])
        .await
        .is_err());
    // An empty stored row has no properties and is never validated.
    db.inner_db()
        .put(node_property_key(200), bytes::Bytes::new())
        .await
        .unwrap();
    assert_eq!(
        ctx.row_property(&current_node(200), &name("name"))
            .await
            .unwrap(),
        None
    );
    let mut resolver = RowValueResolver::new(&ctx);
    assert_eq!(
        resolver
            .row_properties(&current_node(200), true)
            .await
            .unwrap(),
        Vec::new()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_scanned_record_replaces_the_storage_read_and_is_validated_only_when_read() {
    let db = test_support::open_db("stream-eval-resolver-scanned-record").await;
    let stored =
        crate::encoding::property::encode_properties(&[Property::string("name", "stored")]);
    db.inner_db()
        .put(node_property_key(1), stored)
        .await
        .unwrap();
    db.inner_db()
        .put(node_property_key(2), bytes::Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    let scanned = crate::encoding::property::encode_properties(&[
        Property::string("name", "scanned"),
        Property::f32_array("embedding", vec![1.5; 8]),
    ]);
    // An unaligned copy of the record exercises the aligned scratch copy.
    let mut padded = vec![0];
    padded.extend_from_slice(&scanned);
    let unaligned = bytes::Bytes::from(padded).slice(1..);
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let buffers = crate::encoding::v2::values::property::view::Buffers::default();
    for record in [scanned.clone(), unaligned] {
        let mut resolver =
            RowValueResolver::with_record(&ctx, ElementRef::Node(1), record, Default::default());
        let before = ctx.projection_read_snapshot();
        assert_eq!(
            resolver
                .row_property(&current_node(1), &name("name"))
                .await
                .unwrap(),
            Some(DbPropertyValue::String("scanned".into()))
        );
        assert_eq!(
            resolver
                .row_property(&current_node(1), &name("embedding"))
                .await
                .unwrap(),
            Some(DbPropertyValue::F32Array(vec![1.5; 8]))
        );
        let after = ctx.projection_read_snapshot();
        assert_eq!(after.property_gets, before.property_gets);
        assert_eq!(after.property_decodes, before.property_decodes + 1);
        // Another element still reads storage.
        assert_eq!(
            resolver
                .row_property(&current_node(2), &name("$id"))
                .await
                .unwrap(),
            Some(DbPropertyValue::I64(2))
        );
        assert!(resolver
            .row_property(&current_node(2), &name("name"))
            .await
            .is_err());
        assert_eq!(
            ctx.projection_read_snapshot().property_gets,
            before.property_gets + 1
        );
        drop(resolver.into_buffers());
    }
    // A corrupt scanned record fails only when it is read.
    let mut resolver = RowValueResolver::with_record(
        &ctx,
        ElementRef::Node(1),
        bytes::Bytes::from_static(b"corrupt"),
        buffers,
    );
    let before = ctx.projection_read_snapshot();
    assert_eq!(
        resolver
            .row_property(&current_node(1), &name("$id"))
            .await
            .unwrap(),
        Some(DbPropertyValue::I64(1))
    );
    assert_eq!(ctx.projection_read_snapshot(), before);
    assert!(matches!(
        resolver.row_property(&current_node(1), &name("name")).await,
        Err(HelixDbError::Encoding(
            crate::encoding::error::EncodingError::Rkyv(_)
        ))
    ));
    // The full row of a scanned record decodes like the stored decoder.
    let mut resolver = RowValueResolver::with_record(
        &ctx,
        ElementRef::Node(1),
        scanned.clone(),
        Default::default(),
    );
    assert_eq!(
        resolver
            .row_properties(&current_node(1), false)
            .await
            .unwrap(),
        crate::encoding::property::decode_properties(&scanned).unwrap()
    );
    assert_eq!(
        resolver
            .row_properties(&current_node(1), true)
            .await
            .unwrap(),
        crate::encoding::property::decode_properties(&scanned).unwrap()
    );
    // After its last use the record is read from storage again.
    assert_eq!(
        resolver
            .row_properties(&current_node(1), true)
            .await
            .unwrap(),
        vec![Property::string("name", "stored")]
    );
    db.close().await.unwrap();
}
