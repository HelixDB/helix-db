//! Contract tests for database, cache, and index configuration validation.

use super::{
    scoped_secondary_index_property, CacheConfig, CacheMode, DbConfig, DiskCacheConfig,
    EdgeEncoding, EdgeUpdatePolicy, HelixConfig, IndexConfig, NonEmptyPathBuf,
    ObjectStoreWarmLevel, RangeIndexDirection, RuntimeIndexCatalog, SearchIndexBackfillLimits,
    SecondaryBackfillBatchRows, SecondaryIndexDefinition, SecondaryIndexElementType,
    SecondaryIndexKind, SimHasherCacheSettings, SlateHybridCacheConfig,
    SlateObjectStoreCacheSettings, TextAnalyzerKind, TextElementType, TextIndexDefinition,
    VectorElementType, VectorIndexDefinition, VectorMemoryBudget, VectorMemoryHydrationMode,
    VectorMemorySettings,
};
use crate::index_v2::{
    IndexElementKind, IndexIdentityFamily, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition,
};
use crate::search::vector::{VectorDistanceMetric, SIMHASH_BITS};
use helix_planner::{catalog, ir};

fn v2_secondary(definition: SecondaryIndexDefinition) -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(definition).unwrap()
}

fn v2_vector(definition: VectorIndexDefinition) -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(definition).unwrap()
}

fn v2_text(definition: TextIndexDefinition) -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::try_from(definition).unwrap()
}

#[test]
fn text_index_definition_construction_accepts_expected_values() {
    let definition = TextIndexDefinition::new_node("Doc", "body")
        .expect("valid text index definition")
        .with_tenant_property("firmId")
        .expect("valid tenant property")
        .with_analyzer(TextAnalyzerKind::StandardStemEn);
    assert_eq!(definition.label(), "Doc");
    assert_eq!(definition.property(), "body");
    assert_eq!(definition.tenant_property(), Some("firmId"));
}

#[test]
fn text_index_definition_construction_rejects_blank_fields() {
    assert!(TextIndexDefinition::new_node("", "body").is_err());
    assert!(TextIndexDefinition::new_node("Doc", "").is_err());
}

#[test]
fn text_index_definition_construction_rejects_blank_tenant_property() {
    let definition = TextIndexDefinition::new_node("Doc", "body")
        .expect("valid text index definition")
        .with_tenant_property(" ");
    assert!(definition.is_err());
}

#[test]
fn text_index_definition_exposes_validated_semantics_without_persistence_identity() {
    let definition = TextIndexDefinition::new_edge("Message", "body")
        .expect("valid text index definition")
        .with_tenant_property("orgId")
        .expect("valid tenant property")
        .with_analyzer(TextAnalyzerKind::WhitespaceLowercase)
        .with_positions_enabled(true);
    assert_eq!(definition.element_type(), TextElementType::Edge);
    assert_eq!(
        definition.key(),
        (TextElementType::Edge, "Message".into(), "body".into())
    );
    assert_eq!(definition.analyzer().as_str(), "whitespace_lowercase");
    assert!(definition.positions_enabled());
}

#[test]
fn db_config_rejects_invalid_raw_values() {
    assert!(DbConfig::new().try_with_id_lease_size(0).is_err());
    assert!(DbConfig::new().try_with_encoding_type_u8(0xff).is_err());
    assert!(DbConfig::new()
        .try_with_edge_update_policy_u8(0xff)
        .is_err());
    assert!(NonEmptyPathBuf::try_new("").is_err());
    assert!(DiskCacheConfig::try_new("/tmp/cache", 0).is_err());
    assert!(SlateObjectStoreCacheSettings::try_new(
        "/tmp/cache",
        Some(0),
        4096,
        false,
        ObjectStoreWarmLevel::Off,
        None,
        1
    )
    .is_err());
    assert!(SlateObjectStoreCacheSettings::try_new(
        "/tmp/cache",
        Some(1),
        0,
        false,
        ObjectStoreWarmLevel::Off,
        None,
        1
    )
    .is_err());
    assert!(SlateHybridCacheConfig::try_new(0, "/tmp/slate", 1).is_err());
}

#[test]
fn db_config_uses_typed_edge_settings() {
    let config = DbConfig::new()
        .with_encoding_type(EdgeEncoding::Efp)
        .with_edge_update_policy(EdgeUpdatePolicy::Lazy)
        .with_max_concurrent_reads(0);

    assert_eq!(config.default_encoding_type(), EdgeEncoding::Efp);
    assert_eq!(config.edge_update_policy(), EdgeUpdatePolicy::Lazy);
    assert_eq!(EdgeUpdatePolicy::Lazy.as_u8(), 0x01);
    assert_eq!(config.max_concurrent_reads(), 1);
    assert!(matches!(config.cache().mode(), CacheMode::Memory));

    for (raw, expected) in [
        (0, EdgeEncoding::None),
        (0x10, EdgeEncoding::None),
        (1, EdgeEncoding::Efp),
        (0x11, EdgeEncoding::Efp),
    ] {
        assert_eq!(EdgeEncoding::try_from(raw).unwrap(), expected);
    }
    for (raw, expected) in [
        (0, EdgeUpdatePolicy::Eager),
        (1, EdgeUpdatePolicy::Lazy),
        (2, EdgeUpdatePolicy::Adaptive),
    ] {
        assert_eq!(EdgeUpdatePolicy::try_from(raw).unwrap(), expected);
    }
}

#[test]
fn db_config_builders_preserve_checked_runtime_contracts() {
    let cache = CacheConfig::new(VectorMemorySettings::default(), CacheMode::VectorMemoryOnly);
    let config = DbConfig::new()
        .try_with_encoding_type_u8(1)
        .expect("legacy EFP byte is valid")
        .try_with_edge_update_policy_u8(2)
        .expect("legacy adaptive policy byte is valid")
        .try_with_id_lease_size(42)
        .expect("positive lease size is valid")
        .with_wal(false)
        .with_adaptive_updates(2048)
        .with_cache(cache.clone())
        .with_open_attribution(super::OpenAttribution {
            db_client_id: Some("client-1".to_string()),
            writer_open_id: Some("open-1".to_string()),
            open_reason: Some("test".to_string()),
            requested_term: Some(7),
            election_tick_id: Some("tick-1".to_string()),
            fence_op_id: Some("fence-1".to_string()),
            gateway_instance_id: Some("gateway-1".to_string()),
        });

    assert_eq!(config.default_encoding_type(), EdgeEncoding::Efp);
    assert_eq!(
        config.default_encoding_type_byte(),
        EdgeEncoding::Efp.as_u8()
    );
    assert_eq!(config.edge_update_policy(), EdgeUpdatePolicy::Adaptive);
    assert_eq!(
        config.edge_update_policy_byte(),
        EdgeUpdatePolicy::Adaptive.as_u8()
    );
    assert_eq!(config.id_lease_size(), 42);
    assert!(!config.enable_wal);
    assert_eq!(config.high_degree_threshold, 2048);
    assert_eq!(config.cache(), &cache);

    let Some(attribution) = config.open_attribution.as_ref() else {
        panic!("open attribution should be attached");
    };
    assert_eq!(attribution.db_client_id.as_deref(), Some("client-1"));
    assert_eq!(attribution.writer_open_id.as_deref(), Some("open-1"));
    assert_eq!(attribution.open_reason.as_deref(), Some("test"));
    assert_eq!(attribution.requested_term, Some(7));
    assert_eq!(attribution.election_tick_id.as_deref(), Some("tick-1"));
    assert_eq!(attribution.fence_op_id.as_deref(), Some("fence-1"));
    assert_eq!(
        attribution.gateway_instance_id.as_deref(),
        Some("gateway-1")
    );
}

#[test]
fn db_config_accepts_nonzero_id_lease_size_type_directly() {
    let lease_size = std::num::NonZeroU64::new(17).expect("lease size is nonzero");
    let config = DbConfig::new().with_id_lease_size(lease_size);

    assert_eq!(config.id_lease_size(), 17);
}

#[test]
fn db_and_helix_config_defaults_and_builders_expose_runtime_contracts() {
    let default = DbConfig::new();
    assert_eq!(
        DbConfig::default().id_lease_size(),
        crate::id_allocator::DEFAULT_LEASE_SIZE
    );
    assert_eq!(
        super::SlateRuntimeConfig::default()
            .to_reader_options(None)
            .object_store_cache_options
            .root_folder,
        None
    );
    assert_eq!(default.default_encoding_type(), EdgeEncoding::None);
    assert_eq!(
        default.default_encoding_type_byte(),
        EdgeEncoding::None.as_u8()
    );
    assert!(default.enable_wal);
    assert_eq!(default.max_concurrent_reads(), 64);
    assert_eq!(default.edge_update_policy(), EdgeUpdatePolicy::Eager);
    assert_eq!(
        default.edge_update_policy_byte(),
        EdgeUpdatePolicy::Eager.as_u8()
    );
    assert_eq!(default.high_degree_threshold, 1000);
    assert_eq!(
        default.id_lease_size(),
        crate::id_allocator::DEFAULT_LEASE_SIZE
    );
    assert!(matches!(default.cache().mode(), CacheMode::Memory));
    assert_eq!(
        default
            .slate()
            .to_writer_settings(None)
            .object_store_cache_options
            .root_folder,
        None
    );

    let slate = slatedb::Settings {
        flush_interval: Some(std::time::Duration::from_secs(7)),
        ..Default::default()
    };
    let cache = CacheConfig::new(VectorMemorySettings::default(), CacheMode::VectorMemoryOnly);
    let db = DbConfig::new()
        .with_high_degree_threshold(4096)
        .with_slate_settings(slate)
        .with_cache(cache.clone());
    let helix = HelixConfig::new(db);

    assert_eq!(helix.db().high_degree_threshold, 4096);
    assert_eq!(helix.db().cache(), &cache);
    assert_eq!(
        helix.db().slate().to_writer_settings(None).flush_interval,
        Some(std::time::Duration::from_secs(7))
    );
}

#[test]
fn config_error_and_path_wrappers_preserve_display_and_accessors() {
    let error = NonEmptyPathBuf::try_new("").expect_err("empty path is rejected");
    assert_eq!(error.to_string(), "cache path cannot be empty");
    assert!(std::error::Error::source(&error).is_none());

    let path = std::path::PathBuf::from("/tmp/cache");
    let non_empty = NonEmptyPathBuf::try_from(path.clone()).expect("path is nonempty");
    assert_eq!(non_empty.as_path(), path.as_path());
    assert_eq!(non_empty.to_path_buf(), path);

    let disk = DiskCacheConfig::try_new(non_empty.to_path_buf(), 2048)
        .expect("disk cache settings are valid");
    assert_eq!(disk.root(), std::path::Path::new("/tmp/cache"));
    assert_eq!(disk.bytes(), 2048);

    let roundtrip_path: std::path::PathBuf = non_empty.into();
    assert_eq!(roundtrip_path, std::path::PathBuf::from("/tmp/cache"));
}

#[test]
fn object_store_warm_levels_parse_and_project_to_slate_preload() {
    assert_eq!(
        " off "
            .parse::<ObjectStoreWarmLevel>()
            .expect("off level parses"),
        ObjectStoreWarmLevel::Off
    );
    assert_eq!(
        "L0".parse::<ObjectStoreWarmLevel>()
            .expect("L0 level parses"),
        ObjectStoreWarmLevel::L0
    );
    assert_eq!(
        "ALL"
            .parse::<ObjectStoreWarmLevel>()
            .expect("all level parses"),
        ObjectStoreWarmLevel::All
    );

    assert_eq!(ObjectStoreWarmLevel::Off.to_slate_preload(), None);
    assert_eq!(
        ObjectStoreWarmLevel::L0.to_slate_preload(),
        Some(slatedb::config::PreloadLevel::L0Sst)
    );
    assert_eq!(
        ObjectStoreWarmLevel::All.to_slate_preload(),
        Some(slatedb::config::PreloadLevel::AllSst)
    );

    let error = "latest"
        .parse::<ObjectStoreWarmLevel>()
        .expect_err("unknown object-store warm level should fail");
    assert!(error.contains("expected off, l0, or all"));
}

#[test]
fn slate_object_store_cache_settings_project_all_slate_options() {
    let scan_interval = std::time::Duration::from_secs(15);
    let settings = SlateObjectStoreCacheSettings::try_new(
        "/tmp/object-store",
        Some(512),
        4096,
        true,
        ObjectStoreWarmLevel::All,
        Some(scan_interval),
        32,
    )
    .expect("valid object-store cache settings");

    assert_eq!(settings.root(), std::path::Path::new("/tmp/object-store"));
    assert_eq!(settings.warm(), ObjectStoreWarmLevel::All);

    let options = settings.to_slate_options();
    assert_eq!(
        options.root_folder,
        Some(std::path::PathBuf::from("/tmp/object-store"))
    );
    assert_eq!(options.max_cache_size_bytes, Some(512));
    assert_eq!(options.part_size_bytes, 4096);
    assert!(options.cache_puts);
    assert_eq!(
        options.preload_disk_cache_on_startup,
        Some(slatedb::config::PreloadLevel::AllSst)
    );
    assert_eq!(options.scan_interval, Some(scan_interval));
    assert_eq!(options.max_open_file_handles, 32);

    let slate = super::SlateRuntimeConfig::new();
    assert_eq!(
        slate
            .to_writer_settings(Some(&settings))
            .object_store_cache_options
            .root_folder,
        Some(std::path::PathBuf::from("/tmp/object-store"))
    );
    assert_eq!(
        slate
            .to_reader_options(Some(&settings))
            .object_store_cache_options
            .root_folder,
        Some(std::path::PathBuf::from("/tmp/object-store"))
    );
    assert_eq!(
        slate
            .to_writer_settings(None)
            .object_store_cache_options
            .root_folder,
        None
    );
    assert_eq!(
        slate
            .to_reader_options(None)
            .object_store_cache_options
            .root_folder,
        None
    );
}

#[test]
fn cache_constructor_edges_and_accessors_are_explicit_contracts() {
    assert!(SlateObjectStoreCacheSettings::try_new(
        "/tmp/object-store",
        Some(1),
        4096,
        false,
        ObjectStoreWarmLevel::Off,
        None,
        0
    )
    .is_err());

    let slate_hybrid =
        SlateHybridCacheConfig::try_new(64, "/tmp/slate", 256).expect("valid slate cache");
    assert_eq!(slate_hybrid.memory_bytes(), 64);
    assert_eq!(
        slate_hybrid.disk().root(),
        std::path::Path::new("/tmp/slate")
    );
    assert_eq!(slate_hybrid.disk().bytes(), 256);
    assert!(SlateHybridCacheConfig::try_new(64, "", 256).is_err());
}

#[test]
fn cache_config_represents_vector_only_memory_and_hybrid_states() {
    let vector_only =
        CacheConfig::new(VectorMemorySettings::default(), CacheMode::VectorMemoryOnly);
    assert!(vector_only.object_store_cache().is_none());

    let memory = CacheConfig::new(VectorMemorySettings::default(), CacheMode::Memory);
    assert!(memory.object_store_cache().is_none());

    let object_store = SlateObjectStoreCacheSettings::try_new(
        "/tmp/object-store",
        Some(512),
        4096,
        false,
        ObjectStoreWarmLevel::L0,
        None,
        1,
    )
    .expect("valid object-store cache");
    let hybrid = CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Hybrid {
            slate_db: SlateHybridCacheConfig::try_new(64, "/tmp/slate", 128)
                .expect("valid Slate hybrid cache"),
            object_store,
        },
    );

    assert!(hybrid.object_store_cache().is_some());
    assert_eq!(
        hybrid
            .object_store_cache()
            .expect("hybrid object-store cache")
            .warm(),
        ObjectStoreWarmLevel::L0
    );
}

#[test]
fn cache_config_builder_replaces_vector_memory_and_mode_independently() {
    let vector_memory = VectorMemorySettings::try_new(
        VectorMemoryBudget::bounded(4096).expect("valid bounded budget"),
        11,
    )
    .expect("valid vector memory");
    let cache = CacheConfig::default()
        .with_vector_memory(vector_memory)
        .with_mode(CacheMode::VectorMemoryOnly);

    assert_eq!(cache.vector_memory(), &vector_memory);
    assert!(matches!(cache.mode(), CacheMode::VectorMemoryOnly));
    assert!(cache.object_store_cache().is_none());
}

#[test]
fn vector_memory_settings_are_always_enabled_and_may_be_bounded() {
    let unbounded = VectorMemorySettings::try_new(VectorMemoryBudget::unbounded_for_test(), 5)
        .expect("valid unbounded vector memory config");
    assert_eq!(unbounded.budget().bytes(), None);
    assert_eq!(unbounded.poll_interval_secs(), 5);
    assert!(matches!(
        unbounded.hydration(),
        VectorMemoryHydrationMode::Background { .. }
    ));

    let bounded = VectorMemorySettings::try_new(
        VectorMemoryBudget::bounded(1024).expect("valid bounded budget"),
        30,
    )
    .expect("valid bounded vector memory config");
    assert_eq!(bounded.budget().bytes(), Some(1024));
    assert_eq!(bounded.poll_interval_secs(), 30);

    let blocking = VectorMemorySettings::try_new_with_hydration(
        VectorMemoryBudget::unbounded_for_test(),
        VectorMemoryHydrationMode::blocking_then_background(7).expect("valid hydration mode"),
    )
    .expect("valid blocking vector memory config");
    assert!(matches!(
        blocking.hydration(),
        VectorMemoryHydrationMode::BlockingThenBackground { .. }
    ));
    assert_eq!(blocking.poll_interval_secs(), 7);

    let simhasher = SimHasherCacheSettings::try_new(4096, 3).unwrap();
    let configured = blocking.with_simhasher_cache(simhasher);
    assert_eq!(configured.simhasher_cache(), simhasher);
    assert_eq!(simhasher.bytes(), 4096);
    assert_eq!(simhasher.entries(), 3);
    assert_eq!(simhasher.maximum_f32_dimension(), 16);
}

#[test]
fn vector_memory_config_rejects_invalid_zero_values() {
    assert!(VectorMemoryBudget::bounded(0).is_err());
    assert!(VectorMemorySettings::try_new(VectorMemoryBudget::unbounded_for_test(), 0).is_err());
    assert_eq!(
        VectorMemorySettings::default().budget().bytes(),
        Some(super::DEFAULT_VECTOR_MEMORY_BUDGET_BYTES)
    );
    assert!(VectorMemoryHydrationMode::blocking_then_background(0).is_err());
    assert!(SimHasherCacheSettings::try_new(0, 1).is_err());
    assert!(SimHasherCacheSettings::try_new(1, 0).is_err());
    let defaults = SimHasherCacheSettings::default();
    assert_eq!(defaults.bytes(), 32 * 1024 * 1024);
    assert_eq!(defaults.entries(), 64);
    assert_eq!(defaults.maximum_f32_dimension(), 131_072);
}

#[test]
fn index_config_keeps_label_indexes_builtin() {
    let config = IndexConfig::new();

    assert!(config.has_node_label_index());
    assert!(config.has_edge_label_index());
    assert_eq!(config.node_equality_indexes().count(), 0);
    assert_eq!(config.edge_equality_indexes().count(), 0);

    let configured = IndexConfig::new()
        .with_equality_index("User", "email")
        .unwrap()
        .with_range_index("User", "age")
        .unwrap()
        .with_range_desc_index("User", "score")
        .unwrap()
        .with_edge_equality_index("LIKES", "kind")
        .unwrap()
        .with_edge_range_index("LIKES", "createdAt")
        .unwrap()
        .with_edge_range_desc_index("LIKES", "weight")
        .unwrap();
    assert!(configured.has_scoped_equality_index("User", "email"));
    assert!(configured.has_scoped_range_index("User", "age"));
    assert!(configured.has_any_scoped_range_index("User", "score"));
    assert!(configured.has_scoped_edge_equality_index("LIKES", "kind"));
    assert!(configured.has_scoped_edge_range_index("LIKES", "createdAt"));
    assert!(configured.has_any_scoped_edge_range_index("LIKES", "weight"));
    assert!(!configured.has_scoped_equality_index("", "email"));
    assert!(!configured.has_scoped_range_index_direction("", "age", RangeIndexDirection::Asc));
    assert!(!configured.has_scoped_edge_equality_index("", "kind"));
    assert!(!configured.has_scoped_edge_range_index_direction(
        "",
        "createdAt",
        RangeIndexDirection::Asc
    ));
    assert!(IndexConfig::new()
        .with_node_vector_index("Doc", "embedding", 0, VectorDistanceMetric::Cosine)
        .is_err());
    assert!(IndexConfig::new()
        .with_edge_vector_index("SIMILAR", "embedding", 0, VectorDistanceMetric::Cosine)
        .is_err());
}

#[test]
fn index_config_deduplicates_vector_indexes() {
    let definition =
        VectorIndexDefinition::new_node("Doc", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid vector definition");
    let config = IndexConfig::new()
        .with_vector_index(definition.clone())
        .with_vector_index(definition);

    assert_eq!(config.vector_indexes().count(), 1);
}

#[test]
fn vector_index_definition_exposes_validated_semantics_without_persistence_identity() {
    let definition = VectorIndexDefinition::new_edge(
        "MENTIONS",
        "embedding",
        4,
        VectorDistanceMetric::Euclidean,
    )
    .expect("valid vector definition")
    .with_tenant_property("orgId")
    .expect("valid tenant property")
    .with_m(32)
    .expect("valid connection limit")
    .with_m0(48)
    .expect("valid layer-0 connection limit")
    .with_ef_construction(128)
    .expect("valid construction beam")
    .with_ml(0.25)
    .expect("valid layer multiplier")
    .with_simhash_threshold(12)
    .expect("valid SimHash threshold")
    .with_sampling_ratio(0.25)
    .expect("valid sampling ratio")
    .with_adaptive_enabled(false)
    .with_adaptive_failure_prob(0.25)
    .expect("valid failure probability");
    assert_eq!(definition.element_type(), VectorElementType::Edge);
    assert_eq!(
        definition.key(),
        (
            VectorElementType::Edge,
            "MENTIONS".into(),
            "embedding".into()
        )
    );
    assert_eq!(definition.dimension(), 4);
    assert_eq!(definition.metric(), VectorDistanceMetric::Euclidean);
    assert_eq!(definition.m(), 32);
    assert_eq!(definition.m0(), 48);
    assert_eq!(definition.ef_construction(), 128);
    assert_eq!(definition.ml(), 0.25);
    assert_eq!(definition.simhash_threshold(), 12);
    assert_eq!(definition.sampling_ratio(), 0.25);
    assert!(!definition.adaptive_enabled());
    assert_eq!(definition.adaptive_failure_prob(), 0.25);

    let definition =
        VectorIndexDefinition::new_node("Doc", "embedding", 3, VectorDistanceMetric::Cosine)
            .unwrap();
    assert!(definition.clone().with_m(0).is_err());
    assert!(definition.clone().with_m0(0).is_err());
    assert!(definition.clone().with_ef_construction(0).is_err());
    assert!(definition.clone().with_ml(-1.0).is_err());
    assert!(definition
        .clone()
        .with_simhash_threshold(SIMHASH_BITS + 8)
        .is_err());
    assert!(definition.clone().with_sampling_ratio(2.0).is_err());
    assert!(definition.with_adaptive_failure_prob(2.0).is_err());

    assert!(
        VectorIndexDefinition::new_node("Doc", "embedding", 0, VectorDistanceMetric::Cosine)
            .is_err()
    );
}

#[test]
fn runtime_index_catalog_projects_typed_planner_snapshot() {
    let mut indexes = RuntimeIndexCatalog::new()
        .with_range_index("User", "age")
        .expect("valid node range")
        .with_edge_equality_index("FOLLOWS", "kind")
        .expect("valid edge equality")
        .with_edge_range_desc_index("FOLLOWS", "since")
        .expect("valid edge range");
    indexes.insert_dynamic_index(&v2_secondary(
        SecondaryIndexDefinition::node_unique_equality("User", "email")
            .expect("valid unique node equality"),
    ));

    let snapshot = indexes.planner_snapshot();
    let email = catalog::ScopedPropertyKey::try_new("User", "email").expect("valid key");
    let age = catalog::ScopedPropertyDirectionKey::try_new(
        "User",
        "age",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .expect("valid node range key");
    let edge_kind =
        catalog::ScopedPropertyKey::try_new("FOLLOWS", "kind").expect("valid edge equality key");
    let edge_since = catalog::ScopedPropertyDirectionKey::try_new(
        "FOLLOWS",
        "since",
        helix_ast::index::RangeIndexDirection::Desc,
    )
    .expect("valid direction key");

    assert_eq!(
        snapshot.node_eq.get(&email).map(|meta| meta.uniqueness),
        Some(catalog::IndexUniqueness::Unique)
    );
    assert!(snapshot.node_range.contains_key(&age));
    assert!(snapshot.edge_eq.contains_key(&edge_kind));
    assert!(snapshot.edge_range.contains_key(&edge_since));
    assert!(
        indexes.contains_node_equality_scoped(&scoped_secondary_index_property("User", "email"))
    );
    assert!(indexes
        .node_equality_indexes()
        .any(|key| key == scoped_secondary_index_property("User", "email")));
}

#[test]
fn secondary_index_definition_constructors_preserve_validated_semantics() {
    let definitions = [
        SecondaryIndexDefinition::node_equality("User", "email").expect("node equality"),
        SecondaryIndexDefinition::node_unique_equality("User", "slug").expect("unique equality"),
        SecondaryIndexDefinition::node_range_desc("User", "age").expect("node range"),
        SecondaryIndexDefinition::edge_equality("LIKES", "kind").expect("edge equality"),
        SecondaryIndexDefinition::edge_range("LIKES", "rank").expect("edge ascending range"),
        SecondaryIndexDefinition::edge_range_desc("LIKES", "createdAt").expect("edge range"),
    ];

    for definition in definitions {
        assert_eq!(
            definition.display_scope(),
            format!("{}.{}", definition.label(), definition.property())
        );
        assert_eq!(
            super::split_scoped_secondary_index_property(&definition.scoped_property()),
            Some((definition.label(), definition.property()))
        );
    }
    let scoped = scoped_secondary_index_property("User", "email");
    assert!(super::is_scoped_secondary_index_property(&scoped));
    assert!(!super::is_scoped_secondary_index_property("email"));
    assert_eq!(
        super::split_scoped_secondary_index_property("\u{1f}email"),
        None
    );
    assert_eq!(
        super::split_scoped_secondary_index_property("User\u{1f}"),
        None
    );

    let unique = SecondaryIndexDefinition::node_unique_equality("User", "slug").unwrap();
    assert_eq!(unique.element_type(), SecondaryIndexElementType::Node);
    assert_eq!(unique.kind(), SecondaryIndexKind::Equality);
    assert!(unique.unique());
    assert!(unique.is_node_equality());
    assert!(unique.is_unique_node_equality());
    assert!(unique.is_node());
    assert!(!unique.is_edge());
}

#[test]
fn runtime_index_catalog_dynamic_insertion_covers_all_iterators() {
    let node_unique = SecondaryIndexDefinition::node_unique_equality("User", "slug").unwrap();
    let edge_range = SecondaryIndexDefinition::edge_range_desc("LIKES", "createdAt").unwrap();
    let vector =
        VectorIndexDefinition::new_node("Doc", "embedding", 3, VectorDistanceMetric::Cosine)
            .unwrap();
    let text = TextIndexDefinition::new_node("Doc", "body").unwrap();

    let mut catalog = RuntimeIndexCatalog::new()
        .with_equality_index("User", "email")
        .unwrap()
        .with_range_index("User", "rank")
        .unwrap()
        .with_range_desc_index("User", "age")
        .unwrap()
        .with_edge_equality_index("LIKES", "kind")
        .unwrap()
        .with_edge_range_index("LIKES", "rank")
        .unwrap()
        .with_edge_range_desc_index("LIKES", "createdAt")
        .unwrap()
        .with_node_vector_index("Doc", "embedding", 3, VectorDistanceMetric::Cosine)
        .unwrap()
        .with_node_text_index("Doc", "body")
        .unwrap()
        .with_text_index(text.clone());
    catalog.insert_dynamic_index(&v2_secondary(node_unique.clone()));
    catalog.insert_dynamic_index(&v2_secondary(edge_range.clone()));
    catalog.insert_dynamic_index(&v2_vector(vector.clone()));
    catalog.insert_dynamic_index(&v2_text(text.clone()));

    let node_email = scoped_secondary_index_property("User", "email");
    let node_slug = scoped_secondary_index_property("User", "slug");
    let node_age = scoped_secondary_index_property("User", "age");
    let node_rank = scoped_secondary_index_property("User", "rank");
    let edge_kind = scoped_secondary_index_property("LIKES", "kind");
    let edge_created = scoped_secondary_index_property("LIKES", "createdAt");
    let edge_rank = scoped_secondary_index_property("LIKES", "rank");

    assert!(catalog.contains_node_equality_scoped(&node_email));
    assert!(!catalog.contains_node_unique_equality_scoped(&node_email));
    assert!(catalog.contains_node_unique_equality_scoped(&node_slug));
    assert!(!catalog.contains_node_unique_equality_scoped("not-scoped"));
    assert!(catalog.contains_node_range_scoped(&node_rank));
    assert!(catalog.contains_node_range_desc_scoped(&node_age));
    assert!(!catalog.contains_node_range_scoped(&node_age));
    assert!(catalog.contains_edge_equality_scoped(&edge_kind));
    assert!(catalog.contains_edge_range_scoped(&edge_rank));
    assert!(catalog.contains_edge_range_desc_scoped(&edge_created));
    assert!(!catalog.contains_edge_range_scoped(&edge_created));
    assert!(!catalog.contains_node_equality_scoped("not-scoped"));

    assert!(catalog.node_equality_indexes().any(|key| key == node_email));
    assert!(catalog
        .node_unique_equality_indexes()
        .any(|key| key == node_slug));
    assert!(catalog.node_range_desc_indexes().any(|key| key == node_age));
    assert!(catalog.node_range_indexes().any(|key| key == node_rank));
    assert!(catalog.edge_equality_indexes().any(|key| key == edge_kind));
    assert!(catalog
        .edge_range_desc_indexes()
        .any(|key| key == edge_created));
    assert!(catalog.edge_range_indexes().any(|key| key == edge_rank));
    assert_eq!(catalog.vector_indexes().count(), 1);
    assert_eq!(catalog.text_indexes().count(), 1);
}

#[test]
fn runtime_catalog_converts_planner_create_and_drop_specs() {
    let node_eq_key = catalog::ScopedPropertyKey::try_new("User", "email").expect("valid key");
    let node_range_key = catalog::ScopedPropertyDirectionKey::try_new(
        "User",
        "score",
        helix_ast::index::RangeIndexDirection::Desc,
    )
    .expect("valid range key");
    let edge_eq_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "kind").expect("valid key");
    let edge_range_key = catalog::ScopedPropertyDirectionKey::try_new(
        "FOLLOWS",
        "since",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .expect("valid range key");
    let node_vector_key =
        catalog::ScopedPropertyKey::try_new("Doc", "embedding").expect("valid key");
    let edge_vector_key =
        catalog::ScopedPropertyKey::try_new("SIMILAR", "embedding").expect("valid key");
    let node_text_key = catalog::ScopedPropertyKey::try_new("Doc", "body").expect("valid key");
    let edge_text_key = catalog::ScopedPropertyKey::try_new("MENTIONS", "body").expect("valid key");
    let tenant_scope = catalog::SearchIndexScope::Tenant {
        property: ir::NonEmptyString::new("firmId").expect("tenant property is nonempty"),
    };

    let node_eq = super::runtime_catalog::dynamic_index_definition_from_create_spec(
        &ir::IndexDdlCreateSpec::NodeEquality {
            key: node_eq_key.clone(),
            uniqueness: catalog::IndexUniqueness::Unique,
        },
    )
    .expect("node equality converts");
    assert!(matches!(
        node_eq,
        ValidatedDynamicIndexDefinition::Secondary(
            ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. }
        )
    ));

    let node_range = super::runtime_catalog::dynamic_index_definition_from_create_spec(
        &ir::IndexDdlCreateSpec::NodeRange {
            key: node_range_key.clone(),
        },
    )
    .expect("node range converts");
    assert!(matches!(
        node_range,
        ValidatedDynamicIndexDefinition::Secondary(ValidatedSecondaryIndexDefinition::NodeRange {
            direction: RangeIndexDirection::Desc,
            ..
        })
    ));

    let edge_eq = super::runtime_catalog::dynamic_index_definition_from_create_spec(
        &ir::IndexDdlCreateSpec::EdgeEquality {
            key: edge_eq_key.clone(),
        },
    )
    .expect("edge equality converts");
    assert!(matches!(
        edge_eq,
        ValidatedDynamicIndexDefinition::Secondary(
            ValidatedSecondaryIndexDefinition::EdgeEquality { .. }
        )
    ));

    let edge_range = super::runtime_catalog::dynamic_index_definition_from_create_spec(
        &ir::IndexDdlCreateSpec::EdgeRange {
            key: edge_range_key.clone(),
        },
    )
    .expect("edge range converts");
    assert!(matches!(
        edge_range,
        ValidatedDynamicIndexDefinition::Secondary(ValidatedSecondaryIndexDefinition::EdgeRange {
            direction: RangeIndexDirection::Asc,
            ..
        })
    ));

    let node_vector_spec = ir::IndexDdlCreateSpec::NodeVector {
        key: node_vector_key.clone(),
        dimension: ir::VectorIndexDimension::new(4).expect("positive dimension"),
        metric: ir::VectorIndexMetric::Manhattan,
        scope: tenant_scope.clone(),
    };
    let ValidatedDynamicIndexDefinition::Vector(validated_node_vector) =
        super::runtime_catalog::dynamic_index_definition_from_create_spec(&node_vector_spec)
            .expect("node vector dynamic definition converts")
    else {
        panic!("node vector spec must produce the vector ADT variant");
    };
    assert_eq!(validated_node_vector.element_kind(), IndexElementKind::Node);
    assert_eq!(
        validated_node_vector.metric(),
        VectorDistanceMetric::Manhattan
    );
    assert_eq!(
        validated_node_vector
            .tenant_property()
            .map(|property| property.as_str()),
        Some("firmId")
    );
    let node_vector = validated_node_vector.to_runtime();

    let edge_vector_spec = ir::IndexDdlCreateSpec::EdgeVector {
        key: edge_vector_key.clone(),
        dimension: ir::VectorIndexDimension::new(5).expect("positive dimension"),
        metric: ir::VectorIndexMetric::Euclidean,
        scope: catalog::SearchIndexScope::Unscoped,
    };
    let ValidatedDynamicIndexDefinition::Vector(validated_edge_vector) =
        super::runtime_catalog::dynamic_index_definition_from_create_spec(&edge_vector_spec)
            .expect("edge vector dynamic definition converts")
    else {
        panic!("edge vector spec must produce the vector ADT variant");
    };
    assert_eq!(validated_edge_vector.element_kind(), IndexElementKind::Edge);
    assert_eq!(
        validated_edge_vector.metric(),
        VectorDistanceMetric::Euclidean
    );
    assert!(validated_edge_vector.tenant_property().is_none());
    let edge_vector = validated_edge_vector.to_runtime();

    let node_text_spec = ir::IndexDdlCreateSpec::NodeText {
        key: node_text_key.clone(),
        scope: tenant_scope,
    };
    let ValidatedDynamicIndexDefinition::Text(validated_node_text) =
        super::runtime_catalog::dynamic_index_definition_from_create_spec(&node_text_spec)
            .expect("node text dynamic definition converts")
    else {
        panic!("node text spec must produce the text ADT variant");
    };
    assert_eq!(validated_node_text.element_kind(), IndexElementKind::Node);
    assert_eq!(
        validated_node_text
            .tenant_property()
            .map(|property| property.as_str()),
        Some("firmId")
    );
    let node_text = validated_node_text.to_runtime();

    let edge_text = super::runtime_catalog::dynamic_index_definition_from_create_spec(
        &ir::IndexDdlCreateSpec::EdgeText {
            key: edge_text_key.clone(),
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .expect("edge text converts");
    assert!(matches!(
        edge_text,
        ValidatedDynamicIndexDefinition::Text(ref definition)
            if definition.element_kind() == IndexElementKind::Edge
                && definition.tenant_property().is_none()
    ));

    let ValidatedDynamicIndexDefinition::Text(edge_text) = edge_text else {
        panic!("edge text create must return a text definition");
    };
    let mut drop_catalog = IndexConfig::new();
    for definition in [
        v2_secondary(SecondaryIndexDefinition::node_equality("User", "email").unwrap()),
        v2_secondary(SecondaryIndexDefinition::node_range_desc("User", "score").unwrap()),
        v2_secondary(SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind").unwrap()),
        v2_secondary(SecondaryIndexDefinition::edge_range("FOLLOWS", "since").unwrap()),
        v2_vector(node_vector.clone()),
        v2_vector(edge_vector.clone()),
        v2_text(node_text.clone()),
        ValidatedDynamicIndexDefinition::Text(edge_text),
    ] {
        drop_catalog.insert_dynamic_index(&definition);
    }

    let node_eq_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeEquality {
            key: node_eq_key.clone(),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
        &drop_catalog,
    )
    .expect("node equality drop converts");
    assert_eq!(
        node_eq_drop,
        v2_secondary(SecondaryIndexDefinition::node_equality("User", "email").unwrap())
    );

    let mismatched_node_eq_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeEquality {
            key: node_eq_key.clone(),
            uniqueness: catalog::IndexUniqueness::Unique,
        },
        &drop_catalog,
    );
    assert!(mismatched_node_eq_drop.is_err());

    let missing_node_eq_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeEquality {
            key: catalog::ScopedPropertyKey::try_new("Missing", "email").expect("valid key"),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
        &drop_catalog,
    );
    assert!(missing_node_eq_drop.is_err());

    let unique_key = catalog::ScopedPropertyKey::try_new("Account", "id").expect("valid key");
    let mut unique_catalog = IndexConfig::new();
    unique_catalog.insert_dynamic_index(&v2_secondary(
        SecondaryIndexDefinition::node_unique_equality("Account", "id").unwrap(),
    ));
    let unique_node_eq_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeEquality {
            key: unique_key,
            uniqueness: catalog::IndexUniqueness::Unique,
        },
        &unique_catalog,
    )
    .expect("unique node equality drop converts");
    assert_eq!(
        unique_node_eq_drop,
        v2_secondary(SecondaryIndexDefinition::node_unique_equality("Account", "id").unwrap())
    );

    let node_range_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeRange {
            key: node_range_key.clone(),
        },
        &drop_catalog,
    )
    .expect("node range drop converts");
    assert_eq!(
        node_range_drop,
        v2_secondary(
            SecondaryIndexDefinition::node_range_with_direction(
                "User",
                "score",
                RangeIndexDirection::Desc,
            )
            .unwrap()
        )
    );
    assert!(
        super::runtime_catalog::dynamic_index_definition_from_drop_spec(
            &ir::IndexDdlDropSpec::NodeRange {
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "Missing",
                    "score",
                    helix_ast::index::RangeIndexDirection::Desc,
                )
                .expect("valid missing range key"),
            },
            &drop_catalog,
        )
        .is_err()
    );

    let edge_eq_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeEquality {
            key: edge_eq_key.clone(),
        },
        &drop_catalog,
    )
    .expect("edge equality drop converts");
    assert_eq!(
        edge_eq_drop,
        v2_secondary(SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind").unwrap())
    );
    assert!(
        super::runtime_catalog::dynamic_index_definition_from_drop_spec(
            &ir::IndexDdlDropSpec::EdgeEquality {
                key: catalog::ScopedPropertyKey::try_new("Missing", "kind")
                    .expect("valid missing edge equality key"),
            },
            &drop_catalog,
        )
        .is_err()
    );

    let edge_range_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeRange {
            key: edge_range_key.clone(),
        },
        &drop_catalog,
    )
    .expect("edge range drop converts");
    assert_eq!(
        edge_range_drop,
        v2_secondary(
            SecondaryIndexDefinition::edge_range_with_direction(
                "FOLLOWS",
                "since",
                RangeIndexDirection::Asc,
            )
            .unwrap()
        )
    );
    assert!(
        super::runtime_catalog::dynamic_index_definition_from_drop_spec(
            &ir::IndexDdlDropSpec::EdgeRange {
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "Missing",
                    "since",
                    helix_ast::index::RangeIndexDirection::Asc,
                )
                .expect("valid missing edge range key"),
            },
            &drop_catalog,
        )
        .is_err()
    );

    let node_vector_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeVector {
            key: node_vector_key.clone(),
        },
        &drop_catalog,
    )
    .expect("node vector drop converts");
    assert!(matches!(
        node_vector_drop,
        ValidatedDynamicIndexDefinition::Vector(definition)
            if definition.to_runtime() == node_vector
    ));

    let edge_vector_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeVector {
            key: edge_vector_key.clone(),
        },
        &drop_catalog,
    )
    .expect("edge vector drop converts");
    assert!(matches!(
        edge_vector_drop,
        ValidatedDynamicIndexDefinition::Vector(definition)
            if definition.to_runtime() == edge_vector
    ));

    let missing_dynamic_vector = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeVector {
            key: catalog::ScopedPropertyKey::try_new("Missing", "embedding")
                .expect("valid missing key"),
        },
        &drop_catalog,
    );
    assert!(missing_dynamic_vector.is_err());

    let edge_text_drop = super::runtime_catalog::dynamic_index_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeText {
            key: edge_text_key.clone(),
        },
        &drop_catalog,
    )
    .expect("edge text drop converts");
    assert!(matches!(
        edge_text_drop,
        ValidatedDynamicIndexDefinition::Text(definition)
            if definition.element_kind() == IndexElementKind::Edge
                && definition.label().as_str() == "MENTIONS"
                && definition.property().as_str() == "body"
    ));
    assert!(
        super::runtime_catalog::dynamic_index_definition_from_drop_spec(
            &ir::IndexDdlDropSpec::NodeText {
                key: catalog::ScopedPropertyKey::try_new("Missing", "body")
                    .expect("valid missing text key"),
            },
            &drop_catalog,
        )
        .is_err()
    );

    let Some(node_text_drop) = super::runtime_catalog::text_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeText {
            key: node_text_key.clone(),
        },
        &drop_catalog,
    )
    .expect("node text drop converts") else {
        panic!("node text drop should produce a text definition");
    };
    assert_eq!(node_text_drop.tenant_property(), Some("firmId"));
    let Some(edge_text_drop) = super::runtime_catalog::text_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeText { key: edge_text_key },
        &drop_catalog,
    )
    .expect("edge text drop converts") else {
        panic!("edge text drop should produce a text definition");
    };
    assert!(edge_text_drop.tenant_property().is_none());
    assert!(super::runtime_catalog::text_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeText {
            key: catalog::ScopedPropertyKey::try_new("Missing", "body")
                .expect("valid missing text key"),
        },
        &drop_catalog,
    )
    .is_err());

    assert!(super::runtime_catalog::text_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeEquality { key: edge_eq_key },
        &drop_catalog
    )
    .expect("non-text drop is accepted")
    .is_none());

    let vector_config = IndexConfig::new()
        .with_vector_index(node_vector)
        .with_vector_index(edge_vector);
    let Some(existing_node_vector) = super::runtime_catalog::vector_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeVector {
            key: node_vector_key,
        },
        &vector_config,
    )
    .expect("node vector drop definition converts") else {
        panic!("node vector drop should produce a vector definition");
    };
    assert_eq!(existing_node_vector.element_type(), VectorElementType::Node);
    let Some(existing_edge_vector) = super::runtime_catalog::vector_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeVector {
            key: edge_vector_key,
        },
        &vector_config,
    )
    .expect("edge vector drop definition converts") else {
        panic!("edge vector drop should produce a vector definition");
    };
    assert_eq!(existing_edge_vector.element_type(), VectorElementType::Edge);
    assert!(super::runtime_catalog::vector_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeText {
            key: catalog::ScopedPropertyKey::try_new("Doc", "body").expect("valid text key"),
        },
        &vector_config,
    )
    .expect("non-vector drop is accepted")
    .is_none());

    let missing_vector = super::runtime_catalog::vector_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::NodeVector {
            key: catalog::ScopedPropertyKey::try_new("Missing", "embedding")
                .expect("valid missing key"),
        },
        &IndexConfig::new(),
    );
    assert!(missing_vector.is_err());
    let missing_edge_vector = super::runtime_catalog::vector_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeVector {
            key: catalog::ScopedPropertyKey::try_new("Missing", "embedding")
                .expect("valid missing edge vector key"),
        },
        &IndexConfig::new(),
    );
    assert!(missing_edge_vector.is_err());
}

#[test]
fn canonical_drop_resolution_preserves_settings_and_validates_every_identity_lane() {
    let property_key = || catalog::ScopedPropertyKey::try_new("Indexed", "value").unwrap();
    let range_key = || {
        catalog::ScopedPropertyDirectionKey::try_new(
            "Indexed",
            "value",
            helix_ast::index::RangeIndexDirection::Desc,
        )
        .unwrap()
    };
    let cases = [
        (
            ir::IndexDdlDropSpec::NodeEquality {
                key: property_key(),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
        ),
        (
            ir::IndexDdlDropSpec::NodeRange { key: range_key() },
            IndexIdentityFamily::SecondaryRange,
            IndexElementKind::Node,
        ),
        (
            ir::IndexDdlDropSpec::EdgeEquality {
                key: property_key(),
            },
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Edge,
        ),
        (
            ir::IndexDdlDropSpec::EdgeRange { key: range_key() },
            IndexIdentityFamily::SecondaryRange,
            IndexElementKind::Edge,
        ),
        (
            ir::IndexDdlDropSpec::NodeVector {
                key: property_key(),
            },
            IndexIdentityFamily::Vector,
            IndexElementKind::Node,
        ),
        (
            ir::IndexDdlDropSpec::EdgeVector {
                key: property_key(),
            },
            IndexIdentityFamily::Vector,
            IndexElementKind::Edge,
        ),
        (
            ir::IndexDdlDropSpec::NodeText {
                key: property_key(),
            },
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
        ),
        (
            ir::IndexDdlDropSpec::EdgeText {
                key: property_key(),
            },
            IndexIdentityFamily::Text,
            IndexElementKind::Edge,
        ),
    ];
    for (spec, expected_family, expected_kind) in cases {
        let identity = super::runtime_catalog::dynamic_index_identity_from_drop_spec(&spec)
            .expect("validated DROP has one canonical identity");
        assert_eq!(identity.family(), expected_family);
        assert_eq!(identity.element_kind(), expected_kind);
        assert_eq!(identity.label().as_str(), "Indexed");
        assert_eq!(identity.property().as_str(), "value");
    }

    let vector = v2_vector(
        VectorIndexDefinition::new_node("Indexed", "value", 7, VectorDistanceMetric::Manhattan)
            .unwrap()
            .with_m(23)
            .unwrap()
            .with_ef_construction(91)
            .unwrap(),
    );
    assert_eq!(
        super::runtime_catalog::dynamic_index_definition_from_canonical_drop_spec(
            &ir::IndexDdlDropSpec::NodeVector {
                key: property_key(),
            },
            &vector,
        )
        .expect("vector DROP retains canonical tuning"),
        vector
    );

    let unique =
        v2_secondary(SecondaryIndexDefinition::node_unique_equality("Indexed", "value").unwrap());
    assert!(
        super::runtime_catalog::dynamic_index_definition_from_canonical_drop_spec(
            &ir::IndexDdlDropSpec::NodeEquality {
                key: property_key(),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
            &unique,
        )
        .is_err()
    );
}

#[test]
fn vector_index_definition_rejects_invalid_params() {
    let definition =
        VectorIndexDefinition::new_node("Doc", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid vector definition");

    assert!(definition.clone().with_m(0).is_err());
    assert!(definition.clone().with_m0(0).is_err());
    assert!(definition.clone().with_ef_construction(0).is_err());
    assert!(definition
        .clone()
        .with_simhash_threshold(SIMHASH_BITS + 1)
        .is_err());
    assert!(definition.clone().with_sampling_ratio(f32::NAN).is_err());
    assert!(definition.with_adaptive_failure_prob(f32::NAN).is_err());
}

#[test]
fn secondary_backfill_batch_rows_and_search_limits_are_explicit_contracts() {
    assert_eq!(
        SecondaryBackfillBatchRows::new(32)
            .expect("positive batch rows")
            .get(),
        32
    );
    assert_eq!(SecondaryBackfillBatchRows::new(0), None);
    assert_eq!(
        DbConfig::new().search_index_backfill(),
        SearchIndexBackfillLimits::default()
    );
}
