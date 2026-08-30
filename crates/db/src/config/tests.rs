//! Contract tests for database, cache, and index configuration validation.

use super::{
    scoped_secondary_index_property, CacheConfig, CacheMode, CacheWarmMode, DbConfig,
    DiskCacheConfig, EdgeEncoding, EdgeUpdatePolicy, FtsHybridCacheConfig, FtsMemoryCacheConfig,
    FtsWarmConfig, HelixConfig, MigrationActiveIntervalMillis, MigrationBatchBytes,
    MigrationBatchRows, MigrationIdleIntervalMillis, MigrationTuning, MigrationWorkerMode,
    NonEmptyPathBuf, ObjectStoreWarmLevel, RangeIndexDirection, RuntimeIndexCatalog,
    SearchIndexBackfillLimits, SecondaryIndexDefinition, SecondaryIndexElementType,
    SecondaryIndexKind, SecondaryIndexLifecycleBatchRows, SimHasherCacheSettings,
    SlateHybridCacheConfig, SlateMemoryCacheConfig, SlateObjectStoreCacheSettings, SlateWarmConfig,
    TextAnalyzerKind, TextElementType, TextIndexDefinition, VectorElementType,
    VectorIndexDefinition, VectorMemoryBudget, VectorMemoryHydrationMode, VectorMemorySettings,
};
use crate::index_lifecycle::{
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
    assert!(matches!(config.cache().mode(), CacheMode::Memory { .. }));

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
    assert!(matches!(default.cache().mode(), CacheMode::Memory { .. }));
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
        wal_replay: slatedb::config::WalReplaySettings {
            max_concurrent_objects: 17,
            max_inflight_bytes: 23 * 1024 * 1024,
        },
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
    assert_eq!(
        helix.db().slate().to_reader_options(None).wal_replay,
        slatedb::config::WalReplaySettings {
            max_concurrent_objects: 17,
            max_inflight_bytes: 23 * 1024 * 1024,
        }
    );
    assert_eq!(
        helix.db().slate().to_reader_options(None).wal_poll_interval,
        std::time::Duration::from_secs(1)
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
    assert!(SlateMemoryCacheConfig::try_new(0, 1).is_err());
    assert!(SlateMemoryCacheConfig::try_new(1, 0).is_err());
    assert!(super::SlateWarmConfig::background(0, 1).is_err());
    assert!(super::SlateWarmConfig::blocking(1, 0).is_err());
    assert!(FtsWarmConfig::background(0, None).is_err());
    assert!(FtsWarmConfig::blocking(1, Some(0)).is_err());
    assert!(FtsMemoryCacheConfig::try_new(0, FtsWarmConfig::Off, 30).is_err());
    assert!(FtsMemoryCacheConfig::try_new(1, FtsWarmConfig::Off, 0).is_err());
}

#[test]
fn warm_mode_and_slate_cache_contracts_cover_every_variant_and_bound() {
    for (input, expected) in [
        (" BLOCKING ", CacheWarmMode::Blocking),
        ("background", CacheWarmMode::Background),
        ("Off", CacheWarmMode::Off),
    ] {
        assert_eq!(input.parse::<CacheWarmMode>().unwrap(), expected);
    }
    assert!("eager"
        .parse::<CacheWarmMode>()
        .unwrap_err()
        .contains("expected blocking, background, or off"));

    let memory = SlateMemoryCacheConfig::try_new(u64::MAX, 1).unwrap();
    assert_eq!(memory.block_bytes(), u64::MAX);
    assert_eq!(memory.metadata_bytes(), 1);
    assert_eq!(memory.total_bytes(), u64::MAX);

    let background = SlateWarmConfig::background(2, 3).unwrap();
    let blocking = SlateWarmConfig::blocking(4, 5).unwrap();
    for (warm, mode, concurrency, limit) in [
        (SlateWarmConfig::Off, CacheWarmMode::Off, 1, 0),
        (background, CacheWarmMode::Background, 2, 3),
        (blocking, CacheWarmMode::Blocking, 4, 5),
    ] {
        assert_eq!(warm.mode(), mode);
        assert_eq!(warm.concurrency(), concurrency);
        assert_eq!(warm.startup_sst_limit(), limit);
    }
    assert!(SlateWarmConfig::background(1, 0).is_err());
    assert!(SlateWarmConfig::blocking(0, 1).is_err());

    assert!(SlateObjectStoreCacheSettings::try_new(
        "",
        None,
        1,
        false,
        ObjectStoreWarmLevel::Off,
        None,
        1,
    )
    .is_err());
    let uncapped = SlateObjectStoreCacheSettings::try_new(
        "/tmp/uncapped-object-store",
        None,
        2,
        false,
        ObjectStoreWarmLevel::Off,
        None,
        3,
    )
    .unwrap()
    .to_slate_options();
    assert_eq!(uncapped.max_cache_size_bytes, None);
    assert_eq!(uncapped.scan_interval, None);
}

#[test]
fn fts_cache_contracts_cover_every_mode_accessor_and_invalid_tier() {
    let background = FtsWarmConfig::background(2, Some(3)).unwrap();
    let blocking = FtsWarmConfig::blocking(4, None).unwrap();
    for (warm, mode, concurrency, generation_limit) in [
        (FtsWarmConfig::Off, CacheWarmMode::Off, 1, None),
        (background.clone(), CacheWarmMode::Background, 2, Some(3)),
        (blocking, CacheWarmMode::Blocking, 4, None),
    ] {
        assert_eq!(warm.mode(), mode);
        assert_eq!(warm.concurrency(), concurrency);
        assert_eq!(warm.startup_generation_limit(), generation_limit);
    }
    assert!(FtsWarmConfig::background(1, Some(0)).is_err());
    assert!(FtsWarmConfig::blocking(0, None).is_err());

    let memory = FtsMemoryCacheConfig::try_new(7, background.clone(), 11).unwrap();
    assert_eq!(memory.memory_bytes(), 7);
    assert_eq!(memory.warm(), &background);
    assert_eq!(memory.warm_mode(), CacheWarmMode::Background);
    assert_eq!(memory.warm_concurrency(), 2);
    assert_eq!(memory.startup_generation_limit(), Some(3));
    assert_eq!(
        memory.generation_grace_period(),
        std::time::Duration::from_secs(11)
    );

    let hybrid =
        FtsHybridCacheConfig::try_new(13, "/tmp/fts-complete", 17, background.clone(), 19).unwrap();
    assert_eq!(hybrid.memory_bytes(), 13);
    assert_eq!(
        hybrid.disk().root(),
        std::path::Path::new("/tmp/fts-complete")
    );
    assert_eq!(hybrid.warm(), &background);
    assert_eq!(hybrid.warm_mode(), CacheWarmMode::Background);
    assert_eq!(hybrid.warm_concurrency(), 2);
    assert_eq!(hybrid.startup_generation_limit(), Some(3));
    assert_eq!(
        hybrid.generation_grace_period(),
        std::time::Duration::from_secs(19)
    );
    assert_eq!(
        hybrid.disk_root(),
        std::path::PathBuf::from("/tmp/fts-complete")
    );
    assert_eq!(hybrid.disk_bytes(), 17);

    assert!(FtsHybridCacheConfig::try_new(0, "/tmp/fts", 1, FtsWarmConfig::Off, 1).is_err());
    assert!(FtsHybridCacheConfig::try_new(1, "", 1, FtsWarmConfig::Off, 1).is_err());
    assert!(FtsHybridCacheConfig::try_new(1, "/tmp/fts", 0, FtsWarmConfig::Off, 1).is_err());
    assert!(FtsHybridCacheConfig::try_new(1, "/tmp/fts", 1, FtsWarmConfig::Off, 0).is_err());
}

#[test]
fn cache_config_represents_vector_only_memory_and_hybrid_states() {
    let vector_only =
        CacheConfig::new(VectorMemorySettings::default(), CacheMode::VectorMemoryOnly);
    assert!(vector_only.object_store_cache().is_none());

    let memory = CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Memory {
            slate_db: SlateMemoryCacheConfig::try_new(48, 16).expect("valid Slate memory cache"),
            slate_warm: Default::default(),
            fts: Some(FtsMemoryCacheConfig::default()),
        },
    );
    assert!(memory.object_store_cache().is_none());
    assert_eq!(memory.fts_memory_bytes(), 64 * 1024 * 1024);

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
            slate_warm: Default::default(),
            fts: Some(
                FtsHybridCacheConfig::try_new(32, "/tmp/fts", 64, FtsWarmConfig::Off, 30)
                    .expect("valid FTS hybrid cache"),
            ),
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
    assert_eq!(hybrid.fts_memory_bytes(), 32);
    assert_eq!(hybrid.fts_disk_bytes(), 64);
}

#[test]
fn cache_config_projects_every_optional_fts_and_warm_lane() {
    let vector_only =
        CacheConfig::new(VectorMemorySettings::default(), CacheMode::VectorMemoryOnly);
    assert_eq!(vector_only.slate_warm(), None);
    assert_eq!(vector_only.fts_memory_bytes(), 0);
    assert_eq!(vector_only.fts_disk_root(), None);
    assert_eq!(vector_only.fts_disk_bytes(), 0);
    assert_eq!(vector_only.fts_warm_mode(), CacheWarmMode::Off);
    assert_eq!(vector_only.fts_warm_concurrency(), 1);
    assert_eq!(vector_only.fts_startup_generation_limit(), None);
    assert_eq!(
        vector_only.fts_generation_grace_period(),
        std::time::Duration::from_secs(300)
    );

    let memory_without_fts = CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Memory {
            slate_db: SlateMemoryCacheConfig::try_new(1, 1).unwrap(),
            slate_warm: SlateWarmConfig::Off,
            fts: None,
        },
    );
    assert_eq!(memory_without_fts.slate_warm(), Some(&SlateWarmConfig::Off));
    assert_eq!(memory_without_fts.fts_memory_bytes(), 0);
    assert_eq!(memory_without_fts.fts_disk_root(), None);
    assert_eq!(memory_without_fts.fts_warm_mode(), CacheWarmMode::Off);
    assert_eq!(memory_without_fts.fts_warm_concurrency(), 1);
    assert_eq!(memory_without_fts.fts_startup_generation_limit(), None);

    let memory_fts =
        FtsMemoryCacheConfig::try_new(23, FtsWarmConfig::blocking(29, Some(31)).unwrap(), 37)
            .unwrap();
    let memory_with_fts = CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Memory {
            slate_db: SlateMemoryCacheConfig::try_new(1, 1).unwrap(),
            slate_warm: SlateWarmConfig::background(2, 3).unwrap(),
            fts: Some(memory_fts),
        },
    );
    assert_eq!(memory_with_fts.fts_memory_bytes(), 23);
    assert_eq!(memory_with_fts.fts_warm_mode(), CacheWarmMode::Blocking);
    assert_eq!(memory_with_fts.fts_warm_concurrency(), 29);
    assert_eq!(memory_with_fts.fts_startup_generation_limit(), Some(31));
    assert_eq!(
        memory_with_fts.fts_generation_grace_period(),
        std::time::Duration::from_secs(37)
    );

    let object_store = SlateObjectStoreCacheSettings::try_new(
        "/tmp/cache-config-object-store",
        None,
        1,
        false,
        ObjectStoreWarmLevel::Off,
        None,
        1,
    )
    .unwrap();
    let hybrid_without_fts = CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Hybrid {
            slate_db: SlateHybridCacheConfig::try_new(1, "/tmp/cache-config-slate", 1).unwrap(),
            object_store: object_store.clone(),
            slate_warm: SlateWarmConfig::blocking(2, 3).unwrap(),
            fts: None,
        },
    );
    assert!(matches!(
        hybrid_without_fts.slate_warm(),
        Some(SlateWarmConfig::Blocking { .. })
    ));
    assert_eq!(hybrid_without_fts.fts_memory_bytes(), 0);
    assert_eq!(hybrid_without_fts.fts_disk_root(), None);
    assert_eq!(hybrid_without_fts.fts_disk_bytes(), 0);
    assert_eq!(hybrid_without_fts.fts_warm_mode(), CacheWarmMode::Off);
    assert_eq!(hybrid_without_fts.fts_warm_concurrency(), 1);
    assert_eq!(hybrid_without_fts.fts_startup_generation_limit(), None);

    let hybrid_fts = FtsHybridCacheConfig::try_new(
        41,
        "/tmp/cache-config-fts",
        43,
        FtsWarmConfig::background(47, None).unwrap(),
        53,
    )
    .unwrap();
    let hybrid_with_fts = CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Hybrid {
            slate_db: SlateHybridCacheConfig::try_new(1, "/tmp/cache-config-slate", 1).unwrap(),
            object_store,
            slate_warm: SlateWarmConfig::Off,
            fts: Some(hybrid_fts),
        },
    );
    assert_eq!(hybrid_with_fts.fts_memory_bytes(), 41);
    assert_eq!(
        hybrid_with_fts.fts_disk_root(),
        Some(std::path::PathBuf::from("/tmp/cache-config-fts"))
    );
    assert_eq!(hybrid_with_fts.fts_disk_bytes(), 43);
    assert_eq!(hybrid_with_fts.fts_warm_mode(), CacheWarmMode::Background);
    assert_eq!(hybrid_with_fts.fts_warm_concurrency(), 47);
    assert_eq!(hybrid_with_fts.fts_startup_generation_limit(), None);
    assert_eq!(
        hybrid_with_fts.fts_generation_grace_period(),
        std::time::Duration::from_secs(53)
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
fn migration_tuning_types_reject_zero_and_project_every_replacement() {
    assert_eq!(MigrationBatchRows::new(0), None);
    assert_eq!(MigrationBatchBytes::new(0), None);
    assert_eq!(MigrationActiveIntervalMillis::new(0), None);
    assert_eq!(MigrationIdleIntervalMillis::new(0), None);

    let rows = MigrationBatchRows::new(2).unwrap();
    let bytes = MigrationBatchBytes::new(3).unwrap();
    let active = MigrationActiveIntervalMillis::new(5).unwrap();
    let idle = MigrationIdleIntervalMillis::new(7).unwrap();
    assert_eq!(rows.get(), 2);
    assert_eq!(bytes.get(), 3);
    assert_eq!(active.get(), 5);
    assert_eq!(idle.get(), 7);

    let defaults = MigrationTuning::default();
    assert_eq!(defaults.worker_mode(), MigrationWorkerMode::Background);
    assert_eq!(
        defaults.batch_rows().get(),
        MigrationTuning::DEFAULT_BATCH_ROWS
    );
    assert_eq!(
        defaults.batch_bytes().get(),
        MigrationTuning::DEFAULT_BATCH_BYTES
    );
    assert_eq!(
        defaults.active_interval_millis().get(),
        MigrationTuning::DEFAULT_ACTIVE_INTERVAL_MILLIS
    );
    assert_eq!(
        defaults.idle_interval_millis().get(),
        MigrationTuning::DEFAULT_IDLE_INTERVAL_MILLIS
    );

    let tuned = defaults
        .with_worker_mode(MigrationWorkerMode::Disabled)
        .with_batch_rows(rows)
        .with_batch_bytes(bytes)
        .with_active_interval(active)
        .with_active_interval_millis(active)
        .with_idle_interval(idle)
        .with_idle_interval_millis(idle);
    assert_eq!(tuned.worker_mode(), MigrationWorkerMode::Disabled);
    assert_eq!(tuned.batch_rows(), rows);
    assert_eq!(tuned.batch_bytes(), bytes);
    assert_eq!(tuned.active_interval_millis(), active);
    assert_eq!(tuned.idle_interval_millis(), idle);

    let throughput = super::IndexLifecycleThroughputTuning::default();
    let config = DbConfig::new().with_index_lifecycle_throughput_tuning(throughput);
    assert_eq!(config.index_lifecycle_throughput(), throughput);
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
    let mut indexes = RuntimeIndexCatalog::new();
    for definition in [
        SecondaryIndexDefinition::node_range("User", "age").unwrap(),
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind").unwrap(),
        SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "since").unwrap(),
        SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap(),
    ] {
        indexes.insert_dynamic_index(&v2_secondary(definition));
    }

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
    assert!(!SecondaryIndexDefinition::edge_equality("LIKES", "kind")
        .unwrap()
        .unique());
    assert!(SecondaryIndexDefinition::node_equality("User\u{1f}internal", "email").is_err());
}

#[test]
fn runtime_index_catalog_dynamic_insertion_covers_all_iterators() {
    let node_unique = SecondaryIndexDefinition::node_unique_equality("User", "slug").unwrap();
    let edge_range = SecondaryIndexDefinition::edge_range_desc("LIKES", "createdAt").unwrap();
    let vector =
        VectorIndexDefinition::new_node("Doc", "embedding", 3, VectorDistanceMetric::Cosine)
            .unwrap();
    let text = TextIndexDefinition::new_node("Doc", "body").unwrap();

    let mut catalog = RuntimeIndexCatalog::default();
    for definition in [
        SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        SecondaryIndexDefinition::node_range_desc("User", "age").unwrap(),
        SecondaryIndexDefinition::edge_equality("LIKES", "kind").unwrap(),
        SecondaryIndexDefinition::edge_range("LIKES", "rank").unwrap(),
        SecondaryIndexDefinition::edge_range_desc("LIKES", "createdAt").unwrap(),
    ] {
        catalog.insert_dynamic_index(&v2_secondary(definition));
    }
    catalog.insert_dynamic_index(&v2_secondary(node_unique.clone()));
    catalog.insert_dynamic_index(&v2_secondary(edge_range.clone()));
    catalog.insert_dynamic_index(&v2_vector(vector.clone()));
    catalog.insert_dynamic_index(&v2_text(text.clone()));
    catalog.insert_dynamic_index(&v2_vector(vector));
    catalog.insert_dynamic_index(&v2_text(text));

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
    assert!(!catalog.contains_node_range_scoped("not-scoped"));
    assert!(!catalog.contains_node_range_desc_scoped("not-scoped"));
    assert!(!catalog.contains_edge_equality_scoped("not-scoped"));
    assert!(!catalog.contains_edge_range_scoped("not-scoped"));
    assert!(!catalog.contains_edge_range_desc_scoped("not-scoped"));
    assert!(!catalog.has_scoped_equality_index("", "property"));

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
    let mut drop_catalog = RuntimeIndexCatalog::new();
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
    let mut unique_catalog = RuntimeIndexCatalog::new();
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

    let mut vector_config = RuntimeIndexCatalog::new();
    vector_config.insert_dynamic_index(&v2_vector(node_vector));
    vector_config.insert_dynamic_index(&v2_vector(edge_vector));
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
        &RuntimeIndexCatalog::new(),
    );
    assert!(missing_vector.is_err());
    let missing_edge_vector = super::runtime_catalog::vector_definition_from_drop_spec(
        &ir::IndexDdlDropSpec::EdgeVector {
            key: catalog::ScopedPropertyKey::try_new("Missing", "embedding")
                .expect("valid missing edge vector key"),
        },
        &RuntimeIndexCatalog::new(),
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
fn secondary_index_lifecycle_batch_rows_and_search_limits_are_explicit_contracts() {
    assert_eq!(
        DbConfig::new()
            .secondary_index_lifecycle()
            .batch_rows()
            .get(),
        1_024
    );
    assert_eq!(
        SecondaryIndexLifecycleBatchRows::new(32)
            .expect("positive batch rows")
            .get(),
        32
    );
    assert_eq!(SecondaryIndexLifecycleBatchRows::new(0), None);
    assert_eq!(
        DbConfig::new()
            .secondary_index_lifecycle()
            .catch_up_tail_delay_millis()
            .get(),
        1_000
    );
    assert_eq!(
        DbConfig::new().search_index_backfill(),
        SearchIndexBackfillLimits::default()
    );
}
