//! Exhaustive persisted-cursor ownership contracts.

use bytes::Bytes;

use crate::encoding::indexes::range::RangeIndexDirection;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorSimHashDirectoryKey,
    VectorUpperVectorKey,
};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{
    CanonicalSecondaryValue, IndexEntity, IndexEntityStateKey, ManagedIndexKey,
    PartitionFingerprint, ScopedKey, SecondaryEntryKey, SecondaryEntryLane, TextBuildArtifactKey,
    TextCorpusStatisticsKey, TextEntityStateKey, TextManifestPageKey, TextManifestRootKey,
    TextStatisticsEntityKey, TextTermFingerprint, TextTermStatisticsKey, VectorPartitionMappingKey,
};
use crate::encoding::v2::keys::{
    DataKey as GraphKey, DataKeyKind, EdgePropertyByIdKey, NodePropertyKey,
};
use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;

use super::*;

const VALID_INDEX_ID: u64 = 7;
const VALID_GENERATION: u64 = 9;
const WRONG_INDEX_ID: u64 = 8;
const WRONG_GENERATION: u64 = 10;

struct CursorStageCase {
    name: &'static str,
    operation: IndexOperationRecord,
    wrong_owner: IndexCursor,
}

fn index_id(value: u64) -> IndexId {
    IndexId::new(value).expect("cursor fixture index ID is non-zero")
}

fn generation(value: u64) -> IndexGenerationId {
    IndexGenerationId::new(value).expect("cursor fixture generation is non-zero")
}

fn identity(family: IndexIdentityFamily, element_kind: IndexElementKind) -> IndexIdentity {
    IndexIdentity::new(
        family,
        element_kind,
        IndexComponent::try_new("label", "Document").unwrap(),
        IndexComponent::try_new("property", "value").unwrap(),
    )
}

fn operation(
    ordinal: u8,
    family: IndexIdentityFamily,
    element_kind: IndexElementKind,
    progress: IndexOperationProgress,
) -> IndexOperationRecord {
    let operation_family = progress.family();
    IndexOperationRecord::try_new(
        IndexOperationId::from_bytes([ordinal; 16]).unwrap(),
        index_id(VALID_INDEX_ID),
        identity(family, element_kind),
        generation(VALID_GENERATION),
        IndexRevision::initial(),
        IndexOperationRevision::initial(),
        progress.kind(),
        operation_family,
        progress,
        1,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .unwrap()
}

fn cursor(bytes: Bytes) -> IndexCursor {
    IndexCursor::try_new(bytes).unwrap()
}

fn scoped_cursor(scope: DataScope, kind: ScopedKey) -> IndexCursor {
    cursor(ManagedIndexKey::Data { scope, kind }.to_bytes())
}

fn graph_cursor(scope: DataScope, element_kind: IndexElementKind, id: u64) -> IndexCursor {
    let kind = match element_kind {
        IndexElementKind::Node => DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
        IndexElementKind::Edge => DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(id)),
    };
    cursor(GraphKey::Data { scope, kind }.to_bytes())
}

fn legacy_edge_pair_cursor(scope: DataScope, from: u64, to: u64) -> IndexCursor {
    cursor(
        GraphKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyPair(LegacyEdgePropertyPairKey::new(from, to)),
        }
        .to_bytes(),
    )
}

fn source_progress(scope: DataScope, element_kind: IndexElementKind) -> SourceScanProgress {
    SourceScanProgress {
        inclusive_upper_bound: graph_cursor(scope, element_kind, 20),
        cursor: Some(graph_cursor(scope, element_kind, 11)),
        counters: OperationCounters::default(),
    }
}

fn prefix(cursor: IndexCursor) -> PrefixScanProgress {
    PrefixScanProgress {
        cursor: Some(cursor),
        counters: OperationCounters::default(),
    }
}

fn entity(element_kind: IndexElementKind) -> IndexEntity {
    IndexEntity {
        kind: element_kind,
        id: IndexEntityId::new(11),
    }
}

fn state_key(
    index_id: u64,
    generation: u64,
    element_kind: IndexElementKind,
) -> IndexEntityStateKey {
    IndexEntityStateKey {
        index_id: self::index_id(index_id),
        generation: self::generation(generation),
        entity: entity(element_kind),
    }
}

fn root(index_id: u64, generation: u64, partition: u8) -> TextManifestRootKey {
    TextManifestRootKey {
        index_id: self::index_id(index_id),
        generation: self::generation(generation),
        partition: PartitionFingerprint::new([partition; 32]),
    }
}

fn text_partition_upper_bound(scope: DataScope) -> IndexCursor {
    scoped_cursor(
        scope,
        ScopedKey::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: index_id(VALID_INDEX_ID),
                generation: generation(VALID_GENERATION),
                partition: PartitionFingerprint::new([u8::MAX; 32]),
            },
            entity: IndexEntity {
                kind: IndexElementKind::Edge,
                id: IndexEntityId::new(u64::MAX),
            },
        }),
    )
}

fn physical_vector_id(element_kind: IndexElementKind) -> u64 {
    let element_type = match element_kind {
        IndexElementKind::Node => crate::config::VectorElementType::Node,
        IndexElementKind::Edge => crate::config::VectorElementType::Edge,
    };
    crate::search::vector::index_id_from_name(&crate::search::vector_index_name(
        element_type,
        "Document",
        "value",
    ))
}

fn vector_cursor(scope: DataScope, key: VectorKey) -> IndexCursor {
    cursor(
        GraphKey::Data {
            scope,
            kind: DataKeyKind::Vector(key),
        }
        .to_bytes(),
    )
}

fn valid_stage_cases(scope: DataScope) -> Vec<CursorStageCase> {
    let node = IndexElementKind::Node;
    let valid_index = index_id(VALID_INDEX_ID);
    let valid_generation = generation(VALID_GENERATION);
    let applied = |index, generation| {
        scoped_cursor(
            scope,
            ScopedKey::AppliedState(state_key(index, generation, node)),
        )
    };
    let mapping = |index, generation| {
        scoped_cursor(
            scope,
            ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
                index_id: self::index_id(index),
                generation: self::generation(generation),
                partition: PartitionFingerprint::new([0x31; 32]),
            }),
        )
    };
    let text_state = |index, generation| {
        scoped_cursor(
            scope,
            ScopedKey::TextEntityState(TextEntityStateKey {
                root: root(index, generation, 0x41),
                entity: entity(node),
            }),
        )
    };
    let artifact = |index, generation| {
        scoped_cursor(
            scope,
            ScopedKey::TextBuildArtifact(TextBuildArtifactKey {
                root: root(index, generation, 0x42),
                ordinal: 3,
            }),
        )
    };
    let manifest_page = |index, generation, partition, page| {
        scoped_cursor(
            scope,
            ScopedKey::TextManifestPage(TextManifestPageKey {
                root: root(index, generation, partition),
                page,
            }),
        )
    };
    let manifest_root = |index, generation| {
        scoped_cursor(
            scope,
            ScopedKey::TextManifestRoot(root(index, generation, 0x44)),
        )
    };
    let operation_key = || {
        scoped_cursor(
            scope,
            ScopedKey::operation(IndexOperationId::from_bytes([0xEE; 16]).unwrap()),
        )
    };

    let equality_entry = |index, generation, lane| {
        scoped_cursor(
            scope,
            ScopedKey::SecondaryEntry(
                SecondaryEntryKey::try_new(
                    self::index_id(index),
                    self::generation(generation),
                    lane,
                    CanonicalSecondaryValue::equality_string("shared"),
                    match lane {
                        SecondaryEntryLane::NodeUniqueEquality => None,
                        SecondaryEntryLane::NodeEquality | SecondaryEntryLane::EdgeEquality => {
                            Some(IndexEntityId::new(11))
                        }
                        SecondaryEntryLane::NodeRangeAscending
                        | SecondaryEntryLane::NodeRangeDescending
                        | SecondaryEntryLane::EdgeRangeAscending
                        | SecondaryEntryLane::EdgeRangeDescending => {
                            unreachable!("equality fixture selects an equality lane")
                        }
                    },
                )
                .unwrap(),
            ),
        )
    };
    let range_entry = |index, generation| {
        scoped_cursor(
            scope,
            ScopedKey::SecondaryEntry(
                SecondaryEntryKey::try_new(
                    self::index_id(index),
                    self::generation(generation),
                    SecondaryEntryLane::NodeRangeAscending,
                    CanonicalSecondaryValue::range_string(RangeIndexDirection::Asc, "shared"),
                    Some(IndexEntityId::new(11)),
                )
                .unwrap(),
            ),
        )
    };

    let physical_id = physical_vector_id(node);
    let wrong_physical_id = physical_id.wrapping_add(1);
    let legacy_core = vector_cursor(
        scope,
        VectorKey::IndexMetadata(VectorIndexMetadataKey::new(physical_id)),
    );
    let legacy_core_wrong = vector_cursor(
        scope,
        VectorKey::IndexMetadata(VectorIndexMetadataKey::new(wrong_physical_id)),
    );
    let legacy_hot = vector_cursor(
        scope,
        VectorKey::UpperVector(VectorUpperVectorKey::new(physical_id, 11)),
    );
    let legacy_hot_wrong = vector_cursor(
        scope,
        VectorKey::UpperVector(VectorUpperVectorKey::new(wrong_physical_id, 11)),
    );
    let legacy_layer_zero = vector_cursor(
        scope,
        VectorKey::Vector(VectorItemKey::new(physical_id, 7, 11)),
    );
    let legacy_layer_zero_wrong = vector_cursor(
        scope,
        VectorKey::Vector(VectorItemKey::new(wrong_physical_id, 7, 11)),
    );
    let directory = vector_cursor(
        scope,
        VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(physical_id, 7, 11)),
    );
    let directory_wrong = vector_cursor(
        scope,
        VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(wrong_physical_id, 7, 11)),
    );

    let page_partition = TextManifestPartitionValidation::try_new(
        [0x43; 32],
        TextManifestRevision::new(4).unwrap(),
        3,
        3,
        2,
        2,
    )
    .unwrap();

    let mut cases = vec![
        CursorStageCase {
            name: "secondary source scan",
            operation: operation(
                1,
                IndexIdentityFamily::SecondaryEquality,
                node,
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Scan(source_progress(scope, node)),
                )),
            ),
            wrong_owner: graph_cursor(scope, IndexElementKind::Edge, 11),
        },
        CursorStageCase {
            name: "secondary applied-state validation",
            operation: operation(
                2,
                IndexIdentityFamily::SecondaryEquality,
                node,
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Validate(prefix(applied(
                        VALID_INDEX_ID,
                        VALID_GENERATION,
                    ))),
                )),
            ),
            wrong_owner: applied(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "secondary equality cleanup",
            operation: operation(
                3,
                IndexIdentityFamily::SecondaryEquality,
                node,
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                    prefix(equality_entry(
                        VALID_INDEX_ID,
                        VALID_GENERATION,
                        SecondaryEntryLane::NodeEquality,
                    )),
                )),
            ),
            wrong_owner: equality_entry(
                WRONG_INDEX_ID,
                WRONG_GENERATION,
                SecondaryEntryLane::NodeEquality,
            ),
        },
        CursorStageCase {
            name: "secondary range cleanup",
            operation: operation(
                4,
                IndexIdentityFamily::SecondaryRange,
                node,
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                    prefix(range_entry(VALID_INDEX_ID, VALID_GENERATION)),
                )),
            ),
            wrong_owner: range_entry(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "vector legacy core adoption",
            operation: operation(
                5,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                        lane: LegacyVectorValidationLane::Core,
                        cursor: Some(legacy_core),
                        counters: OperationCounters::default(),
                    }),
                )),
            ),
            wrong_owner: legacy_core_wrong,
        },
        CursorStageCase {
            name: "vector legacy hot adoption",
            operation: operation(
                6,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                        lane: LegacyVectorValidationLane::Hot,
                        cursor: Some(legacy_hot),
                        counters: OperationCounters::default(),
                    }),
                )),
            ),
            wrong_owner: legacy_hot_wrong,
        },
        CursorStageCase {
            name: "vector legacy layer-zero adoption",
            operation: operation(
                7,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                        lane: LegacyVectorValidationLane::Layer0,
                        cursor: Some(legacy_layer_zero),
                        counters: OperationCounters::default(),
                    }),
                )),
            ),
            wrong_owner: legacy_layer_zero_wrong,
        },
        CursorStageCase {
            name: "vector adopted-directory validation",
            operation: operation(
                8,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateAdoptedDirectory(
                        LegacyVectorDirectoryValidationProgress {
                            cursor: Some(directory),
                            expected_markers: 3,
                            verified_markers: 1,
                            counters: OperationCounters {
                                output_operations: 3,
                                ..OperationCounters::default()
                            },
                        },
                    ),
                )),
            ),
            wrong_owner: directory_wrong,
        },
        CursorStageCase {
            name: "vector source scan",
            operation: operation(
                9,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::Scan(source_progress(scope, node)),
                )),
            ),
            wrong_owner: graph_cursor(scope, IndexElementKind::Edge, 11),
        },
        CursorStageCase {
            name: "vector applied-state validation",
            operation: operation(
                10,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateDescriptor(prefix(applied(
                        VALID_INDEX_ID,
                        VALID_GENERATION,
                    ))),
                )),
            ),
            wrong_owner: applied(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "vector mapping validation",
            operation: operation(
                11,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateDescriptor(prefix(mapping(
                        VALID_INDEX_ID,
                        VALID_GENERATION,
                    ))),
                )),
            ),
            wrong_owner: mapping(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "vector physical cleanup",
            operation: operation(
                12,
                IndexIdentityFamily::Vector,
                node,
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeletePhysical(
                    prefix(mapping(VALID_INDEX_ID, VALID_GENERATION)),
                )),
            ),
            wrong_owner: mapping(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "text source scan",
            operation: operation(
                13,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanSource(source_progress(scope, node)),
                )),
            ),
            wrong_owner: graph_cursor(scope, IndexElementKind::Edge, 11),
        },
        CursorStageCase {
            name: "text partition scan",
            operation: operation(
                14,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanPartitions(SourceScanProgress {
                        inclusive_upper_bound: text_partition_upper_bound(scope),
                        cursor: Some(text_state(VALID_INDEX_ID, VALID_GENERATION)),
                        counters: OperationCounters::default(),
                    }),
                )),
            ),
            wrong_owner: text_state(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "text compaction",
            operation: operation(
                15,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::Compact(prefix(artifact(VALID_INDEX_ID, VALID_GENERATION))),
                )),
            ),
            wrong_owner: artifact(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "text manifest preparation",
            operation: operation(
                16,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::PrepareManifests(prefix(artifact(
                        VALID_INDEX_ID,
                        VALID_GENERATION,
                    ))),
                )),
            ),
            wrong_owner: artifact(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "text manifest-page validation",
            operation: operation(
                17,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(TextManifestValidationProgress::Pages(
                        TextManifestPageValidationProgress::try_new(
                            Some(manifest_page(VALID_INDEX_ID, VALID_GENERATION, 0x43, 1)),
                            Some(page_partition),
                            OperationCounters::default(),
                        )
                        .unwrap(),
                    )),
                )),
            ),
            wrong_owner: manifest_page(WRONG_INDEX_ID, WRONG_GENERATION, 0x43, 1),
        },
        CursorStageCase {
            name: "text manifest-root validation",
            operation: operation(
                18,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(TextManifestValidationProgress::Roots(
                        prefix(manifest_root(VALID_INDEX_ID, VALID_GENERATION)),
                    )),
                )),
            ),
            wrong_owner: manifest_root(WRONG_INDEX_ID, WRONG_GENERATION),
        },
        CursorStageCase {
            name: "text entity-state validation",
            operation: operation(
                19,
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(
                        TextManifestValidationProgress::EntityStates(prefix(text_state(
                            VALID_INDEX_ID,
                            VALID_GENERATION,
                        ))),
                    ),
                )),
            ),
            wrong_owner: text_state(WRONG_INDEX_ID, WRONG_GENERATION),
        },
    ];

    let cleanup_keys = [
        ScopedKey::TextBuildArtifact(TextBuildArtifactKey {
            root: root(VALID_INDEX_ID, VALID_GENERATION, 0x51),
            ordinal: 1,
        }),
        ScopedKey::TextManifestPage(TextManifestPageKey {
            root: root(VALID_INDEX_ID, VALID_GENERATION, 0x52),
            page: 1,
        }),
        ScopedKey::TextManifestRoot(root(VALID_INDEX_ID, VALID_GENERATION, 0x53)),
        ScopedKey::TextEntityState(TextEntityStateKey {
            root: root(VALID_INDEX_ID, VALID_GENERATION, 0x54),
            entity: entity(node),
        }),
        ScopedKey::TextCorpusStatistics(TextCorpusStatisticsKey {
            index_id: valid_index,
            generation: valid_generation,
            partition: PartitionFingerprint::new([0x55; 32]),
        }),
        ScopedKey::TextTermStatistics(TextTermStatisticsKey {
            corpus: TextCorpusStatisticsKey {
                index_id: valid_index,
                generation: valid_generation,
                partition: PartitionFingerprint::new([0x56; 32]),
            },
            term: TextTermFingerprint::new([0x57; 32]),
        }),
        ScopedKey::TextStatisticsEntity(TextStatisticsEntityKey {
            index_id: valid_index,
            generation: valid_generation,
            entity: entity(node),
        }),
        ScopedKey::BuildDelta(state_key(VALID_INDEX_ID, VALID_GENERATION, node)),
        ScopedKey::AppliedState(state_key(VALID_INDEX_ID, VALID_GENERATION, node)),
    ];
    for (offset, cleanup_key) in cleanup_keys.into_iter().enumerate() {
        cases.push(CursorStageCase {
            name: "text metadata cleanup lane",
            operation: operation(
                u8::try_from(20 + offset).unwrap(),
                IndexIdentityFamily::Text,
                node,
                IndexOperationProgress::TextCleanup(TextCleanupProgress::DeleteMetadata(prefix(
                    scoped_cursor(scope, cleanup_key),
                ))),
            ),
            wrong_owner: operation_key(),
        });
    }
    cases
}

fn reframe_cursor(
    original_scope: DataScope,
    next_scope: DataScope,
    cursor: &IndexCursor,
) -> IndexCursor {
    if let Ok(ManagedIndexKey::Data { kind, .. }) =
        ManagedIndexKey::parse_from_slice(original_scope, cursor.as_bytes())
    {
        return scoped_cursor(next_scope, kind);
    }
    let GraphKey::Data { kind, .. } =
        GraphKey::parse_from_slice(original_scope, cursor.as_bytes()).unwrap()
    else {
        unreachable!("cursor fixtures contain only scoped data keys")
    };
    self::cursor(
        GraphKey::Data {
            scope: next_scope,
            kind,
        }
        .to_bytes(),
    )
}

#[test]
fn every_cursor_bearing_stage_rejects_scope_owner_kind_and_truncation() {
    for scope in [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(TenantId::from_u128(
            0xFD00_0000_0000_0000_0000_0000_0000_0001,
        )),
    ] {
        let other_scope = match scope {
            DataScope::LegacyUnscoped => DataScope::Tenant(TenantId::from_u128(17)),
            DataScope::Tenant(_) => DataScope::Tenant(TenantId::from_u128(18)),
        };
        for case in valid_stage_cases(scope) {
            assert!(
                repository::operation_record_cursors_are_valid(scope, &case.operation),
                "{} rejects its valid cursor",
                case.name
            );

            let wrong_scope = case
                .operation
                .try_map_cursors(|current| {
                    Ok::<_, IndexOperationModelError>(reframe_cursor(scope, other_scope, current))
                })
                .unwrap();
            assert!(
                !repository::operation_record_cursors_are_valid(scope, &wrong_scope),
                "{} accepts another scope",
                case.name
            );

            let wrong_owner_cursor = case.wrong_owner.clone();
            let wrong_owner = case
                .operation
                .try_map_cursors(|_| Ok::<_, IndexOperationModelError>(wrong_owner_cursor.clone()))
                .unwrap();
            assert!(
                !repository::operation_record_cursors_are_valid(scope, &wrong_owner),
                "{} accepts another owner or lane",
                case.name
            );

            let wrong_kind_cursor = scoped_cursor(
                scope,
                ScopedKey::operation(IndexOperationId::from_bytes([0xDD; 16]).unwrap()),
            );
            let wrong_kind = case
                .operation
                .try_map_cursors(|_| Ok::<_, IndexOperationModelError>(wrong_kind_cursor.clone()))
                .unwrap();
            assert!(
                !repository::operation_record_cursors_are_valid(scope, &wrong_kind),
                "{} accepts another record kind",
                case.name
            );

            let truncated = case
                .operation
                .try_map_cursors(|current| {
                    let bytes = current.as_bytes();
                    Ok::<_, IndexOperationModelError>(self::cursor(Bytes::copy_from_slice(
                        &bytes[..bytes.len() - 1],
                    )))
                })
                .unwrap();
            assert!(
                !repository::operation_record_cursors_are_valid(scope, &truncated),
                "{} accepts a truncated cursor",
                case.name
            );
        }
    }
}

#[test]
fn edge_source_scans_accept_non_entity_rows_inside_the_shared_physical_prefix() {
    for scope in [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(TenantId::from_u128(
            0xFD00_0000_0000_0000_0000_0000_0000_0001,
        )),
    ] {
        let progress = SourceScanProgress {
            inclusive_upper_bound: graph_cursor(scope, IndexElementKind::Edge, 20),
            cursor: Some(legacy_edge_pair_cursor(scope, 1, 2)),
            counters: OperationCounters::default(),
        };
        let cases = [
            operation(
                30,
                IndexIdentityFamily::SecondaryEquality,
                IndexElementKind::Edge,
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Scan(progress.clone()),
                )),
            ),
            operation(
                31,
                IndexIdentityFamily::Vector,
                IndexElementKind::Edge,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::Scan(progress.clone()),
                )),
            ),
            operation(
                32,
                IndexIdentityFamily::Text,
                IndexElementKind::Edge,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanSource(progress.clone()),
                )),
            ),
        ];
        for operation in cases {
            assert!(repository::operation_record_cursors_are_valid(
                scope, &operation
            ));
        }
    }
}

#[test]
fn cursorless_stages_reject_persisted_resume_keys() {
    let scope = DataScope::Tenant(TenantId::from_u128(7));
    let invalid = prefix(scoped_cursor(
        scope,
        ScopedKey::BuildDelta(state_key(
            VALID_INDEX_ID,
            VALID_GENERATION,
            IndexElementKind::Node,
        )),
    ));
    let cases = [
        operation(
            40,
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::CatchUp(invalid.clone()),
            )),
        ),
        operation(
            41,
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteDeltas(
                invalid.clone(),
            )),
        ),
        operation(
            42,
            IndexIdentityFamily::Vector,
            IndexElementKind::Node,
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::CatchUp(invalid.clone()),
            )),
        ),
        operation(
            43,
            IndexIdentityFamily::Vector,
            IndexElementKind::Node,
            IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeleteDeltas(
                invalid.clone(),
            )),
        ),
        operation(
            44,
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::CatchUp(invalid),
            )),
        ),
    ];
    for operation in cases {
        assert!(!repository::operation_record_cursors_are_valid(
            scope, &operation
        ));
    }
}

#[test]
fn manifest_page_cursor_is_bound_to_partition_and_next_page() {
    let scope = DataScope::Tenant(TenantId::from_u128(7));
    let progress = |partition, page| {
        TextManifestPageValidationProgress::try_new(
            Some(scoped_cursor(
                scope,
                ScopedKey::TextManifestPage(TextManifestPageKey {
                    root: root(VALID_INDEX_ID, VALID_GENERATION, partition),
                    page,
                }),
            )),
            Some(
                TextManifestPartitionValidation::try_new(
                    [0x66; 32],
                    TextManifestRevision::new(4).unwrap(),
                    3,
                    3,
                    2,
                    2,
                )
                .unwrap(),
            ),
            OperationCounters::default(),
        )
        .unwrap()
    };
    for (partition, page, expected) in [(0x66, 1, true), (0x67, 1, false), (0x66, 0, false)] {
        let operation = operation(
            45,
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ValidateManifests(TextManifestValidationProgress::Pages(progress(
                    partition, page,
                ))),
            )),
        );
        assert_eq!(
            repository::operation_record_cursors_are_valid(scope, &operation),
            expected
        );
    }
}

#[test]
fn adopted_directory_checkpoint_counts_are_owner_bound() {
    let scope = DataScope::LegacyUnscoped;
    let physical_id = physical_vector_id(IndexElementKind::Node);
    let directory = vector_cursor(
        scope,
        VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(physical_id, 7, 11)),
    );
    for (cursor, expected_markers, verified_markers, output_operations, expected) in [
        (None, 3, 0, 3, true),
        (Some(directory.clone()), 3, 1, 3, true),
        (Some(directory.clone()), 3, 0, 3, false),
        (Some(directory), 3, 4, 3, false),
        (None, 3, 0, 2, false),
    ] {
        let operation = operation(
            46,
            IndexIdentityFamily::Vector,
            IndexElementKind::Node,
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::ValidateAdoptedDirectory(
                    LegacyVectorDirectoryValidationProgress {
                        cursor,
                        expected_markers,
                        verified_markers,
                        counters: OperationCounters {
                            output_operations,
                            ..OperationCounters::default()
                        },
                    },
                ),
            )),
        );
        assert_eq!(
            repository::operation_record_cursors_are_valid(scope, &operation),
            expected
        );
    }
}
