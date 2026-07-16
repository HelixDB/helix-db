//! Production contracts for the typed vector-row storage boundary.
//!
//! This feature-gated child module exercises tenant-scoped key construction,
//! current row codecs, opaque canonical/candidate/reverse tokens, measured
//! writes, and exhaustive lane cleanup. It uses only existing `encoding::v1`
//! keys and values in isolated databases, so deployed bytes remain unchanged.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::keys::tenant::TenantId;
use crate::encoding::v1::values::vectors::simhash::encode_simhash;
use crate::search::vector::read_fault_production_support::{FaultingRead, ReadFault};
use crate::search::vector::VectorIndexConfig;

/// Verifies legacy bytes, tenant isolation, and opaque canonical ordering.
fn run_keyspace_contracts() {
    let physical_name = "production-typed-row-keyspace";
    let index_id = index_id_from_name(physical_name);
    let logical = VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id));
    let legacy = VectorRowKeyspace::new(physical_name.to_string(), DataScope::LegacyUnscoped);
    assert_eq!(legacy.physical_name(), physical_name);
    assert_eq!(legacy.index_id(), index_id);
    assert_eq!(legacy.scope(), DataScope::LegacyUnscoped);
    assert_eq!(legacy.key(logical), logical.to_bytes());

    let first = VectorRowKeyspace::new(
        physical_name.to_string(),
        DataScope::Tenant(TenantId::from_u128(1)),
    );
    let second = VectorRowKeyspace::new(
        physical_name.to_string(),
        DataScope::Tenant(TenantId::from_u128(2)),
    );
    let first_key = first.key(logical);
    assert_eq!(
        first.strip_physical_key(&first_key).unwrap(),
        logical.to_bytes()
    );
    assert!(first.strip_physical_key(&second.key(logical)).is_err());

    let first_token = first.canonical_vector_row_key(7, 11);
    let second_token = first.canonical_vector_row_key(3, 12);
    assert_eq!(
        first_token.physical_order(&second_token),
        first_token.physical_key.cmp(&second_token.physical_key)
    );
}

/// Verifies every current typed read/write family and cross-keyspace rejection.
async fn run_row_contracts() {
    let db = Db::open("production-typed-vector-rows", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let keyspace = VectorRowKeyspace::new(
        "production-typed-vector-rows".to_string(),
        DataScope::Tenant(TenantId::from_u128(7)),
    );
    let foreign = VectorRowKeyspace::new(
        "production-typed-vector-rows:foreign".to_string(),
        keyspace.scope(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let writes = VectorWriteRows::new(&measured, &keyspace);
    assert!(!writes.metadata_exists().await.unwrap());
    assert_eq!(
        VectorRows::new(&measured, &keyspace)
            .metadata_input_bytes()
            .await
            .unwrap(),
        u64::try_from(
            keyspace
                .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    keyspace.index_id()
                )))
                .len()
        )
        .unwrap()
    );

    let metadata = VectorIndexMetadata::new(VectorIndexConfig::new(
        keyspace.physical_name(),
        "embedding",
        3,
    ));
    writes.put_metadata(&metadata).unwrap();
    writes.put_layer0_neighbors(1, &[2, 3]).unwrap();
    writes.put_upper_neighbors(2, 1, &[4, 5]).unwrap();
    writes
        .put_upper_vector(1, Bytes::from_static(b"upper-vector"))
        .unwrap();
    let canonical = keyspace.canonical_vector_row_key(1, 17);
    writes
        .put_canonical_vector(&canonical, Bytes::from_static(b"canonical-vector"))
        .unwrap();
    writes.put_entry_candidate(1, 3).unwrap();
    writes.put_entry_candidate(2, 1).unwrap();
    writes.put_reverse_locator(9, 2, 1).unwrap();
    writes.put_reverse_locator(9, 2, 2).unwrap();
    writes.put_reverse_locator(9, 1, 3).unwrap();
    measured
        .put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                1,
            ))),
            encode_simhash(0x1234),
        )
        .unwrap();
    let mut malformed_candidate = keyspace
        .key(VectorKey::EntryCandidatePrefix(
            VectorEntryCandidatePrefixKey::new(keyspace.index_id()),
        ))
        .to_vec();
    malformed_candidate.push(0xFF);
    measured
        .put(malformed_candidate, Bytes::from_static(b"malformed"))
        .unwrap();
    let mut malformed_reverse = keyspace
        .key(VectorKey::ReverseEdgePrefix(
            VectorReverseEdgePrefixKey::new(keyspace.index_id(), 9),
        ))
        .to_vec();
    malformed_reverse.push(0xFF);
    measured
        .put(malformed_reverse, Bytes::from_static(b"malformed"))
        .unwrap();
    measured
        .put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                8,
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    measured
        .put(
            keyspace.key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(keyspace.index_id(), 8),
            )),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();

    assert!(writes.metadata_exists().await.unwrap());
    let rows = VectorRows::new(&measured, &keyspace);
    let decoded_metadata = rows.metadata().await.unwrap().unwrap();
    assert_eq!(
        decoded_metadata.config.index_name,
        metadata.config.index_name
    );
    assert_eq!(decoded_metadata.config.property_name, "embedding");
    assert_eq!(decoded_metadata.config.dimension, 3);
    assert!(rows.metadata_input_bytes().await.unwrap() > 0);
    assert_eq!(rows.layer0_neighbors(1).await.unwrap(), vec![2, 3]);
    assert_eq!(rows.layer0_neighbors(99).await.unwrap(), Vec::<u64>::new());
    assert_eq!(rows.layer0_neighbor_row(1).await.unwrap(), Some(vec![2, 3]));
    assert!(rows.layer0_row_exists(1).await.unwrap());
    assert!(!rows.layer0_row_exists(99).await.unwrap());
    assert_eq!(
        rows.layer0_rows_exist(&[]).await.unwrap(),
        Vec::<bool>::new()
    );
    assert_eq!(
        rows.layer0_rows_exist(&[1, 99]).await.unwrap(),
        vec![true, false]
    );
    assert_eq!(
        rows.layer0_neighbor_rows(&[1, 99]).await.unwrap(),
        vec![Some(vec![2, 3]), None]
    );
    assert_eq!(rows.layer0_neighbor_rows(&[]).await.unwrap(), Vec::new());
    assert_eq!(rows.upper_neighbors(2, 1).await.unwrap(), Some(vec![4, 5]));
    assert_eq!(rows.upper_neighbors(2, 99).await.unwrap(), None);
    assert_eq!(
        rows.upper_vector_row(1).await.unwrap(),
        Some(Bytes::from_static(b"upper-vector"))
    );
    assert_eq!(
        rows.upper_vector_rows(&[1, 99]).await.unwrap(),
        vec![Some(Bytes::from_static(b"upper-vector")), None]
    );
    assert_eq!(rows.upper_vector_rows(&[]).await.unwrap(), Vec::new());
    assert_eq!(
        rows.simhash_rows(&[1, 7, 8]).await.unwrap(),
        vec![
            SimHashRow::Present(SimHash::from_bits(0x1234)),
            SimHashRow::Missing,
            SimHashRow::Corrupt,
        ]
    );
    assert_eq!(rows.simhash_rows(&[]).await.unwrap(), Vec::new());
    assert_eq!(
        rows.canonical_vector_row(&canonical).await.unwrap(),
        Some(Bytes::from_static(b"canonical-vector"))
    );
    assert_eq!(
        rows.canonical_vector_rows(std::slice::from_ref(&canonical))
            .await
            .unwrap(),
        vec![Some(Bytes::from_static(b"canonical-vector"))]
    );
    assert_eq!(rows.canonical_vector_rows(&[]).await.unwrap(), Vec::new());
    let foreign_token = foreign.canonical_vector_row_key(1, 17);
    assert!(rows.canonical_vector_row(&foreign_token).await.is_err());
    assert!(rows
        .canonical_vector_rows(std::slice::from_ref(&foreign_token))
        .await
        .is_err());
    assert_eq!(
        rows.entry_candidate_layer(1).await.unwrap(),
        EntryCandidateLayerRow::Present(3)
    );
    assert_eq!(
        rows.entry_candidate_layer(7).await.unwrap(),
        EntryCandidateLayerRow::Missing
    );
    assert_eq!(
        rows.entry_candidate_layer(8).await.unwrap(),
        EntryCandidateLayerRow::Corrupt
    );

    let candidate = {
        let mut candidates = rows.entry_candidates().await.unwrap();
        let candidate = candidates.next().await.unwrap().unwrap();
        assert_eq!(candidate.node_id(), 1);
        assert_eq!(candidate.layer(), 3);
        candidate
    };
    let reverse = rows.reverse_sources_for_target(9).await.unwrap();
    assert_eq!(reverse.sources_at(2), &[1, 2]);
    assert_eq!(reverse.sources_at(1), &[3]);
    assert!(reverse.sources_at(0).is_empty());
    assert_eq!(reverse.sources_by_layer().len(), 2);

    assert_eq!(
        writes.layer0_neighbor_rows(&[1, 99]).await.unwrap(),
        vec![Some(vec![2, 3]), None]
    );
    assert_eq!(
        writes.entry_candidate_layer(2).await.unwrap(),
        EntryCandidateLayerRow::Present(1)
    );
    let mut writable_candidates = writes.entry_candidates().await.unwrap();
    assert!(writable_candidates.next().await.unwrap().is_some());
    drop(writable_candidates);
    assert_eq!(
        writes
            .reverse_sources_for_target(9)
            .await
            .unwrap()
            .sources_at(2),
        &[1, 2]
    );

    assert!(writes
        .put_canonical_vector(&foreign_token, Bytes::new())
        .is_err());
    assert!(writes.delete_canonical_vector(&foreign_token).is_err());
    writes.delete_scanned_entry_candidate(&candidate).unwrap();
    let foreign_candidate = EntryCandidateRow {
        keyspace: &foreign,
        physical_key: foreign.key(VectorKey::EntryCandidateSorted(
            VectorEntryCandidateKey::new(foreign.index_id(), 1, 1),
        )),
        node_id: 1,
        layer: 1,
    };
    assert!(writes
        .delete_scanned_entry_candidate(&foreign_candidate)
        .is_err());
    let foreign_reverse = ReverseSourcesForTarget {
        keyspace: foreign.clone(),
        sources_by_layer: BTreeMap::new(),
        locator_keys: Vec::new(),
    };
    assert!(writes.delete_reverse_sources(&foreign_reverse).is_err());
    writes.delete_reverse_sources(&reverse).unwrap();
    writes.delete_entry_candidate_sorted(2, 1).unwrap();
    writes.delete_entry_candidate_node(1).unwrap();
    writes.delete_entry_candidate_node(2).unwrap();
    writes.delete_reverse_locator(9, 2, 1).unwrap();
    writes.delete_upper_neighbors(2, 1).unwrap();
    writes.delete_upper_vector(1).unwrap();
    writes.delete_simhash(1).unwrap();
    writes.delete_layer0_neighbors(1).unwrap();
    writes.delete_canonical_vector(&canonical).unwrap();

    writes
        .put_metadata(&VectorIndexMetadata::new(VectorIndexConfig::new(
            keyspace.physical_name(),
            "embedding",
            3,
        )))
        .unwrap();
    writes.put_layer0_neighbors(10, &[11]).unwrap();
    writes
        .put_upper_vector(10, Bytes::from_static(b"cleanup"))
        .unwrap();
    writes.delete_all().await.unwrap();
    assert!(measured.measurement().unwrap().operations() > 0);
    txn.commit().await.unwrap();

    assert!(VectorRows::new(&db, &keyspace)
        .metadata()
        .await
        .unwrap()
        .is_none());
    for lane in VectorStorageLane::ALL {
        let mut scan = db
            .scan_prefix(keyspace.key(lane.prefix_key(keyspace.index_id())), ..)
            .await
            .unwrap();
        assert!(scan.next().await.unwrap().is_none());
    }
}

/// Verifies malformed and identity-mismatched rows fail at the storage boundary.
async fn run_corruption_contracts() {
    let db = Db::open(
        "production-corrupt-typed-vector-rows",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let keyspace = VectorRowKeyspace::new(
        "production-corrupt-typed-vector-rows".to_string(),
        DataScope::LegacyUnscoped,
    );
    let metadata_key = keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
        keyspace.index_id(),
    )));

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorRows::new(&measured, &keyspace);
    measured
        .put(metadata_key.clone(), Bytes::from_static(b"corrupt"))
        .unwrap();
    assert!(matches!(
        rows.metadata().await,
        Err(HelixDbError::Encoding(_))
    ));

    let mut invalid = VectorIndexMetadata::new(VectorIndexConfig::new(
        keyspace.physical_name(),
        "embedding",
        3,
    ));
    invalid.entry_point = None;
    invalid.max_layer = 1;
    measured
        .put(metadata_key.clone(), encode_metadata(&invalid))
        .unwrap();
    assert!(rows.metadata().await.is_err());

    let collision = VectorIndexMetadata::new(VectorIndexConfig::new(
        "production-colliding-vector-name",
        "embedding",
        3,
    ));
    measured
        .put(metadata_key, encode_metadata(&collision))
        .unwrap();
    assert!(matches!(
        rows.metadata().await,
        Err(HelixDbError::Config(_))
    ));

    measured
        .put(
            keyspace.key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                keyspace.index_id(),
                1,
            ))),
            Bytes::from_static(b"corrupt-layer-zero"),
        )
        .unwrap();
    assert!(rows.layer0_neighbor_row(1).await.is_err());
    assert!(rows.layer0_neighbor_rows(&[1]).await.is_err());

    measured
        .put(
            keyspace.key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                keyspace.index_id(),
                2,
                1,
            ))),
            Bytes::from_static(b"corrupt-upper-neighbors"),
        )
        .unwrap();
    assert!(rows.upper_neighbors(2, 1).await.is_err());
    txn.rollback();
}

/// Verifies every typed storage read propagates its backend operation failure.
async fn run_read_fault_contracts() {
    let db = Db::open(
        "production-vector-storage-read-faults",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let keyspace = VectorRowKeyspace::new(
        "production-vector-storage-read-faults".to_string(),
        DataScope::LegacyUnscoped,
    );
    let canonical = keyspace.canonical_vector_row_key(1, 7);

    let point = FaultingRead::new(&db, ReadFault::Point);
    let rows = VectorRows::new(&point, &keyspace);
    assert!(rows.metadata().await.is_err());
    assert!(rows.metadata_input_bytes().await.is_err());
    assert!(rows.layer0_neighbors(1).await.is_err());
    assert!(rows.layer0_neighbor_row(1).await.is_err());
    assert!(rows.layer0_row_exists(1).await.is_err());
    assert!(rows.upper_neighbors(1, 1).await.is_err());
    assert!(rows.upper_vector_row(1).await.is_err());
    assert!(rows.canonical_vector_row(&canonical).await.is_err());
    assert!(rows.entry_candidate_layer(1).await.is_err());

    let multi_get = FaultingRead::new(&db, ReadFault::MultiGet);
    let rows = VectorRows::new(&multi_get, &keyspace);
    assert!(rows.layer0_rows_exist(&[1]).await.is_err());
    assert!(rows.layer0_neighbor_rows(&[1]).await.is_err());
    assert!(rows.upper_vector_rows(&[1]).await.is_err());
    assert!(rows.simhash_rows(&[1]).await.is_err());
    assert!(rows
        .canonical_vector_rows(std::slice::from_ref(&canonical))
        .await
        .is_err());

    let scan = FaultingRead::new(&db, ReadFault::Scan);
    let rows = VectorRows::new(&scan, &keyspace);
    assert!(rows.entry_candidates().await.is_err());
    assert!(rows.reverse_sources_for_target(1).await.is_err());
}

/// Exercises scoped keys, typed row codecs, opaque tokens, and lane cleanup.
pub(crate) async fn run() {
    run_keyspace_contracts();
    run_row_contracts().await;
    run_corruption_contracts().await;
    run_read_fault_contracts().await;
}
