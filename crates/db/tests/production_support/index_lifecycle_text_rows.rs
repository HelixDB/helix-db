//! Durable-row oracles for production-linked text lifecycle contracts.
//!
//! The public lifecycle test drives real DDL and graph mutations. This module
//! remains outside the measured production tree while decoding the rows those
//! paths create with the deployed V1 key/value codecs. It checks relationships
//! rather than reproducing lifecycle decisions.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::time::{Duration, Instant};

use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::index_lifecycle::work::{AppliedFamilyState, SplitRef};
use crate::HelixStorage;

const ROW_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Closed observation of a text generation while finalization may still run.
enum TextRowObservation {
    Pending { rows: Vec<String> },
    Ready,
}

/// Waits for a complete Active text row graph and validates every relationship.
pub(super) async fn verify_steady_state(db: &crate::HelixDB, expected_live_entities: usize) {
    let started = Instant::now();
    loop {
        match observe_steady_state(db, expected_live_entities).await {
            TextRowObservation::Ready => break,
            TextRowObservation::Pending { rows } => {
                assert!(!rows.is_empty(), "pending text rows name exact work");
                assert!(
                    started.elapsed() < ROW_DRAIN_TIMEOUT,
                    "text lifecycle rows did not drain: {rows:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Waits for terminal DROP/abort cleanup to remove all generation-owned rows.
pub(super) async fn verify_dropped(db: &crate::HelixDB) {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("text row contracts require writer storage");
    };
    tokio::time::timeout(ROW_DRAIN_TIMEOUT, async {
        loop {
            let mut residue = 0_usize;
            for kind in [
                index_keys::RecordKind::BuildDelta,
                index_keys::RecordKind::AppliedState,
                index_keys::RecordKind::TextManifestRoot,
                index_keys::RecordKind::TextManifestPage,
                index_keys::RecordKind::TextBuildArtifact,
                index_keys::RecordKind::TextEntityState,
                index_keys::RecordKind::TextCorpusStatistics,
                index_keys::RecordKind::TextTermStatistics,
                index_keys::RecordKind::TextStatisticsEntity,
            ] {
                let prefix = ManagedIndexKey::data_prefix(
                    DataScope::LegacyUnscoped,
                    index_keys::ScopedKey::logical_prefix(kind),
                );
                let mut rows = writer
                    .db()
                    .scan_prefix(
                        &prefix,
                        (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
                    )
                    .await
                    .expect("scoped text cleanup lane remains readable");
                while rows
                    .next()
                    .await
                    .expect("scoped text cleanup scan succeeds")
                    .is_some()
                {
                    residue = residue
                        .checked_add(1)
                        .expect("bounded text cleanup residue count does not overflow");
                }
            }
            for kind in [index_keys::GlobalKind::OperationPointer] {
                let prefix = index_keys::GlobalKey::logical_prefix(kind);
                let mut rows = writer
                    .db()
                    .scan_prefix(
                        &prefix,
                        (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
                    )
                    .await
                    .expect("global text cleanup lane remains readable");
                while rows
                    .next()
                    .await
                    .expect("global text cleanup scan succeeds")
                    .is_some()
                {
                    residue = residue
                        .checked_add(1)
                        .expect("bounded global cleanup residue count does not overflow");
                }
            }
            if residue == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("terminal text lifecycle cleanup drains every generation-owned row");
}

/// Decodes one Active text generation and either reports queued finalization or
/// proves the complete steady-state manifest and entity graph.
async fn observe_steady_state(
    db: &crate::HelixDB,
    expected_live_entities: usize,
) -> TextRowObservation {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("text row contracts require writer storage");
    };
    let scope = DataScope::LegacyUnscoped;
    let mut transient_rows = Vec::new();
    for kind in [
        index_keys::RecordKind::BuildDelta,
        index_keys::RecordKind::TextBuildArtifact,
    ] {
        let prefix =
            ManagedIndexKey::data_prefix(scope, index_keys::ScopedKey::logical_prefix(kind));
        let mut rows = writer
            .db()
            .scan_prefix(
                &prefix,
                (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
            )
            .await
            .expect("transient text row lane remains readable");
        let mut count = 0_usize;
        while rows
            .next()
            .await
            .expect("transient text row scan succeeds")
            .is_some()
        {
            count = count
                .checked_add(1)
                .expect("bounded transient row count does not overflow");
        }
        if count > 0 {
            transient_rows.push(format!("scoped {kind:?}: {count}"));
        }
    }
    for kind in [index_keys::GlobalKind::OperationPointer] {
        let prefix = index_keys::GlobalKey::logical_prefix(kind);
        let mut rows = writer
            .db()
            .scan_prefix(
                &prefix,
                (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
            )
            .await
            .expect("transient global text row lane remains readable");
        let mut count = 0_usize;
        while rows
            .next()
            .await
            .expect("transient global text row scan succeeds")
            .is_some()
        {
            count = count
                .checked_add(1)
                .expect("bounded transient global row count does not overflow");
        }
        if count > 0 {
            transient_rows.push(format!("global {kind:?}: {count}"));
        }
    }
    if !transient_rows.is_empty() {
        return TextRowObservation::Pending {
            rows: transient_rows,
        };
    }

    let mut roots = HashMap::new();
    let root_prefix = ManagedIndexKey::data_prefix(
        scope,
        index_keys::ScopedKey::logical_prefix(index_keys::RecordKind::TextManifestRoot),
    );
    let mut root_rows = writer
        .db()
        .scan_prefix(
            &root_prefix,
            (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
        )
        .await
        .expect("text manifest roots remain readable");
    while let Some(row) = root_rows
        .next()
        .await
        .expect("text manifest root scan succeeds")
    {
        let ManagedIndexKey::Data {
            kind: index_keys::ScopedKey::TextManifestRoot(root_key),
            ..
        } = ManagedIndexKey::parse_from_slice(scope, &row.key)
            .expect("text manifest root key decodes")
        else {
            panic!("text manifest root prefix returned a different typed key");
        };
        let root = index_values::decode_manifest_root(&row.value)
            .expect("text manifest root value decodes");
        assert_eq!(root_key.index_id, root.index_id());
        assert_eq!(root_key.generation, root.generation());
        assert_eq!(root_key.partition, root.partition().fingerprint());
        assert!(
            roots.insert(root_key, root).is_none(),
            "one text partition has one canonical manifest root"
        );
    }
    assert!(
        !roots.is_empty(),
        "Active text generation retains a manifest root"
    );

    let mut pages = HashMap::<index_keys::TextManifestPageKey, Vec<SplitRef>>::new();
    let page_prefix = ManagedIndexKey::data_prefix(
        scope,
        index_keys::ScopedKey::logical_prefix(index_keys::RecordKind::TextManifestPage),
    );
    let mut page_rows = writer
        .db()
        .scan_prefix(
            &page_prefix,
            (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
        )
        .await
        .expect("text manifest pages remain readable");
    while let Some(row) = page_rows
        .next()
        .await
        .expect("text manifest page scan succeeds")
    {
        let ManagedIndexKey::Data {
            kind: index_keys::ScopedKey::TextManifestPage(page_key),
            ..
        } = ManagedIndexKey::parse_from_slice(scope, &row.key)
            .expect("text manifest page key decodes")
        else {
            panic!("text manifest page prefix returned a different typed key");
        };
        let page = index_values::decode_manifest_page(&row.value)
            .expect("text manifest page value decodes");
        assert_eq!(page_key.root.index_id, page.index_id());
        assert_eq!(page_key.root.generation, page.generation());
        assert_eq!(page_key.root.partition, page.partition().fingerprint());
        assert_eq!(page_key.page, page.page());
        assert!(roots.contains_key(&page_key.root));
        assert!(
            pages.insert(page_key, page.entries().to_vec()).is_none(),
            "one manifest page key has one immutable value"
        );
    }

    for (root_key, root) in &roots {
        let mut page_numbers = pages
            .keys()
            .filter_map(|page| (page.root == *root_key).then_some(page.page))
            .collect::<Vec<_>>();
        page_numbers.sort_unstable();
        assert_eq!(
            page_numbers,
            (0..root.page_count()).collect::<Vec<_>>(),
            "manifest pages are contiguous from zero"
        );
        let split_count = pages
            .iter()
            .filter(|(page, _)| page.root == *root_key)
            .map(|(_, entries)| entries.len() as u64)
            .sum::<u64>();
        assert_eq!(split_count, root.split_count());
    }

    let state_prefix = ManagedIndexKey::data_prefix(
        scope,
        index_keys::ScopedKey::logical_prefix(index_keys::RecordKind::TextEntityState),
    );
    let mut state_rows = writer
        .db()
        .scan_prefix(
            &state_prefix,
            (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
        )
        .await
        .expect("text entity states remain readable");
    let mut live_entities = 0_usize;
    let mut state_owners = HashSet::new();
    while let Some(row) = state_rows
        .next()
        .await
        .expect("text entity-state scan succeeds")
    {
        let ManagedIndexKey::Data {
            kind: index_keys::ScopedKey::TextEntityState(state_key),
            ..
        } = ManagedIndexKey::parse_from_slice(scope, &row.key)
            .expect("text entity-state key decodes")
        else {
            panic!("text entity-state prefix returned a different typed key");
        };
        let state = index_values::decode_text_entity_state(&row.value)
            .expect("text entity-state value decodes");
        assert_eq!(state_key.root.index_id, state.index_id);
        assert_eq!(state_key.root.generation, state.generation);
        assert_eq!(state_key.root.partition, state.partition.fingerprint());
        assert_eq!(state_key.entity.kind, state.entity_kind);
        assert_eq!(state_key.entity.id, state.entity_id);
        assert!(roots.contains_key(&state_key.root));
        state_owners.insert((
            state.index_id,
            state.generation,
            state.entity_kind,
            state.entity_id,
            state.partition.fingerprint(),
        ));
        live_entities = live_entities
            .checked_add(usize::from(state.live))
            .expect("bounded live entity count does not overflow");
    }
    assert_eq!(live_entities, expected_live_entities);

    let applied_prefix = ManagedIndexKey::data_prefix(
        scope,
        index_keys::ScopedKey::logical_prefix(index_keys::RecordKind::AppliedState),
    );
    let mut applied_rows = writer
        .db()
        .scan_prefix(
            &applied_prefix,
            (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
        )
        .await
        .expect("builder-applied text states remain readable");
    while let Some(row) = applied_rows
        .next()
        .await
        .expect("builder-applied text-state scan succeeds")
    {
        let ManagedIndexKey::Data {
            kind: index_keys::ScopedKey::AppliedState(applied_key),
            ..
        } = ManagedIndexKey::parse_from_slice(scope, &row.key)
            .expect("builder-applied state key decodes")
        else {
            panic!("builder-applied prefix returned a different typed key");
        };
        let applied = index_values::decode_applied_state(&row.value)
            .expect("builder-applied text-state value decodes");
        assert_eq!(applied_key.index_id, applied.index_id);
        assert_eq!(applied_key.generation, applied.generation);
        assert_eq!(applied_key.entity.kind, applied.entity_kind);
        assert_eq!(applied_key.entity.id, applied.entity_id);
        let AppliedFamilyState::Text(Some((partition, _logical_version))) = applied.state else {
            panic!("Active text build retains only applied live text membership");
        };
        assert!(state_owners.contains(&(
            applied.index_id,
            applied.generation,
            applied.entity_kind,
            applied.entity_id,
            partition.fingerprint(),
        )));
    }

    TextRowObservation::Ready
}
