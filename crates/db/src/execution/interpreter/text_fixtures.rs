//! Raw-row text fixtures shared by interpreter contracts and the text
//! transaction batching benchmark.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};

use crate::config::TextIndexDefinition;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values as index_values;
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
    IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
};

/// Encodes one unscoped V2 logical key through the canonical V1 boundary.
pub(crate) fn scoped_key(logical: index_keys::ScopedKey) -> Bytes {
    index_keys::ManagedIndexKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: logical,
    }
    .to_bytes()
}

/// Seeds one canonical Active text generation before the runtime opens.
pub(crate) async fn seed_active_text_generation(
    database: &str,
    object_store: Arc<dyn ObjectStore>,
    definition: &TextIndexDefinition,
) -> index_keys::TextManifestRootKey {
    seed_active_text_generation_with(
        database,
        object_store,
        definition,
        IndexId::initial(),
        crate::index_lifecycle::TextManifestRevision::initial(),
        true,
    )
    .await
    .expect("the unpartitioned fixture seeds a manifest root")
}

pub(crate) async fn seed_active_text_generation_with(
    database: &str,
    object_store: Arc<dyn ObjectStore>,
    definition: &TextIndexDefinition,
    index_id: IndexId,
    revision: crate::index_lifecycle::TextManifestRevision,
    seed_unpartitioned_root: bool,
) -> Option<index_keys::TextManifestRootKey> {
    let raw = Db::builder(database, object_store)
        .build()
        .await
        .expect("raw Active-text fixture opens");
    crate::migrations::startup::bootstrap_writer(&raw)
        .await
        .expect("raw Active-text fixture bootstraps");
    let active = IndexRecordV2::building(
        index_id,
        ValidatedDynamicIndexDefinition::try_from(definition.clone())
            .expect("text definition validates for V2"),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([0x91; 16]).expect("operation ID is non-nil"),
    )
    .expect("building text record validates")
    .transition(IndexStateTransition::Activate)
    .expect("text record activates");
    let root = seed_unpartitioned_root.then_some(index_keys::TextManifestRootKey {
        index_id,
        generation: IndexGenerationId::initial(),
        partition: crate::index_lifecycle::work::TextPartition::Unpartitioned.fingerprint(),
    });
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("Active-text seed transaction opens");
    transaction
        .put(
            scoped_key(index_keys::ScopedKey::index_record(
                active.identity().clone(),
            )),
            index_values::encode_index_record(&active),
        )
        .expect("Active text record stages");
    if let Some(root) = root {
        transaction
            .put(
                scoped_key(index_keys::ScopedKey::TextManifestRoot(root)),
                index_values::encode_manifest_root(
                    &crate::index_lifecycle::work::TextManifestRootValue::try_new(
                        index_id,
                        IndexGenerationId::initial(),
                        crate::index_lifecycle::work::TextPartition::Unpartitioned,
                        revision,
                        0,
                        0,
                    )
                    .expect("an empty manifest accepts any live revision"),
                ),
            )
            .expect("empty Active manifest stages");
    }
    transaction
        .commit()
        .await
        .expect("Active-text fixture commits");
    raw.close().await.expect("raw Active-text fixture closes");
    root
}
