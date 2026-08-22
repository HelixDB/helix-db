//! Raw storage access contracts for executable interpretation.
//!
//! The planner emits keyspace-aware executable reads. This module is the narrow
//! interpreter boundary that turns those physical read requests into SlateDB
//! raw `get`/`scan` calls and enforces writer-only execution modes.

use bytes::Bytes;
use slatedb::DbReadOps;

use super::*;
use crate::encoding::keys;
use crate::{HelixStorage, HelixWriter};

impl<'db> Interpreter<'db> {
    pub(in crate::execution::interpreter) fn ensure_writer(&self) -> Result<()> {
        writer_from_storage(self.db)
            .map(|_| ())
            .map_err(writer_mode_required)
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn storage_key<'a>(
        &self,
        kind: keys::DataKeyKind<'a>,
    ) -> Bytes {
        keys::DataKey::Data {
            scope: self.tenant_scope,
            kind,
        }
        .to_bytes()
    }

    pub(in crate::execution::interpreter) async fn get_raw(
        &self,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        self.check_execution_deadline()?;
        let key = Bytes::copy_from_slice(key);
        if let Some(active) = self.active_write_tx() {
            return Ok(active.txn.get(&key).await?);
        }
        if let Some(view) = self.request_read_view() {
            return Ok(view.get(&key).await?);
        }
        #[cfg(test)]
        {
            match self.db.storage() {
                HelixStorage::Reader(reader) => Ok(reader.get(&key).await?),
                HelixStorage::Writer(writer) => Ok(writer.get(&key).await?),
            }
        }
        #[cfg(not(test))]
        {
            Err(HelixDbError::InvariantViolation(
                "storage read escaped its request read view".to_string(),
            ))
        }
    }

    pub(in crate::execution::interpreter) async fn multi_get_raw<K>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Bytes>>>
    where
        K: AsRef<[u8]> + Send + Sync,
    {
        self.check_execution_deadline()?;
        match (
            self.active_write_tx(),
            self.request_read_view(),
            self.db.storage(),
        ) {
            (Some(active), _, _) => Ok(active.txn.multi_get(keys).await?),
            (None, Some(view), _) => Ok(view.multi_get(keys).await?),
            #[cfg(test)]
            (None, None, HelixStorage::Reader(reader)) => Ok(reader.multi_get(keys).await?),
            #[cfg(test)]
            (None, None, HelixStorage::Writer(writer)) => Ok(writer.multi_get(keys).await?),
            #[cfg(not(test))]
            (None, None, _) => Err(HelixDbError::InvariantViolation(
                "storage multi-get escaped its request read view".to_string(),
            )),
        }
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) async fn scan_raw_range(
        &self,
        start: Bytes,
        end: Bytes,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_raw_range_limited(start, end, None).await
    }

    pub(in crate::execution::interpreter) async fn scan_raw_range_limited(
        &self,
        start: Bytes,
        end: Bytes,
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let (start, end) = keys::DataKey::data_range(self.tenant_scope, start, end);
        if let Some(active) = self.active_write_tx() {
            let mut iter = active.txn.scan(start..end).await?;
            return collect_limited(
                &mut iter,
                limit,
                self.tenant_scope,
                self.execution_control.clone(),
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            let mut iter = view.scan(start..end).await?;
            return collect_limited(
                &mut iter,
                limit,
                self.tenant_scope,
                self.execution_control.clone(),
            )
            .await;
        }
        #[cfg(test)]
        {
            let mut iter = match self.db.storage() {
                HelixStorage::Reader(reader) => reader.scan(start..end).await?,
                HelixStorage::Writer(writer) => writer.scan(start..end).await?,
            };
            collect_limited(&mut iter, limit, self.tenant_scope, self.execution_control).await
        }
        #[cfg(not(test))]
        {
            Err(HelixDbError::InvariantViolation(
                "storage range scan escaped its request read view".to_string(),
            ))
        }
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) async fn scan_raw_prefix(
        &self,
        prefix: Bytes,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_raw_prefix_limited(prefix, None).await
    }

    pub(in crate::execution::interpreter) async fn scan_raw_prefix_limited(
        &self,
        prefix: Bytes,
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let prefix = keys::DataKey::data_prefix(self.tenant_scope, prefix);
        if let Some(active) = self.active_write_tx() {
            let mut iter = active.txn.scan_prefix(prefix, ..).await?;
            return collect_limited(
                &mut iter,
                limit,
                self.tenant_scope,
                self.execution_control.clone(),
            )
            .await;
        }
        if let Some(view) = self.request_read_view() {
            let mut iter = view.scan_prefix(prefix, ..).await?;
            return collect_limited(
                &mut iter,
                limit,
                self.tenant_scope,
                self.execution_control.clone(),
            )
            .await;
        }
        #[cfg(test)]
        {
            let mut iter = match self.db.storage() {
                HelixStorage::Reader(reader) => reader.scan_prefix(prefix, ..).await?,
                HelixStorage::Writer(writer) => writer.scan_prefix(prefix, ..).await?,
            };
            collect_limited(&mut iter, limit, self.tenant_scope, self.execution_control).await
        }
        #[cfg(not(test))]
        {
            Err(HelixDbError::InvariantViolation(
                "storage prefix scan escaped its request read view".to_string(),
            ))
        }
    }

    pub(in crate::execution::interpreter) fn writer(&self) -> Result<&HelixWriter> {
        writer_from_storage(self.db).map_err(writer_mode_required)
    }
}

async fn collect_limited(
    iter: &mut slatedb::DbIterator,
    limit: Option<usize>,
    tenant_scope: crate::encoding::keys::scope::DataScope,
    execution_control: crate::execution_control::ExecutionControl,
) -> Result<Vec<(Bytes, Bytes)>> {
    let mut rows = Vec::new();
    while let Some(kv) = iter.next().await? {
        execution_control.check()?;
        let Some(key) = tenant_scope.strip_key(&kv.key) else {
            return Err(HelixDbError::InvariantViolation(
                "tenant-scoped scan returned key outside tenant prefix".to_string(),
            ));
        };
        rows.push((Bytes::copy_from_slice(key), kv.value));
        if limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }
    Ok(rows)
}

fn writer_from_storage(db: &HelixDB) -> std::result::Result<&HelixWriter, crate::HelixDbMode> {
    match db.storage() {
        HelixStorage::Writer(writer) => Ok(writer.as_ref()),
        HelixStorage::Reader(_) => Err(db.mode()),
    }
}

fn writer_mode_required(mode: crate::HelixDbMode) -> HelixDbError {
    HelixDbError::WriterModeRequired {
        actual: mode.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::test_support;
    use super::*;

    #[tokio::test]
    async fn writer_guards_accept_writer_and_reject_read_only_handles() {
        let config = test_support::in_memory_config("storage-writer-guards");
        let writer_db = test_support::open_db_with_config(config.clone()).await;
        let reader_db = test_support::open_reader_with_config(config).await;

        Interpreter::new(&writer_db, context::ParamBindings::default())
            .ensure_writer()
            .unwrap();
        let writer_ctx = ExecutionContext::new(&writer_db, context::ParamBindings::default());
        assert!(writer_ctx.writer().is_ok());

        let err = Interpreter::new(&reader_db, context::ParamBindings::default())
            .ensure_writer()
            .unwrap_err();
        assert!(matches!(err, HelixDbError::WriterModeRequired { .. }));
        let reader_ctx = ExecutionContext::new(&reader_db, context::ParamBindings::default());
        let Err(err) = reader_ctx.writer() else {
            panic!("reader context must reject writer access");
        };
        assert!(matches!(err, HelixDbError::WriterModeRequired { .. }));
    }

    #[tokio::test]
    async fn get_raw_reads_present_values_and_preserves_missing_values() {
        let db = test_support::open_db("storage-get-raw").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let key = b"storage/get/present";
        ctx.writer()
            .unwrap()
            .db()
            .put(key, Bytes::from_static(b"value"))
            .await
            .unwrap();

        assert_eq!(
            ctx.get_raw(key).await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(ctx.get_raw(b"storage/get/missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn tenant_scoped_raw_reads_and_scans_are_isolated() {
        let db = test_support::open_db("storage-tenant-scope").await;
        let tenant_a =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("valid tenant");
        let tenant_b =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000B")
                .expect("valid tenant");
        let scope_a = crate::DataScope::Tenant(tenant_a);
        let scope_b = crate::DataScope::Tenant(tenant_b);
        let ctx_a = ExecutionContext::new_scoped(&db, context::ParamBindings::default(), scope_a);
        let ctx_b = ExecutionContext::new_scoped(&db, context::ParamBindings::default(), scope_b);
        let legacy_ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let raw = db.inner_db();

        raw.put(
            keys::DataKey::Data {
                scope: scope_a,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1)),
            }
            .to_bytes(),
            Bytes::from_static(b"a"),
        )
        .await
        .unwrap();
        raw.put(
            keys::DataKey::Data {
                scope: scope_b,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1)),
            }
            .to_bytes(),
            Bytes::from_static(b"b"),
        )
        .await
        .unwrap();
        raw.put(
            keys::DataKey::Data {
                scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1)),
            }
            .to_bytes(),
            Bytes::from_static(b"legacy"),
        )
        .await
        .unwrap();

        let logical_key = keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1));
        assert_eq!(
            ctx_a
                .get_raw(
                    &keys::DataKey::Data {
                        scope: scope_a,
                        kind: logical_key.clone(),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap(),
            Some(Bytes::from_static(b"a"))
        );
        assert_eq!(
            ctx_b
                .get_raw(
                    &keys::DataKey::Data {
                        scope: scope_b,
                        kind: logical_key.clone(),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap(),
            Some(Bytes::from_static(b"b"))
        );
        assert_eq!(
            legacy_ctx
                .get_raw(
                    &keys::DataKey::Data {
                        scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
                        kind: logical_key,
                    }
                    .to_bytes(),
                )
                .await
                .unwrap(),
            Some(Bytes::from_static(b"legacy"))
        );

        let scoped_keys = ctx_a
            .scan_raw_prefix(
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1)).to_bytes(),
            )
            .await
            .unwrap();
        assert_eq!(
            scoped_keys,
            vec![(
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1)).to_bytes(),
                Bytes::from_static(b"a"),
            )]
        );
    }

    #[tokio::test]
    async fn raw_reads_work_against_read_only_handles() {
        let config = test_support::in_memory_config("storage-reader-raw-reads");
        let writer_db = test_support::open_db_with_config(config.clone()).await;
        let raw = writer_db.inner_db();
        for (key, value) in [
            (b"storage/reader/a".as_slice(), b"a".as_slice()),
            (b"storage/reader/b".as_slice(), b"b".as_slice()),
            (b"storage/other/c".as_slice(), b"c".as_slice()),
        ] {
            raw.put(key, Bytes::copy_from_slice(value)).await.unwrap();
        }
        raw.flush().await.unwrap();
        let reader_db = test_support::open_reader_with_config(config).await;
        let ctx = ExecutionContext::new(&reader_db, context::ParamBindings::default());

        assert_eq!(
            ctx.get_raw(b"storage/reader/a").await.unwrap(),
            Some(Bytes::from_static(b"a"))
        );
        assert_eq!(
            ctx.scan_raw_range_limited(
                Bytes::from_static(b"storage/reader/a"),
                Bytes::from_static(b"storage/reader/c"),
                Some(1),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
            vec![Bytes::from_static(b"storage/reader/a")]
        );
        assert_eq!(
            ctx.scan_raw_prefix_limited(Bytes::from_static(b"storage/reader/"), Some(2))
                .await
                .unwrap()
                .into_iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>(),
            vec![
                Bytes::from_static(b"storage/reader/a"),
                Bytes::from_static(b"storage/reader/b"),
            ]
        );
    }

    #[tokio::test]
    async fn multi_get_and_prefix_reads_use_the_selected_storage_view() {
        let config = test_support::in_memory_config("storage-multi-get-views");
        let writer_db = test_support::open_db_with_config(config.clone()).await;
        let committed_key = Bytes::from_static(b"storage/multi/committed");
        let missing_key = Bytes::from_static(b"storage/multi/missing");
        writer_db
            .inner_db()
            .put(committed_key.clone(), Bytes::from_static(b"committed"))
            .await
            .unwrap();
        writer_db.inner_db().flush().await.unwrap();

        let writer = ExecutionContext::new(&writer_db, context::ParamBindings::default());
        assert_eq!(
            writer
                .multi_get_raw(&[committed_key.clone(), missing_key.clone()])
                .await
                .unwrap(),
            vec![Some(Bytes::from_static(b"committed")), None]
        );

        let reader_db = test_support::open_reader_with_config(config).await;
        let reader = ExecutionContext::new(&reader_db, context::ParamBindings::default());
        assert_eq!(
            reader
                .multi_get_raw(&[committed_key.clone(), missing_key])
                .await
                .unwrap(),
            vec![Some(Bytes::from_static(b"committed")), None]
        );

        let staged_key = Bytes::from_static(b"storage/multi/staged");
        let mut transaction = ExecutionContext::new(&writer_db, context::ParamBindings::default());
        transaction.enable_request_write_scope().await.unwrap();
        transaction
            .active_write_tx()
            .expect("write scope owns its transaction")
            .txn
            .put(staged_key.clone(), Bytes::from_static(b"staged"))
            .unwrap();
        assert_eq!(
            transaction
                .multi_get_raw(&[committed_key.clone(), staged_key.clone()])
                .await
                .unwrap(),
            vec![
                Some(Bytes::from_static(b"committed")),
                Some(Bytes::from_static(b"staged")),
            ]
        );
        assert_eq!(
            transaction
                .scan_raw_prefix_limited(Bytes::from_static(b"storage/multi/"), None)
                .await
                .unwrap(),
            vec![
                (committed_key, Bytes::from_static(b"committed")),
                (staged_key, Bytes::from_static(b"staged")),
            ]
        );
        transaction.abort_request_write_scope();
    }

    #[tokio::test]
    async fn range_scans_preserve_storage_order_and_apply_limits() {
        let db = test_support::open_db("storage-range-scans").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        for (key, value) in [
            (b"storage/range/1".as_slice(), b"one".as_slice()),
            (b"storage/range/2".as_slice(), b"two".as_slice()),
            (b"storage/range/3".as_slice(), b"three".as_slice()),
            (b"storage/range/4".as_slice(), b"four".as_slice()),
        ] {
            ctx.writer()
                .unwrap()
                .db()
                .put(key, Bytes::copy_from_slice(value))
                .await
                .unwrap();
        }

        let rows = ctx
            .scan_raw_range(
                Bytes::from_static(b"storage/range/1"),
                Bytes::from_static(b"storage/range/4"),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    Bytes::from_static(b"storage/range/1"),
                    Bytes::from_static(b"one"),
                ),
                (
                    Bytes::from_static(b"storage/range/2"),
                    Bytes::from_static(b"two"),
                ),
                (
                    Bytes::from_static(b"storage/range/3"),
                    Bytes::from_static(b"three"),
                ),
            ]
        );

        let limited = ctx
            .scan_raw_range_limited(
                Bytes::from_static(b"storage/range/1"),
                Bytes::from_static(b"storage/range/4"),
                Some(2),
            )
            .await
            .unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].0, Bytes::from_static(b"storage/range/1"));
        assert_eq!(limited[1].0, Bytes::from_static(b"storage/range/2"));
    }

    #[tokio::test]
    async fn prefix_scans_filter_by_prefix_and_apply_limits() {
        let db = test_support::open_db("storage-prefix-scans").await;
        let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        for (key, value) in [
            (b"storage/prefix/a".as_slice(), b"a".as_slice()),
            (b"storage/prefix/b".as_slice(), b"b".as_slice()),
            (b"storage/other/c".as_slice(), b"c".as_slice()),
        ] {
            ctx.writer()
                .unwrap()
                .db()
                .put(key, Bytes::copy_from_slice(value))
                .await
                .unwrap();
        }

        let rows = ctx
            .scan_raw_prefix(Bytes::from_static(b"storage/prefix/"))
            .await
            .unwrap();
        assert_eq!(
            rows.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![
                Bytes::from_static(b"storage/prefix/a"),
                Bytes::from_static(b"storage/prefix/b"),
            ]
        );

        let limited = ctx
            .scan_raw_prefix_limited(Bytes::from_static(b"storage/prefix/"), Some(1))
            .await
            .unwrap();
        assert_eq!(
            limited.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![Bytes::from_static(b"storage/prefix/a")]
        );
    }
}
