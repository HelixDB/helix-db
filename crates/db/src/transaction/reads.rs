//! Synchronous admission at the backend boxed-read trait boundary.
//! Every entry point creates exactly one admitted future; defaults are overridden
//! so they cannot allocate an extra frame before reaching this boundary.

use super::{call, storage_error, Owned, ReadKind, ReadSource, View};
use bytes::Bytes;
use slatedb::{config, DbReadOps};

// Override every default as well: async_trait defaults would allocate a second,
// unadmitted future before dispatch. Explicit boxed signatures let admission run
// synchronously at the storage trait boundary, before its first allocation.
macro_rules! read_ops {
    ($target:ty) => {
        impl DbReadOps for $target {
            fn get<'life0, 'async_trait, K>(
                &'life0 self,
                key: K,
            ) -> call::Read<'async_trait, Option<Bytes>>
            where
                'life0: 'async_trait,
                K: AsRef<[u8]> + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Point)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.keys(std::slice::from_ref(&key)))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.get(key).await
                })
            }

            fn get_with_options<'life0, 'life1, 'async_trait, K>(
                &'life0 self,
                key: K,
                options: &'life1 config::ReadOptions,
            ) -> call::Read<'async_trait, Option<Bytes>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                K: AsRef<[u8]> + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Point)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.keys(std::slice::from_ref(&key)))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.get_with_options(key, options).await
                })
            }

            fn get_key_value<'life0, 'async_trait, K>(
                &'life0 self,
                key: K,
            ) -> call::Read<'async_trait, Option<slatedb::KeyValue>>
            where
                'life0: 'async_trait,
                K: AsRef<[u8]> + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Point)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.keys(std::slice::from_ref(&key)))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.get_key_value(key).await
                })
            }

            fn get_key_value_with_options<'life0, 'life1, 'async_trait, K>(
                &'life0 self,
                key: K,
                options: &'life1 config::ReadOptions,
            ) -> call::Read<'async_trait, Option<slatedb::KeyValue>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                K: AsRef<[u8]> + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Point)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.keys(std::slice::from_ref(&key)))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.get_key_value_with_options(key, options).await
                })
            }

            fn multi_get<'life0, 'life1, 'async_trait, K>(
                &'life0 self,
                keys: &'life1 [K],
            ) -> call::Read<'async_trait, Vec<Option<Bytes>>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                K: AsRef<[u8]> + Send + Sync + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::MultiGet { keys: keys.len() })?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.keys(keys))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.multi_get(keys).await
                })
            }

            fn multi_get_with_options<'life0, 'life1, 'life2, 'async_trait, K>(
                &'life0 self,
                keys: &'life1 [K],
                options: &'life2 config::ReadOptions,
            ) -> call::Read<'async_trait, Vec<Option<Bytes>>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                K: AsRef<[u8]> + Send + Sync + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::MultiGet { keys: keys.len() })?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.keys(keys))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.multi_get_with_options(keys, options).await
                })
            }

            fn scan<'life0, 'async_trait, T>(
                &'life0 self,
                range: T,
            ) -> call::Read<'async_trait, slatedb::DbIterator>
            where
                'life0: 'async_trait,
                T: slatedb::ByteRangeBounds + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Scan)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.range(&range, None))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.scan(range).await
                })
            }

            fn scan_with_options<'life0, 'life1, 'async_trait, T>(
                &'life0 self,
                range: T,
                options: &'life1 config::ScanOptions,
            ) -> call::Read<'async_trait, slatedb::DbIterator>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                T: slatedb::ByteRangeBounds + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Scan)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.range(&range, None))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.scan_with_options(range, options).await
                })
            }

            fn scan_prefix<'life0, 'async_trait, P, T>(
                &'life0 self,
                prefix: P,
                subrange: T,
            ) -> call::Read<'async_trait, slatedb::DbIterator>
            where
                'life0: 'async_trait,
                P: AsRef<[u8]> + Send + 'async_trait,
                T: slatedb::ByteRangeBounds + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Scan)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.range(&subrange, Some(prefix.as_ref().len())))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw.scan_prefix(prefix, subrange).await
                })
            }

            fn scan_prefix_with_options<'life0, 'life1, 'async_trait, P, T>(
                &'life0 self,
                prefix: P,
                subrange: T,
                options: &'life1 config::ScanOptions,
            ) -> call::Read<'async_trait, slatedb::DbIterator>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                P: AsRef<[u8]> + Send + 'async_trait,
                T: slatedb::ByteRangeBounds + Send + 'async_trait,
                Self: 'async_trait,
            {
                let view = self.read_context().0;
                call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
                    self.before_read(ReadKind::Scan)?;
                    let _memory = view
                        .tracking
                        .map(|tracker| tracker.range(&subrange, Some(prefix.as_ref().len())))
                        .transpose()
                        .map_err(storage_error)?;
                    view.raw
                        .scan_prefix_with_options(prefix, subrange, options)
                        .await
                })
            }
        }
    };
}
read_ops!(Owned);
read_ops!(View<'_>);
read_ops!(crate::search::vector::MeasuredVectorTransaction<'_>);
