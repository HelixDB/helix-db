//! Process-local mutation/activation exclusion by logical data scope.
//!
//! Planning retains a shared [`IndexScopeCatalogPermit`] until its graph
//! transaction has opened and loaded the canonical mutation catalog. Ordinary
//! graph transactions separately retain a shared [`IndexScopeMutationPermit`]
//! until commit or abort. Active lifecycle publication takes both gates
//! exclusively, while DROP takes only catalog exclusivity and relies on the
//! graph transaction's canonical reads for serializable conflict detection.
//! The registry stores weak references so one-off tenant scopes do not
//! accumulate forever.
//!
//! This gate coordinates only the single writer process. Readers need no gate:
//! their request-scoped SlateDB snapshots retain the versions they observe.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

use crate::encoding::v2::keys::scope::DataScope;

/// Shared authority retained by one graph mutation transaction.
#[derive(Debug)]
pub(crate) struct IndexScopeMutationPermit {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Shared catalog authority transferred from planning through write-view open.
#[derive(Debug)]
pub(crate) struct IndexScopeCatalogPermit {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Exclusive authority for a canonical change visible to planning.
#[derive(Debug)]
pub(crate) struct IndexScopeCatalogChangePermit {
    _guard: OwnedRwLockWriteGuard<()>,
}

/// Exclusive catalog and mutation authority for Active publication or cleanup.
#[derive(Debug)]
pub(crate) struct IndexScopeLifecyclePermit {
    _catalog_guard: OwnedRwLockWriteGuard<()>,
    _mutation_guard: OwnedRwLockWriteGuard<()>,
}

/// Exact-scope gate registry shared by mutation contexts and family drivers.
#[derive(Debug, Default)]
pub(crate) struct IndexScopeGates {
    gates: Mutex<HashMap<DataScope, Weak<RwLock<()>>>>,
    catalogs: Mutex<HashMap<DataScope, Weak<RwLock<()>>>>,
    catalog_refreshes: Mutex<HashMap<DataScope, Weak<AsyncMutex<()>>>>,
}

impl IndexScopeGates {
    /// Acquires shared scope authority before a graph transaction takes its snapshot.
    pub(crate) async fn mutation_permit(&self, scope: DataScope) -> IndexScopeMutationPermit {
        IndexScopeMutationPermit {
            _guard: self.mutation_gate(scope).read_owned().await,
        }
    }

    /// Acquires shared authority before refreshing the catalog used for planning.
    pub(crate) async fn catalog_permit(&self, scope: DataScope) -> IndexScopeCatalogPermit {
        IndexScopeCatalogPermit {
            _guard: self.catalog_gate(scope).read_owned().await,
        }
    }

    /// Acquires exclusive catalog authority for Active-to-Dropping publication.
    pub(crate) async fn catalog_change_permit(
        &self,
        scope: DataScope,
    ) -> IndexScopeCatalogChangePermit {
        IndexScopeCatalogChangePermit {
            _guard: self.catalog_gate(scope).write_owned().await,
        }
    }

    /// Acquires catalog then mutation exclusivity for one lifecycle transaction.
    pub(crate) async fn lifecycle_permit(&self, scope: DataScope) -> IndexScopeLifecyclePermit {
        IndexScopeLifecyclePermit {
            _catalog_guard: self.catalog_gate(scope).write_owned().await,
            _mutation_guard: self.mutation_gate(scope).write_owned().await,
        }
    }

    /// Serializes persisted-catalog refreshes for one handle and scope.
    ///
    /// Storage scans happen outside the synchronous runtime-state lock. This
    /// separate gate prevents an older overlapping scan from publishing after
    /// a newer scan without serializing unrelated tenant scopes.
    pub(crate) async fn catalog_refresh_permit(&self, scope: DataScope) -> OwnedMutexGuard<()> {
        self.catalog_refresh_gate(scope).lock_owned().await
    }

    fn mutation_gate(&self, scope: DataScope) -> Arc<RwLock<()>> {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(gate) = gates.get(&scope).and_then(Weak::upgrade) else {
            gates.retain(|_, gate| gate.strong_count() != 0);
            let gate = Arc::new(RwLock::new(()));
            gates.insert(scope, Arc::downgrade(&gate));
            return gate;
        };
        gate
    }

    fn catalog_gate(&self, scope: DataScope) -> Arc<RwLock<()>> {
        let mut gates = self
            .catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(gate) = gates.get(&scope).and_then(Weak::upgrade) else {
            gates.retain(|_, gate| gate.strong_count() != 0);
            let gate = Arc::new(RwLock::new(()));
            gates.insert(scope, Arc::downgrade(&gate));
            return gate;
        };
        gate
    }

    fn catalog_refresh_gate(&self, scope: DataScope) -> Arc<AsyncMutex<()>> {
        let mut gates = self
            .catalog_refreshes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(gate) = gates.get(&scope).and_then(Weak::upgrade) else {
            gates.retain(|_, gate| gate.strong_count() != 0);
            let gate = Arc::new(AsyncMutex::new(()));
            gates.insert(scope, Arc::downgrade(&gate));
            return gate;
        };
        gate
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn lifecycle_scope_waits_for_catalog_and_mutations_but_not_other_scopes() {
        let gates = Arc::new(IndexScopeGates::default());
        let first_scope = DataScope::LegacyUnscoped;
        let other_scope =
            DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
        let catalog = gates.catalog_permit(first_scope).await;
        let mutation = gates.mutation_permit(first_scope).await;
        let waiting = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.lifecycle_permit(first_scope).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        let other = gates.lifecycle_permit(other_scope).await;
        drop(other);
        drop(catalog);
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(mutation);
        drop(waiting.await.expect("lifecycle waiter joins"));
    }

    #[tokio::test]
    async fn catalog_changes_wait_for_planning_but_not_active_mutations() {
        let gates = Arc::new(IndexScopeGates::default());
        let scope = DataScope::LegacyUnscoped;
        let mutation = gates.mutation_permit(scope).await;
        let catalog_change = gates.catalog_change_permit(scope).await;
        drop(catalog_change);

        let catalog = gates.catalog_permit(scope).await;
        let waiting = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.catalog_change_permit(scope).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(catalog);
        drop(waiting.await.expect("catalog-change waiter joins"));
        drop(mutation);
    }

    #[tokio::test]
    async fn catalog_refreshes_serialize_only_within_the_same_scope() {
        let gates = Arc::new(IndexScopeGates::default());
        let first_scope = DataScope::LegacyUnscoped;
        let other_scope =
            DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
        let first = gates.catalog_refresh_permit(first_scope).await;
        let waiting = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.catalog_refresh_permit(first_scope).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        let other = gates.catalog_refresh_permit(other_scope).await;
        drop(other);
        drop(first);
        drop(waiting.await.expect("same-scope refresh waiter joins"));
    }

    #[tokio::test]
    async fn poisoned_registries_recover_without_losing_scope_exclusion() {
        let gates = Arc::new(IndexScopeGates::default());
        let poison_scope_gates = {
            let gates = Arc::clone(&gates);
            std::thread::spawn(move || {
                let _guard = gates.gates.lock().expect("scope registry starts healthy");
                panic!("poison scope registry");
            })
        };
        assert!(poison_scope_gates.join().is_err());
        let poison_catalog_gates = {
            let gates = Arc::clone(&gates);
            std::thread::spawn(move || {
                let _guard = gates
                    .catalogs
                    .lock()
                    .expect("catalog scope registry starts healthy");
                panic!("poison catalog scope registry");
            })
        };
        assert!(poison_catalog_gates.join().is_err());
        let poison_catalog_refresh_gates = {
            let gates = Arc::clone(&gates);
            std::thread::spawn(move || {
                let _guard = gates
                    .catalog_refreshes
                    .lock()
                    .expect("catalog refresh registry starts healthy");
                panic!("poison catalog refresh registry");
            })
        };
        assert!(poison_catalog_refresh_gates.join().is_err());

        let scope = DataScope::LegacyUnscoped;
        let catalog = gates.catalog_permit(scope).await;
        let mutation = gates.mutation_permit(scope).await;
        let waiting = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.lifecycle_permit(scope).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(catalog);
        drop(mutation);
        drop(waiting.await.expect("lifecycle waiter joins"));

        let refresh = gates.catalog_refresh_permit(scope).await;
        drop(refresh);
    }
}
