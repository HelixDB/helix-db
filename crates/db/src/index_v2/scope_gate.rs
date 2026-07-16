//! Process-local mutation/activation exclusion by logical data scope.
//!
//! Ordinary graph transactions retain a shared [`IndexScopeMutationPermit`]
//! from before their SlateDB snapshot begins until after commit or abort.
//! Lifecycle stages that must observe a quiescent scope retain the matching
//! exclusive permit across their repository-owned transaction. The registry
//! stores weak references so one-off tenant scopes do not accumulate forever.
//!
//! This gate coordinates the single writer process. Phase 8 adds the separate
//! cross-process reader-lease contract required before public V2 reads.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

use crate::encoding::v1::keys::tenant::DataScope;

/// Shared authority retained by one graph mutation transaction.
#[derive(Debug)]
pub(crate) struct IndexScopeMutationPermit {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Exact-scope gate registry shared by mutation contexts and family drivers.
#[derive(Debug, Default)]
pub(crate) struct IndexScopeGates {
    gates: Mutex<HashMap<DataScope, Weak<RwLock<()>>>>,
    catalog_refreshes: Mutex<HashMap<DataScope, Weak<AsyncMutex<()>>>>,
}

impl IndexScopeGates {
    /// Acquires shared scope authority before a graph transaction takes its snapshot.
    pub(crate) async fn mutation_permit(&self, scope: DataScope) -> IndexScopeMutationPermit {
        IndexScopeMutationPermit {
            _guard: self.gate(scope).read_owned().await,
        }
    }

    /// Acquires exclusive scope authority before a lifecycle transaction begins.
    pub(crate) async fn exclusive_permit(&self, scope: DataScope) -> OwnedRwLockWriteGuard<()> {
        self.gate(scope).write_owned().await
    }

    /// Serializes persisted-catalog refreshes for one handle and scope.
    ///
    /// Storage scans happen outside the synchronous runtime-state lock. This
    /// separate gate prevents an older overlapping scan from publishing after
    /// a newer scan without serializing unrelated tenant scopes.
    pub(crate) async fn catalog_refresh_permit(&self, scope: DataScope) -> OwnedMutexGuard<()> {
        self.catalog_refresh_gate(scope).lock_owned().await
    }

    fn gate(&self, scope: DataScope) -> Arc<RwLock<()>> {
        let mut gates = self
            .gates
            .lock()
            .expect("index scope gate lock is not poisoned");
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
            .expect("catalog refresh gate lock is not poisoned");
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
    async fn exclusive_scope_waits_for_mutations_but_not_other_scopes() {
        let gates = Arc::new(IndexScopeGates::default());
        let first_scope = DataScope::LegacyUnscoped;
        let other_scope =
            DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(7));
        let mutation = gates.mutation_permit(first_scope).await;
        let waiting = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.exclusive_permit(first_scope).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        let other = gates.exclusive_permit(other_scope).await;
        drop(other);
        drop(mutation);
        drop(waiting.await.expect("exclusive waiter joins"));
    }

    #[tokio::test]
    async fn catalog_refreshes_serialize_only_within_the_same_scope() {
        let gates = Arc::new(IndexScopeGates::default());
        let first_scope = DataScope::LegacyUnscoped;
        let other_scope =
            DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(7));
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
}
