//! Process-local mutation/activation exclusion by logical data scope.
//!
//! Planning retains a shared [`IndexScopeCatalogPermit`] until its graph
//! transaction has opened and loaded the canonical mutation catalog. Ordinary
//! graph transactions separately retain a shared [`IndexScopeMutationPermit`]
//! until commit or abort. Active lifecycle publication takes both gates
//! exclusively, while DROP takes only catalog exclusivity and relies on the
//! graph transaction's canonical reads for serializable conflict detection.
//! Build steps that must see no concurrent graph write but leave the Active
//! index set unchanged take only mutation exclusivity
//! ([`IndexScopeWriteFencePermit`]), so planning and reads continue. Each
//! family names the gate of every step through one exhaustive [`StepGate`]
//! mapping.
//! The mutation, refresh, and publication registries store weak references
//! so one-off tenant scopes do not accumulate forever.
//!
//! Each scope's catalog gate also carries a [`CatalogVersion`] that exclusive
//! catalog permits advance when released, so planning can reuse a catalog
//! loaded under an earlier shared permit instead of rescanning canonical
//! records. Catalog gates are therefore strong and never removed.
//!
//! This gate coordinates only the single writer process. Readers need no gate:
//! their request-scoped SlateDB snapshots retain the versions they observe.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Version of one scope's planner-visible canonical catalog.
///
/// Every canonical change that can alter a scope's Active index set commits
/// while an exclusive catalog permit ([`IndexScopeCatalogChangePermit`] or
/// [`IndexScopeLifecyclePermit`]) is held, and that permit advances the
/// version as it is released, before its gate unlocks. The version cannot
/// change while any shared [`IndexScopeCatalogPermit`] is held. So a catalog
/// loaded under a shared permit at version `v` is still exact for any later
/// request that observes `v` under its own shared permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CatalogVersion(u64);

/// Advances one scope's catalog version when dropped.
///
/// A permit that is released without committing a change still advances the
/// version; that only costs the next planner one catalog reload.
#[derive(Debug)]
struct CatalogVersionAdvance(Arc<AtomicU64>);

impl Drop for CatalogVersionAdvance {
    fn drop(&mut self) {
        // Release pairs with the Acquire load in `catalog_permit`. The gate's
        // own unlock also orders this store before any later shared holder.
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// Shared catalog authority transferred from planning through write-view open.
#[derive(Debug)]
pub(crate) struct IndexScopeCatalogPermit {
    version: CatalogVersion,
    _guard: OwnedRwLockReadGuard<()>,
}

impl IndexScopeCatalogPermit {
    /// Returns the scope's catalog version, fixed for as long as this permit is held.
    pub(crate) const fn version(&self) -> CatalogVersion {
        self.version
    }
}

/// Exclusive authority for a canonical change visible to planning.
///
/// Fields drop in declaration order, so the version advances before the
/// gate unlocks.
#[derive(Debug)]
pub(crate) struct IndexScopeCatalogChangePermit {
    _advance: CatalogVersionAdvance,
    _guard: OwnedRwLockWriteGuard<()>,
}

/// Exclusive catalog and mutation authority for Active publication or cleanup.
///
/// Fields drop in declaration order, so the version advances before either
/// gate unlocks.
#[derive(Debug)]
pub(crate) struct IndexScopeLifecyclePermit {
    _advance: CatalogVersionAdvance,
    _catalog_guard: OwnedRwLockWriteGuard<()>,
    _mutation_guard: OwnedRwLockWriteGuard<()>,
}

/// Exclusive mutation authority for a build step that leaves the Active index
/// set unchanged.
///
/// It excludes every graph transaction in the scope, so a step that drains or
/// validates a generation observes no new build delta. It does not exclude
/// planning: the planner-visible catalog holds only Active indexes, and graph
/// transactions read Building and Dropping records through their own
/// serializable transaction, so the step neither takes the catalog gate nor
/// advances the catalog version.
///
/// Lock order is catalog, then mutation. A holder must never wait for a
/// catalog permit: a graph transaction holding a shared catalog permit can be
/// queued for the mutation gate this permit holds, and an exclusive catalog
/// waiter queued behind that transaction blocks every later shared one.
#[derive(Debug)]
pub(crate) struct IndexScopeWriteFencePermit {
    _mutation_guard: OwnedRwLockWriteGuard<()>,
}

/// The scope coordination one index operation step holds through its commit.
///
/// Every family maps each of its steps to exactly one gate in an exhaustive
/// match, so a new stage cannot inherit a gate by omission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepGate {
    /// The step coordinates through its own transaction alone.
    None,
    /// The step must observe no concurrent graph write but leaves the Active
    /// index set unchanged.
    WriteFence,
    /// The step changes the scope's Active index set.
    Lifecycle,
}

/// Exclusive process-local ownership of one generation's physical publication.
///
/// Queued publication into an Active generation, build steps, activation,
/// retirement, and compaction of the same generation serialize on this
/// permit. Work that touches only the queue (deferral, reconciliation, and
/// discards) does not take it. It complements, and never replaces, the
/// transactional checks each publication makes against the canonical record
/// and SlateDB writer fencing.
#[derive(Debug)]
pub(crate) struct IndexGenerationPublicationPermit {
    target: super::queue::QueueTarget,
    _guard: OwnedMutexGuard<()>,
}

impl IndexGenerationPublicationPermit {
    /// Returns the generation this permit owns.
    pub(crate) const fn target(&self) -> super::queue::QueueTarget {
        self.target
    }
}

/// One scope's catalog gate and the version its exclusive permits advance.
///
/// Entries are strong and never removed: a removed version would restart at
/// a value an earlier cached catalog recorded. That is one lock and one
/// counter per scope, matching the runtime catalog map, which also keeps
/// every scope it has loaded.
#[derive(Debug, Clone, Default)]
struct CatalogGate {
    lock: Arc<RwLock<()>>,
    version: Arc<AtomicU64>,
}

/// Exact-scope gate registry shared by mutation contexts and family drivers.
#[derive(Debug, Default)]
pub(crate) struct IndexScopeGates {
    gates: Mutex<HashMap<DataScope, Weak<RwLock<()>>>>,
    catalogs: Mutex<HashMap<DataScope, CatalogGate>>,
    catalog_refreshes: Mutex<HashMap<DataScope, Weak<AsyncMutex<()>>>>,
    publications: Mutex<HashMap<super::queue::QueueTarget, Weak<AsyncMutex<()>>>>,
    build_steps: Mutex<HashMap<DataScope, Weak<AsyncMutex<()>>>>,
}

impl IndexScopeGates {
    /// Acquires shared scope authority before a graph transaction takes its snapshot.
    pub(crate) async fn mutation_permit(&self, scope: DataScope) -> IndexScopeMutationPermit {
        IndexScopeMutationPermit {
            _guard: self.mutation_gate(scope).read_owned().await,
        }
    }

    /// Acquires shared authority before refreshing the catalog used for planning.
    ///
    /// The version is read after the gate is held, so it is the version every
    /// canonical record visible under this permit was published at.
    pub(crate) async fn catalog_permit(&self, scope: DataScope) -> IndexScopeCatalogPermit {
        let gate = self.catalog_gate(scope);
        let guard = gate.lock.read_owned().await;
        IndexScopeCatalogPermit {
            version: CatalogVersion(gate.version.load(Ordering::Acquire)),
            _guard: guard,
        }
    }

    /// Acquires exclusive catalog authority for Active-to-Dropping publication.
    pub(crate) async fn catalog_change_permit(
        &self,
        scope: DataScope,
    ) -> IndexScopeCatalogChangePermit {
        let gate = self.catalog_gate(scope);
        let guard = gate.lock.write_owned().await;
        IndexScopeCatalogChangePermit {
            _advance: CatalogVersionAdvance(gate.version),
            _guard: guard,
        }
    }

    /// Acquires catalog then mutation exclusivity for one lifecycle transaction.
    pub(crate) async fn lifecycle_permit(&self, scope: DataScope) -> IndexScopeLifecyclePermit {
        let catalog = self.catalog_gate(scope);
        let catalog_guard = catalog.lock.write_owned().await;
        let mutation_guard = self.mutation_gate(scope).write_owned().await;
        IndexScopeLifecyclePermit {
            _advance: CatalogVersionAdvance(catalog.version),
            _catalog_guard: catalog_guard,
            _mutation_guard: mutation_guard,
        }
    }

    /// Acquires mutation exclusivity only, for a step that leaves the Active
    /// index set unchanged. See [`IndexScopeWriteFencePermit`] for the lock
    /// order its holders must keep.
    pub(crate) async fn write_fence_permit(&self, scope: DataScope) -> IndexScopeWriteFencePermit {
        IndexScopeWriteFencePermit {
            _mutation_guard: self.mutation_gate(scope).write_owned().await,
        }
    }

    /// Acquires the permit `gate` names for one operation step.
    ///
    /// Gated steps of one scope first queue on the scope's build-step gate,
    /// so at most one waits for the fair mutation gate at a time: graph
    /// transactions, which take it shared, queue behind one step's exclusive
    /// wait however many of the scope's builds run. Nothing waits for the
    /// build-step gate while holding a scope gate, so it adds no lock cycle.
    /// The tuple drops the scope permit before the build-step gate.
    pub(crate) async fn step_permit(
        &self,
        scope: DataScope,
        gate: StepGate,
    ) -> Box<dyn super::outbox::IndexOperationStepPermit> {
        match gate {
            StepGate::None => Box::new(()),
            StepGate::WriteFence => {
                let queued = self.build_step_gate(scope).lock_owned().await;
                Box::new((self.write_fence_permit(scope).await, queued))
            }
            StepGate::Lifecycle => {
                let queued = self.build_step_gate(scope).lock_owned().await;
                Box::new((self.lifecycle_permit(scope).await, queued))
            }
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

    /// Acquires exclusive physical-publication ownership of one generation.
    pub(crate) async fn publication_permit(
        &self,
        target: super::queue::QueueTarget,
    ) -> IndexGenerationPublicationPermit {
        IndexGenerationPublicationPermit {
            target,
            _guard: self.publication_gate(target).lock_owned().await,
        }
    }

    /// Returns how many tasks hold or await `target`'s publication permit.
    #[cfg(test)]
    pub(crate) fn publication_permit_holders(&self, target: super::queue::QueueTarget) -> usize {
        self.publications
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&target)
            .map_or(0, Weak::strong_count)
    }

    /// Returns how many tasks hold or await `scope`'s catalog gate.
    #[cfg(test)]
    pub(crate) fn catalog_permit_holders(&self, scope: DataScope) -> usize {
        self.catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&scope)
            .map_or(0, |gate| Arc::strong_count(&gate.lock) - 1)
    }

    fn publication_gate(&self, target: super::queue::QueueTarget) -> Arc<AsyncMutex<()>> {
        let mut gates = self
            .publications
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(gate) = gates.get(&target).and_then(Weak::upgrade) else {
            gates.retain(|_, gate| gate.strong_count() != 0);
            let gate = Arc::new(AsyncMutex::new(()));
            gates.insert(target, Arc::downgrade(&gate));
            return gate;
        };
        gate
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

    fn catalog_gate(&self, scope: DataScope) -> CatalogGate {
        self.catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(scope)
            .or_default()
            .clone()
    }

    fn build_step_gate(&self, scope: DataScope) -> Arc<AsyncMutex<()>> {
        let mut gates = self
            .build_steps
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
    use std::time::Duration;

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

    /// Gated build steps of one scope queue on its build-step gate, so a
    /// graph write that arrives behind several of them waits for one step's
    /// exclusive turn, not for all of them: the fair mutation gate only ever
    /// holds one step's wait.
    #[tokio::test]
    async fn graph_writes_wait_behind_one_queued_build_step() {
        let gates = Arc::new(IndexScopeGates::default());
        let scope = DataScope::LegacyUnscoped;
        let settle = Duration::from_millis(20);
        let writing = gates.mutation_permit(scope).await;
        let (acquired, mut acquisitions) = tokio::sync::mpsc::unbounded_channel();
        let mut releases = Vec::new();
        for step in 0..3 {
            let (release, released) = tokio::sync::oneshot::channel::<()>();
            releases.push(release);
            let (gates, acquired) = (Arc::clone(&gates), acquired.clone());
            tokio::spawn(async move {
                let permit = gates.step_permit(scope, StepGate::WriteFence).await;
                acquired.send(step).expect("the test awaits acquisitions");
                let _ = released.await;
                drop(permit);
            });
        }
        tokio::time::sleep(settle).await;
        let next_write = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.mutation_permit(scope).await })
        };
        tokio::time::sleep(settle).await;
        drop(writing);
        let first = acquisitions.recv().await.expect("one step takes the fence");
        tokio::time::sleep(settle).await;
        assert!(
            !next_write.is_finished(),
            "the step holding the fence excludes writes"
        );
        releases
            .remove(first)
            .send(())
            .expect("the step awaits its release");
        let write = tokio::time::timeout(Duration::from_secs(1), next_write)
            .await
            .expect("the write is next, not behind every queued step")
            .expect("the write joins");
        assert!(
            acquisitions.try_recv().is_err(),
            "no other step holds the fence while the write runs"
        );
        drop(write);
        for release in releases {
            let _ = release.send(());
        }
    }

    /// Each step gate excludes exactly what its holder needs: none nothing,
    /// the write fence only graph transactions, and the lifecycle permit
    /// planning too. Only the lifecycle permit advances the catalog version.
    #[tokio::test]
    async fn step_gates_exclude_only_what_their_step_needs() {
        let gates = Arc::new(IndexScopeGates::default());
        let scope = DataScope::LegacyUnscoped;
        let wait = Duration::from_millis(50);
        for (gate, fences_writers, gates_planning) in [
            (StepGate::None, false, false),
            (StepGate::WriteFence, true, false),
            (StepGate::Lifecycle, true, true),
        ] {
            let before = gates.catalog_permit(scope).await.version();
            let held = gates.step_permit(scope, gate).await;
            let mutation = tokio::time::timeout(wait, gates.mutation_permit(scope)).await;
            assert_eq!(
                mutation.is_err(),
                fences_writers,
                "{gate:?} and graph writes"
            );
            drop(mutation);
            let planning = tokio::time::timeout(wait, gates.catalog_permit(scope)).await;
            assert_eq!(planning.is_err(), gates_planning, "{gate:?} and planning");
            drop(planning);
            drop(held);
            assert_eq!(
                gates.catalog_permit(scope).await.version() != before,
                gates_planning,
                "{gate:?}: only a gate that excludes planning advances the version"
            );
        }
    }

    /// A write fence never waits for a catalog permit, so the chain a fenced
    /// step can sit at the head of always drains: a graph transaction holding
    /// shared catalog authority queues for the fenced mutation gate, an
    /// activation queues for exclusive catalog authority behind it, and later
    /// planning queues behind the activation.
    #[tokio::test]
    async fn a_fenced_step_never_deadlocks_writers_activation_or_planning() {
        let gates = Arc::new(IndexScopeGates::default());
        let scope = DataScope::LegacyUnscoped;
        let fence = gates.write_fence_permit(scope).await;
        let writer = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move {
                let catalog = gates.catalog_permit(scope).await;
                let mutation = gates.mutation_permit(scope).await;
                drop((catalog, mutation));
            })
        };
        while gates.catalog_permit_holders(scope) < 1 {
            tokio::task::yield_now().await;
        }
        let activation = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { drop(gates.lifecycle_permit(scope).await) })
        };
        while gates.catalog_permit_holders(scope) < 2 {
            tokio::task::yield_now().await;
        }
        let planning = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { gates.catalog_permit(scope).await.version() })
        };
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "the writer waits for the fence");
        assert!(!activation.is_finished(), "activation waits for the writer");
        assert!(!planning.is_finished(), "planning queues behind activation");

        drop(fence);
        tokio::time::timeout(Duration::from_secs(5), async {
            writer.await.expect("writer joins");
            activation.await.expect("activation joins");
            planning.await.expect("planning joins")
        })
        .await
        .expect("the chain drains once the fence is released");
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
    async fn exclusive_catalog_permits_advance_only_their_own_scope_version() {
        let gates = Arc::new(IndexScopeGates::default());
        let scope = DataScope::LegacyUnscoped;
        let other_scope =
            DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
        let initial = gates.catalog_permit(scope).await.version();
        let other_initial = gates.catalog_permit(other_scope).await.version();

        drop(gates.mutation_permit(scope).await);
        drop(gates.catalog_refresh_permit(scope).await);
        let shared = gates.catalog_permit(scope).await;
        let concurrent_shared = gates.catalog_permit(scope).await;
        assert_eq!(shared.version(), initial);
        assert_eq!(concurrent_shared.version(), initial);
        drop((shared, concurrent_shared));

        drop(gates.catalog_change_permit(scope).await);
        let after_change = gates.catalog_permit(scope).await.version();
        assert_eq!(after_change.0, initial.0 + 1);

        drop(gates.lifecycle_permit(scope).await);
        let after_lifecycle = gates.catalog_permit(scope).await.version();
        assert_eq!(after_lifecycle.0, after_change.0 + 1);

        assert_eq!(
            gates.catalog_permit(other_scope).await.version(),
            other_initial,
            "another scope's catalog changes never invalidate this scope"
        );
    }

    #[tokio::test]
    async fn shared_permit_queued_behind_an_exclusive_one_observes_its_advance() {
        let gates = Arc::new(IndexScopeGates::default());
        let scope = DataScope::LegacyUnscoped;
        let before = gates.catalog_permit(scope).await.version();
        for exclusive in [true, false] {
            let held: Box<dyn std::any::Any + Send> = if exclusive {
                Box::new(gates.catalog_change_permit(scope).await)
            } else {
                Box::new(gates.lifecycle_permit(scope).await)
            };
            let waiting = {
                let gates = Arc::clone(&gates);
                tokio::spawn(async move { gates.catalog_permit(scope).await.version() })
            };
            tokio::task::yield_now().await;
            assert!(!waiting.is_finished(), "planning waits for the change");
            drop(held);
            let observed = waiting.await.expect("shared waiter joins");
            assert_ne!(observed, before, "the waiter must see the released change");
        }
        assert_eq!(gates.catalog_permit(scope).await.version().0, before.0 + 2);
    }

    #[tokio::test]
    async fn catalog_gates_keep_their_version_after_every_permit_drops() {
        let gates = IndexScopeGates::default();
        let scope = DataScope::LegacyUnscoped;
        drop(gates.catalog_change_permit(scope).await);
        assert_eq!(
            gates.catalog_permit_holders(scope),
            0,
            "no permit holds the catalog gate"
        );
        assert_eq!(
            gates.catalog_permit(scope).await.version().0,
            1,
            "an idle gate must not restart the version a cached catalog recorded"
        );
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
        let before = gates.catalog_permit(scope).await.version();
        drop(gates.catalog_change_permit(scope).await);
        assert_eq!(gates.catalog_permit(scope).await.version().0, before.0 + 1);
    }
}
