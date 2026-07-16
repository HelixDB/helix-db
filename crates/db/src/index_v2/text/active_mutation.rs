//! Process-local ownership for request-driven Active text mutations.
//!
//! An Active mutation registers its complete immutable upload identity before
//! the upload intent transaction. The request retains the returned
//! [`ActiveTextMutationGuard`] until its graph transaction outcome is resolved.
//! The global upload worker may observe the same writer epoch, but it may not
//! claim the intent while that exact guard is in flight. A missing or
//! mismatched guard is corruption rather than permission to attach or reclaim
//! request-owned work.
//!
//! Dropping an in-flight guard marks the owner terminal instead of erasing it.
//! This preserves recovery authority after cancellation or panic. Only a
//! definitive pre-intent cancellation may remove an in-flight registration;
//! terminal ownership remains registered until durable intent absence is
//! observed.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::encoding::v1::keys::tenant::DataScope;
use crate::index_v2::{self, work};

/// Complete immutable identity that one request owns across intent and graph commits.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ActiveTextMutationIdentity {
    scope: DataScope,
    intent_id: index_v2::TextUploadIntentId,
    index_id: index_v2::IndexId,
    identity: index_v2::IndexIdentity,
    generation: index_v2::IndexGenerationId,
    partition: work::TextPartition,
    blob: work::BlobRef,
    writer_epoch: index_v2::WriterEpoch,
    mutation_id: index_v2::MutationId,
    active_record_revision: index_v2::IndexRevision,
}

impl ActiveTextMutationIdentity {
    /// Reconstructs the exact request owner from one validated Active upload intent.
    fn try_from_intent(
        scope: DataScope,
        intent: &work::TextUploadIntentValue,
    ) -> Result<Self, ActiveTextMutationRegistryError> {
        let work::TextUploadOwner::ActiveMutation {
            writer_epoch,
            mutation_id,
            active_record_revision,
        } = intent.owner
        else {
            return Err(ActiveTextMutationRegistryError::BuildOwner);
        };
        Ok(Self {
            scope,
            intent_id: intent.intent_id,
            index_id: intent.index_id,
            identity: intent.identity.clone(),
            generation: intent.generation,
            partition: intent.partition.clone(),
            blob: intent.blob,
            writer_epoch,
            mutation_id,
            active_record_revision,
        })
    }
}

/// Monotonic process-local state of one exact request owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegisteredOwnerState {
    InFlight,
    Terminal,
}

/// One registered identity and its monotonic request state.
#[derive(Debug)]
struct RegisteredOwner {
    identity: ActiveTextMutationIdentity,
    state: RegisteredOwnerState,
}

/// Worker-visible result of validating a same-epoch Active upload owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveTextMutationOwnerObservation {
    /// The request still exclusively owns graph-outcome resolution.
    InFlight,
    /// Request resolution ended, so reconciliation may claim the intent.
    Terminal,
}

/// Invalid process-local owner transitions that must fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ActiveTextMutationRegistryError {
    /// The supplied upload belongs to a hidden build rather than an Active request.
    #[error("a build-owned text upload cannot register as an Active mutation")]
    BuildOwner,
    /// An intent ID already names an in-process owner.
    #[error("the Active text mutation intent ID is already registered")]
    AlreadyRegistered,
    /// A same-epoch durable intent has no process-local request owner.
    #[error("the Active text mutation intent has no registered request owner")]
    MissingOwner,
    /// A durable intent disagrees with the immutable registered identity.
    #[error("the Active text mutation intent disagrees with its registered request owner")]
    OwnerMismatch,
    /// Only an in-flight owner can be cancelled or made terminal.
    #[error("the Active text mutation owner is no longer in flight")]
    OwnerNotInFlight,
    /// Cleanup cannot erase an owner until request resolution is terminal.
    #[error("the Active text mutation owner is not terminal")]
    OwnerNotTerminal,
}

/// Shared registry for all request-owned Active text mutations in one writer runtime.
#[derive(Debug, Clone, Default)]
pub(crate) struct ActiveTextMutationRegistry {
    owners: Arc<Mutex<HashMap<index_v2::TextUploadIntentId, RegisteredOwner>>>,
}

impl ActiveTextMutationRegistry {
    /// Creates an empty registry for one writer runtime.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Registers an exact Active upload before its durable intent transaction.
    pub(crate) fn register(
        &self,
        scope: DataScope,
        intent: &work::TextUploadIntentValue,
    ) -> Result<ActiveTextMutationGuard, ActiveTextMutationRegistryError> {
        let identity = ActiveTextMutationIdentity::try_from_intent(scope, intent)?;
        let mut owners = self.owners.lock();
        if owners.contains_key(&identity.intent_id) {
            return Err(ActiveTextMutationRegistryError::AlreadyRegistered);
        }
        owners.insert(
            identity.intent_id,
            RegisteredOwner {
                identity: identity.clone(),
                state: RegisteredOwnerState::InFlight,
            },
        );
        drop(owners);
        Ok(ActiveTextMutationGuard {
            registry: self.clone(),
            identity,
            registered: true,
        })
    }

    /// Validates one same-epoch durable intent against its exact local owner.
    pub(crate) fn observe(
        &self,
        scope: DataScope,
        intent: &work::TextUploadIntentValue,
    ) -> Result<ActiveTextMutationOwnerObservation, ActiveTextMutationRegistryError> {
        let identity = ActiveTextMutationIdentity::try_from_intent(scope, intent)?;
        let owners = self.owners.lock();
        let Some(owner) = owners.get(&identity.intent_id) else {
            return Err(ActiveTextMutationRegistryError::MissingOwner);
        };
        if owner.identity != identity {
            return Err(ActiveTextMutationRegistryError::OwnerMismatch);
        }
        Ok(match owner.state {
            RegisteredOwnerState::InFlight => ActiveTextMutationOwnerObservation::InFlight,
            RegisteredOwnerState::Terminal => ActiveTextMutationOwnerObservation::Terminal,
        })
    }

    /// Forgets a terminal current-epoch owner after durable intent absence is proven.
    ///
    /// An in-flight entry is intentionally retained: pointer absence can occur
    /// before the request commits its independent intent transaction.
    pub(crate) fn forget_terminal_after_absence(
        &self,
        intent_id: index_v2::TextUploadIntentId,
        writer_epoch: index_v2::WriterEpoch,
    ) -> bool {
        let mut owners = self.owners.lock();
        let removable = owners.get(&intent_id).is_some_and(|owner| {
            owner.identity.writer_epoch == writer_epoch
                && owner.state == RegisteredOwnerState::Terminal
        });
        if removable {
            owners.remove(&intent_id);
        }
        removable
    }

    /// Changes one exact in-flight owner to terminal recovery authority.
    fn mark_terminal(
        &self,
        identity: &ActiveTextMutationIdentity,
    ) -> Result<(), ActiveTextMutationRegistryError> {
        let mut owners = self.owners.lock();
        let Some(owner) = owners.get_mut(&identity.intent_id) else {
            return Err(ActiveTextMutationRegistryError::MissingOwner);
        };
        if owner.identity != *identity {
            return Err(ActiveTextMutationRegistryError::OwnerMismatch);
        }
        if owner.state != RegisteredOwnerState::InFlight {
            return Err(ActiveTextMutationRegistryError::OwnerNotInFlight);
        }
        owner.state = RegisteredOwnerState::Terminal;
        Ok(())
    }

    /// Removes one exact in-flight owner after definitive pre-intent cancellation.
    fn cancel_before_durable_intent(
        &self,
        identity: &ActiveTextMutationIdentity,
    ) -> Result<(), ActiveTextMutationRegistryError> {
        let mut owners = self.owners.lock();
        let Some(owner) = owners.get(&identity.intent_id) else {
            return Err(ActiveTextMutationRegistryError::MissingOwner);
        };
        if owner.identity != *identity {
            return Err(ActiveTextMutationRegistryError::OwnerMismatch);
        }
        if owner.state != RegisteredOwnerState::InFlight {
            return Err(ActiveTextMutationRegistryError::OwnerNotInFlight);
        }
        owners.remove(&identity.intent_id);
        Ok(())
    }

    /// Removes one exact terminal owner after durable intent cleanup.
    fn remove_terminal(
        &self,
        identity: &ActiveTextMutationIdentity,
    ) -> Result<(), ActiveTextMutationRegistryError> {
        let mut owners = self.owners.lock();
        let Some(owner) = owners.get(&identity.intent_id) else {
            return Err(ActiveTextMutationRegistryError::MissingOwner);
        };
        if owner.identity != *identity {
            return Err(ActiveTextMutationRegistryError::OwnerMismatch);
        }
        if owner.state != RegisteredOwnerState::Terminal {
            return Err(ActiveTextMutationRegistryError::OwnerNotTerminal);
        }
        owners.remove(&identity.intent_id);
        Ok(())
    }
}

/// Exclusive request ownership retained until graph outcome resolution.
#[derive(Debug)]
pub(crate) struct ActiveTextMutationGuard {
    registry: ActiveTextMutationRegistry,
    identity: ActiveTextMutationIdentity,
    registered: bool,
}

impl ActiveTextMutationGuard {
    /// Removes ownership only when the intent transaction definitively did not commit.
    pub(crate) fn cancel_before_durable_intent(
        mut self,
    ) -> Result<(), ActiveTextMutationRegistryError> {
        self.registry.cancel_before_durable_intent(&self.identity)?;
        self.registered = false;
        Ok(())
    }

    /// Marks request outcome resolution terminal while retaining recovery authority.
    pub(crate) fn finish(
        mut self,
    ) -> Result<TerminalActiveTextMutation, ActiveTextMutationRegistryError> {
        self.registry.mark_terminal(&self.identity)?;
        self.registered = false;
        Ok(TerminalActiveTextMutation {
            registry: self.registry.clone(),
            identity: self.identity.clone(),
        })
    }
}

impl Drop for ActiveTextMutationGuard {
    fn drop(&mut self) {
        if self.registered {
            let _ = self.registry.mark_terminal(&self.identity);
        }
    }
}

/// Terminal request ownership retained until durable intent cleanup is observed.
#[derive(Debug)]
pub(crate) struct TerminalActiveTextMutation {
    registry: ActiveTextMutationRegistry,
    identity: ActiveTextMutationIdentity,
}

impl TerminalActiveTextMutation {
    /// Removes the exact terminal entry after proof/intent/pointer cleanup commits.
    pub(crate) fn cleanup_after_intent_absence(
        self,
    ) -> Result<(), ActiveTextMutationRegistryError> {
        self.registry.remove_terminal(&self.identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v1::keys::tenant::TenantId;

    /// Builds one complete Active upload value for owner-registry contracts.
    fn active_intent(seed: u8) -> work::TextUploadIntentValue {
        let intent_id = index_v2::TextUploadIntentId::from_bytes([seed; 16]).unwrap();
        let blob = work::BlobRef::new([seed; 32], 64);
        let split = work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap();
        work::TextUploadIntentValue::try_new(
            intent_id,
            index_v2::TextIntentRevision::initial(),
            index_v2::IndexId::initial(),
            index_v2::IndexIdentity::new(
                index_v2::IndexIdentityFamily::Text,
                index_v2::IndexElementKind::Node,
                index_v2::IndexComponent::try_new("label", "Document").unwrap(),
                index_v2::IndexComponent::try_new("property", "body").unwrap(),
            ),
            index_v2::IndexGenerationId::initial(),
            work::TextPartition::try_tenant_value(bytes::Bytes::from_static(b"acme")).unwrap(),
            blob,
            index_v2::BlobPublicationPermitId::from_bytes([seed.saturating_add(1); 16]).unwrap(),
            work::TextUploadOwner::ActiveMutation {
                writer_epoch: index_v2::WriterEpoch::from_bytes([seed.saturating_add(2); 16])
                    .unwrap(),
                mutation_id: index_v2::MutationId::from_bytes([seed.saturating_add(3); 16])
                    .unwrap(),
                active_record_revision: index_v2::IndexRevision::initial(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
            work::TextUploadPhase::Prepared,
            0,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn exact_owner_moves_monotonically_from_in_flight_to_terminal_to_absent() {
        let registry = ActiveTextMutationRegistry::new();
        let scope = DataScope::Tenant(TenantId::from_u128(9));
        let intent = active_intent(10);
        let guard = registry.register(scope, &intent).unwrap();
        assert_eq!(
            registry.observe(scope, &intent).unwrap(),
            ActiveTextMutationOwnerObservation::InFlight
        );

        let terminal = guard.finish().unwrap();
        assert_eq!(
            registry.observe(scope, &intent).unwrap(),
            ActiveTextMutationOwnerObservation::Terminal
        );
        terminal.cleanup_after_intent_absence().unwrap();
        assert_eq!(
            registry.observe(scope, &intent),
            Err(ActiveTextMutationRegistryError::MissingOwner)
        );
    }

    #[test]
    fn cancellation_before_durable_intent_removes_in_flight_owner() {
        let registry = ActiveTextMutationRegistry::new();
        let scope = DataScope::LegacyUnscoped;
        let intent = active_intent(20);
        registry
            .register(scope, &intent)
            .unwrap()
            .cancel_before_durable_intent()
            .unwrap();
        assert_eq!(
            registry.observe(scope, &intent),
            Err(ActiveTextMutationRegistryError::MissingOwner)
        );
    }

    #[test]
    fn dropped_request_retains_terminal_recovery_authority() {
        let registry = ActiveTextMutationRegistry::new();
        let scope = DataScope::LegacyUnscoped;
        let intent = active_intent(30);
        let guard = registry.register(scope, &intent).unwrap();
        drop(guard);
        assert_eq!(
            registry.observe(scope, &intent).unwrap(),
            ActiveTextMutationOwnerObservation::Terminal
        );
    }

    #[test]
    fn duplicate_and_mismatched_owners_fail_closed() {
        let registry = ActiveTextMutationRegistry::new();
        let scope = DataScope::LegacyUnscoped;
        let intent = active_intent(40);
        let _guard = registry.register(scope, &intent).unwrap();
        assert!(matches!(
            registry.register(scope, &intent),
            Err(ActiveTextMutationRegistryError::AlreadyRegistered)
        ));

        let mut mismatched = intent.clone();
        mismatched.generation = index_v2::IndexGenerationId::new(2).unwrap();
        assert_eq!(
            registry.observe(scope, &mismatched),
            Err(ActiveTextMutationRegistryError::OwnerMismatch)
        );
        assert_eq!(
            registry.observe(DataScope::Tenant(TenantId::from_u128(99)), &intent),
            Err(ActiveTextMutationRegistryError::OwnerMismatch)
        );
    }

    #[test]
    fn build_uploads_cannot_enter_the_active_owner_registry() {
        let registry = ActiveTextMutationRegistry::new();
        let mut intent = active_intent(50);
        intent.owner = work::TextUploadOwner::Build {
            operation_id: index_v2::IndexOperationId::from_bytes([51; 16]).unwrap(),
            expected_operation_revision: index_v2::IndexOperationRevision::initial(),
        };
        assert!(matches!(
            registry.register(DataScope::LegacyUnscoped, &intent),
            Err(ActiveTextMutationRegistryError::BuildOwner)
        ));
        assert_eq!(
            registry.observe(DataScope::LegacyUnscoped, &intent),
            Err(ActiveTextMutationRegistryError::BuildOwner)
        );
    }

    #[test]
    fn absent_intent_cleanup_removes_only_terminal_current_epoch_owner() {
        let registry = ActiveTextMutationRegistry::new();
        let scope = DataScope::LegacyUnscoped;
        let intent = active_intent(60);
        let work::TextUploadOwner::ActiveMutation { writer_epoch, .. } = intent.owner else {
            unreachable!("fixture is Active-owned");
        };
        let guard = registry.register(scope, &intent).unwrap();
        assert!(!registry.forget_terminal_after_absence(intent.intent_id, writer_epoch));
        let terminal = guard.finish().unwrap();
        assert!(!registry.forget_terminal_after_absence(
            intent.intent_id,
            index_v2::WriterEpoch::from_bytes([70; 16]).unwrap(),
        ));
        assert!(registry.forget_terminal_after_absence(intent.intent_id, writer_epoch));
        assert_eq!(
            terminal.cleanup_after_intent_absence(),
            Err(ActiveTextMutationRegistryError::MissingOwner)
        );
    }

    #[test]
    fn every_invalid_internal_owner_transition_fails_closed() {
        let registry = ActiveTextMutationRegistry::new();
        let scope = DataScope::LegacyUnscoped;
        let intent = active_intent(80);
        let identity = ActiveTextMutationIdentity::try_from_intent(scope, &intent).unwrap();
        let guard = registry.register(scope, &intent).unwrap();
        assert_eq!(
            registry.remove_terminal(&identity),
            Err(ActiveTextMutationRegistryError::OwnerNotTerminal)
        );

        let mut mismatched = identity.clone();
        mismatched.generation = index_v2::IndexGenerationId::new(2).unwrap();
        assert_eq!(
            registry.mark_terminal(&mismatched),
            Err(ActiveTextMutationRegistryError::OwnerMismatch)
        );
        assert_eq!(
            registry.cancel_before_durable_intent(&mismatched),
            Err(ActiveTextMutationRegistryError::OwnerMismatch)
        );

        let terminal = guard.finish().unwrap();
        assert_eq!(
            registry.mark_terminal(&identity),
            Err(ActiveTextMutationRegistryError::OwnerNotInFlight)
        );
        assert_eq!(
            registry.cancel_before_durable_intent(&identity),
            Err(ActiveTextMutationRegistryError::OwnerNotInFlight)
        );
        assert_eq!(
            registry.remove_terminal(&mismatched),
            Err(ActiveTextMutationRegistryError::OwnerMismatch)
        );
        terminal.cleanup_after_intent_absence().unwrap();

        assert_eq!(
            registry.mark_terminal(&identity),
            Err(ActiveTextMutationRegistryError::MissingOwner)
        );
        assert_eq!(
            registry.cancel_before_durable_intent(&identity),
            Err(ActiveTextMutationRegistryError::MissingOwner)
        );
        assert_eq!(
            registry.remove_terminal(&identity),
            Err(ActiveTextMutationRegistryError::MissingOwner)
        );
    }
}
