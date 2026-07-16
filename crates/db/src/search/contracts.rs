//! Secondary-index request contracts.

use slatedb::DbTransaction;

use crate::config;
use crate::encoding::keys::tenant::DataScope;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::{EdgeId, NodeId};

/// Borrowed node secondary-index definitions used while adding or updating a property.
#[derive(Debug, Clone, Copy)]
pub struct NodePropertyIndexUpdateCatalog<'a> {
    pub(super) indexes: &'a config::IndexConfig,
}

impl<'a> NodePropertyIndexUpdateCatalog<'a> {
    /// Create a borrowed catalog view for node property index updates.
    pub fn new(indexes: &'a config::IndexConfig) -> Self {
        Self { indexes }
    }
}

/// Borrowed node secondary-index definitions used while removing a property.
#[derive(Debug, Clone, Copy)]
pub struct NodePropertyIndexRemovalCatalog<'a> {
    pub(super) indexes: &'a config::IndexConfig,
}

impl<'a> NodePropertyIndexRemovalCatalog<'a> {
    /// Create a borrowed catalog view for node property index removals.
    pub fn new(indexes: &'a config::IndexConfig) -> Self {
        Self { indexes }
    }
}

/// Request to update index rows for one node property.
#[derive(Clone, Copy)]
pub struct NodePropertyIndexUpdateRequest<'a> {
    pub(super) txn: &'a DbTransaction,
    pub(super) node_id: NodeId,
    pub(super) label: Option<&'a str>,
    pub(super) property: &'a Property,
    pub(super) old_value: Option<&'a PropertyValue>,
    pub(super) indexes: NodePropertyIndexUpdateCatalog<'a>,
    pub(super) tenant_scope: DataScope,
}

impl<'a> NodePropertyIndexUpdateRequest<'a> {
    /// Create a node property index update request.
    pub fn new(
        txn: &'a DbTransaction,
        node_id: NodeId,
        label: Option<&'a str>,
        property: &'a Property,
        old_value: Option<&'a PropertyValue>,
        indexes: NodePropertyIndexUpdateCatalog<'a>,
    ) -> Self {
        Self {
            txn,
            node_id,
            label,
            property,
            old_value,
            indexes,
            tenant_scope: DataScope::LegacyUnscoped,
        }
    }

    /// Create a node property index update request in a storage namespace.
    pub fn new_scoped(
        txn: &'a DbTransaction,
        node_id: NodeId,
        label: Option<&'a str>,
        property: &'a Property,
        old_value: Option<&'a PropertyValue>,
        indexes: NodePropertyIndexUpdateCatalog<'a>,
        tenant_scope: DataScope,
    ) -> Self {
        Self {
            txn,
            node_id,
            label,
            property,
            old_value,
            indexes,
            tenant_scope,
        }
    }
}

/// Request to remove index rows for one node property.
#[derive(Clone, Copy)]
pub struct NodePropertyIndexRemovalRequest<'a> {
    pub(super) txn: &'a DbTransaction,
    pub(super) node_id: NodeId,
    pub(super) label: Option<&'a str>,
    pub(super) prop_name: &'a str,
    pub(super) old_value: &'a PropertyValue,
    pub(super) indexes: NodePropertyIndexRemovalCatalog<'a>,
    pub(super) tenant_scope: DataScope,
}

impl<'a> NodePropertyIndexRemovalRequest<'a> {
    /// Create a node property index removal request.
    pub fn new(
        txn: &'a DbTransaction,
        node_id: NodeId,
        label: Option<&'a str>,
        prop_name: &'a str,
        old_value: &'a PropertyValue,
        indexes: NodePropertyIndexRemovalCatalog<'a>,
    ) -> Self {
        Self {
            txn,
            node_id,
            label,
            prop_name,
            old_value,
            indexes,
            tenant_scope: DataScope::LegacyUnscoped,
        }
    }

    /// Create a node property index removal request in a storage namespace.
    pub fn new_scoped(
        txn: &'a DbTransaction,
        node_id: NodeId,
        label: Option<&'a str>,
        prop_name: &'a str,
        old_value: &'a PropertyValue,
        indexes: NodePropertyIndexRemovalCatalog<'a>,
        tenant_scope: DataScope,
    ) -> Self {
        Self {
            txn,
            node_id,
            label,
            prop_name,
            old_value,
            indexes,
            tenant_scope,
        }
    }
}

/// Stable edge identity plus directed endpoints for edge secondary indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeIndexTarget {
    pub(super) from: NodeId,
    pub(super) to: NodeId,
    pub(super) edge_id: EdgeId,
}

impl EdgeIndexTarget {
    /// Create an edge index target.
    pub const fn new(from: NodeId, to: NodeId, edge_id: EdgeId) -> Self {
        Self { from, to, edge_id }
    }
}

/// Borrowed edge secondary-index definitions.
#[derive(Debug, Clone, Copy)]
pub struct EdgePropertyIndexCatalog<'a> {
    pub(super) indexes: &'a config::IndexConfig,
}

impl<'a> EdgePropertyIndexCatalog<'a> {
    /// Create a borrowed catalog view for edge property indexes.
    pub fn new(indexes: &'a config::IndexConfig) -> Self {
        Self { indexes }
    }
}

/// Request to update index rows for one edge property.
#[derive(Clone, Copy)]
pub struct EdgePropertyIndexUpdateRequest<'a> {
    pub(super) txn: &'a DbTransaction,
    pub(super) target: EdgeIndexTarget,
    pub(super) label: Option<&'a str>,
    pub(super) prop_name: &'a str,
    pub(super) new_value: &'a PropertyValue,
    pub(super) old_value: Option<&'a PropertyValue>,
    pub(super) indexes: EdgePropertyIndexCatalog<'a>,
    pub(super) tenant_scope: DataScope,
}

impl<'a> EdgePropertyIndexUpdateRequest<'a> {
    /// Create an edge property index update request.
    pub fn new(
        txn: &'a DbTransaction,
        target: EdgeIndexTarget,
        label: Option<&'a str>,
        prop_name: &'a str,
        new_value: &'a PropertyValue,
        old_value: Option<&'a PropertyValue>,
        indexes: EdgePropertyIndexCatalog<'a>,
    ) -> Self {
        Self {
            txn,
            target,
            label,
            prop_name,
            new_value,
            old_value,
            indexes,
            tenant_scope: DataScope::LegacyUnscoped,
        }
    }

    /// Return this request in a storage namespace.
    pub fn with_tenant_scope(mut self, tenant_scope: DataScope) -> Self {
        self.tenant_scope = tenant_scope;
        self
    }
}

/// Request to remove all secondary-index rows for an edge.
#[derive(Clone, Copy)]
pub struct EdgePropertyIndexRemovalRequest<'a> {
    pub(super) txn: &'a DbTransaction,
    pub(super) target: EdgeIndexTarget,
    pub(super) properties: &'a [Property],
    pub(super) indexes: EdgePropertyIndexCatalog<'a>,
    pub(super) tenant_scope: DataScope,
}

impl<'a> EdgePropertyIndexRemovalRequest<'a> {
    /// Create an edge property index removal request.
    pub fn new(
        txn: &'a DbTransaction,
        target: EdgeIndexTarget,
        properties: &'a [Property],
        indexes: EdgePropertyIndexCatalog<'a>,
    ) -> Self {
        Self {
            txn,
            target,
            properties,
            indexes,
            tenant_scope: DataScope::LegacyUnscoped,
        }
    }

    /// Create an edge property index removal request in a storage namespace.
    pub fn new_scoped(
        txn: &'a DbTransaction,
        target: EdgeIndexTarget,
        properties: &'a [Property],
        indexes: EdgePropertyIndexCatalog<'a>,
        tenant_scope: DataScope,
    ) -> Self {
        Self {
            txn,
            target,
            properties,
            indexes,
            tenant_scope,
        }
    }
}
