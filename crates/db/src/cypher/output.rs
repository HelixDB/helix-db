//! Prepare response ownership before the request transaction commits.
use super::{Error, ResourceUsage, Response, Result};
use crate::query_resources::{Budget, Reservation};

/// JSON prepared within the query budget before a modifying statement commits.
/// Moving its bytes does not serialize the result again. Engine admission does
/// not include transport framing, TLS buffers or allocations made by callers.
pub struct EncodedResponse {
    body: Body,
    pub diagnostics: helix_planner::exec::PlannerMetrics,
    pub resources: ResourceUsage,
}

struct Body {
    bytes: Vec<u8>,
    _memory: Reservation,
}
impl AsRef<[u8]> for Body {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::fmt::Debug for EncodedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedResponse")
            .field("body_bytes", &self.body.bytes.len())
            .field("diagnostics", &self.diagnostics)
            .field("resources", &self.resources)
            .finish()
    }
}
impl EncodedResponse {
    /// Borrow the exact JSON body without copying it.
    pub fn body(&self) -> &[u8] {
        &self.body.bytes
    }

    /// Transfer the body to a shared transport buffer without a payload copy.
    /// Clones and slices retain admission until their final owner is dropped.
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self.body)
    }

    /// Move the prepared allocation to an embedded caller without a copy.
    /// The caller assumes ownership and memory accounting for the returned Vec.
    pub fn into_vec(self) -> Vec<u8> {
        self.body.bytes
    }
}

pub(crate) struct Typed;
pub(crate) struct Json;
pub(crate) struct AdmittedResponse {
    response: Response,
    _memory: Reservation,
    wire_bytes: usize,
}
impl AdmittedResponse {
    /// Transfer a materialized response with its live reservation and measured
    /// wire size. The row executor admits every payload before constructing it.
    pub(crate) fn new(response: Response, memory: Reservation, wire_bytes: usize) -> Self {
        Self {
            response,
            _memory: memory,
            wire_bytes,
        }
    }

    /// Conversion follows the single compiled row program and precedes commit.
    pub(crate) fn prepare<O: Format>(
        self,
        budget: &Budget,
        check: impl FnMut() -> Result<()>,
    ) -> Result<O::Prepared> {
        O::prepare(self.response, self._memory, self.wire_bytes, budget, check)
    }
}

/// Only this module chooses output representations. Both use the same frontend,
/// plan and interpreter; output preparation is part of the transaction boundary.
pub(crate) trait Format {
    type Prepared;
    type Value;
    fn prepare(
        response: Response,
        memory: Reservation,
        wire_bytes: usize,
        budget: &Budget,
        check: impl FnMut() -> Result<()>,
    ) -> Result<Self::Prepared>;
    fn finish(prepared: Self::Prepared, resources: ResourceUsage) -> Self::Value;
    fn resources(value: &mut Self::Value) -> &mut ResourceUsage;
}

impl Format for Typed {
    type Prepared = AdmittedResponse;
    type Value = Response;
    fn prepare(
        response: Response,
        memory: Reservation,
        wire_bytes: usize,
        _: &Budget,
        mut check: impl FnMut() -> Result<()>,
    ) -> Result<Self::Prepared> {
        check()?;
        Ok(AdmittedResponse::new(response, memory, wire_bytes))
    }
    fn finish(prepared: Self::Prepared, resources: ResourceUsage) -> Response {
        let mut response = prepared.response;
        response.resources = resources;
        response
    }
    fn resources(value: &mut Response) -> &mut ResourceUsage {
        &mut value.resources
    }
}

impl Format for Json {
    type Prepared = EncodedResponse;
    type Value = EncodedResponse;
    fn prepare(
        response: Response,
        memory: Reservation,
        wire_bytes: usize,
        budget: &Budget,
        mut check: impl FnMut() -> Result<()>,
    ) -> Result<Self::Prepared> {
        check()?;
        // Reserve DOM/JSON overlap and the later Bytes owner before allocation.
        // into_vec simply releases the unused shared-owner allowance.
        let encoded_memory = budget.reserve(
            wire_bytes
                .saturating_add(size_of::<Body>())
                .saturating_add(size_of::<std::sync::atomic::AtomicUsize>())
                .saturating_add(2 * size_of::<bytes::Bytes>()),
        )?;
        let mut writer = Writer {
            bytes: Vec::with_capacity(wire_bytes),
            limit: wire_bytes,
            check: &mut check,
            failure: None,
        };
        if let Err(error) = serde_json::to_writer(&mut writer, &response) {
            return Err(writer.failure.unwrap_or_else(|| error.into()));
        }
        assert_eq!(
            writer.bytes.len(),
            wire_bytes,
            "response size and serializer agree"
        );
        let Response {
            columns,
            rows,
            diagnostics,
            resources,
        } = response;
        drop((columns, rows));
        drop(memory);
        Ok(EncodedResponse {
            body: Body {
                bytes: writer.bytes,
                _memory: encoded_memory,
            },
            diagnostics,
            resources,
        })
    }
    fn finish(mut prepared: Self::Prepared, resources: ResourceUsage) -> EncodedResponse {
        prepared.resources = resources;
        prepared
    }
    fn resources(value: &mut EncodedResponse) -> &mut ResourceUsage {
        &mut value.resources
    }
}

struct Writer<'a, F> {
    bytes: Vec<u8>,
    limit: usize,
    check: &'a mut F,
    failure: Option<Error>,
}
impl<F: FnMut() -> Result<()>> std::io::Write for Writer<'_, F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(error) = (self.check)() {
            self.failure = Some(error);
            return Err(std::io::Error::other("response encoding interrupted"));
        }
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other(
                "response exceeded its measured encoding size",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
