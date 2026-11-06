//! Shared model-based test infrastructure for the Helix planner and database.
//!
//! The crate deliberately depends only on public planner and DB contracts. It
//! provides a typed workload language, independent correctness models,
//! replayable traces, and fixture interfaces that do not commit workloads to
//! one storage or coordinator implementation.
//!
//! ```
//! use helix_db_testkit::{
//!     action::{Action, ElementKind, ReadAction},
//!     ids::{EntityId, RequestId, RuntimeId, Sequence, StableSeed, TenantId},
//!     trace::{ObservedValue, TraceOutcome, TraceRecorder},
//! };
//!
//! let mut recorder = TraceRecorder::new(StableSeed::new(7));
//! let pending = recorder
//!     .start_request(
//!         RequestId::new(1).unwrap(),
//!         RuntimeId::new(1).unwrap(),
//!         TenantId::try_new("tenant-a").unwrap(),
//!         Sequence::initial(),
//!         Action::Read(ReadAction::Point {
//!             kind: ElementKind::Node,
//!             id: EntityId::new(9),
//!         }),
//!     )
//!     .unwrap();
//! recorder.finish_request(
//!     pending,
//!     None,
//!     TraceOutcome::Success(ObservedValue::Entities(Vec::new())),
//! );
//! assert_eq!(recorder.finish().unwrap().requests().len(), 1);
//! ```

#![deny(missing_docs)]
#![deny(unsafe_code)]

pub mod action;
pub mod error;
pub mod fixtures;
pub mod gated_wal;
pub mod ids;
pub mod launch_gate;
pub mod lifecycle;
pub mod model;
pub mod planner_domain;
pub mod sustained;
pub mod trace;
pub mod transport_corpus;

pub use error::{Result, TestkitError};
