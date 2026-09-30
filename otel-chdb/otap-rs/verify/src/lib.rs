//! The consumer's sans-IO modules, mounted from the crate's own sources so
//! that the harnesses verify exactly the code the worker runs (`#[path]`,
//! as `src/bin/consume.rs` mounts them), plus the Kani proof harnesses.
//!
//! `plan.rs` names `otap_s3pq::proto`; `extern crate self` makes that path
//! resolve to the copy of `src/proto.rs` mounted here.

#![allow(dead_code)]

extern crate self as otap_s3pq;

#[path = "../../src/proto.rs"]
pub mod proto;

// Siblings at the root, as in `consumer/`: plan.rs names `super::coord`.
#[path = "../../src/consumer/coord.rs"]
pub mod coord;
#[path = "../../src/consumer/plan.rs"]
pub mod plan;

#[cfg(any(kani, feature = "typecheck"))]
mod proofs_balance;
#[cfg(any(kani, feature = "typecheck"))]
mod proofs_lease;
#[cfg(any(kani, feature = "typecheck"))]
mod proofs_plan;
