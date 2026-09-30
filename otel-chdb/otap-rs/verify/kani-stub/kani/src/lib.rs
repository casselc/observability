//! Type-checking stand-in for Kani's `kani` crate (see Cargo.toml). The
//! signatures are looser than Kani's (no `Arbitrary` bound): this only has
//! to accept what the harnesses call, and nothing here is ever run.

pub use kani_macros::{proof, solver, stub, unwind};

/// A symbolic value, under Kani. Never called here.
pub fn any<T>() -> T {
    unreachable!("kani stub: type-checking only")
}

/// A constraint on the symbolic values, under Kani. Never called here.
pub fn assume(_cond: bool) {
    unreachable!("kani stub: type-checking only")
}

/// A reachability check, under Kani: type-checks its arguments.
#[macro_export]
macro_rules! cover {
    ($cond:expr $(,)?) => {{
        let _: bool = $cond;
    }};
    ($cond:expr, $msg:literal $(,)?) => {{
        let _: bool = $cond;
        let _: &str = $msg;
    }};
}
