//! Library-first entrypoint for Delta Table Service V3.
//!
//! The long-term direction for V3 is in-process native interop between C# and
//! Rust using the Arrow C Data / C Stream interfaces. The crate now exposes a
//! reusable library surface for the native C ABI and lightweight test helpers.

pub mod error;
pub mod interop;
pub mod service;
