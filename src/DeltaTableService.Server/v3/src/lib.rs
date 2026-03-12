// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Library-first entrypoint for Delta Table Service V3.
//!
//! The long-term direction for V3 is in-process native interop between C# and
//! Rust using the Arrow C Data / C Stream interfaces.  To make the existing
//! Arrow Flight transport easy to deprecate, the crate now exposes a reusable
//! library surface that can be consumed by both:
//! - the legacy Flight server binary, and
//! - a native C ABI layer.

pub mod core;
pub mod delta;
pub mod error;
pub mod flight_service;
pub mod handlers;
pub mod interop;
