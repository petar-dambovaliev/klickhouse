extern crate core;

/// Clickhouse major version
pub const VERSION_MAJOR: u64 = 21;
/// Clickhouse minor version
pub const VERSION_MINOR: u64 = 6;

mod block;
mod client;
#[cfg(feature = "compression")]
mod compression;
mod convert;
/// Error generator functions used by `klickhouse_derive`
pub mod errors;
mod internal_client_in;
mod internal_client_out;
mod io;
mod progress;
mod protocol;
mod types;
mod values;

#[cfg(feature = "uuid")]
pub use uuid::Uuid;
#[cfg(not(feature = "uuid"))]
pub mod uuid;
#[cfg(not(feature = "uuid"))]
pub use uuid::Uuid;

#[cfg(feature = "derive")]
pub use klickhouse_derive::Row;

pub use client::*;
pub use convert::{FromSql, Row, ToSql};
pub use types::Type;
pub use values::*;

pub use anyhow::{Error, Result};
