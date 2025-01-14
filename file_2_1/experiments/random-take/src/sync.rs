//! Utilities for bridging sync / async code

use once_cell::sync::Lazy;

// Static runtime to use when we need to run an async function in a sync context
pub static RT: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().unwrap());
