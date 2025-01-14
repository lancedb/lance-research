use std::{
    fmt::{Display, Formatter},
    sync::atomic::AtomicBool,
};

use clap::ValueEnum;
use once_cell::sync::Lazy;

pub mod datagen;
pub mod osutil;
pub mod parq;
pub mod sync;
pub mod take;
pub mod threading;

/// Static flag to enable logging of reads
pub static LOG_READS: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

pub static SHOULD_LOG: AtomicBool = AtomicBool::new(false);
pub fn log(msg: impl AsRef<str>) {
    if SHOULD_LOG.load(std::sync::atomic::Ordering::Acquire) {
        println!("{}", msg.as_ref());
    }
}

#[derive(Copy, Debug, Clone, ValueEnum)]
pub enum DataTypeChoice {
    /// 4 byte integers
    Int,
    /// 8 byte integers
    Long,
    /// 4 byte floating point
    Float,
    /// 8 byte floating point
    Double,
    /// Short stings (e.g. names)
    String,
    /// Vector embedding sized tensors (3KiB)
    Embedding,
    /// Compressed images (e.g. 80KiB random binary)
    Image,
}

impl Display for DataTypeChoice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeChoice::Int => write!(f, "int"),
            DataTypeChoice::Long => write!(f, "long"),
            DataTypeChoice::Float => write!(f, "float"),
            DataTypeChoice::Double => write!(f, "double"),
            DataTypeChoice::String => write!(f, "string"),
            DataTypeChoice::Embedding => write!(f, "embedding"),
            DataTypeChoice::Image => write!(f, "image"),
        }
    }
}
