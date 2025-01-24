use std::{
    fmt::{Display, Formatter},
    sync::atomic::{AtomicBool, AtomicUsize},
};

use clap::ValueEnum;
use once_cell::sync::Lazy;

pub mod datagen;
pub mod lance;
pub mod osutil;
pub mod parq;
pub mod sync;
pub mod take;
pub mod threading;
pub mod util;

/// Static flag to enable logging of reads
pub static LOG_READS: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

pub static SHOULD_LOG: AtomicBool = AtomicBool::new(false);
pub fn log(msg: impl AsRef<str>) {
    if SHOULD_LOG.load(std::sync::atomic::Ordering::Acquire) {
        println!("{}", msg.as_ref());
    }
}

#[derive(Copy, Debug, Clone, ValueEnum, PartialEq)]
pub enum DataTypeChoice {
    Scalar,
    String,
    ScalarList,
    StringList,
    Vector,
    Binary,
    VectorList,
    BinaryList,
}

impl DataTypeChoice {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Scalar => "u64",
            Self::String => "utf8",
            Self::ScalarList => "u64_l",
            Self::StringList => "utf8_l",
            Self::Vector => "vector",
            Self::VectorList => "vector_l",
            Self::Binary => "binary",
            Self::BinaryList => "binary_l",
        }
    }
}

#[derive(Copy, Debug, Clone, ValueEnum)]
pub enum FileFormat {
    Parquet,
    Lance2_0,
    Lance2_1,
}

impl Display for DataTypeChoice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeChoice::Scalar => write!(f, "scalar"),
            DataTypeChoice::String => write!(f, "string"),
            DataTypeChoice::ScalarList => write!(f, "scalar_l"),
            DataTypeChoice::StringList => write!(f, "string_l"),
            DataTypeChoice::Vector => write!(f, "vector"),
            DataTypeChoice::Binary => write!(f, "binary"),
            DataTypeChoice::VectorList => write!(f, "vector_l"),
            DataTypeChoice::BinaryList => write!(f, "binary_l"),
        }
    }
}

pub static TAKE_COUNTER: AtomicUsize = AtomicUsize::new(0);
