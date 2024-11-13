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

pub static IOPS_COUNTER: AtomicUsize = AtomicUsize::new(0);

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
    Nested1,
    Nested2,
    Nested3,
    Nested4,
    Nested5,
    NestedList1,
    NestedList2,
    NestedList3,
    NestedList4,
    NestedList5,
    Packed2,
    Packed3,
    Packed4,
    Packed5,
    Unpacked2,
    Unpacked3,
    Unpacked4,
    Unpacked5,
    SizedMiniBlock1,
    SizedMiniBlock2,
    SizedMiniBlock3,
    SizedMiniBlock4,
    SizedMiniBlock5,
    SizedFullZip1,
    SizedFullZip2,
    SizedFullZip3,
    SizedFullZip4,
    SizedFullZip5,
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
            Self::Nested1 => "nested1",
            Self::Nested2 => "nested2",
            Self::Nested3 => "nested3",
            Self::Nested4 => "nested4",
            Self::Nested5 => "nested5",
            Self::NestedList1 => "nested_l1",
            Self::NestedList2 => "nested_l2",
            Self::NestedList3 => "nested_l3",
            Self::NestedList4 => "nested_l4",
            Self::NestedList5 => "nested_l5",
            Self::Packed2 => "packed2",
            Self::Packed3 => "packed3",
            Self::Packed4 => "packed4",
            Self::Packed5 => "packed5",
            Self::Unpacked2 => "unpacked2",
            Self::Unpacked3 => "unpacked3",
            Self::Unpacked4 => "unpacked4",
            Self::Unpacked5 => "unpacked5",
            Self::SizedMiniBlock1 => "sized1_mb",
            Self::SizedMiniBlock2 => "sized2_mb",
            Self::SizedMiniBlock3 => "sized3_mb",
            Self::SizedMiniBlock4 => "sized4_mb",
            Self::SizedMiniBlock5 => "sized5_mb",
            Self::SizedFullZip1 => "sized1_fz",
            Self::SizedFullZip2 => "sized2_fz",
            Self::SizedFullZip3 => "sized3_fz",
            Self::SizedFullZip4 => "sized4_fz",
            Self::SizedFullZip5 => "sized5_fz",
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
            DataTypeChoice::Nested1 => write!(f, "nested1"),
            DataTypeChoice::Nested2 => write!(f, "nested2"),
            DataTypeChoice::Nested3 => write!(f, "nested3"),
            DataTypeChoice::Nested4 => write!(f, "nested4"),
            DataTypeChoice::Nested5 => write!(f, "nested5"),
            DataTypeChoice::NestedList1 => write!(f, "nested_l1"),
            DataTypeChoice::NestedList2 => write!(f, "nested_l2"),
            DataTypeChoice::NestedList3 => write!(f, "nested_l3"),
            DataTypeChoice::NestedList4 => write!(f, "nested_l4"),
            DataTypeChoice::NestedList5 => write!(f, "nested_l5"),
            DataTypeChoice::Packed2 => write!(f, "packed2"),
            DataTypeChoice::Packed3 => write!(f, "packed3"),
            DataTypeChoice::Packed4 => write!(f, "packed4"),
            DataTypeChoice::Packed5 => write!(f, "packed5"),
            DataTypeChoice::Unpacked2 => write!(f, "unpacked2"),
            DataTypeChoice::Unpacked3 => write!(f, "unpacked3"),
            DataTypeChoice::Unpacked4 => write!(f, "unpacked4"),
            DataTypeChoice::Unpacked5 => write!(f, "unpacked5"),
            DataTypeChoice::SizedMiniBlock1 => write!(f, "sized1_mb"),
            DataTypeChoice::SizedMiniBlock2 => write!(f, "sized2_mb"),
            DataTypeChoice::SizedMiniBlock3 => write!(f, "sized3_mb"),
            DataTypeChoice::SizedMiniBlock4 => write!(f, "sized4_mb"),
            DataTypeChoice::SizedMiniBlock5 => write!(f, "sized5_mb"),
            DataTypeChoice::SizedFullZip1 => write!(f, "sized1_fz"),
            DataTypeChoice::SizedFullZip2 => write!(f, "sized2_fz"),
            DataTypeChoice::SizedFullZip3 => write!(f, "sized3_fz"),
            DataTypeChoice::SizedFullZip4 => write!(f, "sized4_fz"),
            DataTypeChoice::SizedFullZip5 => write!(f, "sized5_fz"),
        }
    }
}

pub static TAKE_COUNTER: AtomicUsize = AtomicUsize::new(0);
