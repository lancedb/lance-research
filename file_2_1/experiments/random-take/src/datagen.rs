use std::{collections::HashMap, sync::Arc};

use arrow_array::{types::Float32Type, RecordBatchReader};
use arrow_schema::{DataType, Field, Fields};
use lance_datagen::{
    array::{cycle_vec, rand_list_any, rand_type, rand_varbin},
    ArrayGenerator, ArrayGeneratorExt, BatchCount, ByteCount, Dimension, RowCount,
};

use crate::DataTypeChoice;

const WRITE_BATCH_SIZE: usize = 32 * 1024;
// 768 x fp32 is a very common embedding type
const EMBEDDING_SIZE: u32 = 768;
// Binary values are randomly generated with a size between 16KB and 24KB
const MIN_BINARY_SIZE: u64 = 16 * 1024;
const MAX_BINARY_SIZE: u64 = 24 * 1024;

fn nested_array_gen(nesting_level: u32, initial_dim: u32) -> Box<dyn ArrayGenerator> {
    let mut gen = rand_type(&DataType::Float32).with_random_nulls(0.1);
    if nesting_level == 0 {
        return gen;
    }
    gen = cycle_vec(gen, Dimension::from(initial_dim));
    for _ in 1..nesting_level {
        gen = cycle_vec(gen, Dimension::from(2)).with_random_nulls(0.1);
    }
    gen
}

pub fn packed_array_gen(num_fields: usize) -> Box<dyn ArrayGenerator> {
    let fields = (0..num_fields)
        .map(|i| {
            Field::new(
                format!("field_{}", i),
                DataType::Float32,
                /*nullable=*/ true,
            )
        })
        .collect::<Fields>();
    rand_type(&DataType::Struct(fields))
}

fn array_gen_for_type(data_type: DataTypeChoice) -> Box<dyn ArrayGenerator> {
    let mb_metadata = HashMap::from_iter(vec![(
        "lance-encoding:structural-encoding".to_string(),
        "miniblock".to_string(),
    )]);
    let fz_metadata = HashMap::from_iter(vec![(
        "lance-encoding:structural-encoding".to_string(),
        "fullzip".to_string(),
    )]);
    let packed_metadata = HashMap::from_iter(vec![
        ("lance-encoding:packed".to_string(), "true".to_string()),
        ("packed".to_string(), "true".to_string()),
    ]);

    DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128);
    match data_type {
        DataTypeChoice::Scalar => lance_datagen::array::rand_type(&DataType::UInt64),
        DataTypeChoice::String => lance_datagen::array::rand_type(&DataType::Utf8),
        DataTypeChoice::ScalarList => lance_datagen::array::rand_type(&DataType::List(Arc::new(
            Field::new("element", DataType::UInt64, true),
        ))),
        DataTypeChoice::StringList => lance_datagen::array::rand_type(&DataType::List(Arc::new(
            Field::new("element", DataType::Utf8, true),
        ))),
        DataTypeChoice::Vector => {
            lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(EMBEDDING_SIZE))
        }
        DataTypeChoice::Binary => rand_varbin(
            ByteCount::from(MIN_BINARY_SIZE),
            ByteCount::from(MAX_BINARY_SIZE),
        ),
        DataTypeChoice::BinaryList => {
            let bingen = rand_varbin(
                ByteCount::from(MIN_BINARY_SIZE),
                ByteCount::from(MAX_BINARY_SIZE),
            );
            rand_list_any(bingen, /*is_large=*/ false)
        }
        DataTypeChoice::VectorList => {
            lance_datagen::array::rand_type(&DataType::List(Arc::new(Field::new(
                "element",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    EMBEDDING_SIZE as i32,
                ),
                true,
            ))))
        }
        DataTypeChoice::Nested1 => nested_array_gen(1, 2),
        DataTypeChoice::Nested2 => nested_array_gen(2, 2),
        DataTypeChoice::Nested3 => nested_array_gen(3, 2),
        DataTypeChoice::Nested4 => nested_array_gen(4, 2),
        DataTypeChoice::Nested5 => nested_array_gen(5, 2),
        DataTypeChoice::Packed2 => packed_array_gen(2).with_metadata(packed_metadata.clone()),
        DataTypeChoice::Packed3 => packed_array_gen(3).with_metadata(packed_metadata.clone()),
        DataTypeChoice::Packed4 => packed_array_gen(4).with_metadata(packed_metadata.clone()),
        DataTypeChoice::Packed5 => packed_array_gen(5).with_metadata(packed_metadata.clone()),
        DataTypeChoice::Unpacked2 => packed_array_gen(2),
        DataTypeChoice::Unpacked3 => packed_array_gen(3),
        DataTypeChoice::Unpacked4 => packed_array_gen(4),
        DataTypeChoice::Unpacked5 => packed_array_gen(5),
        DataTypeChoice::SizedMiniBlock1 => {
            rand_type(&DataType::FixedSizeBinary(32)).with_metadata(mb_metadata.clone())
        }
        DataTypeChoice::SizedMiniBlock2 => {
            rand_type(&DataType::FixedSizeBinary(64)).with_metadata(mb_metadata.clone())
        }
        DataTypeChoice::SizedMiniBlock3 => {
            rand_type(&DataType::FixedSizeBinary(128)).with_metadata(mb_metadata.clone())
        }
        DataTypeChoice::SizedMiniBlock4 => {
            rand_type(&DataType::FixedSizeBinary(256)).with_metadata(mb_metadata.clone())
        }
        DataTypeChoice::SizedMiniBlock5 => {
            rand_type(&DataType::FixedSizeBinary(512)).with_metadata(mb_metadata.clone())
        }
        DataTypeChoice::SizedFullZip1 => {
            rand_type(&DataType::FixedSizeBinary(32)).with_metadata(fz_metadata.clone())
        }
        DataTypeChoice::SizedFullZip2 => {
            rand_type(&DataType::FixedSizeBinary(64)).with_metadata(fz_metadata.clone())
        }
        DataTypeChoice::SizedFullZip3 => {
            rand_type(&DataType::FixedSizeBinary(128)).with_metadata(fz_metadata.clone())
        }
        DataTypeChoice::SizedFullZip4 => {
            rand_type(&DataType::FixedSizeBinary(256)).with_metadata(fz_metadata.clone())
        }
        DataTypeChoice::SizedFullZip5 => {
            rand_type(&DataType::FixedSizeBinary(512)).with_metadata(fz_metadata.clone())
        }
    }
}

pub fn get_datagen(
    data_type: DataTypeChoice,
    num_rows: usize,
    nullable: bool,
) -> impl RecordBatchReader {
    let batch_size = if data_type == DataTypeChoice::BinaryList {
        WRITE_BATCH_SIZE / 4
    } else {
        WRITE_BATCH_SIZE
    };
    let num_batches = num_rows / batch_size;
    let mut col_gen = array_gen_for_type(data_type);
    if nullable {
        col_gen = col_gen.with_random_nulls(0.1);
    }
    lance_datagen::gen().col("value", col_gen).into_reader_rows(
        RowCount::from(batch_size as u64),
        BatchCount::from(num_batches as u32),
    )
}
