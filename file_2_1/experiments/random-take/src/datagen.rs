use std::sync::Arc;

use arrow_array::{types::Float32Type, RecordBatchReader};
use arrow_schema::{DataType, Field};
use lance_datagen::{
    array::{rand_list_any, rand_varbin},
    ArrayGenerator, BatchCount, ByteCount, Dimension, RowCount,
};

use crate::DataTypeChoice;

const WRITE_BATCH_SIZE: usize = 32 * 1024;
// 768 x fp32 is a very common embedding type
const EMBEDDING_SIZE: u32 = 1024;
// Binary values are randomly generated with a size between 16KB and 24KB
const MIN_BINARY_SIZE: u64 = 16 * 1024;
const MAX_BINARY_SIZE: u64 = 24 * 1024;

fn array_gen_for_type(data_type: DataTypeChoice) -> Box<dyn ArrayGenerator> {
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
    }
}

pub fn get_datagen(data_type: DataTypeChoice, num_rows: usize) -> impl RecordBatchReader {
    let batch_size = if data_type == DataTypeChoice::BinaryList {
        WRITE_BATCH_SIZE / 4
    } else {
        WRITE_BATCH_SIZE
    };
    let num_batches = num_rows / batch_size;
    lance_datagen::gen()
        .col("value", array_gen_for_type(data_type))
        .into_reader_rows(
            RowCount::from(batch_size as u64),
            BatchCount::from(num_batches as u32),
        )
}
