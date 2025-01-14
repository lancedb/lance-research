use arrow_array::{types::Float32Type, RecordBatchReader};
use arrow_schema::DataType;
use lance_datagen::{ArrayGenerator, BatchCount, Dimension, RowCount};

use crate::DataTypeChoice;

const WRITE_BATCH_SIZE: usize = 32 * 1024;
// 768 x fp32 is a very common embedding type
const EMBEDDING_SIZE: u32 = 768;

fn array_gen_for_type(data_type: DataTypeChoice) -> Box<dyn ArrayGenerator> {
    match data_type {
        DataTypeChoice::Int => lance_datagen::array::rand_type(&DataType::UInt32),
        DataTypeChoice::Long => lance_datagen::array::rand_type(&DataType::UInt64),
        DataTypeChoice::Float => lance_datagen::array::rand_type(&DataType::Float32),
        DataTypeChoice::Double => lance_datagen::array::rand_type(&DataType::Float64),
        DataTypeChoice::String => todo!(),
        DataTypeChoice::Embedding => {
            lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(EMBEDDING_SIZE))
        }
        DataTypeChoice::Image => todo!(),
    }
}

pub fn get_datagen(data_type: DataTypeChoice, num_rows: usize) -> impl RecordBatchReader {
    let num_batches = num_rows / WRITE_BATCH_SIZE;
    lance_datagen::gen()
        .col("value", array_gen_for_type(data_type))
        .into_reader_rows(
            RowCount::from(WRITE_BATCH_SIZE as u64),
            BatchCount::from(num_batches as u32),
        )
}
