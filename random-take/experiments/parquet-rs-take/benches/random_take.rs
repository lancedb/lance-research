use std::fmt::Display;
use std::fs::remove_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_array::FixedSizeListArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use arrow_array::RecordBatchReader;
use arrow_schema::DataType;
use arrow_schema::Field;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use lance::arrow::fixed_size_list_type;
use lance::arrow::SchemaExt;
use lance::Dataset;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::seq::SliceRandom;
use tokio::runtime::Runtime;

const ROW_GROUP_SIZES: &[u32; 2] = &[50000, 100000];

fn parquet_random_take(file: File, indices: Vec<u32>) {
    let batches = parquet_rs_take::take(file, &indices, &[15], true);
    let num_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(num_rows, indices.len());
}

fn parquet_global_setup() {
    dbg!("Running parquet global setup");
    let path_str = "/home/pace/dev/data/laion_100m/shard_0000.parquet";
    let path = Path::new(path_str);
    let file = OpenOptions::new().read(true).open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let schema = reader.schema().clone();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    for row_group_size in ROW_GROUP_SIZES {
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_size(*row_group_size as usize)
            .build();
        let out_path = format!("/tmp/bench_input_{}.parquet", row_group_size);
        let path = Path::new(&out_path);
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();
        let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), Some(props)).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }
}

fn parquet_setup(num_rows: u32, row_group_size: u32) -> (File, Vec<u32>) {
    let path_str = format!("/tmp/bench_input_{}.parquet", row_group_size);
    Command::new("dd")
        .arg(format!("of={}", path_str))
        .arg("oflag=nocache")
        .arg("conv=notrunc,fdatasync")
        .arg("count=0")
        .output()
        .unwrap();
    let path = Path::new(&path_str);
    let file = OpenOptions::new().read(true).open(path).unwrap();

    let mut rng = rand::thread_rng();
    let mut indices = (1..500000).collect::<Vec<u32>>();
    indices.partial_shuffle(&mut rng, num_rows as usize);
    indices.truncate(num_rows as usize);
    indices.sort();

    assert_eq!(indices.len(), num_rows as usize);

    (file, indices)
}

fn lance_global_setup(runtime: &Runtime) -> Dataset {
    let dest_path = "/tmp/my_lance/dataset";
    remove_dir_all(Path::new(dest_path)).unwrap();
    let path_str = "/home/pace/dev/data/laion_100m/shard_0000.parquet";
    let path = Path::new(path_str);
    let file = OpenOptions::new().read(true).open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batches = reader
        .map(|rb| {
            if let Ok(rb) = rb {
                let mut schema = rb.schema();
                let mut columns = Vec::with_capacity(rb.num_columns());
                for col_index in 0..rb.num_columns() {
                    let col = rb.column(col_index);
                    if col.data_type().is_nested() {
                        let fixed_list_type = fixed_size_list_type(768, DataType::Float64);
                        let new_field = Field::new("vector", fixed_list_type, true);
                        let mut new_schema = schema.as_ref().clone();
                        new_schema.remove(col_index);
                        schema = Arc::new(
                            new_schema
                                .try_with_column_at(col_index, new_field.clone())
                                .unwrap(),
                        );
                        let new_col = FixedSizeListArray::try_new(
                            Arc::new(Field::new("item", DataType::Float64, true)),
                            768,
                            col.as_fixed_size_list_opt().unwrap().values().clone(),
                            col.nulls().cloned(),
                        )
                        .unwrap();
                        columns.push(Arc::new(new_col) as Arc<dyn Array>);
                    } else {
                        columns.push(col.clone());
                    }
                }
                RecordBatch::try_new(schema, columns)
            } else {
                rb
            }
        })
        .collect::<Vec<_>>();
    let schema = batches[0].as_ref().unwrap().schema();
    let reader = RecordBatchIterator::new(batches, schema);
    runtime
        .block_on(Dataset::write(reader, dest_path, None))
        .unwrap()
}

fn lance_setup(num_rows: u32) -> Vec<u64> {
    for entry in glob::glob("/tmp/my_lance/dataset/**/*").unwrap() {
        let path = entry.unwrap();
        Command::new("dd")
            .arg(format!("of={}", path.display()))
            .arg("oflag=nocache")
            .arg("conv=notrunc,fdatasync")
            .arg("count=0")
            .output()
            .unwrap();
    }

    let mut rng = rand::thread_rng();
    let mut indices = (1..500000).collect::<Vec<u64>>();
    indices.partial_shuffle(&mut rng, num_rows as usize);
    indices.truncate(num_rows as usize);
    indices.sort();

    assert_eq!(indices.len(), num_rows as usize);

    indices
}

fn lance_random_take(runtime: &Runtime, dataset: &Dataset, indices: Vec<u64>) {
    let schema = dataset.schema().project(&["vector"]).unwrap();
    // let schema = dataset.schema().project(&["similarity"]).unwrap();
    let batch = runtime.block_on(dataset.take(&indices, &schema)).unwrap();
    assert_eq!(batch.num_rows(), indices.len());
}

struct ParquetBenchmarkParams {
    take_size: u32,
    row_group_size: u32,
}

impl Display for ParquetBenchmarkParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "take_{}_rgs_{}", self.take_size, self.row_group_size)
    }
}

fn setup_benchmarks(c: &mut Criterion) {
    let sizes = [1, 64, 128, 256, 512, 1024];
    let row_group_sizes = [50000, 100000];

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dataset = lance_global_setup(&runtime);
    parquet_global_setup();

    for num_rows in sizes {
        for row_group_size in row_group_sizes {
            let params = ParquetBenchmarkParams {
                take_size: num_rows,
                row_group_size,
            };
            c.bench_with_input(
                BenchmarkId::new("parquet_random_take_{}", &params),
                &params,
                |b, params| {
                    // Insert a call to `to_async` to convert the bencher to async mode.
                    // The timing loops are the same as with the normal bencher.
                    b.iter_batched(
                        || parquet_setup(params.take_size, params.row_group_size),
                        |(file, indices)| parquet_random_take(file, indices),
                        criterion::BatchSize::PerIteration,
                    );
                },
            );
        }
        c.bench_with_input(
            BenchmarkId::new("lance_random_take_{}", num_rows),
            &num_rows,
            |b, &num_rows| {
                // Insert a call to `to_async` to convert the bencher to async mode.
                // The timing loops are the same as with the normal bencher.
                b.iter_batched(
                    || lance_setup(num_rows),
                    |indices| lance_random_take(&runtime, &dataset, indices),
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }
}

criterion_group!(benches, setup_benchmarks);
criterion_main!(benches);
