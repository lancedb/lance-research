use std::fmt::Display;
use std::fs::remove_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_array::FixedSizeListArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use arrow_array::RecordBatchReader;
use arrow_schema::DataType;
use arrow_schema::Field;
use bytes::Bytes;
use criterion::measurement::Measurement;
use criterion::measurement::ValueFormatter;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use lance::arrow::fixed_size_list_type;
use lance::arrow::SchemaExt;
use lance::dataset::builder::DatasetBuilder;
use lance::Dataset;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;
use parquet_rs_take::TryClone;
use rand::seq::SliceRandom;
use tokio::runtime::Runtime;

const ROW_GROUP_SIZES: &[u32; 2] = &[50000, 100000];
const DOUBLE_COLUMN: u32 = 3;
const VECTOR_COLUMN: u32 = 15;

fn parquet_random_take<T: ChunkReader + TryClone + 'static>(
    file: T,
    indices: Vec<u32>,
    metadata: Option<ArrowReaderMetadata>,
    col: u32,
) {
    let batches = parquet_rs_take::take(file, &indices, &[col], true, metadata);
    let num_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(num_rows, indices.len());
}

static IO_COUNTER: AtomicU64 = AtomicU64::new(0);
struct IoMeasurement;
impl ValueFormatter for IoMeasurement {
    fn scale_values(&self, typical_value: f64, values: &mut [f64]) -> &'static str {
        if typical_value < 1024.0 {
            "B"
        } else {
            for val in values {
                *val /= 1024.0;
            }
            "KiB"
        }
    }

    fn scale_throughputs(
        &self,
        _typical_value: f64,
        _throughput: &criterion::Throughput,
        _values: &mut [f64],
    ) -> &'static str {
        todo!()
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        "bytes"
    }
}

impl Measurement for IoMeasurement {
    type Intermediate = u64;
    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        IO_COUNTER.load(std::sync::atomic::Ordering::Acquire)
    }

    fn end(&self, i: Self::Intermediate) -> Self::Value {
        let res = IO_COUNTER.load(std::sync::atomic::Ordering::Acquire) - i;
        // Sadly, criterion panics if a measurement is constant throughout the benchmark.  This adds a tiny bit
        // of random noise without hopefully affecting the measurement too much.
        if std::time::SystemTime::now().elapsed().unwrap().as_nanos() % 2 == 0 {
            res + 1
        } else {
            res
        }
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn criterion::measurement::ValueFormatter {
        self
    }
}

struct TrackedReader(BufReader<File>);
impl Read for TrackedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.0.read(buf)?;
        IO_COUNTER.fetch_add(read as u64, std::sync::atomic::Ordering::Release);
        Ok(read)
    }
}

struct TrackedFile(File);

impl TryClone for TrackedFile {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        self.0.try_clone().map(|f| TrackedFile(f))
    }
}

impl Length for TrackedFile {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

impl ChunkReader for TrackedFile {
    type T = TrackedReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(TrackedReader(self.0.get_read(start)?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let bytes = self.0.get_bytes(start, length)?;
        IO_COUNTER.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Release);
        Ok(bytes)
    }
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

fn parquet_setup(
    num_rows: u32,
    row_group_size: u32,
    cached_metadata: bool,
) -> (TrackedFile, Vec<u32>, Option<ArrowReaderMetadata>) {
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

    let metadata = if cached_metadata {
        let options = ArrowReaderOptions::new().with_page_index(true);
        Some(ArrowReaderMetadata::load(&file, options).unwrap())
    } else {
        None
    };

    let mut rng = rand::thread_rng();
    let mut indices = (1..500000).collect::<Vec<u32>>();
    indices.partial_shuffle(&mut rng, num_rows as usize);
    indices.truncate(num_rows as usize);
    indices.sort();

    assert_eq!(indices.len(), num_rows as usize);

    (TrackedFile(file), indices, metadata)
}

fn lance_global_setup(runtime: &Runtime) -> Dataset {
    let dest_path = Path::new("/tmp/my_lance/dataset");
    if dest_path.exists() {
        remove_dir_all(dest_path).unwrap();
    }
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
        .block_on(Dataset::write(
            reader,
            dest_path.as_os_str().to_str().unwrap(),
            None,
        ))
        .unwrap()
}

fn lance_setup(
    runtime: &Runtime,
    dataset: &Dataset,
    num_rows: u32,
    col: u32,
    cached: bool,
) -> (Dataset, Vec<u64>) {
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

    let dataset = if cached {
        let col_name = &dataset.schema().fields[col as usize].name;
        let schema = dataset.schema().project(&[col_name]).unwrap();
        runtime.block_on(dataset.take(&indices, &schema)).unwrap();
        dataset.clone()
    } else {
        runtime
            .block_on(DatasetBuilder::from_uri("/tmp/my_lance/dataset").load())
            .unwrap()
    };

    (dataset, indices)
}

fn lance_random_take(runtime: &Runtime, dataset: &Dataset, indices: Vec<u64>, col: u32) {
    let col_name = &dataset.schema().fields[col as usize].name;
    let schema = dataset.schema().project(&[col_name]).unwrap();
    // let schema = dataset.schema().project(&["similarity"]).unwrap();
    let batch = runtime.block_on(dataset.take(&indices, &schema)).unwrap();
    assert_eq!(batch.num_rows(), indices.len());
}

struct LanceBenchmarkParams {
    take_size: u32,
    cache_metadata: bool,
    column: u32,
}

impl Display for LanceBenchmarkParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col = if self.column == DOUBLE_COLUMN {
            "double"
        } else {
            "vector"
        };
        let cached = if self.cache_metadata {
            "cached"
        } else {
            "uncached"
        };
        write!(f, "take_{}_{}_{}", self.take_size, cached, col)
    }
}

struct ParquetBenchmarkParams {
    take_size: u32,
    row_group_size: u32,
    cache_metadata: bool,
    column: u32,
}

impl Display for ParquetBenchmarkParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col = if self.column == DOUBLE_COLUMN {
            "double"
        } else {
            "vector"
        };
        let cached = if self.cache_metadata {
            "cached"
        } else {
            "uncached"
        };
        write!(
            f,
            "take_{}_rgs_{}_{}_{}",
            self.take_size, self.row_group_size, cached, col
        )
    }
}

fn setup_benchmarks<M: Measurement + 'static>(c: &mut Criterion<M>) {
    let sizes = [1, 64, 128, 256, 512, 1024];
    let row_group_sizes = [50000, 100000];

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dataset = lance_global_setup(&runtime);
    parquet_global_setup();

    for num_rows in sizes {
        for cache_metadata in [false, true] {
            for column in [DOUBLE_COLUMN, VECTOR_COLUMN] {
                for row_group_size in row_group_sizes {
                    let params = ParquetBenchmarkParams {
                        take_size: num_rows,
                        row_group_size,
                        cache_metadata,
                        column,
                    };
                    c.bench_with_input(
                        BenchmarkId::new("parquet_random_take_{}", &params),
                        &params,
                        |b, params| {
                            // Insert a call to `to_async` to convert the bencher to async mode.
                            // The timing loops are the same as with the normal bencher.
                            b.iter_batched(
                                || {
                                    parquet_setup(
                                        params.take_size,
                                        params.row_group_size,
                                        params.cache_metadata,
                                    )
                                },
                                |(file, indices, metadata)| {
                                    parquet_random_take(file, indices, metadata, params.column)
                                },
                                criterion::BatchSize::PerIteration,
                            );
                        },
                    );
                }
                let params = LanceBenchmarkParams {
                    take_size: num_rows,
                    cache_metadata,
                    column,
                };
                c.bench_with_input(
                    BenchmarkId::new("lance_random_take_{}", &params),
                    &params,
                    |b, params| {
                        // Insert a call to `to_async` to convert the bencher to async mode.
                        // The timing loops are the same as with the normal bencher.
                        b.iter_batched(
                            || {
                                lance_setup(
                                    &runtime,
                                    &dataset,
                                    params.take_size,
                                    params.column,
                                    params.cache_metadata,
                                )
                            },
                            |(dataset, indices)| {
                                lance_random_take(&runtime, &dataset, indices, params.column)
                            },
                            criterion::BatchSize::PerIteration,
                        );
                    },
                );
            }
        }
    }
}

fn track_bytes() -> Criterion<IoMeasurement> {
    Criterion::default().with_measurement(IoMeasurement)
}
criterion_group!(name = benches; config = track_bytes(); targets = setup_benchmarks);
// criterion_group!(benches, setup_benchmarks);
criterion_main!(benches);
