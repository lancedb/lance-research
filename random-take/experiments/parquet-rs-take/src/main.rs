use std::{
    fs::OpenOptions,
    process::Command,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, SystemTime},
};

use arrow_array::{
    cast::AsArray, Array, FixedSizeListArray, RecordBatch, RecordBatchIterator, RecordBatchReader,
};
use arrow_schema::{DataType, Field};
use clap::{Parser, ValueEnum};
use lance::{
    arrow::{fixed_size_list_type, SchemaExt},
    dataset::builder::DatasetBuilder,
    io::object_store::ObjectStoreExt,
    Dataset,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
        async_reader::AsyncFileReader,
        AsyncArrowWriter,
    },
    basic::Compression,
    file::{properties::WriterProperties, reader::ChunkReader},
};
use rand::seq::SliceRandom;
use random_take_bench::r#async::{take as async_take, TryClone as AsyncTryClone};
use random_take_bench::sync::{take as sync_take, TryClone as SyncTryClone};
use url::Url;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(name="random-take", about="A benchmark for tabular file formats", version, long_about = None)]
struct Args {
    /// What file format to benchmark
    #[arg(value_enum)]
    format: Format,

    /// How many rows to put in a row group (parquet only)
    #[arg(short, long, default_value_t = 100000)]
    row_group_size: u32,

    /// How many rows to take
    #[arg(short, long)]
    take_size: u32,

    /// The type of column to take
    #[arg(value_enum)]
    column_type: ColumnType,

    #[arg(short, long, default_value_t = false)]
    cache_metadata: bool,

    #[arg(short, long, default_value_t = false)]
    r#async: bool,

    /// Path to the test file
    src: String,

    /// Number of iteration to run.  The result will be the average across these iterations
    #[arg(short, long, default_value_t = 1)]
    iterations: u32,

    /// If quiet then only print the result
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// URI of the working directory.  Must be file:// or s3://
    #[arg(short, long)]
    workdir: Option<String>,
}

impl Args {
    fn column_index(&self) -> u32 {
        match self.column_type {
            ColumnType::Double => 3,
            ColumnType::Vector => 15,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Format {
    /// The lance file format
    Lance,
    /// The parquet file format
    Parquet,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ColumnType {
    /// A column of fp64 values
    Double,
    /// A column where each row is a vector of 768 fp16 values
    Vector,
}

fn parquet_path(work_dir: &Path, row_group_size: u32) -> Path {
    work_dir.child(format!("input_rgs_{}.parquet", row_group_size))
}

static SHOULD_LOG: AtomicBool = AtomicBool::new(false);
fn log(msg: impl AsRef<str>) {
    if SHOULD_LOG.load(std::sync::atomic::Ordering::Acquire) {
        println!("{}", msg.as_ref());
    }
}

async fn parquet_global_setup(args: &Args, work_store: &dyn ObjectStore, work_dir: &Path) {
    let dest_path = parquet_path(work_dir, args.row_group_size);
    if work_store.exists(&dest_path).await.unwrap() {
        log(format!("Using existing parquet test file at {}", dest_path));
        return;
    }

    log(format!("Creating new parquet test file at {}", dest_path));

    let src_path = std::path::Path::new(&args.src);
    let file = OpenOptions::new().read(true).open(src_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let schema = reader.schema().clone();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_size(args.row_group_size as usize)
        .build();

    let (_, write) = work_store.put_multipart(&dest_path).await.unwrap();
    let mut writer =
        AsyncArrowWriter::try_new(write, schema.clone(), 1024 * 1024, Some(props)).unwrap();
    for batch in &batches {
        writer.write(batch).await.unwrap();
    }
    writer.close().await.unwrap();
}

fn parquet_setup_sync(
    args: &Args,
    _: &dyn ObjectStore,
    work_dir: &Path,
) -> (std::fs::File, Vec<u32>, Option<ArrowReaderMetadata>) {
    // TODO: Handle S3
    let path_str = format!("/{}", parquet_path(work_dir, args.row_group_size));
    Command::new("dd")
        .arg(format!("of={}", path_str))
        .arg("oflag=nocache")
        .arg("conv=notrunc,fdatasync")
        .arg("count=0")
        .output()
        .unwrap();

    let path = std::path::Path::new(&path_str);
    let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();

    let options = ArrowReaderOptions::new().with_page_index(true);
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let num_rows = metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum::<i64>();

    let metadata = if args.cache_metadata {
        Some(metadata)
    } else {
        None
    };

    let mut rng = rand::thread_rng();
    let mut indices = (0..num_rows as u32).collect::<Vec<u32>>();
    let (permuted, _) = indices.partial_shuffle(&mut rng, args.take_size as usize);
    let mut indices = Vec::from(permuted);
    indices.sort();

    (file, indices, metadata)
}

async fn parquet_setup_async(
    args: &Args,
    _: &dyn ObjectStore,
    work_dir: &Path,
) -> (tokio::fs::File, Vec<u32>, Option<ArrowReaderMetadata>) {
    // TODO: Handle S3
    let path_str = format!("/{}", parquet_path(work_dir, args.row_group_size));
    Command::new("dd")
        .arg(format!("of={}", path_str))
        .arg("oflag=nocache")
        .arg("conv=notrunc,fdatasync")
        .arg("count=0")
        .output()
        .unwrap();

    let path = std::path::Path::new(&path_str);
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .unwrap();

    let options = ArrowReaderOptions::new().with_page_index(true);
    let metadata = ArrowReaderMetadata::load_async(&mut file, options)
        .await
        .unwrap();
    let num_rows = metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum::<i64>();

    let metadata = if args.cache_metadata {
        Some(metadata)
    } else {
        None
    };

    let mut rng = rand::thread_rng();
    let mut indices = (0..num_rows as u32).collect::<Vec<u32>>();
    let (permuted, _) = indices.partial_shuffle(&mut rng, args.take_size as usize);
    let mut indices = Vec::from(permuted);
    indices.sort();

    (file, indices, metadata)
}

fn parquet_random_take_sync<T: ChunkReader + SyncTryClone + 'static>(
    file: T,
    indices: Vec<u32>,
    metadata: Option<ArrowReaderMetadata>,
    col: u32,
) {
    let batches = sync_take(file, &indices, &[col], true, metadata);
    let num_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(num_rows, indices.len());
}

async fn parquet_random_take_async<T: AsyncFileReader + Unpin + AsyncTryClone + 'static>(
    file: &mut T,
    indices: Vec<u32>,
    metadata: Option<ArrowReaderMetadata>,
    col: u32,
) {
    let batches = async_take(file, &indices, &[col], true, metadata).await;
    let num_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(num_rows, indices.len());
}

async fn bench_parquet(args: &Args, work_store: &dyn ObjectStore, work_dir: &Path) -> f64 {
    parquet_global_setup(args, work_store, work_dir).await;
    let mut total_duration = Duration::from_nanos(0);
    for _ in 0..args.iterations {
        if args.r#async {
            let (mut file, indices, metadata) =
                parquet_setup_async(args, work_store, work_dir).await;
            let start = SystemTime::now();
            parquet_random_take_async(&mut file, indices, metadata, args.column_index()).await;
            total_duration += start.elapsed().unwrap();
        } else {
            let (file, indices, metadata) = parquet_setup_sync(args, work_store, work_dir);
            let start = SystemTime::now();
            parquet_random_take_sync(file, indices, metadata, args.column_index());
            total_duration += start.elapsed().unwrap();
        }
    }
    (total_duration.as_nanos() as f64 / (1000.0 * 1000.0)) / args.iterations as f64
}

fn lance_path(work_dir: &Path) -> Path {
    work_dir.child("bench_dataset.lance")
}

async fn lance_global_setup(args: &Args, work_store: &dyn ObjectStore, work_dir: &Path) -> Dataset {
    let dest_path = lance_path(work_dir);
    let sample_file = dest_path.child("_latest.manifest");
    let dest_uri = format!("/{}", dest_path);
    if work_store.exists(&sample_file).await.unwrap() {
        log(format!("Using existing lance dataset at {}", dest_path));
        return Dataset::open(&dest_uri).await.unwrap();
    }

    log(format!("Creating new lance dataset at {}", dest_path));
    let path = std::path::Path::new(args.src.as_str());
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
    Dataset::write(reader, &dest_uri, None).await.unwrap()
}

async fn lance_setup(
    dataset: &Dataset,
    args: &Args,
    work_store: Arc<dyn ObjectStore>,
    work_dir: &Path,
) -> (Dataset, Vec<u64>) {
    let dest_path = Path::parse(lance_path(work_dir)).unwrap();

    let path_str = format!("/{}", dest_path);
    for entry in glob::glob(&path_str).unwrap() {
        let path = entry.unwrap();
        Command::new("dd")
            .arg(format!("of={}", path.display()))
            .arg("oflag=nocache")
            .arg("conv=notrunc,fdatasync")
            .arg("count=0")
            .output()
            .unwrap();
    }

    let num_rows = dataset.count_rows().await.unwrap();
    let mut rng = rand::thread_rng();
    let mut indices = (1..num_rows as u64).collect::<Vec<u64>>();
    let (mutated, _) = indices.partial_shuffle(&mut rng, args.take_size as usize);
    let mut indices = Vec::from(mutated);
    indices.sort();

    let dataset = if args.cache_metadata {
        let col_name = &dataset.schema().fields[args.column_index() as usize].name;
        let schema = dataset.schema().project(&[col_name]).unwrap();
        dataset.take(&indices, &schema).await.unwrap();
        dataset.clone()
    } else {
        let url = format!("file://{}", dest_path);
        DatasetBuilder::from_uri(&url)
            .with_object_store(work_store, Url::parse(&url).unwrap())
            .load()
            .await
            .unwrap()
    };

    (dataset, indices)
}

async fn lance_random_take(dataset: &Dataset, indices: Vec<u64>, col: u32) {
    let col_name = &dataset.schema().fields[col as usize].name;
    let schema = dataset.schema().project(&[col_name]).unwrap();
    let batch = dataset.take(&indices, &schema).await.unwrap();
    assert_eq!(batch.num_rows(), indices.len());
}

async fn bench_lance(args: &Args, work_store: Arc<dyn ObjectStore>, work_dir: &Path) -> f64 {
    let dataset = lance_global_setup(args, &work_store, work_dir).await;
    let mut total_duration = Duration::from_nanos(0);
    for _ in 0..args.iterations {
        let (dataset, indices) = lance_setup(&dataset, args, work_store.clone(), work_dir).await;
        let start = SystemTime::now();
        lance_random_take(&dataset, indices, args.column_index()).await;
        total_duration += start.elapsed().unwrap();
    }
    (total_duration.as_nanos() as f64 / (1000.0 * 1000.0)) / args.iterations as f64
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if !args.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    }

    let workdir = args
        .workdir
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("file:///tmp")
        .to_string();
    let (work_store, work_dir) = if workdir.starts_with("file://") {
        let work_dir = Path::parse(workdir[7..].to_string()).unwrap();
        let work_store = Arc::new(LocalFileSystem::new());
        (work_store, work_dir)
    } else if workdir.starts_with("s3://") {
        todo!()
    } else {
        panic!("workdir argument must start with file:// or s3://")
    };

    let avg_duration_ms = match args.format {
        Format::Parquet => bench_parquet(&args, work_store.as_ref(), &work_dir).await,
        Format::Lance => bench_lance(&args, work_store, &work_dir).await,
    };

    println!("{}", avg_duration_ms);
}
