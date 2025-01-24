use std::{iter, time::Instant};

use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;
use clap::Parser;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use once_cell::sync::Lazy;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::WriterProperties,
};
use random_take_bench::{log, parq::io::WorkDir, FileFormat, SHOULD_LOG};
use tracing::Level;
use tracing_chrome::ChromeLayerBuilder;
use tracing_core::LevelFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser, Clone, Debug)]
#[command(name="full-scan", about="A benchmark for tabular file formats", version, long_about = None)]
struct Args {
    /// How many rows to put in a row group (parquet only)
    #[arg(short, long, default_value_t = 100000)]
    row_group_size: usize,

    /// Page size (in bytes, parquet only)
    #[arg(short, long, default_value_t = 1024)]
    page_size_kb: usize,

    /// How many rows to take in each take operation
    #[arg(short, long, default_value_t = 1024)]
    take_size: usize,

    /// Number of seconds to run the benchmark
    #[arg(long, default_value_t = 10.0)]
    duration_seconds: f64,

    /// If true, drop the OS cache before each iteration
    #[arg(short, long, default_value_t = false)]
    drop_caches: bool,

    /// If true, log each read operation
    #[arg(long, default_value_t = false)]
    log_reads: bool,

    /// Number of files to read from
    #[arg(short, long, default_value_t = 32)]
    num_files: usize,

    /// Number of rows per file
    #[arg(short, long, default_value_t = 1024 * 1024)]
    rows_per_file: usize,

    /// If quiet then only print the result
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// The file format to use
    #[arg(long, value_enum, default_value_t = FileFormat::Parquet)]
    format: FileFormat,

    /// The number of concurrent takes to run (note that each take may have its own per-row-group parallelism)
    #[arg(long, default_value_t = 0)]
    concurrency: usize,

    /// URI of the working directory.  Must be file:// or s3://
    #[arg(short, long, default_value = "file:///tmp")]
    workdir: String,

    /// Path to the directory containing test files.
    #[arg(short, long, default_value = "../compression/datafiles")]
    testdir: String,

    /// If true, enables tracing
    #[arg(long, default_value_t = false)]
    tracing: bool,
}

static ARGS: Lazy<Args> = Lazy::new(|| Args::parse());
static TEST_CATEGORIES: Lazy<Vec<String>> = Lazy::new(get_test_categories);

fn get_test_categories() -> Vec<String> {
    let mut categories = Vec::new();
    for testfile in std::fs::read_dir(&ARGS.testdir).unwrap() {
        let testfile = testfile.unwrap();
        let path = testfile.path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        if filename.ends_with("_trimmed.parquet") {
            let category = filename[..filename.len() - 16].to_string();
            categories.push(category);
        }
    }
    categories
}

fn src_path(category: &str) -> String {
    format!("{}/{}_trimmed.parquet", ARGS.testdir, category)
}

fn test_file_path(category: &str, file_index: usize) -> String {
    format!(
        "{}_rg_{}_ps_{}_chunk_{}.parquet",
        category, ARGS.row_group_size, ARGS.page_size_kb, file_index
    )
}

async fn read_test_data(path: &str) -> RecordBatch {
    let file = tokio::fs::File::open(path).await.unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap();

    let batches = reader.try_collect::<Vec<_>>().await.unwrap();
    let schema = batches[0].schema().clone();
    concat_batches(&schema, batches.iter()).unwrap()
}

fn get_parquet_write_batch_size(page_size_kb: usize, array: &dyn Array) -> usize {
    let page_size = page_size_kb * 1024;
    let array_size = array.get_array_memory_size();
    let estimated_elem_size = array_size / array.len() as usize;
    (page_size / (estimated_elem_size * 2)).max(1) as usize
}

async fn write_pq_test_file(workdir: &WorkDir, src_path: &str, dest_path: Path) {
    let test_data = read_test_data(src_path).await;
    let writer = workdir.writer(dest_path);
    let page_size_kb = ARGS.page_size_kb;
    let row_group_size = ARGS.row_group_size;
    let write_batch_size = get_parquet_write_batch_size(page_size_kb, test_data.column(0));
    let options = ArrowWriterOptions::default().with_properties(
        WriterProperties::builder()
            .set_bloom_filter_enabled(false)
            .set_max_row_group_size(row_group_size as usize)
            .set_data_page_size_limit(page_size_kb as usize * 1024)
            .set_write_batch_size(write_batch_size)
            .build(),
    );
    let mut writer =
        AsyncArrowWriter::try_new_with_options(writer, test_data.schema(), options).unwrap();

    let mut remaining = ARGS.rows_per_file;
    while remaining > 0 {
        let batch_size = remaining.min(write_batch_size);
        let batch = test_data.slice(0, batch_size);
        writer.write(&batch).await.unwrap();
        remaining -= batch_size;
    }

    writer.finish().await.unwrap();
}

async fn setup_parquet(workdir: &WorkDir) {
    for category in TEST_CATEGORIES.iter() {
        log(format!(
            "Setting up parquet test files for category {}",
            category
        ));
        let src_path = src_path(&category);
        for file_index in 0..ARGS.num_files {
            let dest_path = workdir
                .child_path("parquets")
                .child(test_file_path(&category, file_index));
            if workdir.exists(&dest_path).await {
                log(format!("Using existing parquet test file at {}", dest_path));
            } else {
                log(format!(
                    "Creating new parquet test file at {} from {}",
                    dest_path, src_path
                ));
                if file_index == 0 {
                    write_pq_test_file(workdir, &src_path, dest_path).await;
                } else {
                    let base_file = workdir
                        .child_path("parquets")
                        .child(test_file_path(&category, 0));
                    log(format!(
                        "Copying parquet test file at {} from {}",
                        dest_path, base_file
                    ));
                    workdir.copy(&base_file, &dest_path).await;
                }
            }
        }
    }
}

async fn bench_parquet_one(work_dir: &WorkDir, file_index: usize) {}

async fn run_bench_parquet(work_dir: &WorkDir) {
    let start = Instant::now();

    let tasks = stream::iter(
        (0..ARGS.num_files)
            .cycle()
            .take_while(|_| start.elapsed().as_secs_f64() < ARGS.duration_seconds)
            .map(|file_index| bench_parquet_one(work_dir, file_index)),
    )
    .buffer_unordered(ARGS.concurrency);

    let iterations = tasks.count().await;

    let elapsed = start.elapsed().as_secs_f64();

    log(format!(
        "Ran {} iterations in {} seconds",
        iterations, elapsed
    ));

    let iterations_per_second = iterations as f64 / elapsed;
    println!("{}", iterations_per_second);
}

async fn bench_parquet(work_dir: WorkDir) {
    setup_parquet(&work_dir).await;

    run_bench_parquet(&work_dir).await;
}

#[tokio::main]
async fn main() {
    let _guard = if ARGS.tracing {
        let (chrome_layer, guard) = ChromeLayerBuilder::new().build();
        tracing_subscriber::registry()
            .with(chrome_layer)
            .with(LevelFilter::from_level(Level::DEBUG))
            .init();
        Some(guard)
    } else {
        None
    };

    if !ARGS.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    }

    let workdir = WorkDir::new(&ARGS.workdir).await;

    match ARGS.format {
        FileFormat::Parquet => bench_parquet(workdir).await,
        FileFormat::Lance2_0 => todo!(),
        FileFormat::Lance2_1 => todo!(),
    }
}
