use std::{sync::Arc, time::Instant};

use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;
use clap::Parser;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use lance_core::cache::FileMetadataCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::{
    v2::{
        reader::{FileReader, FileReaderOptions},
        writer::{FileWriter, FileWriterOptions},
    },
    version::LanceFileVersion,
};
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    ReadBatchParams,
};
use object_store::path::Path;
use once_cell::sync::Lazy;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::{
        metadata::ParquetMetaDataReader, page_index::index::Index, properties::WriterProperties,
    },
};
use random_take_bench::{
    log, osutil::drop_path_from_cache, parq::io::WorkDir, FileFormat, SHOULD_LOG,
};
use tracing::Level;
use tracing_chrome::ChromeLayerBuilder;
use tracing_core::LevelFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser, Clone, Debug)]
#[command(name="full-scan", about="A benchmark for tabular file formats", version, long_about = None)]
struct Args {
    /// How many rows to put in a row group (parquet only)
    #[arg(long, default_value_t = 100000)]
    row_group_size: usize,

    /// Page size (in bytes, parquet only)
    #[arg(long)]
    page_size_kb: Option<usize>,

    /// Number of seconds to run the benchmark
    #[arg(long, default_value_t = 10.0)]
    duration_seconds: f64,

    /// If true, drop the OS cache before each iteration
    #[arg(long, default_value_t = true)]
    drop_caches: bool,

    /// If true, log each read operation
    #[arg(long, default_value_t = false)]
    log_reads: bool,

    /// Number of files to read from
    #[arg(long, default_value_t = 32)]
    num_files: usize,

    /// Number of rows per file
    #[arg(long, default_value_t = 1000000)]
    rows_per_file: usize,

    /// If quiet then only print the result
    #[arg(long, default_value_t = false)]
    quiet: bool,

    /// The file format to use
    #[arg(long, value_enum, default_value_t = FileFormat::Parquet)]
    format: FileFormat,

    /// The number of concurrent takes to run (note that each take may have its own per-row-group parallelism)
    #[arg(long, default_value_t = 0)]
    concurrency: usize,

    /// The category of test files to use, if not speicified all categories will be used
    #[arg(long)]
    category: Option<String>,

    /// URI of the working directory.  Must be file:// or s3://
    #[arg(long, default_value = "file:///tmp")]
    workdir: String,

    /// Path to the directory containing test files.
    #[arg(long, default_value = "../compression/datafiles")]
    testdir: String,

    /// If true, enables tracing
    #[arg(long, default_value_t = false)]
    tracing: bool,
}

static ARGS: Lazy<Args> = Lazy::new(|| Args::parse());

fn get_test_categories() -> Vec<String> {
    if let Some(category) = &ARGS.category {
        return vec![category.clone()];
    }
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

fn test_pq_file_path(category: &str, file_index: usize, page_size_kb: usize) -> String {
    format!(
        "{}_rg_{}_ps_{}_chunk_{}.parquet",
        category, ARGS.row_group_size, page_size_kb, file_index
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

async fn write_pq_test_file(
    workdir: &WorkDir,
    src_path: &str,
    dest_path: Path,
    page_size_kb: usize,
) {
    let test_data = read_test_data(src_path).await;
    let writer = workdir.writer(dest_path);
    let row_group_size = ARGS.row_group_size;
    let write_batch_size = get_parquet_write_batch_size(page_size_kb, test_data.column(0));
    let options = ArrowWriterOptions::default().with_properties(
        WriterProperties::builder()
            .set_bloom_filter_enabled(false)
            .set_max_row_group_size(row_group_size as usize)
            .set_data_page_size_limit(page_size_kb * 1024)
            .set_write_batch_size(write_batch_size)
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build(),
    );
    let mut writer =
        AsyncArrowWriter::try_new_with_options(writer, test_data.schema(), options).unwrap();

    writer.write(&test_data).await.unwrap();

    writer.finish().await.unwrap();
}

async fn setup_parquet(workdir: &WorkDir, category: &str, page_size_kb: usize) {
    log(format!(
        "Setting up parquet test files for category {}",
        category
    ));
    let src_path = src_path(&category);
    for file_index in 0..ARGS.num_files {
        let dest_path = workdir.child_path("parquets").child(test_pq_file_path(
            &category,
            file_index,
            page_size_kb,
        ));
        if workdir.exists(&dest_path).await {
            log(format!("Using existing parquet test file at {}", dest_path));
        } else {
            log(format!(
                "Creating new parquet test file at {} from {}",
                dest_path, src_path
            ));
            if file_index == 0 {
                write_pq_test_file(workdir, &src_path, dest_path, page_size_kb).await;
            } else {
                let base_file = workdir.child_path("parquets").child(test_pq_file_path(
                    &category,
                    0,
                    page_size_kb,
                ));
                log(format!(
                    "Copying parquet test file at {} from {}",
                    dest_path, base_file
                ));
                workdir.copy(&base_file, &dest_path).await;
            }
        }
    }
}

async fn bench_parquet_one(path: Path, is_large: bool) -> (usize, usize) {
    let path = format!("/{}", path);
    drop_path_from_cache(&path);
    let f = tokio::fs::File::open(path).await.unwrap();
    let disk_bytes = f.metadata().await.unwrap().len() as usize;
    let mut stream_builder = ParquetRecordBatchStreamBuilder::new(f).await.unwrap();
    if is_large {
        stream_builder = stream_builder.with_batch_size(128);
    }
    let stream = stream_builder.build().unwrap();

    let bytes_read = stream
        .fold(0, |acc, batch| async move {
            let batch = batch.unwrap();
            let bytes = batch.get_array_memory_size();
            acc + bytes
        })
        .await;

    (disk_bytes, bytes_read)
}

// These categories have such large items we need to use a smaller batch_size
// so that we don't trigger thrashing
fn is_large(category: &str) -> bool {
    category == "websites" || category == "images" || category == "code"
}

async fn run_bench_parquet(work_dir: &WorkDir, category: &str, page_size_kb: usize) {
    log(format!("Benchmarking parquet for category {}", category));
    let start = Instant::now();

    let concurrency = if ARGS.concurrency == 0 {
        num_cpus::get()
    } else {
        ARGS.concurrency
    };

    let is_large = is_large(category);

    let (iterations, disk_bytes, mem_bytes) = if ARGS.duration_seconds == 0.0 {
        let path = test_pq_file_path(category, 0, page_size_kb);
        let path = work_dir.child_path("parquets").child(path);

        // Debugging information, print the # of pages to confirm the file was written correctly
        {
            let path_str = format!("/{}", path);
            let mut f = tokio::fs::File::open(path_str).await.unwrap();

            let disk_bytes = f.metadata().await.unwrap().len() as usize;
            let reader = ParquetMetaDataReader::new().with_page_indexes(true);
            let metadata = reader.load_and_finish(&mut f, disk_bytes).await.unwrap();
            let col_idx = metadata.column_index().unwrap();

            let mut num_pages = 0;
            for rg_idx in col_idx {
                for pg_idx in rg_idx {
                    num_pages += match pg_idx {
                        Index::BOOLEAN(b) => b.indexes.len(),
                        Index::BYTE_ARRAY(b) => b.indexes.len(),
                        Index::DOUBLE(b) => b.indexes.len(),
                        Index::FIXED_LEN_BYTE_ARRAY(b) => b.indexes.len(),
                        Index::FLOAT(b) => b.indexes.len(),
                        Index::INT32(b) => b.indexes.len(),
                        Index::INT64(b) => b.indexes.len(),
                        Index::INT96(b) => b.indexes.len(),
                        Index::NONE => unreachable!(),
                    };
                }
            }
            log(format!("There are {} pages", num_pages));
        }

        let (disk_bytes, mem_bytes) = bench_parquet_one(path, is_large).await;
        (1, disk_bytes, mem_bytes)
    } else {
        let task_iter = (0..ARGS.num_files)
            .cycle()
            .take_while(|_| start.elapsed().as_secs_f64() < ARGS.duration_seconds)
            .map(|file_index| {
                let path = test_pq_file_path(category, file_index, page_size_kb);
                let path = work_dir.child_path("parquets").child(path);
                tokio::task::spawn(bench_parquet_one(path, is_large))
            })
            .map(|task| task.map(|res| res.unwrap()));

        let tasks = stream::iter(task_iter).buffer_unordered(concurrency);

        tasks
            .fold((0, 0, 0), |acc, (disk_bytes, mem_bytes)| async move {
                let (iterations, acc_disk_bytes, acc_mem_bytes) = acc;
                (
                    iterations + 1,
                    disk_bytes + acc_disk_bytes,
                    mem_bytes + acc_mem_bytes,
                )
            })
            .await
    };

    let elapsed = start.elapsed().as_secs_f64();

    log(format!(
        "Ran {} iterations and read {} memory bytes and {} disk bytes in {} seconds",
        iterations, mem_bytes, disk_bytes, elapsed
    ));

    let iterations_per_second = iterations as f64 / elapsed;
    let mem_bytes_per_second = mem_bytes as f64 / elapsed;
    let disk_bytes_per_second = disk_bytes as f64 / elapsed;
    println!(
        "{},{},{},{},{}",
        page_size_kb, category, iterations_per_second, mem_bytes_per_second, disk_bytes_per_second
    );
}

async fn bench_parquet(work_dir: &WorkDir, category: &str, page_size_kb: usize) {
    setup_parquet(work_dir, category, page_size_kb).await;

    run_bench_parquet(work_dir, category, page_size_kb).await;
}

fn test_lance_file_path(category: &str, file_index: usize) -> String {
    format!("{}_chunk_{}.lance", category, file_index)
}

async fn write_lance_test_file(
    workdir: &WorkDir,
    src_path: &str,
    dest_path: Path,
    file_version: LanceFileVersion,
) {
    let test_data = read_test_data(src_path).await;

    let obj_writer = workdir.lance_writer(dest_path).await;

    let mut writer = FileWriter::new_lazy(
        obj_writer,
        FileWriterOptions {
            format_version: Some(file_version),
            ..Default::default()
        },
    );

    writer.write_batch(&test_data).await.unwrap();

    writer.finish().await.unwrap();
}

async fn setup_lance(workdir: &WorkDir, category: &str, version: LanceFileVersion) {
    log(format!(
        "Setting up lance test files for category {}",
        category
    ));
    let src_path = src_path(&category);
    for file_index in 0..ARGS.num_files {
        let dest_path = workdir
            .child_path(&format!("lances_{}", version))
            .child(test_lance_file_path(&category, file_index));
        if workdir.exists(&dest_path).await {
            log(format!("Using existing lance test file at {}", dest_path));
        } else {
            log(format!(
                "Creating new lance test file at {} from {}",
                dest_path, src_path
            ));
            if file_index == 0 {
                write_lance_test_file(workdir, &src_path, dest_path, version).await;
            } else {
                let base_file = workdir
                    .child_path(&format!("lances_{}", version))
                    .child(test_lance_file_path(&category, 0));
                log(format!(
                    "Copying lance test file at {} from {}",
                    dest_path, base_file
                ));
                workdir.copy(&base_file, &dest_path).await;
            }
        }
    }
}

async fn bench_lance_one(
    scheduler: Arc<ScanScheduler>,
    task_index: usize,
    path: Path,
    is_large: bool,
) -> (usize, usize) {
    drop_path_from_cache(&format!("/{}", path));

    let file_scheduler = scheduler
        .open_file_with_priority(&path, task_index as u64)
        .await
        .unwrap();

    let batch_size = if is_large { 128 } else { 32 * 1024 };

    let disk_bytes = file_scheduler.reader().size().await.unwrap();

    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &FileMetadataCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    let stream = reader
        .read_stream(
            ReadBatchParams::RangeFull,
            batch_size,
            8,
            FilterExpression::no_filter(),
        )
        .unwrap();

    let mem_bytes = stream
        .fold(0, |acc, batch| async move {
            let batch = batch.unwrap();
            let bytes = batch.get_array_memory_size();
            acc + bytes
        })
        .await;

    (disk_bytes, mem_bytes)
}

async fn run_bench_lance(work_dir: &WorkDir, category: &str, version: LanceFileVersion) {
    log(format!(
        "Benchmarking lance {} for category {}",
        version, category
    ));
    let start = Instant::now();

    let concurrency = if ARGS.concurrency == 0 {
        num_cpus::get()
    } else {
        ARGS.concurrency
    };

    let is_large = is_large(category);

    let store = work_dir.lance_object_store();
    let scheduler_config = SchedulerConfig::max_bandwidth(&store);
    let scheduler = ScanScheduler::new(store, scheduler_config);

    let (iterations, disk_bytes, mem_bytes) = if ARGS.duration_seconds == 0.0 {
        let path = test_lance_file_path(category, 0);
        let path = work_dir
            .child_path(&format!("lances_{}", version))
            .child(path);

        let (disk_bytes, mem_bytes) = bench_lance_one(scheduler, 0, path, is_large).await;
        (1, disk_bytes, mem_bytes)
    } else {
        let task_iter = (0..ARGS.num_files)
            .cycle()
            .take_while(|_| start.elapsed().as_secs_f64() < ARGS.duration_seconds)
            .enumerate()
            .map(|(task_index, file_index)| {
                let path = test_lance_file_path(category, file_index);
                let path = work_dir
                    .child_path(&format!("lances_{}", version))
                    .child(path);

                tokio::task::spawn(bench_lance_one(
                    scheduler.clone(),
                    task_index,
                    path,
                    is_large,
                ))
            })
            .map(|task| task.map(|res| res.unwrap()));

        let tasks = stream::iter(task_iter).buffer_unordered(concurrency);

        tasks
            .fold((0, 0, 0), |acc, (disk_bytes, mem_bytes)| async move {
                let (iterations, acc_disk_bytes, acc_mem_bytes) = acc;
                (
                    iterations + 1,
                    disk_bytes + acc_disk_bytes,
                    mem_bytes + acc_mem_bytes,
                )
            })
            .await
    };

    let elapsed = start.elapsed().as_secs_f64();

    log(format!(
        "Ran {} iterations and read {} memory bytes and {} disk bytes in {} seconds",
        iterations, mem_bytes, disk_bytes, elapsed
    ));

    let iterations_per_second = iterations as f64 / elapsed;
    let mem_bytes_per_second = mem_bytes as f64 / elapsed;
    let disk_bytes_per_second = disk_bytes as f64 / elapsed;
    println!(
        "0,{},{},{},{}",
        category, iterations_per_second, mem_bytes_per_second, disk_bytes_per_second
    );
}

async fn bench_lance(work_dir: &WorkDir, category: &str, version: LanceFileVersion) {
    setup_lance(work_dir, category, version).await;

    run_bench_lance(work_dir, category, version).await;
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let _guard = if ARGS.tracing {
        let (chrome_layer, guard) = ChromeLayerBuilder::new()
            .trace_style(tracing_chrome::TraceStyle::Async)
            .build();
        tracing_subscriber::registry()
            .with(chrome_layer)
            .with(LevelFilter::from_level(Level::DEBUG))
            .init();
        Some(guard)
    } else {
        env_logger::init();
        None
    };

    if !ARGS.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    } else {
        // We only print the CSV header in quiet mode
        println!("page_size_kb,category,iterations_per_second,mem_bytes_per_second,disk_bytes_per_second");
    }

    if ARGS.concurrency > ARGS.num_files {
        panic!("concurrency must be less than or equal to num_files");
    }

    let page_sizes = match (ARGS.format, ARGS.page_size_kb) {
        (FileFormat::Parquet, Some(page_size_kb)) => vec![page_size_kb],
        (FileFormat::Parquet, None) => vec![8, 16, 32, 64],
        _ => vec![0],
    };

    let workdir = WorkDir::new(&ARGS.workdir).await;

    for page_size in page_sizes {
        for category in get_test_categories() {
            match ARGS.format {
                FileFormat::Parquet => bench_parquet(&workdir, &category, page_size).await,
                FileFormat::Lance2_0 => todo!(),
                FileFormat::Lance2_1 => {
                    bench_lance(&workdir, &category, LanceFileVersion::V2_1).await
                }
            }
        }
    }
}
