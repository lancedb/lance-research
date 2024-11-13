use std::{sync::Arc, time::Instant};

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Field, Schema};
use arrow_select::concat::concat_batches;
use clap::Parser;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use lance_core::cache::FileMetadataCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::{
    v2::{
        reader::{FileReader, FileReaderOptions, ReaderProjection},
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
    arrow::{
        arrow_writer::ArrowWriterOptions, async_reader::AsyncFileReader, AsyncArrowWriter,
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
    file::{
        metadata::ParquetMetaDataReader,
        page_index::index::Index,
        properties::{EnabledStatistics, WriterProperties},
    },
};
use random_take_bench::{
    log, osutil::drop_path_from_cache, parq::io::WorkDir, FileFormat, LOG_READS, SHOULD_LOG,
};
use tracing::Level;
use tracing_chrome::ChromeLayerBuilder;
use tracing_core::LevelFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser, Clone, Debug)]
#[command(name="full-scan", about="A benchmark for tabular file formats", version, long_about = None)]
struct Args {
    /// How many rows to put in a row group (parquet only)
    #[arg(long)]
    row_group_size: Option<usize>,

    /// Page size (in bytes, parquet only)
    #[arg(long)]
    page_size_kb: Option<usize>,

    /// Number of seconds to run the benchmark
    #[arg(long, default_value_t = 10.0)]
    duration_seconds: f64,

    /// If true, log each read operation
    #[arg(long, default_value_t = false)]
    log_reads: bool,

    /// Number of files to read from
    #[arg(long, default_value_t = 32)]
    num_files: usize,

    /// Keep the cache (don't drop)
    #[arg(long, default_value_t = false)]
    keep_cache: bool,

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
        } else if filename.ends_with("_trimmed.lance") {
            let category = filename[..filename.len() - 14].to_string();
            categories.push(category);
        }
    }
    categories
}

fn pq_src_path(category: &str) -> String {
    format!("{}/{}_trimmed.parquet", ARGS.testdir, category)
}

fn lance_src_path(category: &str) -> String {
    format!(
        "{}/{}_trimmed.lance",
        std::fs::canonicalize(&ARGS.testdir)
            .unwrap()
            .to_string_lossy(),
        category
    )
}

fn test_pq_file_path(
    category: &str,
    file_index: usize,
    row_group_size: usize,
    page_size_kb: usize,
) -> String {
    format!(
        "{}_rg_{}_ps_{}_chunk_{}.parquet",
        category, row_group_size, page_size_kb, file_index
    )
}

async fn read_pq_test_data(path: String) -> RecordBatch {
    let file = tokio::fs::File::open(path).await.unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap();

    // Create a file with two columns.  In parquet, if there is a single column, then parquet reads
    // end up being one long stream of sequential reads.  This is great for performance but unrealistic
    // to expect a single column in real world workloads.  To simulate a real world scenario we create
    // two columns.
    let batches = reader.try_collect::<Vec<_>>().await.unwrap();
    let schema = batches[0].schema().clone();
    let combined = concat_batches(&schema, batches.iter()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "value",
            schema.field(0).data_type().clone(),
            schema.field(0).is_nullable(),
        ),
        Field::new(
            "shadow",
            schema.field(0).data_type().clone(),
            schema.field(0).is_nullable(),
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![combined.column(0).clone(), combined.column(0).clone()],
    )
    .unwrap()
}

async fn read_lance_test_data(path: String) -> RecordBatch {
    let fs = lance_io::object_store::ObjectStore::local();
    let config = SchedulerConfig::max_bandwidth(&fs);
    let scan_scheduler = ScanScheduler::new(Arc::new(fs), config);
    let file_scheduler = scan_scheduler
        .open_file(&object_store::path::Path::parse(path).unwrap())
        .await
        .unwrap();
    let reader = lance_file::v2::reader::FileReader::try_open(
        file_scheduler,
        None,
        Arc::default(),
        &FileMetadataCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();
    let num_rows = reader.num_rows();
    let mut batches = reader
        .read_stream(
            ReadBatchParams::RangeFull,
            num_rows as u32,
            1,
            FilterExpression::no_filter(),
        )
        .unwrap();

    batches.try_next().await.unwrap().unwrap()
}

async fn read_test_data(category: &str) -> RecordBatch {
    let pq_src_path = pq_src_path(category);
    if std::path::Path::new(&pq_src_path).exists() {
        read_pq_test_data(pq_src_path).await
    } else {
        read_lance_test_data(lance_src_path(category)).await
    }
}

fn get_parquet_write_batch_size(page_size_kb: usize, array: &dyn Array) -> usize {
    let page_size = page_size_kb * 1024;
    let array_size = array.get_array_memory_size();
    let estimated_elem_size = array_size / array.len() as usize;
    (page_size / (estimated_elem_size * 2)).max(1) as usize
}

async fn write_pq_test_file(
    workdir: &WorkDir,
    category: &str,
    dest_path: Path,
    page_size_kb: usize,
    row_group_size: usize,
) {
    let test_data = read_test_data(category).await;
    let writer = workdir.writer(dest_path);
    let write_batch_size = get_parquet_write_batch_size(page_size_kb, test_data.column(0));
    let mut properties_builder = WriterProperties::builder()
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(row_group_size)
        .set_compression(parquet::basic::Compression::SNAPPY);
    if page_size_kb != 0 {
        properties_builder = properties_builder
            .set_data_page_size_limit(page_size_kb * 1024)
            .set_write_batch_size(write_batch_size);
    }
    let options = ArrowWriterOptions::default().with_properties(properties_builder.build());
    let mut writer =
        AsyncArrowWriter::try_new_with_options(writer, test_data.schema(), options).unwrap();

    writer.write(&test_data).await.unwrap();

    writer.finish().await.unwrap();
}

async fn setup_parquet(
    workdir: &WorkDir,
    category: &str,
    page_size_kb: usize,
    row_group_size: usize,
) {
    log(format!(
        "Setting up parquet test files for category {}",
        category
    ));
    let base_path = workdir.child_path("parquets");
    workdir.clean(&base_path).await;
    for file_index in 0..ARGS.num_files {
        let dest_path = workdir.child_path("parquets").child(test_pq_file_path(
            &category,
            file_index,
            row_group_size,
            page_size_kb,
        ));
        if workdir.exists(&dest_path).await {
            log(format!("Using existing parquet test file at {}", dest_path));
        } else {
            log(format!(
                "Creating new parquet test file at {} from {}",
                dest_path, category
            ));
            if file_index == 0 {
                write_pq_test_file(workdir, &category, dest_path, page_size_kb, row_group_size)
                    .await;
            } else {
                let base_file = workdir.child_path("parquets").child(test_pq_file_path(
                    &category,
                    0,
                    row_group_size,
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

struct LoggingReader<T: AsyncFileReader>(T);

impl<T: AsyncFileReader> AsyncFileReader for LoggingReader<T> {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        if LOG_READS.load(std::sync::atomic::Ordering::Acquire) {
            log(format!("Reading bytes {}-{}", range.start, range.end));
        }
        self.0.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<usize>>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        if LOG_READS.load(std::sync::atomic::Ordering::Acquire) {
            log(format!("Reading byte ranges {:?}", ranges));
        }
        self.0.get_byte_ranges(ranges)
    }

    fn get_metadata(
        &mut self,
    ) -> futures::future::BoxFuture<
        '_,
        parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        self.0.get_metadata()
    }
}

async fn bench_parquet_one(path: Path, batch_size: Option<usize>) -> (usize, usize) {
    let path = format!("/{}", path);
    if !ARGS.keep_cache {
        drop_path_from_cache(&path);
    }
    let f = tokio::fs::File::open(path).await.unwrap();
    // We're only reading one column so we can't consider the entire file.
    // Dividing the file size by 2 isn't exactly correct, but it's a good enough estimate
    let disk_bytes = f.metadata().await.unwrap().len() as usize / 2;
    let mut stream_builder = ParquetRecordBatchStreamBuilder::new(LoggingReader(f))
        .await
        .unwrap();
    let parquet_schema = stream_builder.parquet_schema();
    let projection = ProjectionMask::roots(parquet_schema, [0]);
    stream_builder = stream_builder.with_projection(projection);
    if let Some(batch_size) = batch_size {
        stream_builder = stream_builder.with_batch_size(batch_size);
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
fn get_batch_size(category: &str) -> Option<usize> {
    match category {
        "embeddings" | "code" => Some(1024),
        "websites" => Some(128),
        "images" => Some(32),
        "dates" => Some(2 * 1024 * 1024),
        "names" => Some(2 * 1024 * 1024),
        _ => None,
    }
}

async fn run_bench_parquet(
    work_dir: &WorkDir,
    category: &str,
    page_size_kb: usize,
    row_group_size: usize,
) {
    log(format!("Benchmarking parquet for category {}", category));
    let start = Instant::now();

    let concurrency = if ARGS.concurrency == 0 {
        num_cpus::get()
    } else {
        ARGS.concurrency
    };

    let batch_size = get_batch_size(category);

    let (iterations, one_disk_bytes, all_disk_bytes, mem_bytes) = if ARGS.duration_seconds == 0.0 {
        let path = test_pq_file_path(category, 0, row_group_size, page_size_kb);
        let path = work_dir.child_path("parquets").child(path);

        // Debugging information, print the # of pages to confirm the file was written correctly
        {
            let path_str = format!("/{}", path);
            let mut f = tokio::fs::File::open(path_str).await.unwrap();

            let disk_bytes = f.metadata().await.unwrap().len() as usize;
            let reader = ParquetMetaDataReader::new().with_page_indexes(true);
            let metadata = reader.load_and_finish(&mut f, disk_bytes).await.unwrap();
            let index = metadata.column_index().unwrap();

            let mut num_pages = 0;
            let mut num_row_groups = 0;
            for rg_idx in index {
                num_row_groups += 1;
                for col_idx in rg_idx {
                    num_pages += match col_idx {
                        Index::BOOLEAN(b) => b.indexes.len(),
                        Index::BYTE_ARRAY(b) => b.indexes.len(),
                        Index::DOUBLE(b) => b.indexes.len(),
                        Index::FIXED_LEN_BYTE_ARRAY(b) => b.indexes.len(),
                        Index::FLOAT(b) => b.indexes.len(),
                        Index::INT32(b) => b.indexes.len(),
                        Index::INT64(b) => b.indexes.len(),
                        Index::INT96(b) => b.indexes.len(),
                        Index::NONE => 0,
                    };
                }
            }
            log(format!(
                "There are {} pages and {} row groups",
                num_pages, num_row_groups
            ));
        }

        let (disk_bytes, mem_bytes) = bench_parquet_one(path, batch_size).await;
        (1, disk_bytes, disk_bytes, mem_bytes)
    } else {
        let task_iter = (0..ARGS.num_files)
            .cycle()
            .take_while(|_| start.elapsed().as_secs_f64() < ARGS.duration_seconds)
            .map(|file_index| {
                let path = test_pq_file_path(category, file_index, row_group_size, page_size_kb);
                let path = work_dir.child_path("parquets").child(path);
                tokio::task::spawn(bench_parquet_one(path, batch_size))
            })
            .map(|task| task.map(|res| res.unwrap()));

        let tasks = stream::iter(task_iter).buffer_unordered(concurrency);

        tasks
            .fold((0, 0, 0, 0), |acc, (disk_bytes, mem_bytes)| async move {
                let (iterations, _, acc_disk_bytes, acc_mem_bytes) = acc;
                (
                    iterations + 1,
                    disk_bytes,
                    disk_bytes + acc_disk_bytes,
                    mem_bytes + acc_mem_bytes,
                )
            })
            .await
    };

    let elapsed = start.elapsed().as_secs_f64();

    log(format!(
        "Ran {} iterations and read {} memory bytes and {} disk bytes in {} seconds",
        iterations, mem_bytes, all_disk_bytes, elapsed
    ));

    let iterations_per_second = iterations as f64 / elapsed;
    let mem_bytes_per_second = mem_bytes as f64 / elapsed;
    let disk_bytes_per_second = all_disk_bytes as f64 / elapsed;
    println!(
        "{},{},{},{},{},{},{}",
        page_size_kb,
        row_group_size,
        category,
        iterations_per_second,
        mem_bytes_per_second,
        disk_bytes_per_second,
        one_disk_bytes,
    );
}

async fn bench_parquet(
    work_dir: &WorkDir,
    category: &str,
    page_size_kb: usize,
    row_group_size: usize,
) {
    setup_parquet(work_dir, category, page_size_kb, row_group_size).await;

    run_bench_parquet(work_dir, category, page_size_kb, row_group_size).await;
}

fn test_lance_file_path(category: &str, file_index: usize) -> String {
    format!("{}_chunk_{}.lance", category, file_index)
}

async fn write_lance_test_file(
    workdir: &WorkDir,
    dest_path: Path,
    file_version: LanceFileVersion,
    category: &str,
) {
    let mut test_data = read_test_data(category).await;

    let obj_writer = workdir.lance_writer(dest_path).await;

    let mut writer = FileWriter::new_lazy(
        obj_writer,
        FileWriterOptions {
            format_version: Some(file_version),
            ..Default::default()
        },
    );

    // We slice manually here since auto-slicing of large batches into smaller pages isn't yet implemented in Lance
    let batch_size = get_batch_size(category).unwrap_or(32 * 1024);

    while test_data.num_rows() > 0 {
        let this_batch_size = batch_size.min(test_data.num_rows());
        let batch = test_data.slice(0, this_batch_size);
        writer.write_batch(&batch).await.unwrap();
        test_data = test_data.slice(this_batch_size, test_data.num_rows() - this_batch_size);
    }

    writer.finish().await.unwrap();
}

async fn setup_lance(workdir: &WorkDir, category: &str, version: LanceFileVersion) {
    log(format!(
        "Setting up lance test files for category {}",
        category
    ));
    let base_path = workdir.child_path(&format!("lances_{}", version));
    workdir.clean(&base_path).await;
    for file_index in 0..ARGS.num_files {
        let dest_path = workdir
            .child_path(&format!("lances_{}", version))
            .child(test_lance_file_path(&category, file_index));
        if workdir.exists(&dest_path).await {
            log(format!("Using existing lance test file at {}", dest_path));
        } else {
            log(format!(
                "Creating new lance test file at {} from {}",
                dest_path, category
            ));
            if file_index == 0 {
                write_lance_test_file(workdir, dest_path, version, category).await;
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
    batch_size: Option<usize>,
) -> (usize, usize) {
    if !ARGS.keep_cache {
        drop_path_from_cache(&format!("/{}", path));
    }

    let file_scheduler = scheduler
        .open_file_with_priority(&path, task_index as u64)
        .await
        .unwrap();

    let batch_size = batch_size.unwrap_or(32 * 1024) as u32;

    let disk_bytes = file_scheduler.reader().size().await.unwrap() / 2;

    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &FileMetadataCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    //let projection =
    //    ReaderProjection::from_column_names(reader.schema().as_ref(), &["value"]).unwrap();
    let projection = ReaderProjection {
        column_indices: vec![0],
        schema: reader.schema().clone(),
    };

    let stream = reader
        .read_stream_projected(
            ReadBatchParams::RangeFull,
            batch_size,
            8,
            projection,
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

    let batch_size = get_batch_size(category);

    let store = work_dir.lance_object_store();
    let scheduler_config = SchedulerConfig::max_bandwidth(&store);
    let scheduler = ScanScheduler::new(store, scheduler_config);

    let (iterations, one_disk_bytes, all_disk_bytes, mem_bytes) = if ARGS.duration_seconds == 0.0 {
        let path = test_lance_file_path(category, 0);
        let path = work_dir
            .child_path(&format!("lances_{}", version))
            .child(path);

        let (disk_bytes, mem_bytes) = bench_lance_one(scheduler, 0, path, batch_size).await;
        (1, disk_bytes, disk_bytes, mem_bytes)
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
                    batch_size,
                ))
            })
            .map(|task| task.map(|res| res.unwrap()));

        let tasks = stream::iter(task_iter).buffer_unordered(concurrency);

        tasks
            .fold((0, 0, 0, 0), |acc, (disk_bytes, mem_bytes)| async move {
                let (iterations, _, acc_disk_bytes, acc_mem_bytes) = acc;
                (
                    iterations + 1,
                    disk_bytes,
                    disk_bytes + acc_disk_bytes,
                    mem_bytes + acc_mem_bytes,
                )
            })
            .await
    };

    let elapsed = start.elapsed().as_secs_f64();

    log(format!(
        "Ran {} iterations and read {} memory bytes and {} disk bytes in {} seconds",
        iterations, mem_bytes, all_disk_bytes, elapsed
    ));

    let iterations_per_second = iterations as f64 / elapsed;
    let mem_bytes_per_second = mem_bytes as f64 / elapsed;
    let disk_bytes_per_second = all_disk_bytes as f64 / elapsed;
    println!(
        "0,0,{},{},{},{},{}",
        category,
        iterations_per_second,
        mem_bytes_per_second,
        disk_bytes_per_second,
        one_disk_bytes,
    );
}

async fn bench_lance(work_dir: &WorkDir, category: &str, version: LanceFileVersion) {
    setup_lance(work_dir, category, version).await;

    run_bench_lance(work_dir, category, version).await;
}

fn get_row_group_sizes(_category: &str) -> Vec<usize> {
    vec![1024, 1024 * 10, 1024 * 100, 1024 * 1024]
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
        println!("page_size_kb,row_group_size,category,iterations_per_second,mem_bytes_per_second,disk_bytes_per_second,file_size_bytes");
    }

    if ARGS.log_reads {
        LOG_READS.store(true, std::sync::atomic::Ordering::Release);
    }

    if ARGS.concurrency > ARGS.num_files {
        panic!("concurrency must be less than or equal to num_files");
    }

    let page_sizes = match (ARGS.format, ARGS.page_size_kb) {
        (FileFormat::Parquet, Some(page_size_kb)) => vec![page_size_kb],
        (FileFormat::Parquet, None) => vec![8, 32, 64, 256, 0],
        _ => vec![0],
    };

    let workdir = WorkDir::new(&ARGS.workdir).await;

    for category in get_test_categories() {
        let row_group_sizes = match (ARGS.format, ARGS.row_group_size) {
            (FileFormat::Parquet, Some(row_group_size)) => vec![row_group_size],
            (FileFormat::Parquet, None) => get_row_group_sizes(&category),
            _ => vec![0],
        };

        for page_size in &page_sizes {
            for row_group_size in &row_group_sizes {
                // We end up triggering 'Parquet does not support more than 32767 row groups per file'
                if *row_group_size == 1024 && category == "dates" {
                    continue;
                }
                let _ = std::fs::remove_dir_all("/tmp/parquets");
                let _ = std::fs::remove_dir_all("/tmp/lances_2.1");
                match ARGS.format {
                    FileFormat::Parquet => {
                        bench_parquet(&workdir, &category, *page_size, *row_group_size).await
                    }
                    FileFormat::Lance2_0 => todo!(),
                    FileFormat::Lance2_1 => {
                        bench_lance(&workdir, &category, LanceFileVersion::V2_1).await
                    }
                }
            }
        }
    }
}
