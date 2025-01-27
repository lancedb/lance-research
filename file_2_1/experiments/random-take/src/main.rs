use core::str;
use std::{
    collections::VecDeque,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use arrow_array::UInt32Array;
use arrow_buffer::ScalarBuffer;
use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::FutureExt;
use lance_core::cache::FileMetadataCache;
use lance_encoding::{
    decoder::{DecoderPlugins, FilterExpression},
    EncodingsIo,
};
use lance_file::{
    v2::reader::{FileReader, FileReaderOptions},
    version::LanceFileVersion,
};
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    ReadBatchParams,
};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use random_take_bench::{
    lance::{lance_file_path, lance_global_setup},
    log,
    osutil::drop_caches,
    parq::{
        io::{FileLike, ObjectStoreFile, ReadAtFile, WorkDir},
        parq_file_path, parquet_global_setup,
    },
    take::{take, TryClone},
    threading::TaskPool,
    util::RandomIndices,
    DataTypeChoice, FileFormat, LOG_READS, SHOULD_LOG, TAKE_COUNTER,
};
use tracing::{instrument, Level};
use tracing_chrome::ChromeLayerBuilder;
use tracing_core::LevelFilter;
use tracing_subscriber::prelude::*;

#[derive(Debug)]
struct BlockingEncodingsIo {
    file: ReadAtFile,
}

impl BlockingEncodingsIo {
    fn new(file: ReadAtFile) -> Self {
        Self { file }
    }
}

struct DoOnPoll {
    file: ReadAtFile,
    ranges: Vec<std::ops::Range<u64>>,
}

impl std::future::Future for DoOnPoll {
    type Output = lance_core::Result<Vec<Bytes>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let rsp = self
            .ranges
            .iter()
            .map(|range| {
                let length = (range.end - range.start) as usize;
                let _span = tracing::info_span!("get_bytes").entered();
                if LOG_READS.load(std::sync::atomic::Ordering::Acquire) {
                    log(format!("Reading {} bytes", length));
                }
                let mut buf = BytesMut::with_capacity(length);
                unsafe {
                    buf.set_len(length);
                    let bytes_read = self.file.read_at(&mut buf, range.start).unwrap();
                    buf.set_len(bytes_read);
                }
                buf.into()
            })
            .collect();
        std::task::Poll::Ready(Ok(rsp))
    }
}

impl EncodingsIo for BlockingEncodingsIo {
    fn submit_request(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
        _priority: u64,
    ) -> futures::future::BoxFuture<'static, lance_core::Result<Vec<bytes::Bytes>>> {
        Box::pin(
            DoOnPoll {
                file: self.file.try_clone().unwrap(),
                ranges,
            }
            .boxed(),
        )
    }
}

#[derive(Parser, Clone, Debug)]
#[command(name="random-take", about="A benchmark for tabular file formats", version, long_about = None)]
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

    /// Which data_type to test with
    #[arg(long, value_enum, default_value_t = DataTypeChoice::Scalar)]
    data_type: DataTypeChoice,

    /// Whether or not metadata should be cached between takes
    #[arg(short, long, default_value_t = false)]
    cache_metadata: bool,

    /// If true, use the async reader
    #[arg(short, long, default_value_t = false)]
    r#async: bool,

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
    #[arg(short, long, default_value_t = 1024)]
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
    #[arg(short, long)]
    workdir: Option<String>,

    /// If true, enables tracing
    #[arg(long, default_value_t = false)]
    tracing: bool,
}

fn inner_parquet_setup_sync<T: FileLike>(args: &Args, file: T) -> Option<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index(true);
    let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
    let num_rows = metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .sum::<i64>();

    let offset_index = metadata.metadata().offset_index().unwrap();

    let col_0_num_pages = offset_index
        .iter()
        .map(|rg| rg[0].page_locations().len())
        .sum::<usize>();

    log(format!(
        "Parquet file with type {} has {} rows and {} pages across {} row groups",
        args.data_type,
        num_rows,
        col_0_num_pages,
        metadata.metadata().num_row_groups()
    ));

    let metadata = if args.cache_metadata {
        Some(metadata)
    } else {
        None
    };

    metadata
}

fn parquet_setup_sync(args: &Args, work_dir: &WorkDir) -> Vec<Option<ArrowReaderMetadata>> {
    let mut metadata_lookup = Vec::with_capacity(args.num_files);
    for chunk_index in 0..args.num_files {
        let path = parq_file_path(
            work_dir,
            args.row_group_size,
            args.page_size_kb,
            chunk_index,
            args.data_type,
        );
        if work_dir.is_s3() {
            let file = work_dir.s3_file(path);
            metadata_lookup.push(inner_parquet_setup_sync(args, file));
        } else {
            let file = work_dir.local_file(path);
            metadata_lookup.push(inner_parquet_setup_sync(args, file));
        }
    }
    metadata_lookup
}

async fn lance_setup(
    args: &Args,
    work_dir: &WorkDir,
    file_version: LanceFileVersion,
) -> Vec<Arc<FileReader>> {
    let mut files_lookup = Vec::with_capacity(args.num_files);
    let store = work_dir.lance_object_store();
    let cache = FileMetadataCache::new(usize::MAX);
    let scheduler_config = SchedulerConfig::max_bandwidth(&store);
    let scheduler = ScanScheduler::new(store, scheduler_config);
    for chunk_index in 0..args.num_files {
        let path = lance_file_path(work_dir, file_version, chunk_index, args.data_type);
        let file_scheduler = scheduler
            .open_file_with_priority(&path, chunk_index as u64)
            .await
            .unwrap();
        let options = FileReaderOptions::default();
        let metadata = Arc::new(
            FileReader::read_all_metadata(&file_scheduler)
                .await
                .unwrap(),
        );

        let file = work_dir.local_file(path.clone());
        let io = Arc::new(BlockingEncodingsIo::new(file));

        let reader = FileReader::try_open_with_file_metadata(
            io,
            path,
            None,
            Arc::<DecoderPlugins>::default(),
            metadata,
            &cache,
            options,
        )
        .await
        .unwrap();
        files_lookup.push(Arc::new(reader));
    }
    files_lookup
}

fn parquet_random_take_sync<T: FileLike>(
    files: &[T],
    rows_per_file: usize,
    indices: ScalarBuffer<u64>,
    task_pool: Arc<TaskPool>,
    metadata_lookup: &[Option<ArrowReaderMetadata>],
    col: u32,
) {
    let mut indices = indices.to_vec();
    indices.sort_unstable();

    let mut indices_for_file = Vec::with_capacity(indices.len());
    let mut current_chunk = 0;
    for index in indices {
        let chunk_index = index / rows_per_file as u64;
        let chunk_offset = (index % rows_per_file as u64) as u32;
        if chunk_index == current_chunk {
            indices_for_file.push(chunk_offset);
        } else {
            if !indices_for_file.is_empty() {
                let file = files[current_chunk as usize].try_clone().unwrap();
                let task_indices = indices_for_file.clone();
                let metadata = metadata_lookup[current_chunk as usize].clone();
                take(file, task_indices, col, true, metadata, task_pool.clone());
                indices_for_file.clear();
            }
            current_chunk = chunk_index;
            indices_for_file.push(chunk_offset);
        }
    }
    if !indices_for_file.is_empty() {
        let file = files[current_chunk as usize].try_clone().unwrap();
        let indices_for_file = indices_for_file.clone();
        let metadata = metadata_lookup[current_chunk as usize].clone();
        take(
            file,
            indices_for_file.into(),
            col,
            true,
            metadata,
            task_pool,
        );
    }
}

fn bench_parquet_one<T: FileLike>(
    args: &Args,
    indices: &RandomIndices,
    files: &[T],
    metadata_lookup: &[Option<ArrowReaderMetadata>],
    rt: Arc<tokio::runtime::Runtime>,
) -> Arc<TaskPool> {
    let task_pool = Arc::new(TaskPool::new(rt));

    let args = args.clone();
    let indices = indices.next(args.take_size);
    parquet_random_take_sync(
        files,
        args.rows_per_file,
        indices,
        task_pool.clone(),
        metadata_lookup,
        0,
    );

    task_pool
}

fn bench_lance_one(
    args: &Args,
    indices: &RandomIndices,
    readers: Arc<[Arc<FileReader>]>,
    rt: Arc<tokio::runtime::Runtime>,
) -> Arc<TaskPool> {
    let task_pool = Arc::new(TaskPool::new(rt));
    let indices = indices.next(args.take_size);

    #[instrument]
    fn file_take(reader: Arc<FileReader>, indices: Vec<u32>) {
        let batch_size = indices.len() as u32;
        let batches = reader
            .read_stream_projected_blocking(
                ReadBatchParams::Indices(UInt32Array::from(indices)),
                batch_size,
                None,
                FilterExpression::no_filter(),
            )
            .unwrap();
        for batch in batches {
            TAKE_COUNTER.fetch_add(batch.unwrap().num_rows(), Ordering::Release);
        }
    }

    let rows_per_file = args.rows_per_file;

    task_pool.spawn(move || {
        let _span = tracing::span!(tracing::Level::INFO, "lance_take").entered();
        let mut indices = indices.to_vec();
        indices.sort_unstable();

        let mut indices_for_file = Vec::with_capacity(indices.len());
        let mut current_chunk = 0;

        for index in indices {
            let chunk_index = index / rows_per_file as u64;
            let chunk_offset = (index % rows_per_file as u64) as u32;
            if chunk_index == current_chunk {
                indices_for_file.push(chunk_offset);
            } else {
                if !indices_for_file.is_empty() {
                    let reader = readers[current_chunk as usize].clone();
                    let task_indices = indices_for_file.clone();
                    file_take(reader, task_indices);
                    indices_for_file.clear();
                }
                current_chunk = chunk_index;
                indices_for_file.push(chunk_offset);
            }
        }
        if !indices_for_file.is_empty() {
            let reader = readers[current_chunk as usize].clone();
            file_take(reader, indices_for_file);
        }
    });

    task_pool
}

fn open_s3_files(work_dir: &WorkDir, num_chunks: usize, args: &Args) -> Vec<ObjectStoreFile> {
    (0..num_chunks)
        .map(|chunk_index| {
            let path = parq_file_path(
                work_dir,
                args.row_group_size,
                args.page_size_kb,
                chunk_index,
                args.data_type,
            );
            work_dir.s3_file(path)
        })
        .collect::<Vec<_>>()
}

fn open_local_files(work_dir: &WorkDir, num_chunks: usize, args: &Args) -> Vec<ReadAtFile> {
    (0..num_chunks)
        .map(|chunk_index| {
            let path = parq_file_path(
                work_dir,
                args.row_group_size,
                args.page_size_kb,
                chunk_index,
                args.data_type,
            );
            work_dir.local_file(path)
        })
        .collect::<Vec<_>>()
}

fn run_bench_parquet<T: FileLike>(
    args: &Args,
    indices: &RandomIndices,
    files: &[T],
    metadata_lookup: &[Option<ArrowReaderMetadata>],
) -> f64 {
    if args.drop_caches {
        drop_caches();
    }

    let rt = Arc::new(tokio::runtime::Builder::new_multi_thread().build().unwrap());

    let start = Instant::now();

    log("Running benchmark");
    let mut iterations = 0;
    if args.duration_seconds == 0.0 {
        // Special case, run once
        bench_parquet_one(args, &indices, &files, &metadata_lookup, rt.clone()).join();
        iterations += 1;
    } else {
        let mut parallelism = args.concurrency;
        if parallelism == 0 {
            parallelism = num_cpus::get();
        }
        let mut ongoing_takes = VecDeque::with_capacity(parallelism);
        for _ in 0..parallelism {
            ongoing_takes.push_back(bench_parquet_one(
                args,
                &indices,
                &files,
                &metadata_lookup,
                rt.clone(),
            ));
        }
        // Normal case, run for X seconds
        while start.elapsed().as_secs_f64() < args.duration_seconds {
            ongoing_takes.pop_front().unwrap().join();
            iterations += 1;
            ongoing_takes.push_back(bench_parquet_one(
                args,
                &indices,
                &files,
                &metadata_lookup,
                rt.clone(),
            ));
        }
        // Drain ongoing tasks (it's ok if we go a little past X seconds because we measure elapsed down below)
        for ongoing_take in ongoing_takes {
            ongoing_take.join();
            iterations += 1;
        }
    }

    log(format!("Iterations = {}", iterations));
    log(format!("Take size = {}", args.take_size));
    log(format!(
        "Duration = {} seconds",
        start.elapsed().as_secs_f64()
    ));
    let rows_taken = iterations as f64 * args.take_size as f64;
    assert_eq!(rows_taken as usize, TAKE_COUNTER.load(Ordering::Acquire));
    let rows_taken_per_second = rows_taken / start.elapsed().as_secs_f64();

    Arc::into_inner(rt).unwrap().shutdown_background();

    rows_taken_per_second
}

async fn run_bench_lance(
    args: &Args,
    indices: &RandomIndices,
    readers: Arc<[Arc<FileReader>]>,
) -> f64 {
    if args.drop_caches {
        drop_caches();
    }

    let rt = Arc::new(tokio::runtime::Builder::new_multi_thread().build().unwrap());

    let start = Instant::now();

    log("Running benchmark");
    let iterations = if args.duration_seconds == 0.0 {
        // Special case, run once
        bench_lance_one(args, &indices, readers, rt.clone()).join();
        1
    } else {
        let mut parallelism = args.concurrency;
        if parallelism == 0 {
            parallelism = num_cpus::get();
        }

        let mut iterations = 0;
        let mut ongoing_takes = VecDeque::with_capacity(parallelism);
        for _ in 0..parallelism {
            ongoing_takes.push_front(bench_lance_one(args, &indices, readers.clone(), rt.clone()));
        }
        // Normal case, run for X seconds
        while start.elapsed().as_secs_f64() < args.duration_seconds {
            ongoing_takes.pop_back().unwrap().join();
            iterations += 1;
            ongoing_takes.push_front(bench_lance_one(args, &indices, readers.clone(), rt.clone()));
        }
        // Drain ongoing tasks (it's ok if we go a little past X seconds because we measure elapsed down below)
        for ongoing_take in ongoing_takes {
            ongoing_take.join();
            iterations += 1;
        }
        iterations
    };

    log(format!("Iterations = {}", iterations));
    log(format!("Take size = {}", args.take_size));
    log(format!(
        "Duration = {} seconds",
        start.elapsed().as_secs_f64()
    ));
    let rows_taken = iterations as f64 * args.take_size as f64;
    assert_eq!(rows_taken as usize, TAKE_COUNTER.load(Ordering::Acquire));
    let rows_taken_per_second = rows_taken / start.elapsed().as_secs_f64();

    Arc::into_inner(rt).unwrap().shutdown_background();

    rows_taken_per_second
}

async fn bench_parquet(args: &Args, work_dir: &WorkDir) -> f64 {
    parquet_global_setup(
        args.row_group_size,
        args.page_size_kb,
        args.rows_per_file,
        args.num_files,
        args.data_type,
        work_dir,
    )
    .await;

    log("Randomizing indices");
    let indices = RandomIndices::new(args.rows_per_file, args.num_files).await;

    log("Loading metadata");
    let metadata_lookup = parquet_setup_sync(args, work_dir);

    // Don't set LOG_READS until this point to avoid logging the setup
    if args.log_reads {
        LOG_READS.store(true, std::sync::atomic::Ordering::Release);
    }

    log("Opening files");
    if work_dir.is_s3() {
        let files = open_s3_files(work_dir, args.num_files, args);
        run_bench_parquet(args, &indices, &files, &metadata_lookup)
    } else {
        let files = open_local_files(work_dir, args.num_files, args);
        run_bench_parquet(args, &indices, &files, &metadata_lookup)
    }
}

async fn bench_lance(args: &Args, work_dir: &WorkDir, file_version: LanceFileVersion) -> f64 {
    lance_global_setup(
        args.rows_per_file,
        args.num_files,
        args.data_type,
        work_dir,
        file_version,
    )
    .await;

    log("Randomizing indices");
    let indices = RandomIndices::new(args.rows_per_file, args.num_files).await;

    log("Loading file readers");
    let readers_lookup = lance_setup(args, work_dir, file_version).await;

    // Don't set LOG_READS until this point to avoid logging the setup
    if args.log_reads {
        LOG_READS.store(true, std::sync::atomic::Ordering::Release);
    }

    run_bench_lance(args, &indices, readers_lookup.into()).await
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let _guard = if args.tracing {
        let (chrome_layer, guard) = ChromeLayerBuilder::new().build();
        tracing_subscriber::registry()
            .with(chrome_layer)
            .with(LevelFilter::from_level(Level::DEBUG))
            .init();
        Some(guard)
    } else {
        None
    };

    if !args.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    }

    let workdir = args
        .workdir
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("file:///tmp")
        .to_string();

    let work_dir = WorkDir::new(&workdir).await;

    let rows_taken_per_second = match args.format {
        FileFormat::Parquet => bench_parquet(&args, &work_dir).await,
        FileFormat::Lance2_0 => bench_lance(&args, &work_dir, LanceFileVersion::V2_0).await,
        FileFormat::Lance2_1 => bench_lance(&args, &work_dir, LanceFileVersion::V2_1).await,
    };

    log(format!(
        "Rows taken per second across {} seconds: {}",
        args.duration_seconds, rows_taken_per_second,
    ));
    if args.quiet {
        println!("{}", rows_taken_per_second);
    }
}
