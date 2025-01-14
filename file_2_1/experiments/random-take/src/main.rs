use core::str;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use arrow_buffer::ScalarBuffer;
use clap::Parser;
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    file::reader::ChunkReader,
};
use rand::seq::SliceRandom;
use random_take_bench::{
    log,
    osutil::drop_caches,
    parq::{
        io::{ObjectStoreFile, ReadAtFile, WorkDir},
        parquet_global_setup, work_file_path,
    },
    take::{take, TryClone},
    threading::TaskPool,
    DataTypeChoice, LOG_READS, SHOULD_LOG,
};

const ROWS_PER_CHUNK: usize = 1024 * 1024;

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
    #[arg(long, value_enum, default_value_t = DataTypeChoice::Int)]
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

    /// If quiet then only print the result
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// The number of concurrent takes to run (note that each take may have its own per-row-group parallelism)
    #[arg(long, default_value_t = 0)]
    concurrency: usize,

    /// URI of the working directory.  Must be file:// or s3://
    #[arg(short, long)]
    workdir: Option<String>,
}

struct RandomIndices {
    indices: ScalarBuffer<u64>,
    current_index: Mutex<usize>,
}

impl RandomIndices {
    fn new(rows_per_chunk: usize, num_chunks: usize) -> Self {
        let mut indices = (0..(rows_per_chunk * num_chunks) as u64).collect::<Vec<_>>();
        let mut rng = rand::thread_rng();
        indices.shuffle(&mut rng);
        Self {
            indices: indices.into(),
            current_index: Mutex::new(0),
        }
    }

    fn next(&self, take_size: usize) -> ScalarBuffer<u64> {
        let mut current_index = self.current_index.lock().unwrap();
        let mut start = *current_index;

        if take_size + start > self.indices.len() {
            start = 0;
            *current_index = take_size;
        } else {
            *current_index += take_size;
        };

        self.indices.slice(start, take_size)
    }
}

fn inner_parquet_setup_sync<T: ChunkReader>(args: &Args, file: T) -> Option<ArrowReaderMetadata> {
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
        let path = work_file_path(
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

fn parquet_random_take_sync<T: ChunkReader + TryClone + 'static>(
    files: &[T],
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
        let chunk_index = index / ROWS_PER_CHUNK as u64;
        let chunk_offset = (index % ROWS_PER_CHUNK as u64) as u32;
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

fn bench_parquet_one<T: ChunkReader + TryClone + 'static>(
    args: &Args,
    indices: &RandomIndices,
    files: &[T],
    metadata_lookup: &[Option<ArrowReaderMetadata>],
) -> Arc<TaskPool> {
    let task_pool = Arc::new(TaskPool::new());

    let args = args.clone();
    let indices = indices.next(args.take_size);
    parquet_random_take_sync(files, indices, task_pool.clone(), metadata_lookup, 0);

    task_pool
}

fn open_s3_files(work_dir: &WorkDir, num_chunks: usize, args: &Args) -> Vec<ObjectStoreFile> {
    (0..num_chunks)
        .map(|chunk_index| {
            let path = work_file_path(
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
            let path = work_file_path(
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

fn run_bench_parquet<T: ChunkReader + TryClone + 'static>(
    start: Instant,
    args: &Args,
    indices: &RandomIndices,
    files: &[T],
    metadata_lookup: &[Option<ArrowReaderMetadata>],
) -> f64 {
    if args.drop_caches {
        drop_caches();
    }

    log("Running benchmark");
    let mut iterations = 0;
    if args.duration_seconds == 0.0 {
        // Special case, run once
        bench_parquet_one(args, &indices, &files, &metadata_lookup).join();
        iterations += 1;
    } else {
        let mut parallelism = args.concurrency;
        if parallelism == 0 {
            parallelism = num_cpus::get();
        }
        let mut ongoing_takes = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            ongoing_takes.push(bench_parquet_one(args, &indices, &files, &metadata_lookup));
        }
        // Normal case, run for X seconds
        while start.elapsed().as_secs_f64() < args.duration_seconds {
            ongoing_takes.pop().unwrap().join();
            iterations += 1;
            ongoing_takes.push(bench_parquet_one(args, &indices, &files, &metadata_lookup));
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
    let rows_taken_per_second = rows_taken / start.elapsed().as_secs_f64();

    rows_taken_per_second
}

async fn bench_parquet(args: &Args, work_dir: &WorkDir) -> f64 {
    parquet_global_setup(
        args.row_group_size,
        args.page_size_kb,
        ROWS_PER_CHUNK,
        args.num_files,
        args.data_type,
        work_dir,
    )
    .await;

    log("Randomizing indices");
    let indices = RandomIndices::new(ROWS_PER_CHUNK, args.num_files);

    let start = Instant::now();

    log("Loading metadata");
    let metadata_lookup = parquet_setup_sync(args, work_dir);

    // Don't set LOG_READS until this point to avoid logging the setup
    if args.log_reads {
        LOG_READS.store(true, std::sync::atomic::Ordering::Release);
    }

    log("Opening files");
    if work_dir.is_s3() {
        let files = open_s3_files(work_dir, args.num_files, args);
        run_bench_parquet(start, args, &indices, &files, &metadata_lookup)
    } else {
        let files = open_local_files(work_dir, args.num_files, args);
        run_bench_parquet(start, args, &indices, &files, &metadata_lookup)
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    env_logger::init();

    if !args.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    }

    let workdir = args
        .workdir
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("file:///tmp")
        .to_string();

    let work_dir = WorkDir::new(&workdir);

    let rows_taken_per_second = bench_parquet(&args, &work_dir).await;

    log(format!(
        "Rows taken per second across {} seconds: {}",
        args.duration_seconds, rows_taken_per_second,
    ));
    if args.quiet {
        println!("{}", rows_taken_per_second);
    }
}
