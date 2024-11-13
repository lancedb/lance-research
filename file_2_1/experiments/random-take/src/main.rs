use core::str;
use std::{
    fs::File,
    io::Read,
    process::Command,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, SystemTime},
};

use arrow_array::RecordBatchReader;
use arrow_schema::{DataType, Field};
use bytes::{Buf, BytesMut};
use clap::{Parser, ValueEnum};
use futures::{FutureExt, StreamExt};
use lance_core::cache::{CapacityMode, FileMetadataCache};
use lance_datagen::{BatchCount, RowCount};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::{
    v2::{
        reader::{CachedFileMetadata, FileReader, FileReaderOptions, ReaderProjection},
        writer::FileWriterOptions,
    },
    version::LanceFileVersion,
};
use lance_io::{
    object_store::ObjectStoreExt,
    scheduler::{ScanScheduler, SchedulerConfig},
    ReadBatchParams,
};
use object_store::{
    aws::AmazonS3Builder, buffered::BufWriter, local::LocalFileSystem, path::Path, ObjectStore,
};
use once_cell::sync::Lazy;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::AsyncFileReader,
        AsyncArrowWriter,
    },
    basic::Compression,
    file::{
        properties::WriterProperties,
        reader::{ChunkReader, Length},
    },
};
use rand::seq::SliceRandom;
use random_take_bench::r#async::{take as async_take, TryClone as AsyncTryClone};
use random_take_bench::sync::{scan as sync_scan, take as sync_take, TryClone as SyncTryClone};

const WRITE_ROW_COUNT: u64 = 10 * 1024 * 1024;
const WRITE_BATCH_SIZE: u64 = 64 * 1024;
const WRITE_BATCH_COUNT: u32 = (WRITE_ROW_COUNT / WRITE_BATCH_SIZE) as u32;

/// Simple program to greet a person
#[derive(Parser, Clone, Debug)]
#[command(name="random-take", about="A benchmark for tabular file formats", version, long_about = None)]
struct Args {
    /// What file format to benchmark
    #[arg(value_enum)]
    format: Format,

    #[arg(value_enum)]
    benchmark: Benchmark,

    /// How many rows to put in a row group (parquet only)
    #[arg(short, long, default_value_t = 100000)]
    row_group_size: u32,

    /// Page size (in bytes, parquet only)
    #[arg(short, long, default_value_t = 1024)]
    page_size_kb: u32,

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

    /// Number of iteration to run.  The result will be the average across these iterations
    #[arg(short, long, default_value_t = 1)]
    iterations: u32,

    #[arg(long, default_value_t = 1)]
    num_threads: u32,

    /// If true, drop the OS cache before each iteration
    #[arg(short, long, default_value_t = false)]
    drop_caches: bool,

    /// If true, log each read operation
    #[arg(long, default_value_t = false)]
    log_reads: bool,

    #[arg(long)]
    storage_version: LanceFileVersion,

    /// If quiet then only print the result
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// URI of the working directory.  Must be file:// or s3://
    #[arg(short, long)]
    workdir: Option<String>,
}

const DOUBLE_NAME: &[&str] = &["double"];
const VECTOR_NAME: &[&str] = &["vector"];

impl Args {
    fn column_index(&self) -> u32 {
        match self.column_type {
            ColumnType::Double => 0,
            ColumnType::Vector => 1,
        }
    }

    fn column_names(&self) -> &[&str] {
        match self.column_type {
            ColumnType::Double => DOUBLE_NAME,
            ColumnType::Vector => VECTOR_NAME,
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
enum Benchmark {
    Scan,
    Take,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ColumnType {
    /// A column of fp64 values
    Double,
    /// A column where each row is a vector of 768 fp32 values
    Vector,
}

#[derive(Clone, Debug)]
struct WorkDir {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    is_s3: bool,
}

static RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| tokio::runtime::Runtime::new().unwrap());
static LOG_READS: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

fn drop_caches() {
    Command::new("sync").output().unwrap();
    let out = Command::new("sudo")
        .arg("/sbin/sysctl")
        .arg("vm.drop_caches=3")
        .output()
        .unwrap();
    if !out.status.success() {
        panic!(
            "Failed to drop caches: {}",
            str::from_utf8(&out.stderr).unwrap()
        );
    }
}

struct ObjectStoreFile {
    object_store: Arc<dyn ObjectStore>,
    location: Path,
}

impl Length for ObjectStoreFile {
    fn len(&self) -> u64 {
        RT.block_on(self.object_store.head(&self.location))
            .unwrap()
            .size as u64
    }
}

struct ObjectStoreReader {
    object_store: Arc<dyn ObjectStore>,
    location: Path,
    offset: u64,
}

impl Read for ObjectStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let range = (self.offset as usize)..(self.offset as usize + buf.len());
        let mut bytes = RT
            .block_on(self.object_store.get_range(&self.location, range))
            .unwrap();
        bytes.copy_to_slice(buf);
        Ok(buf.len())
    }
}

impl ChunkReader for ObjectStoreFile {
    type T = ObjectStoreReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(ObjectStoreReader {
            object_store: self.object_store.clone(),
            location: self.location.clone(),
            offset: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let range = (start as usize)..(start as usize + length);
        Ok(RT
            .block_on(self.object_store.get_range(&self.location, range))
            .unwrap())
    }
}

impl SyncTryClone for ObjectStoreFile {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            object_store: self.object_store.clone(),
            location: self.location.clone(),
        })
    }
}

impl WorkDir {
    fn new(workdir: &str) -> Self {
        if workdir.starts_with("file://") {
            let path = Path::parse(workdir[7..].to_string()).unwrap();
            let object_store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
            log(format!("Using local filesystem at {}", path));
            Self {
                path,
                object_store,
                is_s3: false,
            }
        } else if workdir.starts_with("s3://") {
            let path = Path::parse(workdir[5..].to_string()).unwrap();
            let bucket = path.parts().next();
            if let Some(bucket) = bucket {
                let object_store = Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(bucket.as_ref().to_string())
                        .build()
                        .unwrap(),
                ) as Arc<dyn ObjectStore>;
                log(format!(
                    "Using S3 with bucket {} and path {}",
                    bucket.as_ref(),
                    path
                ));
                Self {
                    path,
                    object_store,
                    is_s3: true,
                }
            } else {
                panic!("The workdir did not contain a bucket name");
            }
        } else {
            panic!("workdir argument must start with file:// or s3://")
        }
    }

    fn file(&self, path: Path) -> impl ChunkReader + SyncTryClone {
        ObjectStoreFile {
            location: path,
            object_store: self.object_store.clone(),
        }
    }
}

fn parquet_path(work_dir: &WorkDir, row_group_size: u32, page_size: u32) -> Path {
    work_dir.path.child(format!(
        "input_rgs_{}_ps_{}.parquet",
        row_group_size, page_size
    ))
}

static SHOULD_LOG: AtomicBool = AtomicBool::new(false);
fn log(msg: impl AsRef<str>) {
    if SHOULD_LOG.load(std::sync::atomic::Ordering::Acquire) {
        println!("{}", msg.as_ref());
    }
}

fn get_parquet_write_batch_size(args: &Args) -> usize {
    let page_size = args.page_size_kb * 1024;
    let elem_size = match args.column_type {
        ColumnType::Double => 8,
        ColumnType::Vector => 768 * 4,
    };
    (page_size / elem_size).max(1) as usize
}

fn get_datagen() -> impl RecordBatchReader {
    lance_datagen::gen()
        .col(
            "double",
            lance_datagen::array::rand_type(&DataType::Float64),
        )
        .col(
            "vector",
            lance_datagen::array::rand_type(&DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                768,
            )),
        )
        .into_reader_rows(
            RowCount::from(WRITE_BATCH_SIZE),
            BatchCount::from(WRITE_BATCH_COUNT),
        )
}

async fn parquet_global_setup(args: &Args, work_dir: &WorkDir) {
    let dest_path = parquet_path(work_dir, args.row_group_size, args.page_size_kb);
    if work_dir.object_store.exists(&dest_path).await.unwrap() {
        log(format!("Using existing parquet test file at {}", dest_path));
        return;
    }

    log(format!("Creating new parquet test file at {} with {} row groups and {}kb per page and data_page_size_limit={}", dest_path, args.row_group_size, args.page_size_kb, get_parquet_write_batch_size(args)));

    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_size(args.row_group_size as usize)
        .set_data_page_size_limit(args.page_size_kb as usize * 1024)
        .set_write_batch_size(get_parquet_write_batch_size(args))
        .build();

    let datagen = get_datagen();

    let bufwriter = BufWriter::new(work_dir.object_store.clone(), dest_path);
    let mut writer = AsyncArrowWriter::try_new(bufwriter, datagen.schema(), Some(props)).unwrap();
    for batch in datagen {
        writer.write(&batch.unwrap()).await.unwrap();
    }
    writer.close().await.unwrap();
}

fn random_indices(args: &Args) -> Vec<u32> {
    let mut rng = rand::thread_rng();
    let mut indices = (0..WRITE_ROW_COUNT as u32).collect::<Vec<u32>>();
    let (permuted, _) = indices.partial_shuffle(&mut rng, args.take_size as usize);
    let mut indices = Vec::from(permuted);
    indices.sort();
    indices
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

    let col_0_num_pages = offset_index.iter().map(|rg| rg[0].len()).sum::<usize>();
    let col_1_num_pages = offset_index.iter().map(|rg| rg[1].len()).sum::<usize>();

    log(format!(
        "Parquet file has {} rows and {} (double) pages and {} (vector) pages",
        num_rows, col_0_num_pages, col_1_num_pages
    ));

    let metadata = if args.cache_metadata {
        Some(metadata)
    } else {
        None
    };

    metadata
}

fn parquet_setup_sync(args: &Args, work_dir: &WorkDir) -> Option<ArrowReaderMetadata> {
    if work_dir.is_s3 {
        let file = work_dir.file(parquet_path(
            work_dir,
            args.row_group_size,
            args.page_size_kb,
        ));
        inner_parquet_setup_sync(args, file)
    } else {
        // Evict file from OS cache
        let path_str = format!(
            "/{}",
            parquet_path(work_dir, args.row_group_size, args.page_size_kb)
        );
        let path = std::path::Path::new(&path_str);
        let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
        inner_parquet_setup_sync(args, file)
    }
}

async fn parquet_setup_async(
    args: &Args,
    work_dir: &WorkDir,
) -> (tokio::fs::File, Vec<u32>, Option<ArrowReaderMetadata>) {
    if work_dir.is_s3 {
        todo!()
    }
    let path_str = format!(
        "/{}",
        parquet_path(work_dir, args.row_group_size, args.page_size_kb)
    );

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

struct ReadAtReader {
    target: File,
    cursor: u64,
}

impl Read for ReadAtReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let offset = self.cursor;
        let bytes_read = std::os::unix::fs::FileExt::read_at(&self.target, buf, offset)?;
        self.cursor += bytes_read as u64;
        Ok(bytes_read)
    }
}

struct ReadAtFile(File);

impl Length for ReadAtFile {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

impl ChunkReader for ReadAtFile {
    type T = ReadAtReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(ReadAtReader {
            target: self.0.try_clone().unwrap(),
            cursor: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if LOG_READS.load(std::sync::atomic::Ordering::Acquire) {
            log(format!("Reading {} bytes", length));
        }
        let mut buf = BytesMut::with_capacity(length);
        unsafe {
            buf.set_len(length);
            let bytes_read = std::os::unix::fs::FileExt::read_at(&self.0, &mut buf, start)?;
            buf.set_len(bytes_read);
        }
        Ok(buf.into())
    }
}

impl SyncTryClone for ReadAtFile {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        self.0.try_clone().map(ReadAtFile)
    }
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
    file: T,
    indices: Vec<u32>,
    metadata: Option<ArrowReaderMetadata>,
    col: u32,
) {
    let batches = async_take(file, &indices, &[col], true, metadata).await;
    let num_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(num_rows, indices.len());
}

async fn bench_parquet_one(
    args: &Args,
    work_dir: &WorkDir,
    metadata: Option<ArrowReaderMetadata>,
) -> Duration {
    if args.drop_caches {
        drop_caches();
    }

    let runtime = tokio::runtime::Handle::current();
    let args = args.clone();
    let work_dir = work_dir.clone();
    runtime
        .spawn_blocking(move || {
            let indices = random_indices(&args);
            let path_str = format!(
                "/{}",
                parquet_path(&work_dir, args.row_group_size, args.page_size_kb)
            );
            let path = std::path::Path::new(&path_str);
            let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
            let file = ReadAtFile(file);
            if args.iterations == 1 {
                log("Bench Start");
            }
            let start = SystemTime::now();
            parquet_random_take_sync(file, indices, metadata, args.column_index());
            if args.iterations == 1 {
                log("Bench End");
            }
            start.elapsed().unwrap()
        })
        .map(|r| r.unwrap())
        .await
}

async fn bench_parquet_scan_one(
    args: &Args,
    work_dir: &WorkDir,
    metadata: Option<ArrowReaderMetadata>,
) -> Duration {
    if args.drop_caches {
        drop_caches();
    }

    let runtime = tokio::runtime::Handle::current();
    let args = args.clone();
    let work_dir = work_dir.clone();
    runtime
        .spawn_blocking(move || {
            let path_str = format!(
                "/{}",
                parquet_path(&work_dir, args.row_group_size, args.page_size_kb)
            );
            let path = std::path::Path::new(&path_str);
            let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
            let file = ReadAtFile(file);
            if args.iterations == 1 {
                log("Bench Start");
            }
            let start = SystemTime::now();
            let num_rows = sync_scan(file, &[args.column_index()], metadata);
            assert_eq!(num_rows, WRITE_ROW_COUNT);
            if args.iterations == 1 {
                log("Bench End");
            }
            start.elapsed().unwrap()
        })
        .map(|r| r.unwrap())
        .await
}

async fn bench_parquet_scan(args: &Args, work_dir: &WorkDir) -> f64 {
    parquet_global_setup(args, work_dir).await;
    let mut total_duration = Duration::from_nanos(0);
    if args.drop_caches {
        drop_caches();
    }
    let metadata = parquet_setup_sync(args, work_dir);
    futures::stream::iter(0..args.iterations)
        .map(move |_| bench_parquet_scan_one(args, work_dir, metadata.clone()))
        .buffer_unordered(args.num_threads as usize)
        .for_each(|d| {
            total_duration += d;
            futures::future::ready(())
        })
        .await;

    (total_duration.as_nanos() as f64 / (1000000.0)) / args.iterations as f64
}

async fn bench_parquet(args: &Args, work_dir: &WorkDir) -> f64 {
    parquet_global_setup(args, work_dir).await;
    let mut total_duration = Duration::from_nanos(0);
    if args.drop_caches {
        drop_caches();
    }
    let metadata = parquet_setup_sync(args, work_dir);
    futures::stream::iter(0..args.iterations)
        .map(move |_| bench_parquet_one(args, work_dir, metadata.clone()))
        .buffer_unordered(args.num_threads as usize)
        .for_each(|d| {
            total_duration += d;
            futures::future::ready(())
        })
        .await;

    (total_duration.as_nanos() as f64 / (1000000.0)) / args.iterations as f64
}

fn lance_path(work_dir: &WorkDir, version: LanceFileVersion) -> Path {
    work_dir.path.child(format!("bench_{}.lance", version))
}

async fn lance_global_setup(args: &Args, work_dir: &WorkDir) {
    let dest_path = lance_path(work_dir, args.storage_version);
    if work_dir.object_store.exists(&dest_path).await.unwrap() {
        log(format!("Using existing lance dataset at {}", dest_path));
        return;
    }

    log(format!("Creating new lance dataset at {}", dest_path));

    let datagen = get_datagen();

    let obj_store = lance_io::object_store::ObjectStore::local();
    let obj_writer = lance_io::object_writer::ObjectWriter::new(&obj_store, &dest_path)
        .await
        .unwrap();

    let mut writer = lance_file::v2::writer::FileWriter::new_lazy(
        obj_writer,
        FileWriterOptions {
            format_version: Some(args.storage_version),
            ..Default::default()
        },
    );

    for batch in datagen {
        writer.write_batch(&batch.unwrap()).await.unwrap();
    }
    writer.finish().await.unwrap();
}

struct CachedFileInfo {
    metadata: Arc<CachedFileMetadata>,
    cache: FileMetadataCache,
    projection: ReaderProjection,
    reader: Arc<FileReader>,
}

async fn lance_setup(args: &Args, work_dir: &WorkDir) -> Option<CachedFileInfo> {
    if !args.cache_metadata {
        return None;
    }
    let dest_path = Path::parse(lance_path(work_dir, args.storage_version)).unwrap();
    let obj_store = Arc::new(lance_io::object_store::ObjectStore::local());
    let scheduler = ScanScheduler::new(
        obj_store.clone(),
        SchedulerConfig::max_bandwidth(&obj_store),
    );
    let file_sched = scheduler.open_file(&dest_path).await.unwrap();
    let cache = FileMetadataCache::no_cache();
    let options = FileReaderOptions::default();
    let reader = lance_file::v2::reader::FileReader::try_open(
        file_sched,
        None,
        Arc::<DecoderPlugins>::default(),
        &cache,
        options,
    )
    .await
    .unwrap();

    let schema = reader.schema();
    let projection = ReaderProjection::from_column_names(&schema, &args.column_names()).unwrap();

    Some(CachedFileInfo {
        metadata: reader.metadata().clone(),
        cache: FileMetadataCache::with_capacity(128 * 1024 * 1024, CapacityMode::Bytes),
        projection,
        reader: Arc::new(reader),
    })
}

async fn bench_lance_one(
    args: Args,
    dest_path: Path,
    cached_info: &Option<CachedFileInfo>,
) -> Duration {
    let indices = random_indices(&args);
    if args.iterations == 1 {
        log("Bench Start");
    }
    let start = SystemTime::now();

    let (reader, projection) = if let Some(cached_info) = cached_info.as_ref() {
        (cached_info.reader.clone(), cached_info.projection.clone())
    } else {
        let obj_store = Arc::new(lance_io::object_store::ObjectStore::local());
        let scheduler = ScanScheduler::new(
            obj_store.clone(),
            SchedulerConfig::max_bandwidth(&obj_store),
        );
        let file_sched = scheduler.open_file(&dest_path).await.unwrap();
        let cache = FileMetadataCache::no_cache();
        let options = FileReaderOptions::default();
        let reader = lance_file::v2::reader::FileReader::try_open(
            file_sched,
            None,
            Arc::<DecoderPlugins>::default(),
            &cache,
            options,
        )
        .await
        .unwrap();

        let schema = reader.schema();
        let projection =
            ReaderProjection::from_column_names(&schema, &args.column_names()).unwrap();
        (Arc::new(reader), projection)
    };

    let batch_size = indices.len() as u32;
    let params = ReadBatchParams::Indices(indices.into());
    let read_tasks = reader
        .read_tasks(
            params,
            batch_size,
            Some(projection),
            FilterExpression::no_filter(),
        )
        .unwrap();

    let batch = read_tasks
        .map(|task| task.task)
        .buffer_unordered(1)
        .next()
        .await
        .unwrap()
        .unwrap();

    assert_eq!(batch.num_rows() as u32, batch_size);

    if args.iterations == 1 {
        log("Bench End");
    }
    start.elapsed().unwrap()
}

async fn bench_lance_scan_one(
    args: Args,
    dest_path: Path,
    cached_info: &Option<CachedFileInfo>,
) -> Duration {
    if args.iterations == 1 {
        log("Bench Start");
    }
    let start = SystemTime::now();

    let (reader, projection) = if let Some(cached_info) = cached_info.as_ref() {
        (cached_info.reader.clone(), cached_info.projection.clone())
    } else {
        let obj_store = Arc::new(lance_io::object_store::ObjectStore::local());
        let scheduler = ScanScheduler::new(
            obj_store.clone(),
            SchedulerConfig::max_bandwidth(&obj_store),
        );
        let file_sched = scheduler.open_file(&dest_path).await.unwrap();
        let cache = FileMetadataCache::no_cache();
        let options = FileReaderOptions::default();
        let reader = lance_file::v2::reader::FileReader::try_open(
            file_sched,
            None,
            Arc::<DecoderPlugins>::default(),
            &cache,
            options,
        )
        .await
        .unwrap();

        let schema = reader.schema();
        let projection =
            ReaderProjection::from_column_names(&schema, &args.column_names()).unwrap();
        (Arc::new(reader), projection)
    };

    let params = ReadBatchParams::RangeFull;
    let batch_size = 32 * 1024;
    let read_tasks = reader
        .read_tasks(
            params,
            batch_size,
            Some(projection),
            FilterExpression::no_filter(),
        )
        .unwrap();

    let mut batch_stream = read_tasks
        .map(|task| task.task)
        .buffer_unordered(args.num_threads as usize);

    let mut num_rows = 0;
    while let Some(batch) = batch_stream.next().await {
        let batch = batch.unwrap();
        num_rows += batch.num_rows();
    }

    assert_eq!(num_rows as u64, WRITE_ROW_COUNT);

    if args.iterations == 1 {
        log("Bench End");
    }
    start.elapsed().unwrap()
}

async fn bench_lance(args: &Args, work_dir: &WorkDir) -> f64 {
    lance_global_setup(args, work_dir).await;
    let mut total_duration = Duration::from_nanos(0);

    if args.drop_caches {
        drop_caches();
    }

    let cached_info = Arc::new(lance_setup(args, work_dir).await);

    futures::stream::iter(0..args.iterations)
        .map(move |_| {
            let args = args.clone();
            let cached_info = cached_info.clone();
            let dest_path = Path::parse(lance_path(work_dir, args.storage_version)).unwrap();
            tokio::spawn(async move {
                let cached_info = cached_info.clone();
                bench_lance_one(args, dest_path, &cached_info).await
            })
            .map(|r| r.unwrap())
        })
        .buffer_unordered(args.num_threads as usize)
        .for_each(|d| {
            total_duration += d;
            futures::future::ready(())
        })
        .await;

    (total_duration.as_nanos() as f64 / (1000.0 * 1000.0)) / args.iterations as f64
}

async fn bench_lance_scan(args: &Args, work_dir: &WorkDir) -> f64 {
    lance_global_setup(args, work_dir).await;
    let mut total_duration = Duration::from_nanos(0);

    if args.drop_caches {
        drop_caches();
    }

    let cached_info = Arc::new(lance_setup(args, work_dir).await);

    futures::stream::iter(0..args.iterations)
        .map(move |_| {
            let args = args.clone();
            let cached_info = cached_info.clone();
            let dest_path = Path::parse(lance_path(work_dir, args.storage_version)).unwrap();
            tokio::spawn(async move {
                let cached_info = cached_info.clone();
                bench_lance_scan_one(args, dest_path, &cached_info).await
            })
            .map(|r| r.unwrap())
        })
        .buffer_unordered(args.num_threads as usize)
        .for_each(|d| {
            total_duration += d;
            futures::future::ready(())
        })
        .await;

    (total_duration.as_nanos() as f64 / (1000.0 * 1000.0)) / args.iterations as f64
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    env_logger::init();

    if !args.quiet {
        SHOULD_LOG.store(true, std::sync::atomic::Ordering::Release);
    }

    if args.log_reads {
        LOG_READS.store(true, std::sync::atomic::Ordering::Release);
    }

    let workdir = args
        .workdir
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("file:///tmp")
        .to_string();

    let work_dir = WorkDir::new(&workdir);

    let avg_duration_ms = match (args.format, args.benchmark) {
        (Format::Parquet, Benchmark::Take) => bench_parquet(&args, &work_dir).await,
        (Format::Parquet, Benchmark::Scan) => bench_parquet_scan(&args, &work_dir).await,
        (Format::Lance, Benchmark::Take) => bench_lance(&args, &work_dir).await,
        (Format::Lance, Benchmark::Scan) => bench_lance_scan(&args, &work_dir).await,
    };

    let rows_per_second = 1000.0 / avg_duration_ms * args.take_size as f64;

    println!(
        "Average duration across {} iterations: {}ms ({} rows per second)",
        args.iterations, avg_duration_ms, rows_per_second,
    );
}
