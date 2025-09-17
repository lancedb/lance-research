use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader,
};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use clap::Parser;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::prelude::{col, Expr, ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use futures::stream::BoxStream;
use lance_file::v2::writer::{FileWriter as LanceFileWriter, FileWriterOptions};
use lance_io::object_store::ObjectStore as LanceObjectStore;
use object_store::PutMultipartOptions;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use rand::Rng;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::info;

const NUM_FIELDS: usize = 20;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Output path for I/O tracking CSV
    #[arg(short, long, default_value = "results/io_parallelism.csv")]
    output: String,

    /// Number of rows per row group (default: 1M)
    #[arg(long, default_value_t = 1_000_000)]
    rows_per_group: usize,

    /// Number of row groups (default: 10)
    #[arg(long, default_value_t = 10)]
    num_row_groups: usize,

    /// Number of DataFusion partitions (default: 4)
    #[arg(long, default_value_t = 4)]
    partitions: usize,
}

#[derive(Debug, Clone)]
struct IoRequest {
    duration: u64,
    start_timestamp: u64,
    requests_in_flight: u64,
    path: String,
    range_start: Option<u64>,
    range_end: Option<u64>,
}

#[derive(Debug)]
struct TrackingObjectStore {
    inner: Box<dyn ObjectStore>,
    request_counter: AtomicU64,
    requests: Arc<Mutex<Vec<IoRequest>>>,
}

impl std::fmt::Display for TrackingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrackingObjectStore({})", self.inner)
    }
}

impl TrackingObjectStore {
    fn new(inner: Box<dyn ObjectStore>) -> Self {
        Self {
            inner,
            request_counter: AtomicU64::new(0),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record_request(
        &self,
        path: &str,
        duration: u64,
        start_timestamp: u64,
        range_start: Option<u64>,
        range_end: Option<u64>,
    ) {
        let current_count = self.request_counter.fetch_add(1, Ordering::SeqCst);
        let request = IoRequest {
            duration,
            start_timestamp,
            requests_in_flight: current_count + 1,
            path: path.to_string(),
            range_start,
            range_end,
        };

        if let Ok(mut requests) = self.requests.lock() {
            requests.push(request);
        }
    }

    fn finish_request(&self) {
        self.request_counter.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn get_requests(&self) -> Vec<IoRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.requests.lock().unwrap().clear();
        self.request_counter.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for TrackingObjectStore {
    async fn put(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn get(&self, location: &ObjectPath) -> ObjectStoreResult<GetResult> {
        let start = SystemTime::now();
        let result = self.inner.get(location).await;
        self.finish_request();
        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap().as_nanos() as u64;
        let start_timestamp = start.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.record_request(location.as_ref(), duration, start_timestamp, None, None);
        result
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let (range_start, range_end) = if let Some(range) = &options.range {
            match range {
                GetRange::Bounded(r) => (Some(r.start), Some(r.end)),
                GetRange::Offset(start) => (Some(*start), None),
                GetRange::Suffix(len) => (None, Some(*len)),
            }
        } else {
            (None, None)
        };
        let start = SystemTime::now();
        let result = self.inner.get_opts(location, options).await;
        self.finish_request();
        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap().as_nanos() as u64;
        let start_timestamp = start.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.record_request(
            location.as_ref(),
            duration,
            start_timestamp,
            range_start,
            range_end,
        );
        result
    }

    async fn delete(&self, location: &ObjectPath) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn head(&self, location: &ObjectPath) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }
}

fn create_synthetic_data(
    rows_per_batch: usize,
    num_batches: usize,
) -> Result<impl RecordBatchReader, Box<dyn std::error::Error>> {
    let mut fields = Vec::new();
    fields.push(Field::new("id", DataType::Int64, false));

    for i in 0..NUM_FIELDS {
        fields.push(Field::new(format!("value_{}", i), DataType::Float64, false));
        fields.push(Field::new(
            format!("padding_{}", i),
            DataType::Float64,
            false,
        ));
    }

    let schema = Arc::new(Schema::new(fields));
    let schema_clone = schema.clone();

    let mut rng = rand::thread_rng();

    let iter = (0..num_batches).map(move |batch_idx| {
        // Generate data for this row group
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(NUM_FIELDS + 1);
        arrays.push(Arc::new(Int64Array::from_iter_values(
            (0..rows_per_batch).map(|i| (batch_idx * rows_per_batch + i) as i64),
        )));
        for _ in 0..NUM_FIELDS {
            arrays.push(Arc::new(Float64Array::from_iter_values(
                (0..rows_per_batch).map(|_| rng.gen::<f64>() * 100.0),
            )));
            arrays.push(Arc::new(Float64Array::from_iter_values(
                (0..rows_per_batch).map(|_| rng.gen::<f64>() * 100.0),
            )));
        }

        RecordBatch::try_new(schema.clone(), arrays)
    });

    Ok(RecordBatchIterator::new(iter, schema_clone))
}

async fn create_synthetic_lance_file(
    path: &str,
    num_row_groups: usize,
    rows_per_group: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic lance file with {} rows",
        num_row_groups * rows_per_group
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    let obj_store = LanceObjectStore::local();
    let obj_writer = obj_store.create(&ObjectPath::parse(path)?).await?;
    let mut writer = LanceFileWriter::new_lazy(obj_writer, FileWriterOptions::default());

    for batch in reader {
        writer.write_batch(&batch?).await?;
    }

    writer.finish().await?;

    Ok(())
}

async fn create_synthetic_parquet_file(
    path: &str,
    num_row_groups: usize,
    rows_per_group: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic parquet file with {} row groups, {} rows each",
        num_row_groups, rows_per_group
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    let file = std::fs::File::create(path)?;

    // Configure writer properties for specific row group size
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(rows_per_group)
        .build();

    let mut writer = ArrowWriter::try_new(file, reader.schema(), Some(props))?;

    for batch in reader {
        writer.write(&batch?)?;
    }

    writer.close()?;
    info!("Synthetic parquet file created at: {}", path);

    Ok(())
}

fn clear_disk_cache() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    {
        use std::fs::OpenOptions;
        use std::io::Write;

        info!("Attempting to clear Linux page cache");

        // Try to clear page cache, dentries and inodes
        // This requires root privileges or appropriate capabilities
        let result = std::process::Command::new("sync").output();

        if result.is_ok() {
            info!("Successfully called sync");
        } else {
            info!("Warning: Could not call sync");
        }

        // Try to drop caches (requires root)
        let drop_result = OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")
            .and_then(|mut file| file.write_all(b"3\n"));

        match drop_result {
            Ok(_) => info!("Successfully cleared disk cache via /proc/sys/vm/drop_caches"),
            Err(e) => {
                info!(
                    "Warning: Could not clear disk cache (may require root): {}",
                    e
                );
                info!("Cache clearing will be limited - consider running with sudo for full cache clearing");
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        info!("Disk cache clearing not implemented for this platform");
    }

    Ok(())
}

async fn run_datafusion_query(
    parquet_path: &str,
    partitions: usize,
) -> Result<Arc<TrackingObjectStore>, Box<dyn std::error::Error>> {
    info!("Running DataFusion query with {} partitions", partitions);

    // Create local filesystem object store with tracking
    let local_store = object_store::local::LocalFileSystem::new();
    let tracking_store = Arc::new(TrackingObjectStore::new(Box::new(local_store)));

    let file_path = format!("file://{}", parquet_path);
    info!("Reading parquet file: {}", file_path);

    // Create runtime environment and register our tracking object store
    let runtime_env = RuntimeEnvBuilder::new().build_arc().unwrap();

    runtime_env
        .object_store_registry
        .register_store(&url::Url::parse("file://")?, tracking_store.clone());

    // Create ObjectStoreUrl and file metadata

    let session_context = SessionContext::new_with_config_rt(
        SessionConfig::default().with_target_partitions(partitions),
        runtime_env,
    );
    let session_context = &session_context;

    let make_df = || async {
        let df = session_context
            .read_parquet(file_path.as_str(), ParquetReadOptions::default())
            .await
            .unwrap();

        let mut sum_exprs = Vec::new();
        for i in 0..NUM_FIELDS {
            sum_exprs.push(Expr::AggregateFunction(AggregateFunction::new_udf(
                sum_udaf(),
                vec![col(format!("value_{}", i))],
                false,
                None,
                vec![],
                None,
            )));
        }

        df.aggregate(vec![], sum_exprs).unwrap()
    };

    let res = make_df()
        .await
        .explain(true, false)
        .unwrap()
        .collect()
        .await
        .unwrap();

    for batch in res {
        let plans = batch.column(1).as_string::<i32>();
        println!("{}", plans.value(plans.len() - 1));
    }

    make_df().await.collect().await.unwrap();

    // Clear the tracking store
    tracking_store.clear();

    // Clear the OS disk cache to ensure real I/O timing
    info!("Clearing disk cache to measure real I/O performance");
    clear_disk_cache()?;

    // Add a small delay to ensure cache clearing has taken effect
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    make_df().await.collect().await.unwrap();

    Ok(tracking_store)
}

async fn write_io_results(
    requests: &[IoRequest],
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Writing I/O tracking results to: {}", output_path);

    // Ensure output directory exists
    if let Some(parent) = Path::new(output_path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = File::create(output_path).await?;

    // Write CSV header
    file.write_all(b"timestamp_ns,duration_ns,requests_in_flight,path,range_start,range_end\n")
        .await?;

    // Write data
    for request in requests {
        let range_start = request
            .range_start
            .map_or("".to_string(), |s| s.to_string());
        let range_end = request.range_end.map_or("".to_string(), |e| e.to_string());

        let line = format!(
            "{},{},{},{},{},{}\n",
            request.start_timestamp,
            request.duration,
            request.requests_in_flight,
            request.path,
            range_start,
            range_end
        );
        file.write_all(line.as_bytes()).await?;
    }

    file.flush().await?;
    info!("I/O tracking results written successfully");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Starting row group I/O parallelism experiment");
    info!(
        "Configuration: {} row groups, {} rows per group, {} partitions",
        args.num_row_groups, args.rows_per_group, args.partitions
    );

    // Check if we can clear disk cache
    #[cfg(target_os = "linux")]
    {
        if std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")
            .is_err()
        {
            info!(
                "Note: For accurate I/O timing measurements, consider running with sudo privileges"
            );
            info!("This allows clearing the disk cache between runs to measure real disk I/O performance");
        }
    }

    // Create temporary parquet file
    let temp_file = NamedTempFile::with_suffix(".parquet")?;
    let parquet_path = temp_file
        .path()
        .canonicalize()?
        .to_string_lossy()
        .to_string();

    // Generate synthetic parquet file
    create_synthetic_parquet_file(&parquet_path, args.num_row_groups, args.rows_per_group).await?;

    // Run DataFusion query with tracking
    let tracking_store = run_datafusion_query(&parquet_path, args.partitions).await?;

    // Get I/O requests and write to CSV
    let requests = tracking_store.get_requests();
    info!("Recorded {} I/O requests", requests.len());

    write_io_results(&requests, &args.output).await?;

    info!("Experiment completed successfully");
    Ok(())
}
