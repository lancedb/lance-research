use std::collections::HashMap;
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
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{ReadParams, WriteParams};
use lance::io::{ObjectStoreParams, WrappingObjectStore};
use lance::Dataset;
use lance_file::version::LanceFileVersion;
use object_store::PutMultipartOptions;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use rand::Rng;
use tempfile;
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

    /// File format to test (parquet, lance, or both)
    #[arg(long, default_value = "both")]
    format: String,

    /// Base path or URI for test files (e.g., /tmp, s3://bucket/prefix)
    #[arg(long, default_value = "/tmp")]
    base_path: String,
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
        let duration = start.elapsed().unwrap().as_nanos() as u64;
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

#[derive(Debug, Clone)]
struct TrackingWrapper;

impl WrappingObjectStore for TrackingWrapper {
    fn wrap(
        &self,
        original: Arc<dyn ObjectStore>,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(TrackingObjectStore::new(Box::new(original)))
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

    let iter = (0..num_batches).map(move |batch_idx| {
        let mut rng = rand::thread_rng();

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

async fn create_synthetic_lance_dataset(
    uri: &str,
    num_row_groups: usize,
    rows_per_group: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic lance dataset with {} rows at {}",
        num_row_groups * rows_per_group,
        uri
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        }),
    )
    .await?;

    Ok(())
}

async fn create_synthetic_parquet_file(
    uri: &str,
    num_row_groups: usize,
    rows_per_group: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic parquet file with {} row groups, {} rows each at {}",
        num_row_groups, rows_per_group, uri
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    // Configure writer properties for specific row group size
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(rows_per_group)
        .build();

    if uri.starts_with("s3://") || uri.starts_with("gs://") || uri.contains("://") {
        // Handle remote URIs using object store
        let (object_store, path) = object_store::parse_url(&url::Url::parse(uri)?)?;

        // Create a temporary file first, then upload
        let temp_file = tempfile::NamedTempFile::new()?;
        let mut writer = ArrowWriter::try_new(
            temp_file.as_file().try_clone()?,
            reader.schema(),
            Some(props),
        )?;

        for batch in reader {
            writer.write(&batch?)?;
        }
        writer.close()?;

        // Read the temporary file and upload to object store
        let data = tokio::fs::read(temp_file.path()).await?;
        object_store.put(&path, data.into()).await?;

        info!("Synthetic parquet file uploaded to: {}", uri);
    } else {
        // Handle local file paths
        let file = std::fs::File::create(uri)?;
        let mut writer = ArrowWriter::try_new(file, reader.schema(), Some(props))?;

        for batch in reader {
            writer.write(&batch?)?;
        }
        writer.close()?;
        info!("Synthetic parquet file created at: {}", uri);
    }

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

async fn run_datafusion_parquet_query(
    parquet_uri: &str,
    partitions: usize,
) -> Result<Arc<TrackingObjectStore>, Box<dyn std::error::Error>> {
    info!(
        "Running DataFusion Parquet query with {} partitions",
        partitions
    );

    // Determine the file URI format
    let file_uri = if parquet_uri.contains("://") {
        parquet_uri.to_string()
    } else {
        format!("file://{}", parquet_uri)
    };

    info!("Reading parquet file: {}", file_uri);

    // Create appropriate object store with tracking based on URI scheme
    let uri_parsed = url::Url::parse(&file_uri)?;
    let (base_store, tracking_store) = match uri_parsed.scheme() {
        "file" => {
            let local_store = object_store::local::LocalFileSystem::new();
            let tracking = Arc::new(TrackingObjectStore::new(Box::new(local_store)));
            (uri_parsed.clone(), tracking)
        }
        "s3" => {
            let (s3_store, _) = object_store::parse_url(&uri_parsed)?;
            let tracking = Arc::new(TrackingObjectStore::new(s3_store));
            (uri_parsed.clone(), tracking)
        }
        _ => {
            let (store, _) = object_store::parse_url(&uri_parsed)?;
            let tracking = Arc::new(TrackingObjectStore::new(store));
            (uri_parsed.clone(), tracking)
        }
    };

    // Create runtime environment and register our tracking object store
    let runtime_env = RuntimeEnvBuilder::new().build_arc().unwrap();

    runtime_env
        .object_store_registry
        .register_store(&base_store, tracking_store.clone());

    let session_context = SessionContext::new_with_config_rt(
        SessionConfig::default().with_target_partitions(partitions),
        runtime_env,
    );

    let make_df = || async {
        let df = session_context
            .read_parquet(&file_uri, ParquetReadOptions::default())
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

    // Clear the OS disk cache to ensure real I/O timing (only for local files)
    if uri_parsed.scheme() == "file" {
        info!("Clearing disk cache to measure real I/O performance");
        clear_disk_cache()?;

        // Add a small delay to ensure cache clearing has taken effect
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    make_df().await.collect().await.unwrap();

    Ok(tracking_store)
}

async fn run_datafusion_lance_query(
    lance_uri: &str,
    partitions: usize,
) -> Result<Vec<IoRequest>, Box<dyn std::error::Error>> {
    info!(
        "Running DataFusion Lance query with {} partitions",
        partitions
    );

    info!("Reading lance dataset: {}", lance_uri);

    let session_context = SessionContext::new_with_config(
        SessionConfig::default().with_target_partitions(partitions),
    );

    let lance_dataset = DatasetBuilder::from_uri(lance_uri)
        .with_read_params(ReadParams {
            store_options: Some(ObjectStoreParams {
                object_store_wrapper: Some(Arc::new(TrackingWrapper)),
                ..Default::default()
            }),
            ..Default::default()
        })
        .load()
        .await?;
    let lance_dataset = Arc::new(lance_dataset);

    let lance_provider = LanceTableProvider::new(lance_dataset, false, false);

    // Register the table
    session_context
        .register_table("lance_table", Arc::new(lance_provider))
        .unwrap();

    let make_df = || async {
        let df = session_context.table("lance_table").await.unwrap();

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

    // Clear the OS disk cache to ensure real I/O timing (only for local files)
    if !lance_uri.contains("://") || lance_uri.starts_with("file://") {
        info!("Clearing disk cache to measure real I/O performance");
        clear_disk_cache()?;

        // Add a small delay to ensure cache clearing has taken effect
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    make_df().await.collect().await.unwrap();

    // For Lance, we need to get requests from the tracking wrapper
    // This is a simplified approach - in practice you'd need to store requests globally
    Ok(vec![])
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
        "Configuration: {} row groups, {} rows per group, {} partitions, format: {}, base_path: {}",
        args.num_row_groups, args.rows_per_group, args.partitions, args.format, args.base_path
    );

    // Check if we can clear disk cache (only relevant for local file systems)
    if !args.base_path.contains("://") || args.base_path.starts_with("file://") {
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
    }

    let should_test_parquet = args.format == "parquet" || args.format == "both";
    let should_test_lance = args.format == "lance" || args.format == "both";

    // Construct file paths based on base_path
    let parquet_uri = if args.base_path.ends_with('/') {
        format!("{}test.parquet", args.base_path)
    } else {
        format!("{}/test.parquet", args.base_path)
    };

    let lance_uri = if args.base_path.ends_with('/') {
        format!("{}test.lance", args.base_path)
    } else {
        format!("{}/test.lance", args.base_path)
    };

    // Test Parquet format
    if should_test_parquet {
        info!("=== Testing Parquet Format ===");

        // Generate synthetic parquet file
        create_synthetic_parquet_file(&parquet_uri, args.num_row_groups, args.rows_per_group)
            .await?;

        // Run DataFusion query with tracking
        let tracking_store = run_datafusion_parquet_query(&parquet_uri, args.partitions).await?;

        // Get I/O requests and write to CSV
        let requests = tracking_store.get_requests();
        info!("Recorded {} I/O requests for Parquet", requests.len());

        let parquet_output = if args.format == "both" {
            args.output.replace(".csv", "_parquet.csv")
        } else {
            args.output.clone()
        };
        write_io_results(&requests, &parquet_output).await?;
    }

    // Test Lance format
    if should_test_lance {
        info!("=== Testing Lance Format ===");

        // Generate synthetic lance dataset
        create_synthetic_lance_dataset(&lance_uri, args.num_row_groups, args.rows_per_group)
            .await?;

        // Run DataFusion query with tracking
        let requests = run_datafusion_lance_query(&lance_uri, args.partitions).await?;

        info!("Recorded {} I/O requests for Lance", requests.len());

        let lance_output = if args.format == "both" {
            args.output.replace(".csv", "_lance.csv")
        } else {
            args.output.clone()
        };
        write_io_results(&requests, &lance_output).await?;
    }

    info!("Experiment completed successfully");
    Ok(())
}
