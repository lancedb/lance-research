use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow_array::{
    cast::AsArray, Array, Float64Array, Int64Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use clap::Parser;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::prelude::{col, Expr, ParquetReadOptions, SessionConfig, SessionContext};
use futures::stream::BoxStream;
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_file::version::LanceFileVersion;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore,
    PutOptions, PutPayload, PutResult,
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
    #[arg(short, long, default_value = "results/s3_io_parallelism.csv")]
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

    /// S3 URI prefix for test files (e.g., s3://bucket/prefix)
    #[arg(long)]
    s3_uri: String,

    /// AWS region (optional, will use default profile region if not specified)
    #[arg(long)]
    region: Option<String>,
}

#[derive(Debug, Clone)]
struct IoRequest {
    duration: u64,
    start_timestamp: u64,
    path: String,
    range_start: Option<u64>,
    range_end: Option<u64>,
}

// Tracking wrapper around an ObjectStore
#[derive(Debug)]
struct TrackingObjectStore {
    inner: Arc<dyn ObjectStore>,
    requests: Arc<Mutex<Vec<IoRequest>>>,
    start_time: Instant,
}

impl TrackingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            requests: Arc::new(Mutex::new(Vec::new())),
            start_time: Instant::now(),
        }
    }

    fn get_requests(&self) -> Vec<IoRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn record_request(&self, path: &str, range: Option<(u64, u64)>, duration: u64) {
        let start_timestamp = self.start_time.elapsed().as_nanos() as u64;

        let (range_start, range_end) = range.map_or((None, None), |(s, e)| (Some(s), Some(e)));

        let request = IoRequest {
            duration,
            start_timestamp,
            path: path.to_string(),
            range_start,
            range_end,
        };

        self.requests.lock().unwrap().push(request);
    }
}

impl std::fmt::Display for TrackingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrackingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for TrackingObjectStore {
    async fn put(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
    ) -> object_store::Result<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &ObjectPath,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, _location: &ObjectPath) -> object_store::Result<GetResult> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        _location: &ObjectPath,
        _options: GetOptions,
    ) -> object_store::Result<GetResult> {
        unimplemented!()
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: std::ops::Range<u64>,
    ) -> object_store::Result<Bytes> {
        let start = Instant::now();
        let result = self.inner.get_range(location, range.clone()).await;
        let duration = start.elapsed().as_nanos() as u64;
        self.record_request(
            &location.to_string(),
            Some((range.start, range.end)),
            duration,
        );
        result
    }

    async fn head(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
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
                (0..rows_per_batch).map(|_| rng.gen::<f64>()),
            )));
            arrays.push(Arc::new(Float64Array::from_iter_values(
                (0..rows_per_batch).map(|_| rng.gen::<f64>()),
            )));
        }

        RecordBatch::try_new(schema.clone(), arrays)
    });

    Ok(RecordBatchIterator::new(iter, schema_clone))
}

async fn create_synthetic_lance_dataset_s3(
    s3_uri: &str,
    num_row_groups: usize,
    rows_per_group: usize,
    _object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic lance dataset with {} rows at {}",
        num_row_groups * rows_per_group,
        s3_uri
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    // Create dataset - for now, Lance will use default S3 credentials from environment
    Dataset::write(
        reader,
        s3_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_1),
            max_rows_per_file: rows_per_group * num_row_groups,
            ..Default::default()
        }),
    )
    .await?;

    Ok(())
}

async fn create_synthetic_parquet_file_s3(
    s3_uri: &str,
    num_row_groups: usize,
    rows_per_group: usize,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic parquet file with {} row groups, {} rows each at {}",
        num_row_groups, rows_per_group, s3_uri
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    // Configure writer properties for specific row group size
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(rows_per_group)
        .build();

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

    // Read the temporary file and upload to S3
    let data = tokio::fs::read(temp_file.path()).await?;
    let (_, path) = object_store::parse_url(&url::Url::parse(s3_uri)?)?;
    object_store.put(&path, data.into()).await?;

    info!("Synthetic parquet file uploaded to: {}", s3_uri);

    Ok(())
}

async fn run_parquet_query_s3(
    s3_uri: &str,
    partitions: usize,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Running DataFusion Parquet query with {} partitions on S3",
        partitions
    );

    info!("Reading parquet file: {}", s3_uri);

    let session_context = SessionContext::new_with_config(
        SessionConfig::default().with_target_partitions(partitions),
    );

    // Register the object store with the session context
    let url = url::Url::parse(s3_uri)?;
    session_context
        .runtime_env()
        .register_object_store(&url, object_store);

    let make_df = || async {
        let df = session_context
            .read_parquet(s3_uri, ParquetReadOptions::default())
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

    // Show execution plan
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

    // Run the query
    info!("Running query");
    make_df().await.collect().await.unwrap();

    Ok(())
}

async fn run_lance_query_s3(
    s3_uri: &str,
    partitions: usize,
    _object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Running DataFusion Lance query with {} partitions on S3",
        partitions
    );

    info!("Reading lance dataset: {}", s3_uri);

    let session_context = SessionContext::new_with_config(
        SessionConfig::default().with_target_partitions(partitions),
    );

    // Create Lance dataset - it will use default S3 credentials from environment
    let lance_dataset = DatasetBuilder::from_uri(s3_uri).load().await?;
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

    // Show execution plan
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

    // Run the query
    info!("Running query");
    make_df().await.collect().await.unwrap();

    Ok(())
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
            request.start_timestamp, request.duration, 0, request.path, range_start, range_end
        );
        file.write_all(line.as_bytes()).await?;
    }

    file.flush().await?;
    info!("I/O tracking results written successfully");

    Ok(())
}

async fn setup_s3_object_store(
    s3_uri: &str,
    region: Option<String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    let url = url::Url::parse(s3_uri)?;

    // Create S3 object store using object_store crate
    let mut s3_builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(url.host_str().ok_or("Invalid S3 URL")?);

    if let Some(region) = region {
        s3_builder = s3_builder.with_region(region);
    }

    let s3_store = s3_builder.build()?;

    Ok(Arc::new(s3_store))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Starting S3 row group I/O parallelism benchmark");
    info!(
        "Configuration: {} row groups, {} rows per group, {} partitions, format: {}, s3_uri: {}",
        args.num_row_groups, args.rows_per_group, args.partitions, args.format, args.s3_uri
    );

    // Validate S3 URI
    if !args.s3_uri.starts_with("s3://") {
        return Err("S3 URI must start with s3://".into());
    }

    // Set up S3 object store
    let s3_store = setup_s3_object_store(&args.s3_uri, args.region).await?;
    let tracking_store = Arc::new(TrackingObjectStore::new(s3_store));

    let should_test_parquet = args.format == "parquet" || args.format == "both";
    let should_test_lance = args.format == "lance" || args.format == "both";

    // Construct file URIs based on s3_uri
    let parquet_uri = if args.s3_uri.ends_with('/') {
        format!("{}test.parquet", args.s3_uri)
    } else {
        format!("{}/test.parquet", args.s3_uri)
    };

    let lance_uri = if args.s3_uri.ends_with('/') {
        format!("{}test.lance", args.s3_uri)
    } else {
        format!("{}/test.lance", args.s3_uri)
    };

    // Test Parquet format
    if should_test_parquet {
        info!("=== Testing Parquet Format on S3 ===");

        // Generate synthetic parquet file
        create_synthetic_parquet_file_s3(
            &parquet_uri,
            args.num_row_groups,
            args.rows_per_group,
            tracking_store.clone(),
        )
        .await?;

        // Reset tracking for the query portion
        let query_tracking_store = Arc::new(TrackingObjectStore::new(tracking_store.inner.clone()));

        // Run query
        run_parquet_query_s3(&parquet_uri, args.partitions, query_tracking_store.clone()).await?;

        let requests = query_tracking_store.get_requests();
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
        info!("=== Testing Lance Format on S3 ===");

        // Generate synthetic lance dataset
        create_synthetic_lance_dataset_s3(
            &lance_uri,
            args.num_row_groups,
            args.rows_per_group,
            tracking_store.clone(),
        )
        .await?;

        // Reset tracking for the query portion
        let query_tracking_store = Arc::new(TrackingObjectStore::new(tracking_store.inner.clone()));

        // Run query
        run_lance_query_s3(&lance_uri, args.partitions, query_tracking_store.clone()).await?;

        let requests = query_tracking_store.get_requests();
        info!("Recorded {} I/O requests for Lance", requests.len());

        let lance_output = if args.format == "both" {
            args.output.replace(".csv", "_lance.csv")
        } else {
            args.output.clone()
        };
        write_io_results(&requests, &lance_output).await?;
    }

    info!("S3 benchmark completed successfully");
    Ok(())
}
