use std::{
    cell::LazyCell,
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
    vec,
};

use async_trait::async_trait;
use clap::Parser;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::{path::Path, ObjectStore, PutOptions, PutPayload};
use rand::RngCore;

use futures_util::stream::StreamExt;
const CHUNK_SIZE: usize = 32 * 1024 * 1024;
const PATH_PARENT: LazyCell<Path> = LazyCell::new(|| Path::parse("fsprof").unwrap());
const DURATION: Duration = Duration::from_secs(10);

struct BenchmarkResult {
    operation: String,
    iteration: usize,
    worker: usize,
    concurrency: usize,
    start: Instant,
    end: Instant,
    bytes_transferred: usize,
    error: bool,
}

#[async_trait]
trait Benchmark {
    async fn setup(&self, store: Arc<dyn ObjectStore>);
    async fn run(
        &self,
        store: Arc<dyn ObjectStore>,
        worker: usize,
        iteration: usize,
        concurrency: usize,
    ) -> BenchmarkResult;

    async fn cleanup(&self, store: Arc<dyn ObjectStore>) {
        let items = store.list(Some(&PATH_PARENT)).collect::<Vec<_>>().await;
        let handles = items.into_iter().map(|item| {
            let store = store.clone();
            tokio::task::spawn(async move {
                store.delete(&item.unwrap().location).await.unwrap();
            })
        });
        for handle in handles {
            handle.await.unwrap();
        }
    }
}

async fn driver(
    store: Arc<dyn ObjectStore>,
    concurrencies: Vec<usize>,
    benchmark: &mut Arc<impl Benchmark + Send + Sync + 'static>,
) -> Vec<BenchmarkResult> {
    benchmark.setup(store.clone()).await;
    let mut results = Vec::new();
    for concurrency in concurrencies {
        println!("concurrency={}", concurrency);
        let start = Instant::now();
        let finished: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let handles = (0..concurrency)
            .map(|worker| {
                let finished = finished.clone();
                let store = store.clone();
                let benchmark = benchmark.clone();
                let jitter = rand::random::<u64>() % 200;
                tokio::task::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(jitter)).await;
                    let mut results = Vec::new();
                    let mut iteration = 0;
                    while !finished.load(Ordering::Acquire) {
                        let result = benchmark
                            .run(store.clone(), worker, iteration, concurrency)
                            .await;
                        results.push(result);
                        iteration += 1;
                    }
                    results
                })
            })
            .collect::<Vec<_>>();

        while start.elapsed() < DURATION {
            let remaining = DURATION - start.elapsed();
            let next_sleep = Duration::from_millis(50).min(remaining);
            tokio::time::sleep(next_sleep).await;
        }
        finished.store(true, Ordering::Release);

        for handle in handles {
            let mut output = handle.await.unwrap();
            results.append(&mut output);
        }
    }

    println!("Cleaning up...");
    benchmark.cleanup(store.clone()).await;
    println!("Done");

    results
}

#[derive(Clone)]
struct MultipartUploadBenchmark {
    data: Arc<bytes::Bytes>,
    file_size: usize,
}

impl MultipartUploadBenchmark {
    fn new(file_size: usize) -> Self {
        let mut buf = vec![0; file_size];
        let mut rng = rand::rngs::OsRng;
        rng.fill_bytes(&mut buf);
        Self {
            data: Arc::new(bytes::Bytes::from(buf)),
            file_size,
        }
    }
}

#[async_trait]
impl Benchmark for MultipartUploadBenchmark {
    async fn setup(&self, _: Arc<dyn ObjectStore>) {}
    async fn run(
        &self,
        store: Arc<dyn ObjectStore>,
        worker: usize,
        iteration: usize,
        concurrency: usize,
    ) -> BenchmarkResult {
        let path = PATH_PARENT.child(format!("multipart-uploads/{}", worker));
        let start = Instant::now();
        let mut error = false;
        if let Err(_) = upload_multipart(store.clone(), path, self.data.clone()).await {
            error = true;
        }
        BenchmarkResult {
            operation: "multipart_upload".to_string(),
            worker,
            iteration,
            concurrency,
            start,
            end: Instant::now(),
            bytes_transferred: self.file_size,
            error: error,
        }
    }
}

struct PutBenchmark {
    data: Arc<bytes::Bytes>,
    file_size: usize,
}

impl PutBenchmark {
    fn new(file_size: usize) -> Self {
        let mut buf = vec![0; file_size];
        let mut rng = rand::rngs::OsRng;
        rng.fill_bytes(&mut buf);
        Self {
            data: Arc::new(bytes::Bytes::from(buf)),
            file_size,
        }
    }
}

#[async_trait]
impl Benchmark for PutBenchmark {
    async fn setup(&self, _: Arc<dyn ObjectStore>) {}

    async fn run(
        &self,
        store: Arc<dyn ObjectStore>,
        worker: usize,
        iteration: usize,
        concurrency: usize,
    ) -> BenchmarkResult {
        let path = PATH_PARENT.child(format!("puts/file_{}.dat", worker));
        let start = Instant::now();
        let payload = PutPayload::from_bytes((*self.data).clone());
        let mut error = false;
        if let Err(_) = store.put_opts(&path, payload, PutOptions::default()).await {
            error = true;
        }
        BenchmarkResult {
            operation: "put".to_string(),
            worker,
            iteration,
            concurrency,
            start,
            end: Instant::now(),
            bytes_transferred: self.file_size,
            error,
        }
    }
}

struct GetBenchmark {
    data: Arc<bytes::Bytes>,
    read_size: usize,
}

impl GetBenchmark {
    fn new(read_size: usize) -> Self {
        let mut buf = vec![0; 1024 * 1024 * 1024]; // 1 GB target file out of which ranges will be gotten
        let mut rng = rand::rngs::OsRng;
        rng.fill_bytes(&mut buf);
        Self {
            data: Arc::new(bytes::Bytes::from(buf)),
            read_size: read_size,
        }
    }
}

#[async_trait]
impl Benchmark for GetBenchmark {
    async fn setup(&self, store: Arc<dyn ObjectStore>) {
        let path = PATH_PARENT.child("get0");
        upload_multipart(store.clone(), path, self.data.clone())
            .await
            .unwrap();
    }

    async fn run(
        &self,
        store: Arc<dyn ObjectStore>,
        worker: usize,
        iteration: usize,
        concurrency: usize,
    ) -> BenchmarkResult {
        let path = Path::parse("fsprof/get0").unwrap();
        let start = Instant::now();
        let offset = worker * self.read_size;
        let mut error = false;
        if let Err(_) = store
            .get_range(&path, offset..offset + self.read_size)
            .await
        {
            error = true;
        }
        BenchmarkResult {
            operation: "get".to_string(),
            worker,
            iteration,
            concurrency,
            start,
            end: Instant::now(),
            bytes_transferred: 1024,
            error: error,
        }
    }
}

struct ListBenchmark {}

impl ListBenchmark {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Benchmark for ListBenchmark {
    async fn setup(&self, store: Arc<dyn ObjectStore>) {
        let handles = (0..1000).map(|worker| {
            let path = PATH_PARENT.child(format!("list/{}", worker));
            let store = store.clone();
            let data = Arc::new(bytes::Bytes::from(vec![0; 1024]));
            tokio::task::spawn(async move {
                store
                    .put_opts(
                        &path,
                        PutPayload::from_iter(data.iter().cloned()),
                        PutOptions::default(),
                    )
                    .await
                    .unwrap();
            })
        });

        for handle in handles {
            handle.await.unwrap();
        }
    }

    async fn run(
        &self,
        store: Arc<dyn ObjectStore>,
        worker: usize,
        iteration: usize,
        concurrency: usize,
    ) -> BenchmarkResult {
        let start = Instant::now();
        let mut error = false;

        let mut listing = store.list(Some(&PATH_PARENT)).take(1000);

        while let Some(item) = listing.next().await {
            if let Err(_) = item {
                error = true;
                break;
            }
        }

        BenchmarkResult {
            operation: "list".to_string(),
            worker,
            iteration,
            concurrency,
            start,
            end: Instant::now(),
            bytes_transferred: 0,
            error,
        }
    }
}

struct CopyBenchmark {
    data: Arc<bytes::Bytes>,
    file_size: usize,
}

impl CopyBenchmark {
    fn new(file_size: usize) -> Self {
        let mut buf = vec![0; file_size];
        let mut rng = rand::rngs::OsRng;
        rng.fill_bytes(&mut buf);
        Self {
            data: Arc::new(bytes::Bytes::from(buf)),
            file_size: file_size,
        }
    }
}

#[async_trait]
impl Benchmark for CopyBenchmark {
    async fn setup(&self, store: Arc<dyn ObjectStore>) {
        let path = PATH_PARENT.child("copy/000");
        upload_multipart(store.clone(), path, self.data.clone())
            .await
            .unwrap();
    }

    async fn run(
        &self,
        store: Arc<dyn ObjectStore>,
        worker: usize,
        iteration: usize,
        concurrency: usize,
    ) -> BenchmarkResult {
        let start = Instant::now();
        let path = PATH_PARENT.child("copy/000");
        let new_path = PATH_PARENT.child(format!("copy/{}", worker));

        let mut error = false;
        if let Err(_) = store.copy(&path, &new_path).await {
            error = true;
        }
        BenchmarkResult {
            operation: "copy".to_string(),
            worker,
            iteration,
            concurrency,
            start,
            end: Instant::now(),
            bytes_transferred: self.file_size,
            error,
        }
    }
}

async fn upload_multipart(
    store: Arc<dyn ObjectStore>,
    path: Path,
    data: Arc<bytes::Bytes>,
) -> Result<(), bool> {
    let mut write = store.put_multipart(&path).await.unwrap();
    let mut remaining = data.len();
    while remaining > 0 {
        let batch_size = remaining.min(CHUNK_SIZE);
        let offset = data.len() - remaining;
        let payload = PutPayload::from_bytes(data.slice(offset..offset + batch_size).into());
        write.put_part(payload).await.unwrap();
        remaining -= batch_size;
    }
    if let Err(_) = write.complete().await {
        return Err(true);
    }
    Ok(())
}

#[derive(Parser)]
struct Config {
    #[arg(long = "bucket-name")]
    bucket_name: String,

    #[arg(long = "s3-endpoint")]
    s3_endpoint: Option<String>,

    #[arg(long = "provider")]
    provider: String,

    #[arg(long = "azure-storage-account")]
    azure_storage_account: Option<String>,

    #[arg(long = "azure-storage-access-key")]
    azure_storage_access_key: Option<String>,

    #[arg(long = "small-file-size")]
    small_file_size: usize,

    #[arg(long = "large-file-size")]
    large_file_size: usize,
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let config = Arc::new(Config::parse());

    let store: Arc<dyn object_store::ObjectStore> = match config.provider.as_str() {
        "s3" => {
            let mut builder = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(&config.bucket_name)
                .with_allow_http(true);
            builder = if let Some(endpoint) = &config.s3_endpoint {
                builder.with_endpoint(endpoint)
            } else {
                builder
            };

            Arc::new(builder.build().unwrap())
        }
        "azure" => {
            let builder = MicrosoftAzureBuilder::new()
                .with_account(config.azure_storage_account.clone().unwrap())
                .with_access_key(config.azure_storage_access_key.clone().unwrap())
                .with_container_name(config.bucket_name.clone());

            Arc::new(builder.build().unwrap())
        }

        _ => panic!("Unknown provider"),
    };

    println!("Multipart uploads");
    let mut results = Vec::new();
    results.append(&mut rt.block_on(driver(
        store.clone(),
        vec![1, 2, 4],
        &mut Arc::new(MultipartUploadBenchmark::new(config.large_file_size)),
    )));

    println!("Puts");
    results.append(&mut rt.block_on(driver(
        store.clone(),
        vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
        &mut Arc::new(PutBenchmark::new(config.small_file_size)),
    )));

    println!("Gets");
    results.append(&mut rt.block_on(driver(
        store.clone(),
        vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
        &mut Arc::new(GetBenchmark::new(config.small_file_size)),
    )));

    println!("Copy");
    results.append(&mut rt.block_on(driver(
        store.clone(),
        vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
        &mut Arc::new(CopyBenchmark::new(config.small_file_size)),
    )));

    println!("List");
    results.append(&mut rt.block_on(driver(
        store.clone(),
        vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
        &mut Arc::new(ListBenchmark::new()),
    )));

    let mut buf = BufWriter::new(Vec::new());

    write!(
        buf,
        "op,worker,iteration,concurrency,elapsed,bytes_transferred,error\n"
    )
    .unwrap();

    for result in results {
        write!(
            buf,
            "{},{},{},{},{},{},{}\n",
            result.operation,
            result.worker,
            result.iteration,
            result.concurrency,
            result.end.duration_since(result.start).as_secs_f64(),
            result.bytes_transferred,
            result.error,
        )
        .unwrap();
    }

    let path = PATH_PARENT.child("results.csv");

    rt.block_on(async {
        store
            .put_opts(
                &path,
                PutPayload::from_iter(buf.get_ref().iter().cloned()),
                PutOptions::default(),
            )
            .await
            .unwrap();
    });
}
