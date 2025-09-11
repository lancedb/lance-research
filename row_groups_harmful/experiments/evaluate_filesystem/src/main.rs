use bytes::Bytes;
use clap::Parser;
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use rand::seq::SliceRandom;
use rand::Rng;
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;
use tokio::fs;
use tracing::{info, warn};
use url::Url;

#[derive(Parser)]
#[command(name = "evaluate_filesystem")]
#[command(about = "Benchmark filesystem/object store performance with sequential and random reads")]
struct Args {
    #[arg(
        short,
        long,
        help = "Path or URI (supports local paths, file://, s3://bucket/prefix)"
    )]
    path: String,

    #[arg(
        short = 'f',
        long,
        help = "Filesystem name for identification in results"
    )]
    filesystem_name: String,

    #[arg(short = 'c', long, default_value = "16")]
    parallel_reads: usize,

    #[arg(short, long, default_value = "results/filesystem_benchmark.csv")]
    output: String,

    #[arg(short = 's', long, default_value = "4294967296")] // 4 GB default
    file_size: u64,
}

#[derive(Clone)]
struct BenchmarkResult {
    filesystem_name: String,
    parallel_reads: usize,
    read_size: usize,
    random_bandwidth_mbps: f64,
}

trait BenchmarkJob: Send + Sync {
    fn ensure_test_file(&mut self, target_size: u64);
    fn run(&self, ranges: Arc<[Range<u64>]>, counter: Arc<AtomicUsize>);
}

struct ObjectStoreJob {
    object_store: Arc<dyn ObjectStore>,
    file_path: String,
    runtime: tokio::runtime::Runtime,
}

impl ObjectStoreJob {
    fn new(object_store: Arc<dyn ObjectStore>, base_path: String) -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let file_path = format!("{}/benchmark_test_file.bin", base_path);
        Self {
            object_store,
            file_path,
            runtime,
        }
    }
}

impl BenchmarkJob for ObjectStoreJob {
    fn ensure_test_file(&mut self, target_size: u64) {
        self.runtime.block_on(async {
            let path = object_store::path::Path::from(self.file_path.as_str());

            // Check if file exists and has correct size
            match self.object_store.head(&path).await {
                Ok(meta) => {
                    if meta.size as u64 >= target_size {
                        info!("Test file already exists with correct size");
                        return;
                    } else {
                        info!("Test file exists but is too small, recreating");
                    }
                }
                Err(_) => {
                    info!("Test file doesn't exist, creating");
                }
            }

            // Create the file
            info!("Creating test file of {} bytes", target_size);

            let mut rng = rand::thread_rng();

            // For reasonable sized files (up to 1GB), create in memory
            if target_size <= 1024 * 1024 * 1024 {
                let mut file_data = vec![0u8; target_size as usize];
                rng.fill(&mut file_data[..]);
                let file_bytes = Bytes::from(file_data);
                self.object_store
                    .put(&path, file_bytes.into())
                    .await
                    .unwrap();
            } else {
                // For larger files, create using streaming approach
                // This is a simplified approach - a production system would use multipart uploads
                let chunk_size = 64 * 1024 * 1024; // 64 MB chunks
                let mut all_data = Vec::with_capacity(target_size as usize);

                let mut remaining = target_size;
                while remaining > 0 {
                    let current_chunk_size = std::cmp::min(chunk_size, remaining) as usize;
                    let mut chunk_data = vec![0u8; current_chunk_size];
                    rng.fill(&mut chunk_data[..]);
                    all_data.extend_from_slice(&chunk_data);
                    remaining -= current_chunk_size as u64;

                    if all_data.len() % (chunk_size as usize * 10) == 0 {
                        info!("Generated {} / {} bytes", all_data.len(), target_size);
                    }
                }

                let file_bytes = Bytes::from(all_data);
                self.object_store
                    .put(&path, file_bytes.into())
                    .await
                    .unwrap();
            }

            info!("Test file created successfully");
        })
    }

    fn run(&self, ranges: Arc<[Range<u64>]>, counter: Arc<AtomicUsize>) {
        let runtime = &self.runtime;

        let path = object_store::path::Path::from(self.file_path.as_str());

        runtime.block_on(async {
            loop {
                // Atomically get the next range index
                let index = counter.fetch_add(1, Ordering::Relaxed);

                // Check if we've processed all ranges
                if index >= ranges.len() {
                    break;
                }

                // Get the range to read
                let range = &ranges[index];

                // Perform the read
                self.object_store
                    .get_range(&path, range.start as usize..range.end as usize)
                    .await
                    .unwrap();
            }
        });
    }
}

struct StdFileJob {
    file_path: String,
    file: Option<File>,
}

impl StdFileJob {
    fn new(base_path: String) -> Self {
        let file_path = format!("{}/benchmark_test_file.bin", base_path);

        Self {
            file_path,
            file: None,
        }
    }
}

impl BenchmarkJob for StdFileJob {
    fn ensure_test_file(&mut self, target_size: u64) {
        let path = Path::new(self.file_path.as_str());

        // Check if file exists and has correct size
        if let Ok(metadata) = std::fs::metadata(path) {
            if metadata.len() == target_size {
                info!("Test file already exists with correct size");
                self.file = Some(File::open(path).unwrap());
                return;
            } else {
                info!("Test file exists but is too small, recreating");
            }
        }

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        info!("Creating test file of {} bytes", target_size);

        // Create the file with random data
        let mut file = File::create(path).unwrap();

        let mut rng = rand::thread_rng();

        if target_size <= 100 * 1024 * 1024 {
            // 100 MB
            // For smaller files, create all data in memory
            let mut data = vec![0u8; target_size as usize];
            rng.fill(&mut data[..]);
            std::io::Write::write_all(&mut file, &data).unwrap();
        } else {
            // For larger files, create using streaming approach
            let chunk_size = 64 * 1024 * 1024; // 64 MB chunks
            let mut remaining = target_size;

            while remaining > 0 {
                let current_chunk_size = std::cmp::min(chunk_size, remaining) as usize;
                let mut chunk_data = vec![0u8; current_chunk_size];
                rng.fill(&mut chunk_data[..]);
                std::io::Write::write_all(&mut file, &chunk_data).unwrap();
                remaining -= current_chunk_size as u64;

                if (target_size - remaining) % (chunk_size * 10) == 0 {
                    info!(
                        "Generated {} / {} bytes",
                        target_size - remaining,
                        target_size
                    );
                }
            }
        }

        drop(file);
        self.file = Some(File::open(path).unwrap());
        info!("Test file created successfully");
    }

    fn run(&self, ranges: Arc<[Range<u64>]>, counter: Arc<AtomicUsize>) {
        let file = self.file.as_ref().unwrap();
        let mut buffer = vec![0u8; (ranges[0].end - ranges[0].start) as usize];

        loop {
            // Atomically get the next range index
            let index = counter.fetch_add(1, Ordering::Relaxed);

            // Check if we've processed all ranges
            if index >= ranges.len() {
                break;
            }

            // Get the range to read
            let range = &ranges[index];

            // Perform the read using std::fs
            file.read_exact_at(&mut buffer, range.start).unwrap();
        }
    }
}

fn clear_page_cache() {
    info!("Clearing kernel page cache");

    // Try to clear page cache by syncing and dropping caches
    // This requires root privileges or appropriate capabilities
    unsafe {
        // sync() - flush filesystem buffers
        libc::sync();
    }

    // Try to write to /proc/sys/vm/drop_caches
    // 3 = drop page cache, dentries and inodes
    match std::fs::write("/proc/sys/vm/drop_caches", "3") {
        Ok(_) => {
            info!("Successfully cleared page cache");
        }
        Err(e) => {
            warn!("Failed to clear page cache (may require root): {}", e);
            warn!("Continuing without cache clearing - results may be affected by caching");
        }
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let args = Args::parse();

    info!("Starting filesystem benchmark with path: {}", args.path);

    // Determine job type and create appropriate resources
    let mut job: Box<dyn BenchmarkJob> = if let Ok(_url) = Url::parse(&args.path) {
        info!("Benchmarking with object_store");
        // It's a URL - use object store
        let (object_store, file_path) = runtime.block_on(create_object_store(&args.path));
        Box::new(ObjectStoreJob::new(object_store.clone(), file_path))
    } else {
        info!("Benchmarking with std file");
        // It's a local path - use std file
        Box::new(StdFileJob::new(args.path.clone()))
    };

    job.ensure_test_file(args.file_size);
    let job: Arc<dyn BenchmarkJob> = job.into();

    let mut results = Vec::new();

    // Random read benchmarks with different sizes
    let read_sizes = generate_read_sizes();

    for &read_size in &read_sizes {
        info!(
            "Running random read benchmark with {} byte reads (5 iterations)",
            read_size
        );
        let mut random_results = Vec::new();
        for i in 1..=5 {
            clear_page_cache();
            info!("Random benchmark (size {}) iteration {}/5", read_size, i);
            let bandwidth =
                benchmark_random_reads(&job, read_size, args.file_size, args.parallel_reads);
            random_results.push(bandwidth);
        }
        let random_bandwidth = random_results
            .iter()
            .map(|(bandwidth, _)| bandwidth)
            .sum::<f64>()
            / random_results.len() as f64;
        let random_iops =
            random_results.iter().map(|(_, iops)| iops).sum::<f64>() / random_results.len() as f64;
        info!(
            "Random benchmark (size {}) average: {:.2} MB/s, {:.2} IOPS",
            read_size, random_bandwidth, random_iops
        );

        results.push(BenchmarkResult {
            filesystem_name: args.filesystem_name.clone(),
            parallel_reads: args.parallel_reads,
            read_size,
            random_bandwidth_mbps: random_bandwidth,
        });
    }

    // Write results to CSV
    runtime.block_on(write_results_to_csv(&results, &args.output));

    info!("Benchmark completed. Results written to {}", args.output);
}

async fn create_object_store(path: &str) -> (Arc<dyn ObjectStore>, String) {
    if let Ok(url) = Url::parse(path) {
        match url.scheme() {
            "file" => {
                let local_path = url.to_file_path().unwrap();
                let store = LocalFileSystem::new();
                (Arc::new(store), local_path.to_str().unwrap().to_string())
            }
            "s3" => {
                let bucket = url.host_str().unwrap();
                let prefix = url.path().trim_start_matches('/');

                info!("Connecting to S3 bucket: {}, prefix: {}", bucket, prefix);

                let builder = AmazonS3Builder::new().with_bucket_name(bucket);

                // Build the S3 store - credentials will be picked up from environment
                // (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, etc.)
                let store = builder.build().unwrap();

                (Arc::new(store), prefix.to_string())
            }
            _ => panic!("Unsupported URL scheme"),
        }
    } else {
        // Treat as local file path
        let local_path = Path::new(path);
        if !local_path.exists() {
            tokio::fs::create_dir_all(local_path).await.unwrap();
        }
        let store = LocalFileSystem::new();
        (Arc::new(store), path.to_string())
    }
}

fn benchmark_random_reads(
    job: &Arc<dyn BenchmarkJob>,
    read_size: usize,
    file_size: u64,
    parallel_reads: usize,
) -> (f64, f64) {
    // Generate non-overlapping random ranges
    let ranges = generate_non_overlapping_ranges(file_size, read_size);

    let num_reads = ranges.len();
    if num_reads < parallel_reads * 4 {
        panic!(
            "File not large enough to run {} parallel reads of {} bytes.  Can only find {} ranges but need at least {}",
            parallel_reads, read_size, num_reads, parallel_reads * 4
        );
    }

    // Create atomic counter for coordinating work
    let counter = Arc::new(AtomicUsize::new(0));

    // Start timing
    let start_time = Instant::now();

    // Spawn threads to run jobs
    let mut handles = Vec::new();
    for _ in 0..parallel_reads {
        let ranges_clone = ranges.clone();
        let counter_clone = counter.clone();

        let thread_job = job.clone();
        let handle = thread::spawn(move || thread_job.run(ranges_clone, counter_clone));
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start_time.elapsed();
    let total_bytes = (num_reads * read_size) as u64;
    let bandwidth_mbps = (total_bytes as f64) / (1024.0 * 1024.0) / duration.as_secs_f64();
    let iops_per_second = (num_reads as f64) / duration.as_secs_f64();

    (bandwidth_mbps, iops_per_second)
}

fn generate_non_overlapping_ranges(file_size: u64, read_size: usize) -> Arc<[Range<u64>]> {
    let read_size_u64 = read_size as u64;
    // Ensure step is at least 4KB so reads are sector-aligned and to speed up smaller read size tests
    let step = read_size_u64.max(4 * 1024);
    let num_reads = (file_size / step) as usize;

    // Generate all possible non-overlapping starting positions
    let mut possible_offsets = Vec::new();
    let mut offset = 0u64;
    while offset + step <= file_size {
        possible_offsets.push(offset);
        offset += step;
    }

    // Randomly select the required number of offsets
    let total_possible = possible_offsets.len();
    let mut rng = rand::thread_rng();
    possible_offsets.shuffle(&mut rng);
    possible_offsets.truncate(num_reads);

    // Convert offsets to ranges
    let ranges: Vec<Range<u64>> = possible_offsets
        .into_iter()
        .map(|offset| offset..(offset + read_size_u64))
        .collect();

    info!(
        "Generated {} non-overlapping ranges from {} possible positions",
        num_reads, total_possible
    );

    ranges.into()
}

fn generate_read_sizes() -> Vec<usize> {
    // For testing the new job-based approach
    (7..=24).map(|i| (1_u32 << i) as usize).collect()
}

async fn write_results_to_csv(new_results: &[BenchmarkResult], output_path: &str) {
    // Get the filesystem name we're updating
    let current_filesystem = &new_results[0].filesystem_name;
    let current_parallel_reads = new_results[0].parallel_reads;

    // Read existing results if the file exists
    let mut all_results = Vec::new();

    if Path::new(output_path).exists() {
        let existing_content = fs::read_to_string(output_path).await.unwrap();
        let mut lines = existing_content.lines();

        // Skip header line
        if let Some(_header) = lines.next() {
            // Parse existing results, filtering out the current filesystem
            for line in lines {
                if line.trim().is_empty() {
                    continue;
                }

                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 4 {
                    let filesystem_name = parts[0].to_string();
                    let parallel_reads: usize = parts[1].parse().unwrap_or(0);

                    // Keep results from other filesystems or different parallel_reads configs
                    if filesystem_name != *current_filesystem
                        || parallel_reads != current_parallel_reads
                    {
                        let read_size: usize = parts[2].parse().unwrap_or(0);
                        let random_bandwidth_mbps: f64 = parts[3].parse().unwrap_or(0.0);

                        all_results.push(BenchmarkResult {
                            filesystem_name,
                            parallel_reads,
                            read_size,
                            random_bandwidth_mbps,
                        });
                    }
                }
            }
        }
    }

    // Add the new results
    all_results.extend_from_slice(new_results);

    // Sort results for consistent output (by filesystem, then parallel_reads, then read_size)
    all_results.sort_by(|a, b| {
        a.filesystem_name
            .cmp(&b.filesystem_name)
            .then(a.parallel_reads.cmp(&b.parallel_reads))
            .then(a.read_size.cmp(&b.read_size))
    });

    // Write the combined results
    let mut csv_content = String::new();
    csv_content.push_str("filesystem_name,parallel_reads,read_size,random_bandwidth_mbps\n");

    for result in &all_results {
        csv_content.push_str(&format!(
            "{},{},{},{:.2}\n",
            result.filesystem_name,
            result.parallel_reads,
            result.read_size,
            result.random_bandwidth_mbps
        ));
    }

    // Ensure output directory exists
    if let Some(parent) = Path::new(output_path).parent() {
        fs::create_dir_all(parent).await.unwrap();
    }

    fs::write(output_path, csv_content).await.unwrap();
    info!(
        "Updated results in {} (replaced {} entries for {})",
        output_path,
        new_results.len(),
        current_filesystem
    );
}
