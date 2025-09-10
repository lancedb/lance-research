use bytes::Bytes;
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use object_store::{local::LocalFileSystem, aws::AmazonS3Builder, ObjectStore};
use rand::seq::SliceRandom;
use rand::Rng;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tracing::{info, warn};
use url::Url;

#[derive(Parser)]
#[command(name = "evaluate_filesystem")]
#[command(about = "Benchmark filesystem/object store performance with sequential and random reads")]
struct Args {
    #[arg(short, long, help = "Path or URI (supports local paths, file://, s3://bucket/prefix)")]
    path: String,
    
    #[arg(short = 'f', long, help = "Filesystem name for identification in results")]
    filesystem_name: String,
    
    #[arg(short = 'c', long, default_value = "16")]
    parallel_reads: usize,
    
    #[arg(short = 'n', long, default_value = "1000")]
    num_random_reads: usize,
    
    #[arg(short, long, default_value = "results/filesystem_benchmark.csv")]
    output: String,
    
    #[arg(short = 's', long, default_value = "4294967296")] // 4 GB default
    file_size: u64,
}

struct BenchmarkResult {
    filesystem_name: String,
    parallel_reads: usize,
    read_size: usize,
    sequential_bandwidth_mbps: f64,
    random_bandwidth_mbps: f64,
}

fn clear_page_cache() -> Result<(), Box<dyn std::error::Error>> {
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
            Ok(())
        }
        Err(e) => {
            warn!("Failed to clear page cache (may require root): {}", e);
            warn!("Continuing without cache clearing - results may be affected by caching");
            Ok(()) // Don't fail the benchmark, just warn
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    info!("Starting filesystem benchmark with path: {}", args.path);
    
    // Create object store from path/URI
    let object_store = create_object_store(&args.path).await?;
    
    // Ensure test file exists - include prefix for S3 URLs
    let test_file_path = if args.path.starts_with("s3://") {
        if let Ok(url) = Url::parse(&args.path) {
            let prefix = url.path().trim_start_matches('/');
            if prefix.is_empty() {
                "benchmark_test_file.bin".to_string()
            } else {
                format!("{}/benchmark_test_file.bin", prefix)
            }
        } else {
            "benchmark_test_file.bin".to_string()
        }
    } else {
        "benchmark_test_file.bin".to_string()
    };
    
    ensure_test_file(&object_store, &test_file_path, args.file_size).await?;
    
    let mut results = Vec::new();
    
    // Sequential 32 MiB read benchmark - run 5 times and average
    info!("Running sequential 32 MiB read benchmark (5 iterations)");
    let mut sequential_results = Vec::new();
    for i in 1..=5 {
        clear_page_cache()?;
        info!("Sequential benchmark iteration {}/5", i);
        let bandwidth = benchmark_sequential_reads(
            &object_store,
            &test_file_path,
            32 * 1024 * 1024, // 32 MiB
            args.file_size,
            args.parallel_reads,
        ).await?;
        sequential_results.push(bandwidth);
    }
    let sequential_bandwidth = sequential_results.iter().sum::<f64>() / sequential_results.len() as f64;
    info!("Sequential benchmark average: {:.2} MB/s", sequential_bandwidth);
    
    // Random read benchmarks with different sizes
    let read_sizes = generate_read_sizes();
    
    for &read_size in &read_sizes {
        info!("Running random read benchmark with {} byte reads (5 iterations)", read_size);
        let mut random_results = Vec::new();
        for i in 1..=5 {
            clear_page_cache()?;
            info!("Random benchmark (size {}) iteration {}/5", read_size, i);
            let bandwidth = benchmark_random_reads(
                &object_store,
                &test_file_path,
                read_size,
                args.file_size,
                args.parallel_reads,
                args.num_random_reads,
            ).await?;
            random_results.push(bandwidth);
        }
        let random_bandwidth = random_results.iter().sum::<f64>() / random_results.len() as f64;
        info!("Random benchmark (size {}) average: {:.2} MB/s", read_size, random_bandwidth);
        
        results.push(BenchmarkResult {
            filesystem_name: args.filesystem_name.clone(),
            parallel_reads: args.parallel_reads,
            read_size,
            sequential_bandwidth_mbps: sequential_bandwidth,
            random_bandwidth_mbps: random_bandwidth,
        });
    }
    
    // Write results to CSV
    write_results_to_csv(&results, &args.output).await?;
    
    info!("Benchmark completed. Results written to {}", args.output);
    
    Ok(())
}

async fn create_object_store(path: &str) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    if let Ok(url) = Url::parse(path) {
        match url.scheme() {
            "file" => {
                let local_path = url.to_file_path().map_err(|_| "Invalid file URL")?;
                let store = LocalFileSystem::new_with_prefix(local_path)?;
                Ok(Arc::new(store))
            },
            "s3" => {
                let bucket = url.host_str().ok_or("Invalid S3 URL: missing bucket name")?;
                let prefix = url.path().trim_start_matches('/');
                
                info!("Connecting to S3 bucket: {}, prefix: {}", bucket, prefix);
                
                let builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket);
                
                // Build the S3 store - credentials will be picked up from environment
                // (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, etc.)
                let store = builder.build()?;
                
                Ok(Arc::new(store))
            },
            scheme => {
                Err(format!("Unsupported URL scheme: {}. Supported schemes: file, s3", scheme).into())
            }
        }
    } else {
        // Treat as local file path
        let local_path = Path::new(path);
        if !local_path.exists() {
            fs::create_dir_all(local_path).await?;
        }
        let store = LocalFileSystem::new_with_prefix(local_path)?;
        Ok(Arc::new(store))
    }
}

async fn ensure_test_file(
    object_store: &Arc<dyn ObjectStore>,
    file_path: &str,
    target_size: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = object_store::path::Path::from(file_path);
    
    // Check if file exists and has correct size
    match object_store.head(&path).await {
        Ok(meta) => {
            if meta.size as u64 >= target_size {
                info!("Test file already exists with correct size");
                return Ok(());
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
        object_store.put(&path, file_bytes.into()).await?;
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
        object_store.put(&path, file_bytes.into()).await?;
    }
    
    info!("Test file created successfully");
    Ok(())
}

async fn benchmark_sequential_reads(
    object_store: &Arc<dyn ObjectStore>,
    file_path: &str,
    read_size: usize,
    file_size: u64,
    parallel_reads: usize,
) -> Result<f64, Box<dyn std::error::Error>> {
    let path = object_store::path::Path::from(file_path);
    let num_reads = file_size.div_ceil(read_size as u64);
    
    let start_time = Instant::now();
    
    let mut futures = FuturesUnordered::new();
    let mut current_offset = 0u64;
    let mut active_reads = 0;
    let mut completed_reads = 0u64;
    
    while completed_reads < num_reads {
        // Launch new reads up to parallel limit
        while active_reads < parallel_reads && current_offset < file_size {
            let remaining = file_size - current_offset;
            let current_read_size = std::cmp::min(read_size as u64, remaining) as usize;
            
            let future = object_store.get_range(&path, current_offset as usize..(current_offset as usize + current_read_size));
            futures.push(future);
            
            current_offset += current_read_size as u64;
            active_reads += 1;
        }
        
        // Wait for at least one read to complete
        if let Some(result) = futures.next().await {
            match result {
                Ok(_) => completed_reads += 1,
                Err(e) => warn!("Read failed: {}", e),
            }
            active_reads -= 1;
        }
    }
    
    let duration = start_time.elapsed();
    let total_bytes = file_size;
    let bandwidth_mbps = (total_bytes as f64) / (1024.0 * 1024.0) / duration.as_secs_f64();
    
    info!("Sequential reads completed: {} MB/s", bandwidth_mbps);
    Ok(bandwidth_mbps)
}

async fn benchmark_random_reads(
    object_store: &Arc<dyn ObjectStore>,
    file_path: &str,
    read_size: usize,
    file_size: u64,
    parallel_reads: usize,
    num_reads: usize,
) -> Result<f64, Box<dyn std::error::Error>> {
    let path = object_store::path::Path::from(file_path);
    // Generate non-overlapping random offsets
    let offsets = generate_non_overlapping_offsets(file_size, read_size, num_reads)?;
    
    let start_time = Instant::now();
    
    let mut futures = FuturesUnordered::new();
    let mut offset_idx = 0;
    let mut active_reads = 0;
    let mut completed_reads = 0;
    
    while completed_reads < num_reads {
        // Launch new reads up to parallel limit
        while active_reads < parallel_reads && offset_idx < offsets.len() {
            let offset = offsets[offset_idx];
            let future = object_store.get_range(&path, offset as usize..(offset as usize + read_size));
            futures.push(future);
            
            offset_idx += 1;
            active_reads += 1;
        }
        
        // Wait for at least one read to complete
        if let Some(result) = futures.next().await {
            match result {
                Ok(_) => completed_reads += 1,
                Err(e) => warn!("Random read failed: {}", e),
            }
            active_reads -= 1;
        }
    }
    
    let duration = start_time.elapsed();
    let total_bytes = num_reads * read_size;
    let bandwidth_mbps = (total_bytes as f64) / (1024.0 * 1024.0) / duration.as_secs_f64();
    
    info!("Random reads completed: {} MB/s", bandwidth_mbps);
    Ok(bandwidth_mbps)
}

fn generate_non_overlapping_offsets(
    file_size: u64,
    read_size: usize,
    num_reads: usize,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let read_size_u64 = read_size as u64;
    let max_possible_reads = file_size / read_size_u64;
    
    if num_reads as u64 > max_possible_reads {
        return Err(format!(
            "Cannot generate {} non-overlapping reads of size {} in file of size {}. Max possible: {}",
            num_reads, read_size, file_size, max_possible_reads
        ).into());
    }
    
    // Generate all possible non-overlapping starting positions
    let mut possible_offsets = Vec::new();
    let mut offset = 0u64;
    while offset + read_size_u64 <= file_size {
        possible_offsets.push(offset);
        offset += read_size_u64;
    }
    
    // Randomly select the required number of offsets
    let total_possible = possible_offsets.len();
    let mut rng = rand::thread_rng();
    possible_offsets.shuffle(&mut rng);
    possible_offsets.truncate(num_reads);
    
    info!("Generated {} non-overlapping offsets from {} possible positions", 
          num_reads, total_possible);
    
    Ok(possible_offsets)
}

fn generate_read_sizes() -> Vec<usize> {
    let mut sizes = Vec::new();
    let mut size = 128; // Start at 128 bytes
    while size <= 128 * 1024 * 1024 { // Up to 128 MiB
        sizes.push(size);
        size *= 2;
    }
    sizes
}


async fn write_results_to_csv(
    results: &[BenchmarkResult],
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut csv_content = String::new();
    csv_content.push_str("filesystem_name,parallel_reads,read_size,sequential_bandwidth_mbps,random_bandwidth_mbps\n");
    
    for result in results {
        csv_content.push_str(&format!(
            "{},{},{},{:.2},{:.2}\n",
            result.filesystem_name,
            result.parallel_reads,
            result.read_size,
            result.sequential_bandwidth_mbps,
            result.random_bandwidth_mbps
        ));
    }
    
    // Ensure output directory exists
    if let Some(parent) = Path::new(output_path).parent() {
        fs::create_dir_all(parent).await?;
    }
    
    fs::write(output_path, csv_content).await?;
    Ok(())
}