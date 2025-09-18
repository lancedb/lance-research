use std::collections::HashMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader,
};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_file::version::LanceFileVersion;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use rand::Rng;
use regex::Regex;
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

    /// Path to run_query binary (default: auto-detect)
    #[arg(long)]
    run_query_bin: Option<String>,
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

async fn create_synthetic_lance_dataset(
    path: &str,
    num_row_groups: usize,
    rows_per_group: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic lance dataset with {} rows at {}",
        num_row_groups * rows_per_group,
        path
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    Dataset::write(
        reader,
        path,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_1),
            max_rows_per_file: rows_per_group * num_row_groups,
            ..Default::default()
        }),
    )
    .await?;

    Ok(())
}

async fn create_synthetic_parquet_file(
    path: &str,
    num_row_groups: usize,
    rows_per_group: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Creating synthetic parquet file with {} row groups, {} rows each at {}",
        num_row_groups, rows_per_group, path
    );

    let reader = create_synthetic_data(rows_per_group, num_row_groups)?;

    // Configure writer properties for specific row group size
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(rows_per_group)
        .build();

    let file = std::fs::File::create(path)?;
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

fn parse_request(
    timestamp_str: &str,
    duration_str: &str,
    offset: u64,
    bytes_read: u64,
    first_timestamp: &mut Option<f64>,
    request_counter: u64,
) -> IoRequest {
    let time_parts: Vec<&str> = timestamp_str.split(':').collect();
    assert_eq!(time_parts.len(), 3);
    let hour: f64 = time_parts[0].parse().unwrap_or(0.0);
    let minute: f64 = time_parts[1].parse().unwrap_or(0.0);
    let second: f64 = time_parts[2].parse().unwrap_or(0.0);
    let timestamp_seconds = hour * 3600.0 + minute * 60.0 + second;

    // Calculate relative timestamp from first occurrence
    if first_timestamp.is_none() {
        println!("First timestamp: {}", timestamp_str);
        *first_timestamp = Some(timestamp_seconds);
    }
    let relative_timestamp = timestamp_seconds - first_timestamp.unwrap();
    let start_timestamp = (relative_timestamp * 1e9) as u64;

    // Convert duration to nanoseconds
    let duration_seconds: f64 = duration_str.parse().unwrap_or(0.0);
    let duration = (duration_seconds * 1e9) as u64;

    IoRequest {
        duration,
        start_timestamp,
        requests_in_flight: request_counter,
        path: "traced_file".to_string(),
        range_start: Some(offset),
        range_end: Some(offset + bytes_read),
    }
}

fn parse_strace_output(strace_output: &str) -> Vec<IoRequest> {
    let mut requests = Vec::new();

    // Regex to parse strace output for pread64 and read syscalls
    // Example: 1718304 14:32:15.123456 pread64(3, "...", 8192, 1048576) = 8192 <0.000123>
    // Example: 1718304 05:52:12.989134 lseek(9, 665722183, SEEK_SET) = 665722183 <0.000019>
    // Example: 1718301 05:52:12.989342 openat(AT_FDCWD, "/tmp/test.parquet", O_RDONLY|O_CLOEXEC) = 10 <0.000027>
    // Example: 1718304 05:52:12.989580 read(9, "..."..., 32768) = 32768 <0.000186>
    let read_re =
        Regex::new(r"\d+\s+(\d+:\d+:\d+\.\d+)\s+read\((\d+),.*\s+(\d+)\s+<([\d.]+)>").unwrap();
    let pread_re = Regex::new(
        r"\d+\s+(\d+:\d+:\d+\.\d+)\s+pread64\((\d+),.*\s+(\d+)\)\s+=\s+(\d+)\s+<([\d.]+)>",
    )
    .unwrap();
    let seek_re = Regex::new(r".*lseek\((\d+),\s+(\d+),.*").unwrap();
    let open_re = Regex::new(r#".*openat.*"(.*)".*=\s+(\d+).*"#).unwrap();

    let mut request_counter = 0;
    let mut first_timestamp: Option<f64> = None;
    let mut positions = HashMap::new();
    let mut open_fds = HashMap::new();

    for line in strace_output.lines() {
        if let Some(caps) = seek_re.captures(line) {
            let fd = &caps[1].parse::<u64>().unwrap_or(0);
            if !open_fds.contains_key(fd) {
                continue;
            }
            let offset = &caps[2].parse::<u64>().unwrap_or(0);
            positions.insert(*fd, *offset);
            println!("Seek to offset {} for fd {}", offset, fd);
        } else if let Some(caps) = open_re.captures(line) {
            let filename = &caps[1];
            let fd = caps[2].parse::<u64>().unwrap_or(0);
            if filename.ends_with("parquet") || filename.ends_with("lance") {
                println!("Open fd {}", fd);
                open_fds.insert(fd, fd);
            } else {
                println!("Open fd (non-data-file) {}", fd);
                open_fds.remove(&fd);
            }
        } else if let Some(caps) = read_re.captures(line) {
            let timestamp_str = &caps[1];
            let fd = caps[2].parse::<u64>().unwrap_or(0);
            if !open_fds.contains_key(&fd) {
                continue;
            }
            let bytes_read = &caps[3].parse::<u64>().unwrap_or(0);
            let duration_str = &caps[4];

            let offset = positions.get(&fd).copied().unwrap_or(0);
            positions.insert(fd, offset + *bytes_read);

            let request = parse_request(
                timestamp_str,
                duration_str,
                offset,
                *bytes_read,
                &mut first_timestamp,
                request_counter,
            );

            requests.push(request);
            request_counter += 1;

            println!(
                "Read {} bytes from offset {} for fd {}",
                bytes_read, offset, fd
            );
        } else if let Some(caps) = pread_re.captures(line) {
            let timestamp_str = &caps[1];
            let fd = caps[2].parse::<u64>().unwrap_or(0);
            if !open_fds.contains_key(&fd) {
                continue;
            }
            let offset = caps[3].parse::<u64>().unwrap_or(0);
            let bytes_read = caps[4].parse::<u64>().unwrap_or(0);
            let duration_str = &caps[5];

            let request = parse_request(
                timestamp_str,
                duration_str,
                offset,
                bytes_read,
                &mut first_timestamp,
                request_counter,
            );

            requests.push(request);
            request_counter += 1;

            println!(
                "Pread {} bytes from offset {} for fd {}",
                bytes_read, offset, fd
            );
        }
    }

    requests
}

async fn run_query_with_strace(
    run_query_bin: &str,
    file_path: &str,
    format: &str,
    partitions: usize,
) -> Result<Vec<IoRequest>, Box<dyn std::error::Error>> {
    // Create a temporary file for strace output
    let strace_output_file = tempfile::NamedTempFile::new()?;
    let strace_output_path = strace_output_file.path().to_string_lossy().to_string();

    // Clear disk cache before running the query
    clear_disk_cache()?;

    info!("Running query with strace tracking");

    // Run the query binary under strace
    let output = Command::new("strace")
        .env("LANCE_IO_THREADS", "32")
        .args(&[
            "-e",
            "pread64,read,lseek,open,openat", // Trace read and open syscalls
            "-T",                             // Show time spent in syscalls
            "-tt",                            // Show absolute timestamps with microseconds
            "-z",                             // Only show completed calls
            "-o",
            &strace_output_path, // Output to file
            "-f",                // Follow forks
            run_query_bin,       // The binary to trace
            "--file-path",
            file_path,
            "--format",
            format,
            "--partitions",
            &partitions.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.is_empty() {
        info!("Query stderr:\n{}", stderr);
    }

    if !output.status.success() {
        return Err(format!("Query execution failed: {}", stderr).into());
    }

    // Print stdout from the query execution
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.is_empty() {
        info!("Query output:\n{}", stdout);
    }

    // Read strace output
    let strace_output = tokio::fs::read_to_string(&strace_output_path)
        .await
        .unwrap_or_default();

    println!("Strace output:\n{}", strace_output);

    // Parse strace output
    let requests = parse_strace_output(&strace_output);

    Ok(requests)
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

fn find_run_query_binary() -> Result<String, Box<dyn std::error::Error>> {
    // Try to find the run_query binary in the same directory as the current executable
    let current_exe = std::env::current_exe()?;
    let current_dir = current_exe
        .parent()
        .ok_or("Cannot get executable directory")?;

    let run_query_path = current_dir.join("run_query");
    if run_query_path.exists() {
        return Ok(run_query_path.to_string_lossy().to_string());
    }

    // Try in target/release directory (for development)
    let target_release_path = Path::new("target/release/run_query");
    if target_release_path.exists() {
        return Ok(target_release_path.to_string_lossy().to_string());
    }

    // Try just "run_query" (hope it's in PATH)
    Ok("run_query".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Starting row group I/O parallelism benchmark");
    info!(
        "Configuration: {} row groups, {} rows per group, {} partitions, format: {}, base_path: {}",
        args.num_row_groups, args.rows_per_group, args.partitions, args.format, args.base_path
    );

    // Check if strace is available
    let strace_check = Command::new("strace").arg("--version").output();
    if strace_check.is_err() {
        return Err(
            "strace is not available. Please install strace to use this experiment.".into(),
        );
    }

    // Find the run_query binary
    let run_query_bin = args.run_query_bin.unwrap_or_else(|| {
        find_run_query_binary().unwrap_or_else(|_| {
            eprintln!("Warning: Could not auto-detect run_query binary, using 'run_query'");
            "run_query".to_string()
        })
    });

    info!("Using run_query binary: {}", run_query_bin);

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

        // Run query with strace tracking
        let requests =
            run_query_with_strace(&run_query_bin, &parquet_uri, "parquet", args.partitions).await?;

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

        // Run query with strace tracking
        let requests =
            run_query_with_strace(&run_query_bin, &lance_uri, "lance", args.partitions).await?;

        info!("Recorded {} I/O requests for Lance", requests.len());

        let lance_output = if args.format == "both" {
            args.output.replace(".csv", "_lance.csv")
        } else {
            args.output.clone()
        };
        write_io_results(&requests, &lance_output).await?;
    }

    info!("Benchmark completed successfully");
    Ok(())
}
