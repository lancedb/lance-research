use arrow_array::RecordBatchReader;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;
use tracing::{info, warn};
use utilities::DatasetRegistry;

#[derive(Parser, Debug)]
#[command(name = "write_memory_experiment")]
#[command(
    about = "Measures memory usage when writing parquet files with different row group sizes"
)]
struct Args {
    /// Dataset short name to use for testing
    #[arg(short, long, default_value = "fineweb")]
    dataset: String,

    /// Output CSV file path
    #[arg(short, long, default_value = "results/write_memory.csv")]
    output: PathBuf,

    /// Minimum row group size (power of 2)
    #[arg(long, default_value = "128")]
    min_row_group_size: usize,

    /// Maximum row group size (power of 2)
    #[arg(long, default_value = "1048576")] // 1Mi = 2^20
    max_row_group_size: usize,
}

struct MemoryStats {
    peak_rss_kb: usize,
    requested_row_group_size: usize,
    actual_max_row_group_size: usize,
    dataset_name: String,
}

fn get_current_rss_kb() -> Result<usize, std::io::Error> {
    let pid = unsafe { libc::getpid() };
    let status_path = format!("/proc/{}/status", pid);
    let status = std::fs::read_to_string(status_path)?;

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return parts[1].parse().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to parse RSS")
                });
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "VmRSS not found",
    ))
}

async fn measure_write_memory(
    input_path: &PathBuf,
    row_group_size: usize,
    dataset_name: &str,
) -> Result<MemoryStats, Box<dyn std::error::Error>> {
    info!("Testing row group size: {}", row_group_size);

    // Create temporary directory for output
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("output.parquet");

    let initial_rss = get_current_rss_kb().unwrap_or(0);
    let mut peak_rss = initial_rss;

    // Read the input file
    let file = File::open(input_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    // Set up writer properties with specified row group size
    let props = WriterProperties::builder()
        .set_max_row_group_size(row_group_size)
        .build();

    let output_file = File::create(&output_path)?;
    let mut writer = ArrowWriter::try_new(output_file, reader.schema(), Some(props))?;

    // Process batches and track memory
    for batch_result in reader {
        let batch = batch_result?;
        writer.write(&batch)?;

        // Check current RSS
        if let Ok(current_rss) = get_current_rss_kb() {
            if current_rss > peak_rss {
                peak_rss = current_rss;
            }
        }
    }

    writer.close()?;

    // Final memory check
    if let Ok(current_rss) = get_current_rss_kb() {
        if current_rss > peak_rss {
            peak_rss = current_rss;
        }
    }

    let peak_above_initial = peak_rss.saturating_sub(initial_rss);

    // Read the footer to get actual row group sizes
    let output_file_read = File::open(&output_path)?;
    let parquet_reader = SerializedFileReader::new(output_file_read)?;
    let metadata = parquet_reader.metadata();
    
    let actual_max_row_group_size = metadata.row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .max()
        .unwrap_or(0);

    info!(
        "Row group size: {} (requested), {} (actual max), Peak RSS above initial: {} KB",
        row_group_size, actual_max_row_group_size, peak_above_initial
    );

    Ok(MemoryStats {
        peak_rss_kb: peak_above_initial,
        requested_row_group_size: row_group_size,
        actual_max_row_group_size,
        dataset_name: dataset_name.to_string(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    
    // Get dataset from registry
    let registry = DatasetRegistry::default_registry();
    let dataset = registry.get_dataset(&args.dataset)
        .ok_or_else(|| format!("Dataset '{}' not found. Available datasets: {:?}", 
                              args.dataset, registry.dataset_names()))?;

    info!("Starting memory experiment with dataset: {} ({})", 
          dataset.name, dataset.path.display());
    info!(
        "Row group size range: {} to {}",
        args.min_row_group_size, args.max_row_group_size
    );

    let mut results = Vec::new();
    let mut current_size = args.min_row_group_size;

    // Do a warmup pass with the smallest row group size to hide reader memory usage
    info!("Performing warmup pass to hide reader memory overhead...");
    match measure_write_memory(&dataset.path, current_size, &dataset.name).await {
        Ok(_) => {
            info!("Warmup pass completed, starting actual measurements");
        }
        Err(e) => {
            warn!("Warmup pass failed: {}, continuing anyway", e);
        }
    }

    // Test powers of 2 from min to max
    while current_size <= args.max_row_group_size {
        match measure_write_memory(&dataset.path, current_size, &dataset.name).await {
            Ok(stats) => {
                results.push(stats);
            }
            Err(e) => {
                warn!(
                    "Failed to measure memory for row group size {}: {}",
                    current_size, e
                );
            }
        }

        current_size *= 2;
    }

    // Print results
    println!("\n=== Memory Usage Results ===");
    println!("Dataset\t\tRequested Size\tActual Max Size\tPeak RSS (KB)");
    println!("-------\t\t--------------\t---------------\t-------------");
    for result in &results {
        println!("{}\t\t{}\t\t{}\t\t{}", 
                result.dataset_name,
                result.requested_row_group_size, 
                result.actual_max_row_group_size, 
                result.peak_rss_kb);
    }

    // Find optimal size (minimum memory usage)
    if let Some(optimal) = results.iter().min_by_key(|r| r.peak_rss_kb) {
        println!(
            "\nOptimal row group size: {} requested / {} actual (Peak RSS: {} KB)",
            optimal.requested_row_group_size, optimal.actual_max_row_group_size, optimal.peak_rss_kb
        );
    }

    // Write CSV output
    write_csv_results(&args.output, &results)?;
    info!("Results written to: {}", args.output.display());

    Ok(())
}

fn write_csv_results(output_path: &PathBuf, results: &[MemoryStats]) -> Result<(), Box<dyn std::error::Error>> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = File::create(output_path)?;
    
    // Write CSV header
    writeln!(file, "dataset,requested_row_group_size,actual_max_row_group_size,peak_rss_kb")?;
    
    // Write data rows
    for result in results {
        writeln!(
            file,
            "{},{},{},{}",
            result.dataset_name,
            result.requested_row_group_size,
            result.actual_max_row_group_size,
            result.peak_rss_kb
        )?;
    }
    
    Ok(())
}
