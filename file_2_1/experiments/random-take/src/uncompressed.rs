use std::{sync::Arc, time::Instant};

use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;
use clap::Parser;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use lance_core::cache::FileMetadataCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::{
    v2::{
        reader::{FileReader, FileReaderOptions},
        writer::{FileWriter, FileWriterOptions},
    },
    version::LanceFileVersion,
};
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    ReadBatchParams,
};
use object_store::path::Path;
use once_cell::sync::Lazy;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::WriterProperties,
};
use random_take_bench::{
    log, osutil::drop_path_from_cache, parq::io::WorkDir, FileFormat, SHOULD_LOG,
};
use tracing::Level;
use tracing_chrome::ChromeLayerBuilder;
use tracing_core::LevelFilter;
use tracing_subscriber::prelude::*;

fn get_test_categories() -> Vec<String> {
    let mut categories = Vec::new();
    for testfile in std::fs::read_dir("../compression/datafiles").unwrap() {
        let testfile = testfile.unwrap();
        let path = testfile.path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        if filename.ends_with("_trimmed.parquet") {
            let category = filename[..filename.len() - 16].to_string();
            categories.push(category);
        }
    }
    categories
}

fn src_path(category: &str) -> String {
    format!("../compression/datafiles/{}_trimmed.parquet", category)
}

fn dst_path(category: &str) -> String {
    format!("/tmp/{}_uncompressed.parquet", category)
}

async fn read_test_data(path: &str) -> RecordBatch {
    let file = tokio::fs::File::open(path).await.unwrap();
    let reader = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap();

    let batches = reader.try_collect::<Vec<_>>().await.unwrap();
    let schema = batches[0].schema().clone();
    concat_batches(&schema, batches.iter()).unwrap()
}

async fn write_uncompressed(src_path: &str, dest_path: &str) {
    let test_data = read_test_data(src_path).await;
    let writer = tokio::fs::File::create(dest_path).await.unwrap();
    let options = ArrowWriterOptions::default().with_properties(
        WriterProperties::builder()
            .set_bloom_filter_enabled(false)
            .set_max_row_group_size(test_data.num_rows())
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build(),
    );
    let mut writer =
        AsyncArrowWriter::try_new_with_options(writer, test_data.schema(), options).unwrap();

    writer.write(&test_data).await.unwrap();

    writer.finish().await.unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("category,size_bytes");
    for category in get_test_categories() {
        let src_path = src_path(&category);
        let dst_path = dst_path(&category);
        write_uncompressed(&src_path, &dst_path).await;
        let size = std::fs::File::open(&dst_path)
            .unwrap()
            .metadata()
            .unwrap()
            .len();
        println!("{},{}", category, size);
    }
}
