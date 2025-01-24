//! Utilities for working with Parquet files.

use arrow_array::RecordBatchReader;
use io::WorkDir;
use object_store::path::Path;
use parquet::{arrow::AsyncArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::{datagen::get_datagen, log, DataTypeChoice};

pub mod io;

/// If the parquet write batch size is too large then parquet will be unable to
/// create small pages because it only checks to close a page after each write batch
fn get_parquet_write_batch_size(page_size_kb: usize, data_type: DataTypeChoice) -> usize {
    let page_size = page_size_kb * 1024;
    let elem_size = match data_type {
        DataTypeChoice::Scalar => 8,
        DataTypeChoice::String => 12,
        DataTypeChoice::ScalarList => 12,
        DataTypeChoice::StringList => 16,
        DataTypeChoice::Vector => 3 * 1024,
        DataTypeChoice::VectorList => 5 * 3 * 1024,
        DataTypeChoice::Binary => 20 * 1024,
        DataTypeChoice::BinaryList => 5 * 20 * 1024,
    };
    (page_size / elem_size).max(1) as usize
}

pub fn parq_file_path(
    work_dir: &WorkDir,
    row_group_size: usize,
    page_size_kb: usize,
    chunk_index: usize,
    data_type: DataTypeChoice,
) -> Path {
    work_dir.child_path(&format!(
        "parquet_row_groups_{}_page_{}_kib_type_{}_chunk_{}.parquet",
        row_group_size,
        page_size_kb,
        data_type.as_str(),
        chunk_index,
    ))
}

pub async fn make_parquet_file(
    row_group_size: usize,
    page_size_kb: usize,
    rows_per_chunk: usize,
    chunk_index: usize,
    data_type: DataTypeChoice,
    work_dir: &WorkDir,
) {
    let dest_path = parq_file_path(
        work_dir,
        row_group_size,
        page_size_kb,
        chunk_index,
        data_type,
    );
    if work_dir.exists(&dest_path).await {
        log(format!("Using existing parquet test file at {}", dest_path));
        return;
    }

    let write_batch_size = get_parquet_write_batch_size(page_size_kb, data_type);

    log(format!("Creating new parquet test file at {} with {} rows per row group and {}kb per page and data_page_size_limit={}", dest_path, row_group_size, page_size_kb, write_batch_size));

    let props = WriterProperties::builder()
        // Compression can only hurt random access
        .set_compression(Compression::UNCOMPRESSED)
        // Even with random data parquet uses dictionary encoding very aggressively.  This hurts random access because
        // Parquet does not cache dictionaries and we end up doing two IOPS per value read.
        .set_dictionary_enabled(false)
        .set_max_row_group_size(row_group_size as usize)
        .set_data_page_size_limit(page_size_kb as usize * 1024)
        .set_write_batch_size(write_batch_size)
        .build();

    let datagen = get_datagen(data_type, rows_per_chunk);

    let bufwriter = work_dir.writer(dest_path);
    let mut writer = AsyncArrowWriter::try_new(bufwriter, datagen.schema(), Some(props)).unwrap();
    for batch in datagen {
        writer.write(&batch.unwrap()).await.unwrap();
    }
    writer.close().await.unwrap();
}

pub async fn parquet_global_setup(
    row_group_size: usize,
    page_size_kb: usize,
    rows_per_chunk: usize,
    num_chunks: usize,
    data_type: DataTypeChoice,
    work_dir: &WorkDir,
) {
    for chunk_index in 0..num_chunks {
        make_parquet_file(
            row_group_size,
            page_size_kb,
            rows_per_chunk,
            chunk_index,
            data_type,
            work_dir,
        )
        .await;
    }
}
