//! Utilities for working with Parquet files.

use arrow_array::RecordBatchReader;
use io::WorkDir;
use object_store::path::Path;
use parquet::{
    arrow::AsyncArrowWriter,
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};

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
        DataTypeChoice::Nested1 => 4 * 32,
        DataTypeChoice::Nested2 => 4 * 32,
        DataTypeChoice::Nested3 => 4 * 32,
        DataTypeChoice::Nested4 => 4 * 32,
        DataTypeChoice::Nested5 => 4 * 32,
        DataTypeChoice::NestedList1 => 8,
        DataTypeChoice::NestedList2 => 12,
        DataTypeChoice::NestedList3 => 16,
        DataTypeChoice::NestedList4 => 20,
        DataTypeChoice::NestedList5 => 24,
        DataTypeChoice::Packed2 => 16,
        DataTypeChoice::Packed3 => 24,
        DataTypeChoice::Packed4 => 32,
        DataTypeChoice::Packed5 => 40,
        DataTypeChoice::Unpacked2 => 16,
        DataTypeChoice::Unpacked3 => 24,
        DataTypeChoice::Unpacked4 => 32,
        DataTypeChoice::Unpacked5 => 40,
        DataTypeChoice::SizedMiniBlock1 => 32,
        DataTypeChoice::SizedMiniBlock2 => 64,
        DataTypeChoice::SizedMiniBlock3 => 128,
        DataTypeChoice::SizedMiniBlock4 => 256,
        DataTypeChoice::SizedMiniBlock5 => 512,
        DataTypeChoice::SizedFullZip1 => 32,
        DataTypeChoice::SizedFullZip2 => 64,
        DataTypeChoice::SizedFullZip3 => 128,
        DataTypeChoice::SizedFullZip4 => 256,
        DataTypeChoice::SizedFullZip5 => 512,
    };
    (page_size / elem_size).max(1) as usize
}

pub fn parq_file_path(
    work_dir: &WorkDir,
    row_group_size: usize,
    page_size_kb: usize,
    chunk_index: usize,
    data_type: DataTypeChoice,
    compression: bool,
    dictionary: bool,
) -> Path {
    let encoding_str = if compression {
        "_c"
    } else if dictionary {
        "_d"
    } else {
        ""
    };
    work_dir.child_path(&format!(
        "parquet{}_row_groups_{}_page_{}_kib_type_{}_chunk_{}.parquet",
        encoding_str,
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
    compression: bool,
    dictionary: bool,
    nullable: bool,
) {
    let dest_path = parq_file_path(
        work_dir,
        row_group_size,
        page_size_kb,
        chunk_index,
        data_type,
        compression,
        dictionary,
    );
    if work_dir.exists(&dest_path).await {
        log(format!("Using existing parquet test file at {}", dest_path));
        return;
    }

    let write_batch_size = get_parquet_write_batch_size(page_size_kb, data_type);

    log(format!("Creating new parquet test file at {} with {} rows per row group and {}kb per page and data_page_size_limit={}", dest_path, row_group_size, page_size_kb, write_batch_size));

    let compression = if compression {
        Compression::SNAPPY
    } else {
        Compression::UNCOMPRESSED
    };

    let props = WriterProperties::builder()
        // Compression can only hurt random access
        .set_compression(compression)
        // Even with random data parquet uses dictionary encoding very aggressively.  This hurts random access because
        // Parquet does not cache dictionaries and we end up doing two IOPS per value read.
        .set_dictionary_enabled(dictionary)
        .set_max_row_group_size(row_group_size as usize)
        .set_data_page_size_limit(page_size_kb as usize * 1024)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_write_batch_size(write_batch_size)
        .build();

    let datagen = get_datagen(data_type, rows_per_chunk, nullable);

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
    compression: bool,
    dictionary: bool,
    nullable: bool,
) {
    for chunk_index in 0..num_chunks {
        make_parquet_file(
            row_group_size,
            page_size_kb,
            rows_per_chunk,
            chunk_index,
            data_type,
            work_dir,
            compression,
            dictionary,
            nullable,
        )
        .await;
    }
}
