use lance_file::{
    v2::writer::{FileWriter, FileWriterOptions},
    version::LanceFileVersion,
};
use object_store::path::Path;

use crate::{datagen::get_datagen, log, parq::io::WorkDir, DataTypeChoice};

pub fn lance_file_path(
    work_dir: &WorkDir,
    version: LanceFileVersion,
    chunk_index: usize,
    data_type: DataTypeChoice,
) -> Path {
    let version = match version {
        LanceFileVersion::V2_0 => "2_0",
        LanceFileVersion::V2_1 => "2_1",
        _ => unimplemented!(),
    };
    work_dir.child_path(&format!(
        "lance_version_{}_type_{}_chunk_{}.lance",
        version,
        data_type.as_str(),
        chunk_index,
    ))
}

pub async fn make_lance_file(
    rows_per_chunk: usize,
    chunk_index: usize,
    data_type: DataTypeChoice,
    work_dir: &WorkDir,
    file_version: LanceFileVersion,
) {
    let dest_path = lance_file_path(work_dir, file_version, chunk_index, data_type);
    if work_dir.exists(&dest_path).await {
        log(format!("Using existing lance test file at {}", dest_path));
        return;
    }

    log(format!(
        "Creating new lance test file at {} with version {}",
        dest_path, file_version
    ));

    let datagen = get_datagen(data_type, rows_per_chunk);

    let obj_writer = work_dir.lance_writer(dest_path).await;

    let mut writer = FileWriter::new_lazy(
        obj_writer,
        FileWriterOptions {
            format_version: Some(file_version),
            ..Default::default()
        },
    );

    for batch in datagen {
        writer.write_batch(&batch.unwrap()).await.unwrap();
    }
    writer.finish().await.unwrap();
}

pub async fn lance_global_setup(
    rows_per_chunk: usize,
    num_chunks: usize,
    data_type: DataTypeChoice,
    work_dir: &WorkDir,
    file_version: LanceFileVersion,
) {
    for chunk_index in 0..num_chunks {
        make_lance_file(
            rows_per_chunk,
            chunk_index,
            data_type,
            work_dir,
            file_version,
        )
        .await;
    }
}
