use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance_file::{v2::writer::FileWriterOptions, version::LanceFileVersion};
use lance_io::object_store::ObjectStore;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::WriterProperties,
};

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

fn dst_path(category: &str, format: &str, ext: &str) -> String {
    format!("/tmp/{}_{}.{}", category, format, ext)
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

async fn write_parquet(src_path: &str, dest_path: &str, compressed: bool) {
    let test_data = read_test_data(src_path).await;
    let writer = tokio::fs::File::create(dest_path).await.unwrap();
    let options = ArrowWriterOptions::default().with_properties(
        WriterProperties::builder()
            .set_bloom_filter_enabled(false)
            .set_max_row_group_size(test_data.num_rows())
            .set_compression(if compressed {
                parquet::basic::Compression::SNAPPY
            } else {
                parquet::basic::Compression::UNCOMPRESSED
            })
            .set_dictionary_enabled(compressed)
            .build(),
    );
    let mut writer =
        AsyncArrowWriter::try_new_with_options(writer, test_data.schema(), options).unwrap();

    writer.write(&test_data).await.unwrap();

    writer.finish().await.unwrap();
}

async fn write_lance(src_path: &str, dest_path: &str) {
    let test_data = read_test_data(src_path).await;
    let obj_store = ObjectStore::local();
    let obj_writer = obj_store
        .create(&object_store::path::Path::parse(dest_path).unwrap())
        .await
        .unwrap();
    let mut writer = lance_file::v2::writer::FileWriter::new_lazy(
        obj_writer,
        FileWriterOptions {
            format_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        },
    );
    writer.write_batch(&test_data).await.unwrap();
    writer.finish().await.unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("category,uncompressed_size,compressed_size,lance_size");
    for category in get_test_categories() {
        let src_path = src_path(&category);
        let path = dst_path(&category, "uncompressed", "parquet");
        write_parquet(&src_path, &path, false).await;
        let uncompressed = std::fs::File::open(&path)
            .unwrap()
            .metadata()
            .unwrap()
            .len();
        let path = dst_path(&category, "compressed", "parquet");
        write_parquet(&src_path, &path, true).await;
        let compressed = std::fs::File::open(&path)
            .unwrap()
            .metadata()
            .unwrap()
            .len();
        let path = dst_path(&category, "compressed", "lance");
        write_lance(&src_path, &path).await;
        let lance = std::fs::File::open(&path)
            .unwrap()
            .metadata()
            .unwrap()
            .len();
        println!("{},{},{},{}", category, uncompressed, compressed, lance);
    }
}
