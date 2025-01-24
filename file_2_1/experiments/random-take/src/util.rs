use std::sync::{Arc, Mutex};

use arrow_array::{cast::AsArray, types::UInt64Type, RecordBatch, UInt64Array};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance_core::cache::FileMetadataCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::v2::{
    reader::{FileReader, FileReaderOptions},
    writer::{FileWriter, FileWriterOptions},
};
use lance_io::{
    scheduler::{ScanScheduler, SchedulerConfig},
    ReadBatchParams,
};
use object_store::path::Path;
use rand::seq::SliceRandom;

use crate::{log, parq::io::WorkDir};

pub const MAX_RANDOM_INDICES: usize = 10 * 1024 * 1024;

pub struct RandomIndices {
    indices: ScalarBuffer<u64>,
    current_index: Mutex<usize>,
}

impl RandomIndices {
    fn generate_indices(num_indices: usize) -> Vec<u64> {
        log(format!("Generating {} random indices", num_indices));
        let mut indices = (0..(num_indices) as u64).collect::<Vec<_>>();
        let mut rng = rand::thread_rng();
        if indices.len() > MAX_RANDOM_INDICES {
            // If there are more than MAX_INDICES indices, then randomly select MAX_INDICES
            // from the larger set (we still sample from the entire set) and run on that.  This
            // avoids massive shuffles for large numbers of indices.
            //
            // Note: MAX_INDICES must be large enough to avoid repeating indices in the duration
            // and so we might need to make it larger if playing with durations beyond 10 seconds.
            //
            // If we exceed the indices we will panic so we can catch this and increase MAX_INDICES
            let (shuffled, _) = indices.partial_shuffle(&mut rng, MAX_RANDOM_INDICES);
            shuffled.to_vec()
        } else {
            indices.shuffle(&mut rng);
            indices
        }
    }

    async fn load_indices(work_dir: &WorkDir, path: &Path) -> ScalarBuffer<u64> {
        log(format!("Loading indices from {}", path));
        let scan_scheduler = ScanScheduler::new(
            work_dir.lance_object_store(),
            SchedulerConfig::default_for_testing(),
        );
        let file_scheduler = scan_scheduler.open_file(path).await.unwrap();
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &FileMetadataCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let num_rows = reader.num_rows();
        let batch = reader
            .read_stream(
                ReadBatchParams::RangeFull,
                num_rows as u32,
                1,
                FilterExpression::no_filter(),
            )
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let indices = batch.column(0);
        let indices = indices.as_primitive::<UInt64Type>();
        indices.values().clone()
    }

    async fn save_indices(work_dir: &WorkDir, path: &Path, indices: Vec<u64>) -> ScalarBuffer<u64> {
        let indices = UInt64Array::from(indices);
        let indices_buffer = indices.values().clone();
        let indices = Arc::new(indices);
        let indices_schema = Schema::new(vec![Field::new("indices", DataType::UInt64, false)]);
        let indices_batch = RecordBatch::try_new(Arc::new(indices_schema), vec![indices]).unwrap();

        let object_writer = work_dir.lance_writer(path.clone()).await;
        let mut writer = FileWriter::new_lazy(object_writer, FileWriterOptions::default());
        writer.write_batch(&indices_batch).await.unwrap();
        writer.finish().await.unwrap();
        indices_buffer
    }

    pub async fn new(rows_per_chunk: usize, num_chunks: usize) -> Self {
        // Sorting a billion indices is slow so we save the computation if we can
        let num_indices = rows_per_chunk * num_chunks;
        let tmpdir = WorkDir::new("file:///tmp").await;
        let indices_path =
            Path::parse(format!("tmp/random_indices_{}.lance", num_indices)).unwrap();
        let indices = if tmpdir.exists(&indices_path).await {
            Self::load_indices(&tmpdir, &indices_path).await
        } else {
            let indices = Self::generate_indices(num_indices);
            Self::save_indices(&tmpdir, &indices_path, indices).await
        };
        Self {
            indices: indices,
            current_index: Mutex::new(0),
        }
    }

    pub fn next(&self, take_size: usize) -> ScalarBuffer<u64> {
        let mut current_index = self.current_index.lock().unwrap();
        let start = *current_index;

        if take_size + start > self.indices.len() {
            panic!("Not enough input data for duration.  Would repeat indices");
        } else {
            *current_index += take_size;
        };

        self.indices.slice(start, take_size)
    }

    pub fn all_indices(&self) -> ScalarBuffer<u64> {
        self.indices.clone()
    }
}
