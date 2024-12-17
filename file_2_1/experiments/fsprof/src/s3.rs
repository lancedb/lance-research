use std::{
    cell::LazyCell,
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use object_store::{path::Path, ObjectStore, PutPayload};
use rand::{
    distributions::uniform::{UniformInt, UniformSampler},
    RngCore,
};

const DURATION: Duration = Duration::from_secs(10);
const SECTOR_SIZE: usize = 4096;
const FILE_SIZE: usize = 1024 * 1024 * 1024;
const NUM_FILES: usize = 5;
const CHUNK_SIZE: usize = 32 * 1024 * 1024;
const BUCKET_NAME: &str = "weston-s3-lance-test";
const PATH_PARENT: LazyCell<Path> = LazyCell::new(|| Path::parse("fsprof").unwrap());

struct Experiment {
    read_size_sectors: usize,
    num_threads: usize,
}

#[derive(Debug)]
struct ExperimentResults {
    num_iterations: usize,
    bytes_read: usize,
}

impl Experiment {
    async fn setup(&self, store: &dyn ObjectStore) {
        if store
            .list(Some(&PATH_PARENT))
            .take(NUM_FILES)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
            < NUM_FILES
        {
            println!("Generating random data");
            let mut rng = rand::thread_rng();
            let bar = ProgressBar::new(FILE_SIZE as u64 * NUM_FILES as u64);
            for file_index in 0..NUM_FILES {
                let path = PATH_PARENT.child(format!("file_{}.dat", file_index));
                let mut write = store.put_multipart(&path).await.unwrap();
                let mut remaining = FILE_SIZE;
                while remaining > 0 {
                    let batch_size = remaining.min(CHUNK_SIZE);
                    let mut buffer = bytes::BytesMut::zeroed(batch_size);
                    rng.fill_bytes(&mut buffer);
                    let payload = PutPayload::from_bytes(buffer.freeze());
                    write.put_part(payload).await.unwrap();
                    remaining -= batch_size;
                    bar.inc(batch_size as u64);
                }
                write.complete().await.unwrap();
            }
            bar.finish();
        } else {
            println!("Using existing files");
        }
    }

    async fn profile_random_reads(&self) -> ExperimentResults {
        let store = Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(BUCKET_NAME)
                .build()
                .unwrap(),
        );

        self.setup(store.as_ref()).await;
        let num_iterations = Arc::new(AtomicUsize::new(0));
        let finished: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let start = Instant::now();

        println!("Beginning measurements");
        let handles = (0..self.num_threads)
            .map(|_| {
                let finished = finished.clone();
                let num_iterations = num_iterations.clone();
                let read_size = self.read_size_sectors * SECTOR_SIZE;
                let store = store.clone();
                tokio::task::spawn(async move {
                    let avail_pos = FILE_SIZE - read_size;
                    let dist = UniformInt::<usize>::new(0, avail_pos * NUM_FILES);
                    while !finished.load(Ordering::Acquire) {
                        let pos = dist.sample(&mut rand::thread_rng());
                        let file_index = pos / avail_pos;
                        let file_offset = pos % avail_pos;
                        let file = PATH_PARENT.child(format!("file_{}.dat", file_index));
                        store
                            .get_range(&file, file_offset..file_offset + read_size)
                            .await
                            .unwrap();
                        num_iterations.fetch_add(1, Ordering::Release);
                    }
                })
            })
            .collect::<Vec<_>>();

        let bar = ProgressBar::new(DURATION.as_secs() * 100);
        while start.elapsed() < DURATION {
            let remaining = DURATION - start.elapsed();
            let next_sleep = Duration::from_millis(50).min(remaining);
            bar.set_position((start.elapsed().as_secs_f64() * 100.0) as u64);
            tokio::time::sleep(next_sleep).await;
        }
        finished.store(true, Ordering::Release);

        for handle in handles {
            handle.await.unwrap();
        }

        let num_iterations = num_iterations.load(Ordering::Acquire);
        let bytes_read = num_iterations * self.read_size_sectors * SECTOR_SIZE;
        ExperimentResults {
            num_iterations,
            bytes_read,
        }
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut results_file = std::fs::File::create("s3_results.csv").unwrap();
    write!(
        results_file,
        "num_threads,read_size_sectors,num_iterations,bytes_read\n"
    )
    .unwrap();

    for num_threads in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] {
        for read_size_sectors in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] {
            let experiment = Experiment {
                num_threads,
                read_size_sectors,
            };
            let results = rt.block_on(experiment.profile_random_reads());
            write!(
                results_file,
                "{},{},{},{}\n",
                num_threads, read_size_sectors, results.num_iterations, results.bytes_read
            )
            .unwrap();
        }
    }
}
