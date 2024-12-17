use std::{
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{FileExt, OpenOptionsExt},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use indicatif::ProgressBar;
use rand::{
    distributions::uniform::{UniformInt, UniformSampler},
    RngCore,
};

const DURATION: Duration = Duration::from_secs(10);
const SECTOR_SIZE: usize = 4096;
const FILE_SIZE: usize = 1024 * 1024 * 1024;
const FILE_SIZE_SECTORS: usize = FILE_SIZE / SECTOR_SIZE;
const CHUNK_SIZE: usize = 32 * 1024 * 1024;
const MAX_READ_SIZE: usize = 1024 * 1024 * 8;
const PATH: &str = "/mnt/oldhdd/experiment/file.dat";

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
    async fn setup(&self) {
        if !std::fs::exists(PATH).unwrap() {
            println!("Generating random data");
            let mut rng = rand::thread_rng();
            let mut remaining = FILE_SIZE;
            let bar = ProgressBar::new(FILE_SIZE as u64);
            let mut file = std::fs::File::create_new(PATH).unwrap();
            let mut buffer = bytes::BytesMut::zeroed(CHUNK_SIZE);
            while remaining > 0 {
                let batch_size = remaining.min(CHUNK_SIZE);
                let buffer = &mut buffer[..batch_size];
                rng.fill_bytes(buffer);
                file.write_all(buffer).unwrap();
                remaining -= batch_size;
                bar.inc(batch_size as u64);
            }
            drop(file);
            bar.finish();
        } else {
            println!("Using existing file");
        }
    }

    async fn profile_random_reads(&self) -> ExperimentResults {
        self.setup().await;
        let num_iterations = Arc::new(AtomicUsize::new(0));
        let finished: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let start = Instant::now();

        #[repr(align(4096))]
        struct Buffer {
            data: [u8; MAX_READ_SIZE],
        }

        println!("Beginning measurements");
        let handles = (0..self.num_threads)
            .map(|_| {
                let finished = finished.clone();
                let num_iterations = num_iterations.clone();
                let read_size_sectors = self.read_size_sectors;
                std::thread::spawn(move || {
                    let dist = UniformInt::<usize>::new(0, FILE_SIZE_SECTORS - read_size_sectors);
                    let buffer = unsafe {
                        let buffer =
                            std::alloc::alloc(std::alloc::Layout::new::<Buffer>()) as *mut Buffer;
                        &mut (*buffer).data[..read_size_sectors * SECTOR_SIZE]
                    };
                    let file = OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_DIRECT)
                        .open(PATH)
                        .unwrap();
                    while !finished.load(Ordering::Acquire) {
                        let pos = dist.sample(&mut rand::thread_rng()) * SECTOR_SIZE;
                        file.read_exact_at(buffer, pos as u64).unwrap();
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
            handle.join().unwrap();
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

    let mut results_file = std::fs::File::create("disk_results.csv").unwrap();
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
