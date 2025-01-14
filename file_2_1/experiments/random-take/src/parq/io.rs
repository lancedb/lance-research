//! I/O adapters for Parquet

use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::file::reader::{ChunkReader, Length};

use crate::sync::RT;
use crate::take::TryClone;
use crate::{log, LOG_READS};

/// Wraps an ObjectStore and a Path to provide a ChunkReader implementation
pub struct ObjectStoreFile {
    object_store: Arc<dyn ObjectStore>,
    location: Path,
}

impl Length for ObjectStoreFile {
    fn len(&self) -> u64 {
        RT.block_on(self.object_store.head(&self.location))
            .unwrap()
            .size as u64
    }
}

/// Satisfies the Read trait by reading from an ObjectStore
pub struct ObjectStoreReader {
    object_store: Arc<dyn ObjectStore>,
    location: Path,
    offset: u64,
}

impl std::io::Read for ObjectStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let range = (self.offset as usize)..(self.offset as usize + buf.len());
        let mut bytes = RT
            .block_on(self.object_store.get_range(&self.location, range))
            .unwrap();
        bytes.copy_to_slice(buf);
        Ok(buf.len())
    }
}

impl ChunkReader for ObjectStoreFile {
    type T = ObjectStoreReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(ObjectStoreReader {
            object_store: self.object_store.clone(),
            location: self.location.clone(),
            offset: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let range = (start as usize)..(start as usize + length);
        Ok(RT
            .block_on(self.object_store.get_range(&self.location, range))
            .unwrap())
    }
}

impl TryClone for ObjectStoreFile {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            object_store: self.object_store.clone(),
            location: self.location.clone(),
        })
    }
}

pub struct ReadAtReader {
    target: File,
    cursor: u64,
}

impl Read for ReadAtReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let offset = self.cursor;
        let bytes_read = std::os::unix::fs::FileExt::read_at(&self.target, buf, offset)?;
        self.cursor += bytes_read as u64;
        Ok(bytes_read)
    }
}

/// Wraps a local file and provides a ChunkReader impl using pread64
pub struct ReadAtFile(File);

impl ReadAtFile {
    pub fn new(file: File) -> Self {
        Self(file)
    }
}

impl Length for ReadAtFile {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

impl ChunkReader for ReadAtFile {
    type T = ReadAtReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(ReadAtReader {
            target: self.0.try_clone().unwrap(),
            cursor: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if LOG_READS.load(std::sync::atomic::Ordering::Acquire) {
            log(format!("Reading {} bytes", length));
        }
        let mut buf = BytesMut::with_capacity(length);
        unsafe {
            buf.set_len(length);
            let bytes_read = std::os::unix::fs::FileExt::read_at(&self.0, &mut buf, start)?;
            buf.set_len(bytes_read);
        }
        Ok(buf.into())
    }
}

impl TryClone for ReadAtFile {
    fn try_clone(&self) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        self.0.try_clone().map(ReadAtFile)
    }
}

/// A workdir that can be either a local filesystem or S3
#[derive(Clone, Debug)]
pub struct WorkDir {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    is_s3: bool,
}

impl WorkDir {
    pub fn new(workdir: &str) -> Self {
        if workdir.starts_with("file://") {
            let path = Path::parse(workdir[7..].to_string()).unwrap();
            let object_store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
            log(format!("Using local filesystem at {}", path));
            Self {
                path,
                object_store,
                is_s3: false,
            }
        } else if workdir.starts_with("s3://") {
            let path = Path::parse(workdir[5..].to_string()).unwrap();
            let bucket = path.parts().next();
            if let Some(bucket) = bucket {
                let object_store = Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(bucket.as_ref().to_string())
                        .build()
                        .unwrap(),
                ) as Arc<dyn ObjectStore>;
                log(format!(
                    "Using S3 with bucket {} and path {}",
                    bucket.as_ref(),
                    path
                ));
                Self {
                    path,
                    object_store,
                    is_s3: true,
                }
            } else {
                panic!("The workdir did not contain a bucket name");
            }
        } else {
            panic!("workdir argument must start with file:// or s3://")
        }
    }

    pub async fn exists(&self, path: &Path) -> bool {
        match self.object_store.head(path).await {
            Ok(_) => true,
            Err(object_store::Error::NotFound { path: _, source: _ }) => false,
            Err(e) => panic!("{}", e),
        }
    }

    pub fn s3_file(&self, path: Path) -> ObjectStoreFile {
        assert!(self.is_s3);
        ObjectStoreFile {
            location: path,
            object_store: self.object_store.clone(),
        }
    }

    pub fn local_file(&self, path: Path) -> ReadAtFile {
        assert!(!self.is_s3);
        let path_str = format!("/{}", path);
        let path = std::path::Path::new(&path_str);
        let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
        ReadAtFile::new(file)
    }

    pub fn child_path(&self, path: &str) -> Path {
        self.path.child(path)
    }

    pub fn writer(&self, dest_path: Path) -> BufWriter {
        BufWriter::new(self.object_store.clone(), dest_path)
    }

    pub fn is_s3(&self) -> bool {
        self.is_s3
    }
}
