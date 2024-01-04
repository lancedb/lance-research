use arrow_array::RecordBatch;
use parquet::arrow::{
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector},
    ProjectionMask,
};

struct IndicesToRowSelection<'a, I: Iterator<Item = &'a u32>> {
    iter: I,
    start: u32,
    end: u32,
    last: u32,
    next: Option<RowSelector>,
}

impl<'a, I: Iterator<Item = &'a u32>> Iterator for IndicesToRowSelection<'a, I> {
    type Item = RowSelector;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.next.take() {
            return Some(next);
        }
        loop {
            let next_idx = self.iter.next();
            if let Some(next_idx) = next_idx {
                if *next_idx < self.start {
                    continue;
                }
                if *next_idx >= self.end {
                    return None;
                }
                let to_skip = *next_idx - self.last;
                self.last = *next_idx + 1;
                if to_skip > 0 {
                    self.next = Some(RowSelector::select(1));
                    return Some(RowSelector::skip(to_skip as usize));
                } else {
                    return Some(RowSelector::select(1));
                }
            } else {
                return None;
            }
        }
    }
}

pub mod sync {
    use std::fs::File;

    use parquet::{
        arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::ChunkReader,
    };

    use super::*;

    pub fn take_task<T: ChunkReader + 'static>(
        file: T,
        metadata: ArrowReaderMetadata,
        row_group_number: u32,
        row_indices: &[u32],
        column_indices: &[u32],
        use_selection: bool,
    ) -> Vec<RecordBatch> {
        let start = if row_group_number == 0 {
            0
        } else {
            (0..row_group_number)
                .map(|rg_num| metadata.metadata().row_group(rg_num as usize).num_rows())
                .sum()
        };
        let end = start
            + metadata
                .metadata()
                .row_group(row_group_number as usize)
                .num_rows();

        let selection = IndicesToRowSelection {
            iter: row_indices.iter(),
            start: start as u32,
            end: end as u32,
            last: start as u32,
            next: None,
        };
        let selection = RowSelection::from_iter(selection);
        if !selection.selects_any() {
            return Vec::new();
        }
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata);
        let parquet_schema = builder.parquet_schema();
        let projection = ProjectionMask::roots(
            parquet_schema,
            column_indices.iter().map(|col_idx| *col_idx as usize),
        );
        if use_selection {
            let reader = builder
                .with_limit(row_indices.len())
                .with_row_groups(vec![row_group_number as usize])
                .with_row_selection(selection)
                .with_projection(projection)
                .build()
                .unwrap();
            reader
                .collect::<std::result::Result<Vec<_>, arrow_schema::ArrowError>>()
                .unwrap()
        } else {
            let reader = builder.with_projection(projection).build().unwrap();
            reader
                .collect::<std::result::Result<Vec<_>, arrow_schema::ArrowError>>()
                .unwrap()
        }
    }

    pub trait TryClone {
        fn try_clone(&self) -> std::io::Result<Self>
        where
            Self: Sized;
    }

    impl TryClone for File {
        fn try_clone(&self) -> std::io::Result<Self>
        where
            Self: Sized,
        {
            self.try_clone()
        }
    }

    pub fn take<T: ChunkReader + TryClone + 'static>(
        file_opener: impl Fn() -> T,
        row_indices: &[u32],
        column_indices: &[u32],
        use_selection: bool,
        metadata: Option<ArrowReaderMetadata>,
    ) -> Vec<RecordBatch> {
        std::thread::scope(|scope| {
            let metadata = metadata.unwrap_or_else(|| {
                let options = ArrowReaderOptions::new().with_page_index(true);
                ArrowReaderMetadata::load(&file_opener(), options).unwrap()
            });
            let task_handles = (0..metadata.metadata().num_row_groups())
                .map(|row_group_number| {
                    let file = file_opener();
                    let metadata = metadata.clone();
                    scope.spawn(move || {
                        take_task(
                            file,
                            metadata,
                            row_group_number as u32,
                            row_indices,
                            column_indices,
                            use_selection,
                        )
                    })
                })
                .collect::<Vec<_>>();

            task_handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap().into_iter())
                .collect::<Vec<_>>()
        })
    }
}

pub mod r#async {

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use parquet::arrow::{async_reader::AsyncFileReader, ParquetRecordBatchStreamBuilder};

    use super::*;

    pub async fn take_task<T: AsyncFileReader + Send + TryClone + Unpin + 'static>(
        file: T,
        metadata: ArrowReaderMetadata,
        row_indices: &[u32],
        column_indices: &[u32],
        use_selection: bool,
    ) -> Vec<RecordBatch> {
        let total_num_rows = metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum::<i64>();

        let selection = IndicesToRowSelection {
            iter: row_indices.iter(),
            start: 0,
            end: total_num_rows as u32,
            last: 0 as u32,
            next: None,
        };
        let selection = RowSelection::from_iter(selection);
        if !selection.selects_any() {
            return Vec::new();
        }
        let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata);
        let parquet_schema = builder.parquet_schema();
        let projection = ProjectionMask::roots(
            parquet_schema,
            column_indices.iter().map(|col_idx| *col_idx as usize),
        );
        if use_selection {
            let reader = builder
                .with_limit(row_indices.len())
                .with_row_selection(selection)
                .with_projection(projection)
                .build()
                .unwrap();
            reader.try_collect::<Vec<_>>().await.unwrap()
        } else {
            let reader = builder.with_projection(projection).build().unwrap();
            reader.try_collect::<Vec<_>>().await.unwrap()
        }
    }

    #[async_trait]
    pub trait TryClone {
        async fn try_clone(&self) -> std::io::Result<Self>
        where
            Self: Sized;
    }

    #[async_trait]
    impl TryClone for tokio::fs::File {
        async fn try_clone(&self) -> std::io::Result<Self>
        where
            Self: Sized,
        {
            self.try_clone().await
        }
    }

    pub async fn take<T: AsyncFileReader + Unpin + TryClone + 'static>(
        mut file: T,
        row_indices: &[u32],
        column_indices: &[u32],
        use_selection: bool,
        metadata: Option<ArrowReaderMetadata>,
    ) -> Vec<RecordBatch> {
        let metadata = if metadata.is_some() {
            metadata.unwrap()
        } else {
            let options = ArrowReaderOptions::new().with_page_index(true);
            ArrowReaderMetadata::load_async(&mut file, options)
                .await
                .unwrap()
        };
        take_task(file, metadata, row_indices, column_indices, use_selection).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    async fn test_take_selection_async() {
        let path_str = "/tmp/input_rgs_100000.parquet";
        let path = Path::new(path_str);
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .unwrap();
        r#async::take(file, &[1], &[3], true, None).await;
    }

    #[tokio::test]
    async fn test_take_no_selection_async() {
        let path_str = "/tmp/input_rgs_100000.parquet";
        let path = Path::new(path_str);
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .unwrap();
        r#async::take(file, &[1], &[3], false, None).await;
    }

    #[test]
    fn test_take_selection_sync() {
        let path_str = "/tmp/input_rgs_100000.parquet";
        let path = Path::new(path_str);
        sync::take(
            || std::fs::OpenOptions::new().read(true).open(path).unwrap(),
            &[1],
            &[3],
            true,
            None,
        );
    }

    #[test]
    fn test_take_no_selection_sync() {
        let path_str = "/tmp/input_rgs_100000.parquet";
        let path = Path::new(path_str);
        sync::take(
            || std::fs::OpenOptions::new().read(true).open(path).unwrap(),
            &[1],
            &[3],
            false,
            None,
        );
    }
}
