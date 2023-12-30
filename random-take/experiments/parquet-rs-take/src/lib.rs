use arrow_array::RecordBatch;
use parquet::arrow::{
    arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
        RowSelector,
    },
    ProjectionMask,
};
use std::fs::File;

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

pub fn take_task(
    file: File,
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

pub fn take(
    file: File,
    row_indices: &[u32],
    column_indices: &[u32],
    use_selection: bool,
) -> Vec<RecordBatch> {
    std::thread::scope(|scope| {
        let options = ArrowReaderOptions::new().with_page_index(true);
        let metadata = ArrowReaderMetadata::load(&file, options).unwrap();
        (0..metadata.metadata().num_row_groups())
            .map(|row_group_number| {
                let file = file.try_clone().unwrap();
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
            .flat_map(|handle| handle.join().unwrap().into_iter())
            .collect::<Vec<_>>()
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use std::fs::OpenOptions;

    use super::*;

    #[tokio::test]
    async fn test_take_selection() {
        let path_str = "/tmp/bench_input_50000.parquet";
        let path = Path::new(path_str);
        let file = OpenOptions::new().read(true).open(path).unwrap();
        take(file, &[1], &[3], true);
    }

    #[tokio::test]
    async fn test_take_no_selection() {
        let path_str = "/tmp/bench_input_50000.parquet";
        let path = Path::new(path_str);
        let file = OpenOptions::new().read(true).open(path).unwrap();
        take(file, &[1], &[3], false);
    }
}
