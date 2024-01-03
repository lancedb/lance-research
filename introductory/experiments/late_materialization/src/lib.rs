use datafusion::error::Result as DFResult;
use datafusion::prelude::*;
use futures::stream::StreamExt;
use pyo3::prelude::*;

lazy_static::lazy_static! {
    /// The async runtime. This will default to a multi-threaded runtime.
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
    /// The DataFusion session context.
    static ref CTX: SessionContext = SessionContext::new();
}

#[derive(FromPyObject, Default)]
pub struct ScanConfig {
    #[pyo3(item)]
    pub late_materialization: bool,
    #[pyo3(item)]
    pub measure_io: bool,
}
// TODO: it might be an unfair comparison to make the `read_parquet` call here. 
//      We should try to put the initialization in another call.
#[pyfunction]
#[pyo3(signature = (path, columns, min_value, **config))]
fn scan_datafusion(
    path: String,
    columns: Vec<String>,
    min_value: u64,
    config: Option<ScanConfig>,
) -> PyResult<usize> {
    let config = config.unwrap_or_default();

    let res: DFResult<usize> = RT.block_on(async move {
        let read_options = ParquetReadOptions {
            parquet_pruning: Some(config.late_materialization),
            ..Default::default()
        };

        let df = CTX.read_parquet(path, read_options).await?;

        let df = df.filter(col("id").gt_eq(lit(min_value)))?;

        let columns = columns.iter().map(col).collect::<Vec<_>>();
        let df = df.select(columns)?;

        let mut count = 0;
        let mut stream = df.execute_stream().await?;
        while let Some(batch) = stream.next().await {
            count += batch?.num_rows();
        }
        Ok(count)
    });

    res.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn late_materialization(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_datafusion, m)?)?;
    Ok(())
}
