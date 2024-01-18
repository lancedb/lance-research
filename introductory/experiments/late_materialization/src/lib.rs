use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use futures::stream::StreamExt;
use pyo3::prelude::*;
use url::Url;

use crate::metered_store::ReadMetrics;

mod metered_store;

lazy_static::lazy_static! {
    /// The async runtime. This will default to a multi-threaded runtime.
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();

    static ref METRICS: Arc<ReadMetrics> = Arc::new(ReadMetrics::default());

    /// The DataFusion session context.
    static ref CTX: SessionContext = {
        let os_registry = DefaultObjectStoreRegistry::new();
        let default_store = os_registry.get_store(&Url::parse("file:///").unwrap()).unwrap();
        let store = metered_store::MeteredObjectStore::new(default_store, METRICS.clone());
        os_registry.register_store(&Url::parse("metered://").unwrap(), Arc::new(store));

        let runtime_env = RuntimeEnv {
            object_store_registry: Arc::new(os_registry),
            ..Default::default()
        };

        // Enable late materialization (it is disabled by default)
        let config = SessionConfig::default().set_bool("datafusion.execution.parquet.pushdown_filters", true);

        SessionContext::new_with_config_rt(config, Arc::new(runtime_env))
    };
}

#[derive(FromPyObject, Default)]
pub struct ScanConfig {
    #[pyo3(item)]
    pub late_materialization: bool,
    #[pyo3(item)]
    pub measure_io: bool,
    #[pyo3(item)]
    pub explain: bool,
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
) -> PyResult<(usize, Option<(usize, usize)>)> {
    let config = config.unwrap_or_default();

    let path = if config.measure_io {
        // Convert path to absolute path
        let path = std::fs::canonicalize(path).unwrap();
        let path = path.to_str().unwrap();
        let path = Url::from_file_path(path).unwrap();
        let path = path.to_string();
        // Note: the trailing slash is important to get DataFusion to search the
        // directory for parquet files, rather than treat the path as a single file.
        format!("metered{}/", &path[4..])
    } else {
        path
    };

    let res: DFResult<(usize, Option<(usize, usize)>)> = RT.block_on(async move {
        let read_options = ParquetReadOptions {
            parquet_pruning: Some(config.late_materialization),
            ..Default::default()
        };

        let df = CTX.read_parquet(path, read_options).await?;

        let df = df.filter(col("id").gt_eq(lit(min_value)))?;

        let columns = columns.iter().map(col).collect::<Vec<_>>();
        let df = df.select(columns)?;

        if config.explain {
            println!("{:?}", df.clone().create_physical_plan().await?);
            println!("{:?}", df.clone().explain(false, false)?.collect().await);
        }

        let mut row_count = 0;
        let mut stream = df.execute_stream().await?;
        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();
        }

        if config.measure_io {
            let (io_count, io_bytes) = METRICS.metrics();
            METRICS.reset();
            Ok((row_count, Some((io_count, io_bytes))))
        } else {
            Ok((row_count, None))
        }
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
