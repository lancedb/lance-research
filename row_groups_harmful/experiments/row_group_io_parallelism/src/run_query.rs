use std::sync::Arc;

use arrow_array::{Array, cast::AsArray};
use clap::Parser;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::prelude::{col, Expr, ParquetReadOptions, SessionConfig, SessionContext};
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use tracing::info;

const NUM_FIELDS: usize = 20;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// File path or URI to read
    #[arg(short, long)]
    file_path: String,

    /// File format (parquet or lance)
    #[arg(short, long)]
    format: String,

    /// Number of DataFusion partitions (default: 4)
    #[arg(long, default_value_t = 4)]
    partitions: usize,

}


async fn run_parquet_query(
    file_path: &str,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running DataFusion Parquet query with {} partitions", partitions);

    // Determine the file URI format
    let file_uri = if file_path.contains("://") {
        file_path.to_string()
    } else {
        format!("file://{}", file_path)
    };

    info!("Reading parquet file: {}", file_uri);

    let session_context = SessionContext::new_with_config(
        SessionConfig::default().with_target_partitions(partitions),
    );

    let make_df = || async {
        let df = session_context
            .read_parquet(&file_uri, ParquetReadOptions::default())
            .await
            .unwrap();

        let mut sum_exprs = Vec::new();
        for i in 0..NUM_FIELDS {
            sum_exprs.push(Expr::AggregateFunction(AggregateFunction::new_udf(
                sum_udaf(),
                vec![col(format!("value_{}", i))],
                false,
                None,
                vec![],
                None,
            )));
        }

        df.aggregate(vec![], sum_exprs).unwrap()
    };

    // Show execution plan
    let res = make_df()
        .await
        .explain(true, false)
        .unwrap()
        .collect()
        .await
        .unwrap();

    for batch in res {
        let plans = batch.column(1).as_string::<i32>();
        println!("{}", plans.value(plans.len() - 1));
    }

    // Run the query
    info!("Running query");
    make_df().await.collect().await.unwrap();

    Ok(())
}

async fn run_lance_query(
    file_path: &str,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running DataFusion Lance query with {} partitions", partitions);

    info!("Reading lance dataset: {}", file_path);

    let session_context = SessionContext::new_with_config(
        SessionConfig::default().with_target_partitions(partitions),
    );

    let lance_dataset = DatasetBuilder::from_uri(file_path).load().await?;
    let lance_dataset = Arc::new(lance_dataset);

    let lance_provider = LanceTableProvider::new(lance_dataset, false, false);

    // Register the table
    session_context
        .register_table("lance_table", Arc::new(lance_provider))
        .unwrap();

    let make_df = || async {
        let df = session_context.table("lance_table").await.unwrap();

        let mut sum_exprs = Vec::new();
        for i in 0..NUM_FIELDS {
            sum_exprs.push(Expr::AggregateFunction(AggregateFunction::new_udf(
                sum_udaf(),
                vec![col(format!("value_{}", i))],
                false,
                None,
                vec![],
                None,
            )));
        }

        df.aggregate(vec![], sum_exprs).unwrap()
    };

    // Show execution plan
    let res = make_df()
        .await
        .explain(true, false)
        .unwrap()
        .collect()
        .await
        .unwrap();

    for batch in res {
        let plans = batch.column(1).as_string::<i32>();
        println!("{}", plans.value(plans.len() - 1));
    }

    // Run the query
    info!("Running query");
    make_df().await.collect().await.unwrap();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Starting DataFusion query execution");
    info!(
        "Configuration: file={}, format={}, partitions={}",
        args.file_path, args.format, args.partitions
    );

    // Run the query based on format
    match args.format.as_str() {
        "parquet" => run_parquet_query(&args.file_path, args.partitions).await?,
        "lance" => run_lance_query(&args.file_path, args.partitions).await?,
        _ => return Err(format!("Unsupported format: {}", args.format).into()),
    }

    info!("Query execution completed successfully");
    Ok(())
}