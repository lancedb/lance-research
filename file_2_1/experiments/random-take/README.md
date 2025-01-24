# Random Take Experiment

This experiment measures the performance of the Lance and Parquet file formats
on a "random take" operation. This operation searches a files for K rows using
the row number.

## Data Generation

Data is randomly generated across a number of different data types. The number of
data files and row group size (for parquet) differs based on the data type. The goal
was to create enough data that we never sampled the same row twice. This means we need
more data for smaller data types and only a few data files for the extremely large types.

## Baseline Model

The file baseline.csv was created by using `fio` as a disk benchmarking tool. For details
see the `baseline.py` script. This represents the baseline performance of the disk.

## Building the benchmark

The benchmark program is a rust application built on top of the parquet-rs and
lance rust packages. It supports various CLI arguments to configure the benchmark
parameters.

## Running the benchmark

The benchmarks script run the benchmark program across a range of parameters. For
each run it collects the latency result and writes a line to a CSV file.

Before running any of the benchmark scripts you must first build the benchmarking
tool. This can be done by running `cargo build --release`.

You can then run the benchmark scripts. These scripts will print CSV data to stdout
which can be redirected to a file. The various chart scripts will then read this CSV
data and create the appropriate plots.

## Generating plots / figures

To generate the paper we take a CSV file created by the benchmark runner script and
copy that into the directory structure for the paper. The paper build process will
then run the various python scripts that are embedded in the qmb files. These will
analyze the CSV file to create the correct plots figures. This CSV file is also
committed as part of the repository.
