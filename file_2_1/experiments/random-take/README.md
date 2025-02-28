# Random Take Experiment

This experiment measures the performance of the Lance and Parquet file formats
on a "random take" operation. This operation searches a files for K rows using
the row number.

## Data Generation

Data is randomly generated across a number of different data types. The number of
data files and row group size (for parquet) differs based on the data type. The goal
was to create enough data that we never sampled the same row twice. This means we need
more data for smaller data types and only a few data files for the extremely large types.

Note: Most of the benchmark scripts (and the default workdir for the binaries) will place
files in the /tmp directory. Some of these scripts will generate a considerable amount of
data. You will want to ensure you have about 200GiB of free space in /tmp before running
benchmarks.

## Baseline Model

The file baseline.csv was created by using `fio` as a disk benchmarking tool. For details
see the `baseline.py` script. This represents the baseline performance of the disk.

## Building the benchmark

The benchmark program is a rust application built on top of the parquet-rs and
lance rust packages. It supports various CLI arguments to configure the benchmark
parameters.

Note: Make sure to build in release mode :)

## Running the benchmark

The benchmarks script run the benchmark program across a range of parameters. For
each run it collects the latency result and writes a line to a CSV file.

Before running any of the benchmark scripts you must first build the benchmarking
tool. This can be done by running `cargo build --release`.

You can then run the benchmark scripts. These scripts will print CSV data to stdout
which can be redirected to a file. The various chart scripts will then read this CSV
data and create the appropriate plots.

## Benchmark binaries

There are multiple binaries in this directory. To build a specific binary you will
need to run:

```bash
cargo build --release --bin <binary_name>
```

If the `--bin` flag is omitted then all binaries will be built. The binaries are:

- `main` - The main benchmarking tool that tests random access
- `full-scan` - A supplementary benchmarking tool that tests full scan performance
- `page-count` - A small tool that does a monte carlo simulation to estimate benefits of coalesced reads at different sizes
- `uncompressed` - A small tool that writes uncompressed files and compressed files for each format and compares the sizes

## The Full Scan Benchmark

This benchmark looks for files in a configurable data directory. It looks for any files
with names that end in `_trimmed.parquet`. It then considers the first part to be the category
name for reporting purposes. For example, if there is a file `websites_trimmed.parquet` then
the category name will be `websites`. The benchmark will then write several copies of the data
to the file system and read it back to measure full scan performance.

See the readme at `data/real/README.md` for instructions on how to download the data that was used
for this paper.

There are two `prep_` scripts in this directory that are used to generate synthetic files for full
scan testing.

The `prep_packed.py` script generates packed structs with 2, 3, 4, and 5 fields. These
files are placed in the `/tmp/packed_prep` directory. We then copied these files (and renamed them to
end with `_trimmed.parquet`) to the `data/synthetic/packed` directory. We then pointed the benchmark
script at this directory to run the full scan benchmark.

The `prep_sized.py` script generates files with data with 32, 64, 128, 256, and 512 bytes per value.
In addition, it generates one file with the mini-block encoding and one file with the full-zip encoding.
These files we placed in the `data/synthetic/sizes` directory. We then pointed the benchmark script at
this directory to run the full scan benchmark.

Both of these prep scripts only generate Lance 2.1 files. Parquet has no concept of packed structs and only
has a single structural encoding so there is nothing interesting to test there.

## Storing the results

The benchmarks print to stdout. If run in quiet mode they will print CSV output. We
then manually copy this output into the appropriate CSV file in the `results` directory.

## Generating charts

To generate the charts we use scripts in the `chart-scripts` directory which look for
CSV files in the `results` directory and use that data to generate charts in the
`charts` directory.
