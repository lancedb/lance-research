# Random Take Experiment

This experiment measures the performance of the Lance and Parquet file formats
on a "random take" operation.  This operation searches a files for K rows using
the row number.

## Downloading the data

TODO: Fill in datagen

## Building the benchmark

The benchmark program is a rust application built on top of the parquet-rs and
lance rust packages.  It supports various CLI arguments to configure the benchmark
parameters.

## Running the benchmark

The benchmark script runs the benchmark program across a range of parameters.  For
each run it collects the latency result and writes a line to a CSV file.

## Generating plots / figures

To generate the paper we take a CSV file created by the benchmark runner script and
copy that into the directory structure for the paper.  The paper build process will
then run the various python scripts that are embedded in the qmb files.  These will
analyze the CSV file to create the correct plots figures.  This CSV file is also
committed as part of the repository.
