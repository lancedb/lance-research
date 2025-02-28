# Storage Profiling

The purpose of this experiment is to evaluate the performance of different storage solutions.
In particular, we are interested in meausuring the number of IOPS and the bandwidth and
looking at the tradeoff between the two.

## Setup

To run the experiment on a local disk no real setup is required. A file will be created at
/tmp/file.dat (change this if /tmp is a ramdisk) and the tests will run against this file.

To run the experiment on S3 you will need to create a bucket and the appropriate credentials.
We construct the S3 object store using `AmazonS3Builder::from_env()` and so you will need to
ensure the credentials are accessible in the environment.

Specifically, we used IAM profiles assigned to an EC2 instance for authorization. S3 results
were generated on a c7gn.8xlarge instance.

## Running the experiment

To run the experiment, simply run one of the following commands:

```bash
# Tests on a local disk
cargo run --release --bin local
# Tests on s3
cargo run --release --bin s3
```

## Results

Results are written to a file named `disk_results.csv` or `s3_results.csv` depending on the
storage solution used.

To generate a chart you can run the following command:

```bash
# Must run this in the root of the repository
python3 scripts/plot.py
```

This will generate a chart named `chart.png` in the root of the repository.

## Notes

We have checked in the results of the experiment that were used in the paper. Here are some notes
on how these results were generated.

- The NVMe results were generated on a local desktop with a Samsung 970 EVO Plus 2TB drive.
- The S3 results were generated on a c7gn.8xlarge instance
