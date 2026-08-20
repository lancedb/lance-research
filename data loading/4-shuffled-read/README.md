# SIFT1M shuffled read: local NVMe vs. AWS S3

This benchmark writes the same public SIFT1M base vectors to local NVMe and AWS
S3, then consumes identical seeded permutations using open-source LanceDB and
`pylance` (`LanceDataset.take`). Both backends run by default.

## How programmatic S3 access works

S3 exposes an HTTPS API. LanceDB's native object-store client sends API requests
directly; no browser is involved. Each request is cryptographically signed with
temporary or long-lived AWS credentials obtained from the standard AWS
credential chain. Prefer an EC2 IAM role. For local development, AWS environment
variables can be used. This script never accepts or stores secret keys itself.

The IAM identity needs `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`,
`s3:ListBucket`, and `s3:GetBucketLocation`, restricted to the experiment bucket
and prefix.

## Set up

```bash
cd "data loading/4-shuffled-read"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Set the S3 database prefix and region. Authenticate using an EC2 IAM role or
your normal AWS credential setup; do not put secrets in the command or source.

```bash
export SIFT_SHUFFLE_S3_URI="s3://my-bucket/benchmarks/sift1m-shuffled-read"
export AWS_REGION="us-west-2"
```

## Run both backends

Loading and reading in separate processes reduces immediate post-write cache
effects. Loading is untimed setup — it just gets the SIFT1M vectors onto each
backend so the shuffled-read benchmark has something to read:

```bash
python shuffled_read.py --mode load --overwrite
python shuffled_read.py --mode benchmark --trials 5
python plot_results.py
```

`--backend both` is the default. To test only one target:

```bash
python shuffled_read.py --backend local_nvme --mode all --overwrite
python shuffled_read.py --backend aws_s3 --s3-uri s3://my-bucket/prefix --mode all --overwrite
```

For a storage-focused comparison, run the program on an EC2 instance in the
same region as the bucket and use an instance type with local NVMe. Running from
a laptop instead measures the public internet path in addition to S3.

## Outputs

The `results/` directory (data only, gitignored, regenerated on each run) contains:

- `shuffled_read_results.csv`: shuffled-read trials for both backends.
- `shuffled_read_results_readable.txt`: the same trials as a fixed-width text table.
- `metadata.json`: configuration, target URIs, comparison summary, and versions.

Loading (writing SIFT1M onto each backend) is untimed setup for the shuffled-read
benchmark, not something this experiment measures, so no load metrics are recorded.

The `plots/` directory (tracked in git) contains:

- `shuffled_read_trials_by_backend.png`: trial-by-trial comparison.
- `shuffled_read_backend_comparison.png`: median NVMe versus S3 throughput.

Each SIFT vector is one 512-byte value. The comparison uses the same row count,
take size, and permutation seeds on both backends. OS and Lance caches are not
flushed between trials.
