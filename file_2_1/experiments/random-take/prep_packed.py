# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess

PACKEDS = [
    2,
    3,
    4,
    5,
]


def get_bench_args(column_type: str):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/packed_prep",
        "--data-type",
        column_type,
        "--num-files",
        "1",
        "--concurrency",
        "256",
        "--format",
        "lance2-1",
        "--duration-seconds",
        "0.0",
    ]

    return args


def bench_throughput(
    column_type: str,
):
    args = get_bench_args(column_type)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False


if __name__ == "__main__":
    shutil.rmtree("/tmp/packed_prep", ignore_errors=True)
    os.mkdir("/tmp/packed_prep")
    for packed in PACKEDS:
        takes_per_second = bench_throughput(
            f"packed{packed}",
        )
