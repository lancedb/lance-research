# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess

ENCODINGS = ["mb", "fz"]
SIZES = [
    32,
    64,
    128,
    256,
    512,
]


def get_bench_args(column_type: str):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/sized_prep",
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
    shutil.rmtree("/tmp/sized_prep", ignore_errors=True)
    os.mkdir("/tmp/sized_prep")
    for encoding in ENCODINGS:
        for size_idx, size in enumerate(SIZES):
            column = ""
            if encoding == "mb":
                column = f"sized-mini-block{size_idx + 1}"
            else:
                column = f"sized-full-zip{size_idx + 1}"
            takes_per_second = bench_throughput(
                column,
            )
