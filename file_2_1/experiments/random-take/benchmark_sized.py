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
NUM_FILES = [
    1000,
    1000,
    800,
    400,
    200,
]


def get_bench_args(column_type: str, num_files: int):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/sizeds",
        "--take-size",
        "256",
        "--cache-metadata",
        "--data-type",
        column_type,
        "--page-size-kb",
        "8",
        "--drop-caches",
        "--concurrency",
        "256",
        "--quiet",
        "--num-files",
        str(num_files),
        "--format",
        "lance2-1",
    ]

    return args


def bench_throughput(
    column_type: str,
    num_files: int,
):
    args = get_bench_args(column_type, num_files)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)


if __name__ == "__main__":
    print("encoding,size_bytes,takes_per_second")
    for encoding in ENCODINGS:
        for size_idx, size in enumerate(SIZES):
            shutil.rmtree("/tmp/sizeds", ignore_errors=True)
            os.mkdir("/tmp/sizeds")
            column = ""
            if encoding == "mb":
                column = f"sized-mini-block{size_idx + 1}"
            else:
                column = f"sized-full-zip{size_idx + 1}"
            takes_per_second = bench_throughput(
                column,
                NUM_FILES[size_idx],
            )
            print(f"{encoding},{size},{takes_per_second}")
