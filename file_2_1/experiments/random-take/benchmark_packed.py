# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess

PACKED = ["unpacked", "packed"]
NUM_FIELDS = [2, 3, 4, 5]


def get_bench_args(column_type: str):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/packeds",
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
        "1000",
        "--format",
        "lance2-1",
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

    return float(result.stdout)


if __name__ == "__main__":
    print("packed,num_fields,takes_per_second")
    for packed in PACKED:
        for num_fields in NUM_FIELDS:
            shutil.rmtree("/tmp/packeds", ignore_errors=True)
            os.mkdir("/tmp/packeds")
            column = f"{packed}{num_fields}"
            takes_per_second = bench_throughput(
                column,
            )
            print(f"{packed},{num_fields},{takes_per_second}")
