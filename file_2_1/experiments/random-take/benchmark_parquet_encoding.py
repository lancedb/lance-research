# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess


def get_bench_args(compression, dictionary):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/parquets",
        "--take-size",
        "16",
        "--cache-metadata",
        "--data-type",
        "scalar",
        "--page-size-kb",
        "8",
        "--drop-caches",
        "--concurrency",
        "256",
        "--quiet",
    ]

    if compression:
        args.append("--compression")
    if dictionary:
        args.append("--dictionary")

    return args


def bench_throughput(compression, dictionary):
    args = get_bench_args(compression, dictionary)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)


if __name__ == "__main__":
    print("compression,dictionary,takes_per_second")
    for compression, dictionary in [(False, False), (True, False), (False, True)]:
        shutil.rmtree("/tmp/parquets", ignore_errors=True)
        os.mkdir("/tmp/parquets")
        takes_per_second = bench_throughput(compression, dictionary)
        print(f"{compression},{dictionary},{takes_per_second}")
