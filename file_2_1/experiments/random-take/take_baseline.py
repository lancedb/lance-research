import json
import subprocess
import sys

print("page_size_kb,iops")
for page_size_kb in [4, 1, 3, 15, 20, 100]:
    args = [
        "fio",
        "--name",
        "benchmark",
        "--group_reporting",
        "--filename=fio.dat",
        "--rw=randread",
        "--size=500m",
        "--io_size=10g",
        "--iodepth=1",
        f"--blocksize={page_size_kb}k",
        "--ioengine=pvsync",
        "--direct=1",
        "--fsync=1",
        "--numjobs=256",
        "--runtime=60",
        "--output-format=json",
    ]
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        sys.exit(1)
    assert result.returncode == 0
    results = json.loads(result.stdout)
    iops = results["jobs"][0]["read"]["iops"]
    print(f"{page_size_kb},{iops}")
