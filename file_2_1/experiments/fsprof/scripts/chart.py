import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

configs = [
    ("disk_results.csv", "NVMe", 64, "center right"),
    ("s3_results.csv", "S3", 512, "center left"),
]


fig, axes = plt.subplots(len(configs))

fig.set_dpi(150)
fig.set_size_inches(4, 2)

for i, (filename, label, num_threads, loc) in enumerate(configs):
    # Disk plot
    ax1 = axes[i]
    ax2 = ax1.twinx()

    ax1.set_xscale("log")
    ax2.set_xscale("log")

    ax1.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))
    ax2.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))
    ax1.ticklabel_format(style="sci", axis="y")
    ax1.yaxis.set_major_formatter(
        matplotlib.ticker.ScalarFormatter(useOffset=False, useMathText=True)
    )

    df = pd.read_csv(filename)
    filtered_df = df[df["num_threads"] == num_threads]
    read_size = filtered_df["read_size_sectors"] * 4096

    if label == "S3":
        num_reqs = df["num_iterations"].sum()
        print(f"There are {num_reqs} total reqs")

    ax1.plot(
        read_size,
        filtered_df["num_iterations"] / 10,
        label="IOPS/s",
    )
    ax2.plot(
        read_size,
        filtered_df["bytes_read"] / 10 / (1024 * 1024 * 1024),
        "--",
        label="GiB/s",
    )

    if i == len(configs) - 1:
        ax1.set_xlabel("Read Size (Bytes)")
    ax2.set_ylabel(f"{label} GiB/s")
    ax1.set_ylabel(f"{label} IOP/s")

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    if i == 0:
        ax2.legend(lines + lines2, labels + labels2, loc=loc)

plt.subplots_adjust(hspace=0.6)
plt.savefig("disk_perf.png", bbox_inches="tight")
plt.close()
