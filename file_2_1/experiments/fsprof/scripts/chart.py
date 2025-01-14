import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

configs = [
    ("disk_results.csv", "NVMe", 64),
    ("hdd_results.csv", "HDD", 64),
    ("s3_results.csv", "S3", 512),
]


fig, axes = plt.subplots(len(configs))

# fig.set_figwidth(12.8)
# fig.set_figheight(9.6)

for i, (filename, label, num_threads) in enumerate(configs):
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
        filtered_df["num_iterations"],
        label="IOPS/s",
    )
    ax2.plot(
        read_size,
        filtered_df["bytes_read"],
        "--",
        label="Bytes/s",
    )

    if i == len(configs) - 1:
        ax1.set_xlabel("Read Size (Bytes)")
    ax2.set_ylabel(label)
    ax1.set_ylabel("IOP/s")

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="center right")

plt.subplots_adjust(hspace=0.6)
plt.suptitle("Disk Characteristics")
plt.savefig("chart.png", bbox_inches="tight")
plt.close()
