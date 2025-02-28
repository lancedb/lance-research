import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker
from pathlib import Path

results_dir = Path(__file__).resolve().parent.parent.joinpath("results")
charts_dir = Path(__file__).resolve().parent.parent.joinpath("charts")


plt.rc("axes", axisbelow=True)

df = pd.read_csv(results_dir.joinpath("parquet_full_scan.csv"))
PAGE_SIZE = 64
categories = df["category"].unique().tolist()

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 1.75)

bests = []
for category in categories:
    filtered = df[df["category"] == category]
    best_score = filtered["iterations_per_second"].max()
    best_dbps = filtered[filtered["iterations_per_second"] == best_score][
        "disk_bytes_per_second"
    ].values[0]
    bests.append(best_dbps)

df = pd.DataFrame({"category": categories, "best": bests})
df = df.sort_values(by="best", ascending=False)

ax.set_ylabel("GiB/s")
ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))
ax.set_ylim([0, 3.5])

ax.plot(
    df["category"],
    [3.309 for _ in df["category"]],
    alpha=0.5,
    color="gray",
    linestyle="--",
)

ax.bar(
    df["category"],
    df["best"] / (1024 * 1024 * 1024),
)

ax.tick_params(axis="x", labelrotation=90)

plt.savefig(charts_dir.joinpath("parquet_scan_disk.png"), bbox_inches="tight")
plt.close()
