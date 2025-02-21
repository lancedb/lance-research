import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

cat_to_size = {
    "mb1": 1,
    "mb2": 4,
    "mb3": 16,
    "mb4": 64,
    "mb5": 256,
    "fz1": 1,
    "fz2": 4,
    "fz3": 16,
    "fz4": 64,
    "fz5": 256,
}

plt.rc("axes", axisbelow=True)

df = pd.read_csv("sized_scan.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_xscale("log")
ax.set_xlabel("bytes per value")
ax.minorticks_off()
ax.set_xticks([1, 4, 16, 64, 256], labels=["1", "4", "16", "64", "256"])
ax.set_ylim([0, 800])
ax.set_yticks([0, 400, 800])
ax.set_ylabel("Mi rows per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

filtered = df[df["category"].str.startswith("mb") & (df["keep_cache"] == False)]

sizes = [cat_to_size[cat] for cat in filtered["category"]]
ax.plot(
    sizes,
    filtered["iterations_per_second"],
    label="miniblock (disk)",
)

filtered = df[df["category"].str.startswith("fz") & (df["keep_cache"] == False)]

sizes = [cat_to_size[cat] for cat in filtered["category"]]
ax.plot(
    sizes,
    filtered["iterations_per_second"],
    label="fullzip (disk)",
)

ax2 = ax.twinx()
ax2.set_ylim([0, 2_000])
ax2.set_yticks([0, 500, 1_000])

filtered = df[df["category"].str.startswith("mb") & (df["keep_cache"] == True)]

sizes = [cat_to_size[cat] for cat in filtered["category"]]
ax2.plot(
    sizes,
    filtered["iterations_per_second"],
    label="miniblock (memory)",
    linestyle="--",
)

filtered = df[df["category"].str.startswith("fz") & (df["keep_cache"] == True)]

sizes = [cat_to_size[cat] for cat in filtered["category"]]
ax2.plot(
    sizes,
    filtered["iterations_per_second"],
    label="fullzip (memory)",
    linestyle="--",
)

lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(lines + lines2, labels + labels2, loc=0)

plt.savefig("sized_scan.png", bbox_inches="tight")
plt.close()
