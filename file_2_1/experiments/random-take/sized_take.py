import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("sized.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_xscale("log")
ax.set_xlabel("bytes per value")
ax.minorticks_off()
ax.set_xticks([1, 4, 16, 64, 256], labels=["1", "4", "16", "64", "256"])
ax.set_ylim([0, 800_000])
ax.set_yticks([0, 400_000, 800_000], labels=["0", "400K", "800K"])
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

filtered = df[(df["encoding"] == "mb") & (df["keep_cache"] == False)]

ax.plot(
    filtered["size_bytes"],
    filtered["takes_per_second"],
    label="miniblock (disk)",
)

filtered = df[(df["encoding"] == "fz") & (df["keep_cache"] == False)]

ax.plot(
    filtered["size_bytes"],
    filtered["takes_per_second"],
    label="fullzip (disk)",
)

ax2 = ax.twinx()
ax2.set_ylim([0, 10_000_000])
ax2.set_yticks([0, 5_000_000, 10_000_000], labels=["0", "5M", "10M"])

filtered = df[(df["encoding"] == "mb") & (df["keep_cache"] == True)]

ax2.plot(
    filtered["size_bytes"],
    filtered["takes_per_second"],
    label="miniblock (memory)",
    linestyle="--",
)

filtered = df[(df["encoding"] == "fz") & (df["keep_cache"] == True)]

ax2.plot(
    filtered["size_bytes"],
    filtered["takes_per_second"],
    label="fullzip (memory)",
    linestyle="--",
)

lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(lines + lines2, labels + labels2, loc=0)

plt.savefig("sized_take.png", bbox_inches="tight")
plt.close()
