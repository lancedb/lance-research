import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

categories = [
    "names",
    "embeddings",
    "images",
    "reviews",
    "prompts",
    "code",
    "websites",
    "dates",
]

# Parquet
pqdf = pd.read_csv("parquet_full_scan.csv")
pqbests = []
for category in categories:
    filtered = pqdf[pqdf["category"] == category]
    best_score = filtered["iterations_per_second"].max()
    best_dbps = filtered[filtered["iterations_per_second"] == best_score][
        "disk_bytes_per_second"
    ].values[0]
    pqbests.append(best_dbps)
pqdf = pd.DataFrame({"category": categories, "best": pqbests})

# Lance
df = pd.read_csv("lance_full_scan.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 2)

print(df)
df = df.sort_values(by="category", key=lambda x: x.map(categories.index))
print(df)
pqdf = pqdf[pqdf["category"].isin(df["category"])]
print(pqdf)

ax.set_ylabel("GiB/s")
ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))
ax.set_ylim([0, 3.5])

categories = df["category"]

x = np.arange(len(categories))
width = 0.35

ax.plot(
    x,
    [3.309 for _ in df["category"]],
    alpha=0.5,
    color="gray",
    linestyle="--",
)

ax.bar(
    x + width,
    df["disk_bytes_per_second"] / (1024 * 1024 * 1024),
    label="lance",
    width=width,
)

ax.bar(
    pqdf["category"], pqdf["best"] / (1024 * 1024 * 1024), label="parquet", width=width
)

ax.tick_params(axis="x", labelrotation=90)
ax.set_xticks(x + (width / 2), categories)

ax.legend(loc="upper right")

plt.savefig("lance_scan_disk.png", bbox_inches="tight")
plt.close()
