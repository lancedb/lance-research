import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

# Parquet
pqdf = pd.read_csv("parquet_full_scan.csv")
categories = pqdf["category"].unique().tolist()
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
categories = df["category"].unique().tolist()

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

df = df.sort_values(by="disk_bytes_per_second", ascending=False)
pqdf = pqdf[pqdf["category"].isin(df["category"])]

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
    df["category"], df["disk_bytes_per_second"] / (1024 * 1024 * 1024), label="lance"
)

ax.bar(
    pqdf["category"], pqdf["best"] / (1024 * 1024 * 1024), alpha=0.5, label="parquet"
)

ax.tick_params(axis="x", labelrotation=90)

ax.legend()

plt.savefig("lance_scan_disk.png", bbox_inches="tight")
plt.close()
