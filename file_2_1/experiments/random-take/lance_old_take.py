import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("lance_old_take.csv")
take_sizes = df["take_size"].unique().tolist()

pqdf = pd.read_csv("parquet_local.csv")

datatypes = [
    "scalar",
    "string",
    "scalar-list",
    "string-list",
    "vector",
    "vector-list",
    "binary",
    "binary-list",
]

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.tick_params(axis="x", labelrotation=90)

ax.set_ylim([0, 800_000])
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

pqfiltered = pqdf[(pqdf["take_size"] == 256) & (pqdf["page_size_kb"] == 8)]
ax.plot(
    pqfiltered["column"],
    pqfiltered["takes_per_second"],
    label="parquet",
    color="gray",
    linestyle="--",
    alpha=0.5,
)

for ts_idx, take_size in enumerate(take_sizes):
    filtered = df[df["take_size"] == take_size]

    ax.plot(
        filtered["column"],
        filtered["takes_per_second"],
        label=f"k={take_size}",
    )

ax.legend()

plt.savefig("lance_old_take.png", bbox_inches="tight")
plt.close()
