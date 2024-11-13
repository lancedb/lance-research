import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("take_lance.csv")
dfold = pd.read_csv("take_lance_old.csv")
take_sizes = [256]

pqdf = pd.read_csv("take_parquet.csv")

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

datasizes = {
    "scalar": 8,
    "string": 16,
    "scalar-list": 40,
    "string-list": 80,
    "vector": 3 * 1024,
    "vector-list": 15 * 1024,
    "binary": 20 * 1024,
    "binary-list": 100 * 1024,
}

baselinedf = pd.read_csv("take_baseline.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(3.5, 2.5)

ax.tick_params(axis="x", labelrotation=90)

ticks = sorted(datasizes.items(), key=lambda x: x[1])
labels = [tick[0] for tick in ticks]
xticks = [tick[1] for tick in ticks]

ax.set_ylim([0, 800_000])
ax.set_ylabel("values per second")
ax.set_xlim([7, 128 * 1024])
ax.set_xscale("log")
ax.set_xticks(xticks, labels=labels)

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

pqfiltered = pqdf[(pqdf["take_size"] == 256) & (pqdf["page_size_kb"] == 8)]
xvals = [datasizes[datatype] for datatype in pqfiltered["column"]]

ax.scatter(
    xvals,
    pqfiltered["takes_per_second"],
    label="parquet",
    color="gray",
    alpha=0.5,
)

for ts_idx, take_size in enumerate(take_sizes):
    for df, format in [(df, "Lance 2.1"), (dfold, "Lance 2.0")]:
        filtered = df[df["take_size"] == take_size]

        xvals = [datasizes[datatype] for datatype in filtered["column"]]

        if ts_idx == 0 and format == "Lance 2.1":
            num_kis = [1 if x < 1024 else x / 1024 for x in xvals]
            baseline = [
                baselinedf[baselinedf["page_size_kb"] == num_ki]["iops"].max()
                for num_ki in num_kis
            ]
            ax.scatter(
                xvals,
                baseline,
                label="baseline",
                color="black",
                zorder=1,
            )

        ax.scatter(
            xvals,
            filtered["takes_per_second"],
            label=f"{format}",
            zorder=2,
        )

ax.legend()

plt.savefig("take_lance.png", bbox_inches="tight")
plt.close()
