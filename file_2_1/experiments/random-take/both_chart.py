import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

SMALL_PAGE_SIZE = 8

plt.rc("axes", axisbelow=True)

pq_df = pd.read_csv("parquet_local.csv")
take_sizes = pq_df["take_size"].unique().tolist()
datatypes = pq_df["column"].unique().tolist()

lance_df = pd.read_csv("lance_local.csv")
assert lance_df["take_size"].unique().tolist() == take_sizes
assert lance_df["column"].unique().tolist() == datatypes

num_take_sizes = len(take_sizes)
print(f"There are {num_take_sizes} take sizes")
fig, axes = plt.subplots(nrows=1, ncols=num_take_sizes, sharex="col", sharey="row")

for ts_idx, take_size in enumerate(take_sizes):
    pq_filtered = pq_df[
        (pq_df["page_size_kb"] == SMALL_PAGE_SIZE) & (pq_df["take_size"] == take_size)
    ]

    ax = axes[ts_idx]
    ax.set_title(f"take={take_size}")
    ax.tick_params(axis="x", labelrotation=90)
    ax.set_ylim([0, 500_000])

    ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

    ax.plot(
        pq_filtered["column"],
        pq_filtered["takes_per_second"],
        label="Parquet Rows/s",
    )

    lance_filtered = lance_df[lance_df["take_size"] == take_size]

    ax.plot(
        lance_filtered["column"],
        lance_filtered["takes_per_second"],
        label="Lance Rows/s",
    )

    if ts_idx == 1:
        lines, labels = ax.get_legend_handles_labels()
        ax.legend(lines, labels, loc="upper center")

plt.subplots_adjust(hspace=0.4, wspace=0.0)
plt.suptitle("Random Access")
plt.savefig("both_chart.png", bbox_inches="tight")
plt.close()
