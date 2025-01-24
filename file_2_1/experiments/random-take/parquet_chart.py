import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("parquet_local.csv")
page_sizes = df["page_size_kb"].unique().tolist()
take_sizes = df["take_size"].unique().tolist()
datatypes = df["column"].unique().tolist()

num_page_sizes = len(page_sizes)
num_take_sizes = len(take_sizes)
print(f"There are {num_page_sizes} page sizes and {num_take_sizes} take sizes")
fig, axes = plt.subplots(
    nrows=num_page_sizes, ncols=num_take_sizes, sharex="col", sharey="row"
)

for ps_idx, page_size in enumerate(page_sizes):
    for ts_idx, take_size in enumerate(take_sizes):
        filtered = df[
            (df["page_size_kb"] == page_size) & (df["take_size"] == take_size)
        ]

        ax = axes[ps_idx][ts_idx]
        if ps_idx == 0 and ts_idx == 0:
            ax.set_title(f"p={page_size} t={take_size}")
        elif ps_idx == 0:
            ax.set_title(f"take={take_size}")
        elif ts_idx == 0:
            ax.set_title(f"page={page_size} KiB")
        ax.tick_params(axis="x", labelrotation=90)

        ax.set_ylim([0, 250_000])

        ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

        ax.plot(
            filtered["column"],
            filtered["takes_per_second"],
            label="Takes/s",
        )

plt.subplots_adjust(hspace=0.4, wspace=0.0)
plt.suptitle("Parquet Random Access")
plt.savefig("chart.png", bbox_inches="tight")
plt.close()
