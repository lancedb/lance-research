import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("lance_local.csv")
take_sizes = df["take_size"].unique().tolist()
datatypes = df["column"].unique().tolist()

num_take_sizes = len(take_sizes)
print(f"There are {num_take_sizes} take sizes")
fig, axes = plt.subplots(nrows=1, ncols=num_take_sizes, sharex="col", sharey="row")

for ts_idx, take_size in enumerate(take_sizes):
    filtered = df[df["take_size"] == take_size]

    ax = axes[ts_idx]
    ax.set_title(f"take={take_size}")
    ax.tick_params(axis="x", labelrotation=90)

    ax.set_ylim([0, 500_000])

    ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

    ax.plot(
        filtered["column"],
        filtered["takes_per_second"],
        label="Rows/s",
    )

plt.subplots_adjust(hspace=0.4, wspace=0.0)
plt.suptitle("Lance Random Access")
plt.savefig("lance_chart.png", bbox_inches="tight")
plt.close()
