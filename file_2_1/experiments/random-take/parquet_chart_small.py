import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

SMALL_PAGE_SIZE = 8

df = pd.read_csv("parquet_local.csv")
take_sizes = df["take_size"].unique().tolist()
datatypes = df["column"].unique().tolist()

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

num_take_sizes = len(take_sizes)
print(f"There are {num_take_sizes} take sizes")
fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.tick_params(axis="x", labelrotation=90)

ax.set_ylim([0, 500_000])
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

for ts_idx, take_size in enumerate(take_sizes):
    filtered = df[
        (df["page_size_kb"] == SMALL_PAGE_SIZE) & (df["take_size"] == take_size)
    ]

    ax.plot(
        filtered["column"],
        filtered["takes_per_second"],
        label=f"k={take_size}",
    )

# How many KiB of data must be read for each element.  For most datatypes,
# this is the page size (8 KiB).  Some of the larger types are larger than
# the page size however.
#
# These are approximations.  The list and binary types have some level of randomness
# involved in the size of data.
datatypes_to_read_size = {
    "scalar": 8,
    "scalar-list": 8,
    "string": 8,
    "string-list": 8,
    "vector": 8,
    "vector-list": 15,
    "binary": 20,
    "binary-list": 100,
}

ax.legend()

plt.savefig("parquet_chart_small.png", bbox_inches="tight")
plt.close()
