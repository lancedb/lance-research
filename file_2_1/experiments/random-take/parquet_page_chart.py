import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

MAX_TAKE = 256

df = pd.read_csv("parquet_local.csv")
page_sizes = df["page_size_kb"].unique().tolist()
datatypes = df["column"].unique().tolist()

fig, ax = plt.subplots()
fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.tick_params(axis="x", labelrotation=90)

ax.set_ylim([0, 500_000])
ax.set_xlabel("page size (KiB)")
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

for dt_idx, datatype in enumerate(datatypes):
    filtered = df[(df["take_size"] == MAX_TAKE) & (df["column"] == datatype)]

    ax.plot(
        filtered["page_size_kb"],
        filtered["takes_per_second"],
        marker="o",
        label=datatype,
    )

baseline = pd.read_csv("baseline.csv")

# Remove the 4KB row to align with tests
baseline = baseline.drop(0)

ax.plot(
    baseline["page_size_kb"],
    baseline["iops"],
    label="baseline",
    linestyle="--",
    color="black",
)

ax.legend()

plt.savefig("parquet_page.png", bbox_inches="tight")
plt.close()
