import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker
from pathlib import Path

results_dir = Path(__file__).resolve().parent.parent.joinpath("results")
charts_dir = Path(__file__).resolve().parent.parent.joinpath("charts")


plt.rc("axes", axisbelow=True)

MAX_TAKE = 256

df = pd.read_csv(results_dir.joinpath("take_parquet.csv"))
page_sizes = df["page_size_kb"].unique().tolist()
datatypes = df["column"].unique().tolist()

fig, ax = plt.subplots()
fig.set_dpi(150)
fig.set_size_inches(4, 2)

ax.tick_params(axis="x", labelrotation=90)

ax.set_ylim([0, 400_000])
ax.set_xlabel("page size (KiB)")
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

for dt_idx, datatype in enumerate(datatypes):
    if "list" in datatype:
        continue

    filtered = df[(df["take_size"] == MAX_TAKE) & (df["column"] == datatype)]

    ax.plot(
        filtered["page_size_kb"],
        filtered["takes_per_second"],
        marker="o",
        label=datatype,
    )

baseline = pd.read_csv(results_dir.joinpath("baseline.csv"))

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

plt.savefig(charts_dir.joinpath("parquet_page.png"), bbox_inches="tight")
plt.close()
