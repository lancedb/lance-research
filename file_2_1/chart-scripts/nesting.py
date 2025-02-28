import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker
from pathlib import Path

results_dir = Path(__file__).resolve().parent.parent.joinpath("results")
charts_dir = Path(__file__).resolve().parent.parent.joinpath("charts")

plt.rc("axes", axisbelow=True)

df = pd.read_csv(results_dir.joinpath("nesting.csv"))
fmts = df["format"].unique().tolist()

fmt_names = {
    "lance2-0": "lance 2.0",
    "lance2-1": "lance 2.1",
}

fig, axes = plt.subplots(nrows=1, ncols=3)

fig.set_dpi(150)
fig.set_size_inches(5, 2)

ax = axes[0]
ax.set_ylim([0, 500_000])
ax.set_yticks([0, 500_000], labels=["0", "500K"])
ax.set_ylabel("values per second")
ax.set_xlim([1, 5])
ax.set_title("Validity (NVMe)")

for fmt in fmts:
    filtered = df[(df["format"] == fmt) & (df["nesting_type"] == "validity")]

    ax.plot(
        filtered["nesting_level"],
        filtered["takes_per_second"],
        label=fmt_names[fmt],
    )

dfs3 = pd.read_csv(results_dir.joinpath("nesting_s3.csv"))

ax = axes[1]
ax.set_ylim([0, 13_000])
ax.set_yticks([0, 13_000], labels=["0", "13K"])
ax.set_xlim([1, 5])
ax.set_title("Validity (S3)")

for fmt in fmts:
    filtered = dfs3[dfs3["format"] == fmt]

    ax.plot(
        filtered["nesting_level"],
        filtered["takes_per_second"],
        label=fmt_names[fmt],
    )

ax.legend()

ax = axes[2]
ax.set_ylim([0, 500_000])
ax.set_yticks([0, 500_000], labels=["0", "500K"])
ax.set_xlim([1, 5])
ax.set_title("Offsets (NVMe)")

for fmt in fmts:
    filtered = df[(df["format"] == fmt) & (df["nesting_type"] == "list")]

    ax.plot(
        filtered["nesting_level"],
        filtered["takes_per_second"],
        label=fmt_names[fmt],
    )

plt.subplots_adjust(wspace=0.5)
plt.savefig(charts_dir.joinpath("nesting.png"), bbox_inches="tight")
plt.close()
