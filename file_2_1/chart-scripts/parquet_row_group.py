import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker
from pathlib import Path

results_dir = Path(__file__).resolve().parent.parent.joinpath("results")
charts_dir = Path(__file__).resolve().parent.parent.joinpath("charts")


plt.rc("axes", axisbelow=True)

df = pd.read_csv(results_dir.joinpath("parquet_full_scan.csv"))
page_sizes = df["page_size_kb"].unique().tolist()
row_group_sizes = df["row_group_size"].unique().tolist()
categories = df["category"].unique().tolist()

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_ylabel("row group size")
ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))
ax.set_xticks(
    range(len(categories)),
    labels=categories,
    rotation=45,
    ha="right",
    rotation_mode="anchor",
)
ax.set_yticks(range(len(row_group_sizes)), labels=["1Ki", "10Ki", "100Ki", "1Mi"])

bests = {}
for category in categories:
    subdf = df[df["category"] == category]
    bests[category] = subdf["iterations_per_second"].max()

chart = []
for row_group_size in row_group_sizes:
    chart_rg = []
    for category in categories:
        subdf = df[
            (df["category"] == category) & (df["row_group_size"] == row_group_size)
        ]
        best = subdf["iterations_per_second"].max()
        if pd.isna(best):
            chart_rg.append(0)
        else:
            norm = best / bests[category]
            chart_rg.append(norm)

    chart.append(chart_rg)

for i in range(len(categories)):
    for j in range(len(row_group_sizes)):
        score = chart[j][i]
        color = "black"
        if score < 0.85:
            color = "white"
        ax.text(i, j, f"{chart[j][i]:.2f}", ha="center", va="center", color=color)

print(np.array(chart))
ax.imshow(np.array(chart), vmin=0.8)

plt.savefig(charts_dir.joinpath("parquet_row_group.png"), bbox_inches="tight")
plt.close()
