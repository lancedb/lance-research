import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("parquet_full_scan.csv")
page_sizes = df["page_size_kb"].unique().tolist()
categories = df["category"].unique().tolist()

num_page_sizes = len(page_sizes)
fig, ax = plt.subplots()

for cat_idx, category in enumerate(categories):
    filtered = df[df["category"] == category]

    max_perf = filtered["iterations_per_second"].max()
    norm_perf = filtered["iterations_per_second"] / max_perf

    ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

    ax.plot(
        filtered["page_size_kb"],
        norm_perf,
        label=category,
    )

ax.legend()

plt.suptitle("Parquet Full Scan")
plt.savefig("parquet_scan.png", bbox_inches="tight")
plt.close()
