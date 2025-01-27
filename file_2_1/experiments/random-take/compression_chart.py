import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plt.rc("axes", axisbelow=True)

df = pd.read_csv("parquet_full_scan.csv")

fig, ax = plt.subplots()
fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_xlabel("Category")
ax.set_ylabel("Compression ratio")

ax.set_yscale("log")

compression_ratios = df["mem_bytes_per_second"] / df["disk_bytes_per_second"]
categories = df["category"]

x = np.arange(len(categories))

ax.bar(
    x,
    compression_ratios,
    label="parquet",
)

ax.set_xticks(x, categories)
ax.legend()

plt.savefig("compression.png", bbox_inches="tight")
plt.close()
