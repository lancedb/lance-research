import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plt.rc("axes", axisbelow=True)

df = pd.read_csv("uncompressed.csv")
df = df.sort_values(by="uncompressed_size", ascending=False)

fig, ax = plt.subplots()
fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_xlabel("Category")
ax.set_ylabel("File size (bytes)")

categories = df["category"]

x = np.arange(len(categories))
width = 0.25

ax.bar(
    x,
    df["uncompressed_size"],
    width,
    label="uncompressed",
)

ax.bar(
    x + width,
    df["compressed_size"],
    width,
    label="parquet",
)

ax.bar(
    x + width + width,
    df["lance_size"],
    width,
    label="lance",
)

ax.tick_params(axis="x", labelrotation=90)
ax.set_xticks(x + width, categories)
ax.legend()

plt.savefig("uncompression.png", bbox_inches="tight")
plt.close()


fig, ax = plt.subplots()
fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_xlabel("Category")
ax.set_ylabel("Compression ratio")

pq_compression_ratios = df["uncompressed_size"] / df["compressed_size"]
lance_compression_ratios = df["uncompressed_size"] / df["lance_size"]

width = 0.35

ax.bar(
    x,
    pq_compression_ratios,
    width,
    label="parquet",
)

ax.bar(
    x + width,
    lance_compression_ratios,
    width,
    label="lance",
)

ax.tick_params(axis="x", labelrotation=90)
ax.set_xticks(x + (width / 2), categories)
ax.legend()

plt.savefig("uncompression-ratios.png", bbox_inches="tight")
plt.close()
