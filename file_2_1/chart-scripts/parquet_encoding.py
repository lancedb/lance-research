import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

results_dir = Path(__file__).resolve().parent.parent.joinpath("results")
charts_dir = Path(__file__).resolve().parent.parent.joinpath("charts")


plt.rc("axes", axisbelow=True)

df = pd.read_csv(results_dir.joinpath("parquet_encoding.csv"))

print(df)

plain = df["takes_per_second"].values[0]
compression = df["takes_per_second"].values[1]
dictionary = df["takes_per_second"].values[2]

x = ["compression", "dictionary", "plain"]
y = [compression, dictionary, plain]

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_ylabel("values per second")

ax.bar(
    x,
    y,
)

ax.tick_params(axis="x", labelrotation=90)

plt.savefig(charts_dir.joinpath("parquet_encoding.png"), bbox_inches="tight")
plt.close()
