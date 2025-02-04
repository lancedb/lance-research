import pandas as pd
import matplotlib.pyplot as plt

plt.rc("axes", axisbelow=True)

df = pd.read_csv("parquet_encoding.csv")

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

plt.savefig("parquet_encoding.png", bbox_inches="tight")
plt.close()
