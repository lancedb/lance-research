import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("small_nesting.csv")
fmts = df["format"].unique().tolist()

fmt_names = {
    "parquet": "parquet",
    "lance2-0": "lance 2.0 (arrow-style)",
    "lance2-1": "lance 2.1",
}

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_ylim([0, 800_000])
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

for fmt in fmts:
    filtered = df[df["format"] == fmt]

    ax.plot(
        filtered["nesting_level"],
        filtered["takes_per_second"],
        label=fmt_names[fmt],
    )

ax.legend()

plt.savefig("nesting.png", bbox_inches="tight")
plt.close()
