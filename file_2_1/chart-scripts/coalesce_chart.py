import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

results_dir = Path(__file__).resolve().parent.parent.joinpath("results")
charts_dir = Path(__file__).resolve().parent.parent.joinpath("charts")

plt.rc("axes", axisbelow=True)

df = pd.read_csv(results_dir.joinpath("coalesce.csv"))

fig, ax = plt.subplots()
fig.set_dpi(150)
fig.set_size_inches(4, 1.75)

ax.set_xlabel("Number of rows")
ax.set_ylabel("Number of pages")

ax.set_xscale("log")

# ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

ax.plot(
    df["num_values"],
    df["pages_hit_4b"],
    label="scalar",
)

ax.plot(
    df["num_values"],
    df["pages_hit_3k"],
    label="embedding",
)

ax.legend()

plt.savefig(charts_dir.joinpath("coalesce.png"), bbox_inches="tight")
plt.close()
