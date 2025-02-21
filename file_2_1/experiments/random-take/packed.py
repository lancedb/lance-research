import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

ENCODINGS = ["unpacked", "packed"]

df = pd.read_csv("packed_take.csv")
NUM_FIELDS = df["num_fields"].unique().tolist()

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_ylabel("values per second")
ax.set_xlabel("# fields in column")
ax.set_xticks(NUM_FIELDS)
ax.set_ylim([0, 250_000])
ax.set_yticks([0, 175_000, 350_000], labels=["0", "175K", "350K"])

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

for encoding in ENCODINGS:
    filtered = df[df["packed"] == encoding]
    ax.plot(
        filtered["num_fields"],
        filtered["takes_per_second"],
        label=encoding,
        marker="o",
    )

ax2 = ax.twinx()
ax2.set_ylim([0, 250])
ax2.set_yticks([0, 125, 250])
ax2.set_ylabel("Mi rows per second (scan)")
df = pd.read_csv("packed_scan.csv")

ax2.plot(
    [2, 3, 4, 5],
    df["iterations_per_second"],
    label="packed scan",
    linestyle="--",
    zorder=2,
    marker="x",
    color="orange",
)

lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(lines + lines2, labels + labels2, loc=0)

plt.savefig("packed.png", bbox_inches="tight")
plt.close()
