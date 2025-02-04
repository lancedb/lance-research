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

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

for encoding in ENCODINGS:
    filtered = df[df["packed"] == encoding]
    ax.plot(
        filtered["num_fields"],
        filtered["takes_per_second"],
        label=encoding,
        marker="o",
    )

ax.legend()

plt.savefig("packed.png", bbox_inches="tight")
plt.close()
