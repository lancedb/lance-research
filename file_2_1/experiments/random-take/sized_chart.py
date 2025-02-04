import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("sized.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_xlabel("bytes per value")
ax.set_ylim([0, 800_000])
ax.set_ylabel("values per second")

ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))

dfmb = df[df["encoding"] == "mb"]

ax.plot(
    dfmb["size_bytes"],
    dfmb["takes_per_second"],
    label="miniblock",
)

dffz = df[df["encoding"] == "fz"]

ax.plot(
    dffz["size_bytes"],
    dffz["takes_per_second"],
    label="fullzip",
)

ax.legend()

plt.savefig("sized.png", bbox_inches="tight")
plt.close()
