import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

dfpq = pd.read_csv("parquet_full_scan.csv")
pq_page_sizes = dfpq["page_size_kb"].unique().tolist()
categories = dfpq["category"].unique().tolist()
dflance = pd.read_csv("lance_full_scan.csv")

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

ax.set_ylabel("normalized performance")

scores = []
for cat_idx, category in enumerate(categories):
    pq_filtered = dfpq[dfpq["category"] == category]

    pq_ips = pq_filtered["iterations_per_second"].max()
    lance_filtered = dflance[dflance["category"] == category]
    scores.append(lance_filtered["iterations_per_second"].max() / pq_ips)

combined = pd.DataFrame({"category": categories, "score": scores})
combined = combined.sort_values(by="score", ascending=False)

ax.set_yscale("log")
ax.yaxis.set_major_locator(matplotlib.ticker.FixedLocator([0.5, 1, 5, 10, 15]))

ax.bar(
    combined["category"],
    combined["score"],
)

ax.plot(
    combined["category"],
    [1.0 for _ in combined["category"]],
    alpha=0.5,
    color="gray",
    linestyle="--",
)

ax.tick_params(axis="x", labelrotation=90)

plt.savefig("pq_v_lance_scan.png", bbox_inches="tight")
plt.close()
