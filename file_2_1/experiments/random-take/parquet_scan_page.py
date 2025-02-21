import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker

plt.rc("axes", axisbelow=True)

df = pd.read_csv("parquet_full_scan.csv")
page_sizes = df["page_size_kb"].unique().tolist()
categories = df["category"].unique().tolist()

page_sizes = [page_size for page_size in page_sizes if page_size > 0]

fig, ax = plt.subplots()

fig.set_dpi(150)
fig.set_size_inches(4, 3)

categories_of_interest = {
    "websites": "-",
    "reviews": "--",
    "images": "-.",
}

ax.set_ylabel("normalized performance")
ax.set_ylim([0.6, 1.0])
ax.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(3))
ax.set_xticks(page_sizes)

first_other = True
for cat_idx, category in enumerate(categories):
    filtered = df[df["category"] == category]

    max_perf = filtered["iterations_per_second"].max()

    bests_by_size = []
    for page_size in page_sizes:
        page_filtered = filtered[filtered["page_size_kb"] == page_size]
        best_score = page_filtered["iterations_per_second"].max() / max_perf
        bests_by_size.append(best_score)

    print(category)
    print(page_sizes)
    print(bests_by_size)

    if True:
        ax.scatter(
            page_sizes,
            bests_by_size,
            label=category,
        )
    else:
        if first_other:
            first_other = False
            ax.scatter(page_sizes, bests_by_size, color="gray", label="other")
        else:
            ax.scatter(page_sizes, bests_by_size, color="gray")

ax.legend()

plt.savefig("parquet_scan_page.png", bbox_inches="tight")
plt.close()
