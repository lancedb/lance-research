# Data Files

This folder contains sample parquet files from various datasets.  These are intended to provide real world samples of various
types of data.  These files are samples from publicly available datasets and their licenses are suitable for research purposes.
We provide the files here to allow for accurate reproduction of our results and no other purpose.  If you are interested in doing
your own research or using these files for other purposes then we recommend obtaining the data directly from the data sources
that we list below.

# websites.parquet

This file is a subset of [Common Crawl](https://commoncrawl.org) data.  This data is subject to the terms
outlined in the [Common Crawl Terms of Use](https://commoncrawl.org/terms-of-use).  It represents website data
collected by crawlers and released to the public.  It was converted to Parquet and shared in the hugging face
repository [amazingvince/common-crawl-diverse-sample](https://huggingface.co/datasets/amazingvince/common-crawl-diverse-sample)

# code.parquet

This file is a subset of code scraped from GitHub as part of a public
[dataset release from GitHub](https://github.blog/news-insights/research/making-open-source-data-more-available/).
The data was converted to Parquet and shared in the hugging face repository
[codeparrot/github-code](https://huggingface.co/datasets/codeparrot/github-code).  The code itself is covered
by a number of different open source licenses as detailed in the repository.

# reviews.parquet

This file is a collection of fine food reviews from the Amazon website.  It was collected and release as
a dataset in the public domain as part of the Standford Network Analysis Project.  It is available as a
[Kaggle dataset](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews).  The original file was CSV
and was converted to parquet by us.

# names.parquet

This file is a collection of baby names.  The source is US census data.  It was converted to Paruqet and shared
in the hugging face repository [jbrazzy/baby_names](https://huggingface.co/datasets/jbrazzy/baby_names)

# prompts.parquet

This file is a collection off prompts to LLMs.  It is a subset of [UltraChat](https://github.com/thunlp/UltraChat)
dataset.  It was collected from a trimmed version of the dataset that has been shared on hugging face as
[ultrachat_200k](https://huggingface.co/datasets/HuggingFaceH4/ultrachat_200k).  The dataset is released under
the MIT license.