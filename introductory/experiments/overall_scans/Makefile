# These commands can be run from top to bottom to generate the report.

# Install Python dependencies to run the experiments.
setup:
	pip install -r requirements.txt
.PHONY: setup

# Generate the datasets.
#
# The Laion dataset requires the environment variable `LAION_1M_DATA` to be set
# to the path of the downloaded data in ../datasets/laion_5b/loader.py.
datagen:
	python datagen.py --verbose
.PHONY: datagen

# Run the benchmarks.
bench:
	pytest bench.py -v --benchmark-save=data
	python postprocess.py
.PHONY: bench

# Build the GH Markdown file.
build:
	quarto render overall_scans.qmd
.PHONY: build

# (Optional) Build an HTML version of the report.
build-html:
	quarto render overall_scans.qmd --to html
.PHONY: build-html

# Build up test data and ancilary files.
clean:
	rm -rf *.bbl *.blg *.fls *.html *.pdf data
.PHONY: clean

