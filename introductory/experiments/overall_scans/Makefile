# These commands can be run from top to bottom to generate the report.

# Install Python dependencies to run the experiments.
setup:
	pip install -r requirements.txt
.PHONY: setup

# Generate the datasets.
#
# The Laion dataset requires the environment variable `LAION_1M_DATA` to be set
# to the path of the downloaded data in ../datasets/laion_5b/loader.py.
#
# You can generate a specific dataset by passing the `--dataset` flag to
# datagen.py.
datagen:
	python datagen.py --verbose
.PHONY: datagen

# Run the benchmarks.
bench:
	pytest bench.py -v --benchmark-save=data
	python postprocess.py
.PHONY: bench

# Build up test data and ancilary files.
clean:
	rm -rf *.bbl *.blg *.fls *.html *.pdf data
.PHONY: clean
