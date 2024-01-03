# These commands can be run from top to bottom to generate the report.

# Install Python dependencies to run the experiments.
setup:
	pip install -r requirements.txt
	maturin develop --release
.PHONY: setup

# Generate the datasets.
datagen:
	python datagen.py --verbose
.PHONY: datagen

# Run the benchmarks.
bench:
	pytest bench.py -v --benchmark-save=data
.PHONY: bench

# Build up test data and ancilary files.
clean:
	rm -rf data
.PHONY: clean