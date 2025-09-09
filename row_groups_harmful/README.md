# Row Groups Harmful - Research Experiments

This workspace contains Rust experiments for a research paper investigating the impact of row groups on query performance.

## Structure

- `experiments/` - Individual experiment crates
- `data/` - Input datasets (real and synthetic)
- `results/` - Benchmark results and output files
- `charts/` - Generated visualization files
- `chart-scripts/` - Python scripts for generating charts
- `figures/` - Publication-ready figures

## Getting Started

Each experiment is a separate crate in the `experiments/` directory. To run an experiment:

```bash
cargo run --bin <experiment-name>
```

To build all experiments:

```bash
cargo build --release
```

## Adding New Experiments

1. Create a new directory in `experiments/`
2. Add a `Cargo.toml` with the workspace dependencies
3. Implement your experiment in `src/`
4. The workspace will automatically discover the new member