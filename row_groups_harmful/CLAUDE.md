# Claude Code Configuration

## Project Overview
This is a Rust workspace for research experiments investigating the impact of row groups on query performance. Each experiment is implemented as a separate crate within the workspace.

## Build Commands
- `cargo build --release` - Build all experiments in release mode
- `cargo test` - Run all tests across the workspace
- `cargo clippy` - Run linter on all crates
- `cargo fmt` - Format all code

## Workspace Structure
- `experiments/` - Individual experiment crates (workspace members)
- `data/` - Input datasets for benchmarks
- `results/` - Output from benchmark runs
- `charts/` - Generated visualizations
- `chart-scripts/` - Python scripts for generating charts
- `figures/` - Publication-ready figures

## Dependencies
The workspace uses shared dependencies defined in the root Cargo.toml including:
- Arrow ecosystem (arrow-array, arrow-schema, etc.)
- Lance database libraries
- Tokio for async runtime
- Common utilities (clap, tracing, etc.)

## Development Notes
- Each experiment should be a separate crate in `experiments/`
- Use workspace dependencies where possible
- Follow the established patterns from the reference implementation in `../file_2_1`
- Benchmark results should be saved to `results/`
- Charts and visualizations go in `charts/`