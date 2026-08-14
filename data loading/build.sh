#!/usr/bin/env bash
# Build Docker images for all experiments.
# Must be run from the repo root (the directory containing this script).
set -euo pipefail

cd "$(dirname "$0")"

case "${1:-}" in
  instruct)
    docker build -f instruct_fine_tuning/Dockerfile . "${@:2}"
    ;;
  resnet)
    docker build -f resnet_benchmark/Dockerfile . "${@:2}"
    ;;
  *)
    echo "Usage: $0 {instruct|resnet} [extra docker build args]"
    echo ""
    echo "  $0 instruct                      # build instruct fine-tuning image"
    echo "  $0 resnet                         # build resnet benchmark image"
    echo "  $0 instruct -t my-registry/img:v1 # tag the image"
    exit 1
    ;;
esac
