# LanceDB Research

This repository contains papers, experiments, and other research performed by the LanceDB team.
It's intended to serve as a transparent and open source resource for the community to learn
about the work we're doing and also to reproduce our results.

## Papers

### Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings

In this paper we introduce the 2.1 version of the Lance file format. We explore random access
performance on NVMe storage for Lance, Parquet, as well as Arrow-style approaches (what we used
in Lance 2.0). We justify the top-level structural encoding scheme for 2.1, demonstrate that we
are able to make good use of NVMe storage, and identify areas for future work.

We also explore full scan performance and verify that 2.1 is able to achieve similar compression
performance to Parquet and meet or exceed Parquet's scan performance in most cases.

Contents for this paper are located in the `file_2_1` directory.
