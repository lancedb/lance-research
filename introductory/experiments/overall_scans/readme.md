# Scans in Lance

These benchmarks demonstrate overall performance while scanning Lance
files. These give a high-level idea of how Lance can perform against
other formats in current libraries. However, it does not necessarily
demonstrate that there are no optimizations that could make Lance as
fast as Parquet, or vice-versa. We try to discuss those potential
obersavtion when they are obvious. However, this does give a good idea
of the state of scanning library right now, which is what end users care
about most.

## TPC-H Queries

We look at performance of queries 1 and 6. Neither of these have joins
and are aggregations of the `lineitem` table.

Generated TPC-H lineitem table with scale factor of 10 using DuckDB. The
generated data sizes are 9.9 GB for Lance and 2.5 GB for Parquet.
Already, we can see one of the advantages of Parquet: it has a lot more
encodings for analytics data, such as RLE and bitpacking.

In runtime, Lance is 7.3 times slower than Parquet to scan the whole
table. Only 4x of that is the number of bytes needing to be read, so
there is some other factor involved. What about page size? If we
increase the page size, Lance is only 2x slower.

The combination of these points to two pieces of future work needed to
make Lance a decent alternative to Parquet for metadata columns:

1.  Add encodings for integer-based data such as RLE and bitpacking.
2.  Find a way to de-couple page sizes of wide and narrow columns, so
    that we can use larger pages for scalar columns, while keeping the
    shorter pages for wide columns.
