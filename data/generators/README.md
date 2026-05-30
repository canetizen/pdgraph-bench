# Data generators

Wrappers around the official LDBC data generators.

- `ldbc_snb_datagen_spark` — drives https://github.com/ldbc/ldbc_snb_datagen_spark
  to produce LDBC SNB Interactive v2 and LDBC SNB BI datasets at the requested
  scale factor.
- `ldbc_finbench_datagen` — drives https://github.com/ldbc/ldbc_finbench_datagen
  to produce LDBC FinBench datasets.

Generated output is written to `data/generated/<dataset>_sf<N>/` on node1; from
there it is distributed to SUT nodes via `rsync` prior to bulk load.
