#include "benchmark/fts/config.h"

DEFINE_bool(fts_warehouse_affinity, false, "Whether to pin warehouse to a specific worker");
DEFINE_uint32(fts_warehouse_count, 2, "Number of warehouses (40 to get ~10 million orderline entries from the start)");
DEFINE_uint64(fts_exec_seconds, 60, "Execution time (only for FTS 2!)");
DEFINE_uint32(fts_run_queries_count_multiplier, 100, "how many queries should be executed per worker (multiplier * (20000/10/1) for (small/medium/full))");
