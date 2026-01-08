#include "benchmark/fts/config.h"

DEFINE_bool(fts_warehouse_affinity, false, "Whether to pin warehouse to a specific worker");
DEFINE_uint32(fts_warehouse_count, 40, "Number of warehouses (40 to get ~10 million orderline entries from the start)");
DEFINE_uint64(fts_exec_seconds, 60, "Execution time");
DEFINE_uint32(fts_run_queries_count, 1, "how often the queries  at end when tramnsactions are abortedshould be executed");
