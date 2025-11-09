#include "benchmark/tpcc/config.h"

DEFINE_int32(tpch_scale_factor, 10, "Scale factor (=approximate db size in GB)"); // must be a value from (1, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000)
DEFINE_bool(tpch_log_process, false, "log process");
// DEFINE_bool(tpcc_warehouse_affinity, false, "Whether to pin warehouse to a specific worker");
// DEFINE_uint32(tpcc_warehouse_count, 4, "Number of TPC-C warehouses");
// DEFINE_uint64(tpcc_exec_seconds, 20, "Execution time");