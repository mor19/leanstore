#include "benchmark/tpcc/config.h"

DEFINE_bool(tpcc_warehouse_affinity, false, "Whether to pin warehouse to a specific worker");
DEFINE_uint32(tpcc_warehouse_count, 2, "Number of TPC-C warehouses");
DEFINE_uint64(tpcc_exec_seconds, 20, "Execution time");
DEFINE_bool(tpcc_neworder_only, false, "Whether to run only new order transaction type");
