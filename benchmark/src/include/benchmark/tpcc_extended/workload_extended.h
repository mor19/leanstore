#pragma once

#include "benchmark/tpcc/workload.h"
#include "benchmark/tpcc_extended/schema_extended.h"
#include "benchmark/tpcc_extended/workload_extended.h"
#include "benchmark/utils/scheduler.h"
#include "common/rand.h"
#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"

#include <algorithm>
#include <array>
#include <variant>
#include <vector>

namespace tpcc {

// copied from
// https://github.com/sfu-dis/preemptdb/blob/beb9765c4af009e45c6e60deb5c627dd1ec93155/benchmarks/tpcc/tpcc-config.h#L57
struct NationData {
  Integer id;
  std::string name;
  Integer rId;
};

extern const NationData NATIONS[];
extern const char *REGIONS[];
extern std::vector<std::vector<std::pair<Integer, Integer>>> supp_stock_map;

template <template <typename> class AdapterType>
struct TPCCWorkloadExtended : public TPCCWorkload<AdapterType> {
  // extra relations
  AdapterType<NationType> nation;
  AdapterType<RegionType> region;
  AdapterType<SupplierType> supplier;

  // -------------------------------------------------------------------------------------
  // Constructor
  template <typename... Params>
  TPCCWorkloadExtended(Integer warehouse_count, bool enable_order_wdc_index, bool enable_cross_warehouses,
                       bool manually_handle_isolation_anomalies, double txn_rate_per_worker, Params &&...params)
      : TPCCWorkload<AdapterType>(warehouse_count, enable_order_wdc_index, enable_cross_warehouses,
                                  manually_handle_isolation_anomalies, txn_rate_per_worker,
                                  std::forward<Params>(params)...),
        nation(AdapterType<NationType>(std::forward<Params>(params)..., NationType::ColumnSizes())),
        region(AdapterType<RegionType>(std::forward<Params>(params)..., RegionType::ColumnSizes())),
        supplier(AdapterType<SupplierType>(std::forward<Params>(params)..., SupplierType::ColumnSizes())) {}

  // -------------------------------------------------------------------------------------
  // extra data loading
  void LoadSupplier();

  // Workload operation
  void Query2();

  // -------------------------------------------------------------------------------------
  auto ExecuteTransaction(Integer w_id) -> int override;
};

}  // namespace tpcc