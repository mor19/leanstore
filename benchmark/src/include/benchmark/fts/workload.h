#pragma once

#include "benchmark/fts/schema.h"
#include "benchmark/utils/scheduler.h"
#include "common/rand.h"
#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"

#include <algorithm>
#include <array>
#include <variant>
#include <vector>

namespace fts {

template <template <typename> class AdapterType>
struct FTSWorkload {
  // Default constraints for TPC-C
  static constexpr Integer OL_I_ID_C = 7911;  // in range [0, 8191]
  static constexpr Integer C_ID_C    = 259;   // in range [0, 1023]
  // NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
  // be within [65, 119]
  static constexpr Integer C_LAST_LOAD_C = 157;  // in range [0, 255]
  static constexpr Integer C_LAST_RUN_C  = 223;  // in range [0, 255]

  // TPC-C (/FTS) run-time constraints
  static constexpr Integer C_PER_D        = 3000;    // Each district has 3k customers
  static constexpr Integer D_PER_WH       = 10;      // Each warehouse has 10 districts
  static constexpr Integer ITEMS_CNT      = 100000;  // Maximum of 100K items
  static constexpr Integer MAX_CARRIER_ID = 10;      // Maximum carrier ID

  // TPC-C name generator
  static constexpr std::array NAME_PARTS = {"Bar", "OUGHT", "ABLE",  "PRI",   "PRES",
                                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};

  // relation
  AdapterType<OrderLineType> orderline;

  // handle isolation anomalies manually (mostly for LeanStore)
  //  a hack because of the missing transaction and concurrency control
  const bool manually_handle_isolation_anomalies = true;
  // rate limit
  benchmark::PoissonScheduler scheduler;

  // Run-time FTS counters
  inline static thread_local Integer fts_thread_id         = 0;
  inline static std::atomic<Integer> fts_thread_id_counter = 1;

  // Random utilities
  auto GenName(Integer id) -> Varchar<16> {
    return Varchar<16>(NAME_PARTS[(id / 100) % 10]) || Varchar<16>(NAME_PARTS[(id / 10) % 10]) ||
           Varchar<16>(NAME_PARTS[id % 10]);
  }

  auto RandomZip() -> Varchar<9> {
    Integer id = Rand(10000);
    Varchar<9> result;
    result.Append(48 + (id / 1000));
    result.Append(48 + (id / 100) % 10);
    result.Append(48 + (id / 10) % 10);
    result.Append(48 + (id % 10));
    return result || Varchar<9>("11111");
  }

  inline auto GetItemID() -> Integer {
    // OL_I_ID_C
    return NonUniformRand(8191, 1, ITEMS_CNT, OL_I_ID_C);
  }

  inline auto GetCustomerID() -> Integer {
    // C_ID_C
    return NonUniformRand(1023, 1, C_PER_D, C_ID_C);
  }

  inline auto GetNonUniformRandomLastNameForRun() -> Integer {
    // C_LAST_RUN_C
    return NonUniformRand(255, 0, 999, C_LAST_RUN_C);
  }

  inline auto GetNonUniformRandomLastNameForLoad() -> Integer {
    // C_LAST_LOAD_C
    return NonUniformRand(255, 0, 999, C_LAST_LOAD_C);
  }

  inline auto CurrentTimestamp() -> Timestamp { return 1; }

  // -------------------------------------------------------------------------------------
  // Workload operations
  Numeric GetOrderTotalPrice(Integer w_id, Integer d_id, Integer o_id);
  Numeric GetRevenueInDistrict(Integer w_id, Integer d_id);
  Numeric GetTotalRevenue();

  // -------------------------------------------------------------------------------------
  // Constructor
  template <typename... Params>
  FTSWorkload(double txn_rate_per_worker, Params &&...params)
      : orderline(AdapterType<OrderLineType>(std::forward<Params>(params)..., OrderLineType::ColumnSizes())),
        scheduler(txn_rate_per_worker) {}

  // -------------------------------------------------------------------------------------
  // Initial data loader
  void LoadOrderLineForWarehouse(Integer w_id);

  // -------------------------------------------------------------------------------------
  void InitializeThread();
  auto NextTransactionArrivalTime(const std::function<void()> &idle_fn) -> uint64_t;
};

}  // namespace fts