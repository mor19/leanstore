#include "benchmark/fts/workload.h"
#include "benchmark/adapters/leanstore_adapter.h"

#include <algorithm>
#include <random>
#include <unordered_set>

namespace fts {

// -------------------------------------------------------------------------------------
// Workload operations
template <template <typename> class AdapterType>
Numeric FTSWorkload<AdapterType>::GetOrderTotalPrice(Integer w_id, Integer d_id, Integer o_id) {
  Numeric orderTotalPrice = 0.0;
  this->orderline.ScanOptimized(
    {w_id, d_id, o_id, 0}, {4},
    [&](const OrderLineType::Key &ol_key, const OrderLineType &ol_rec) {
      if (ol_key.ol_w_id == w_id && ol_key.ol_d_id == d_id && ol_key.ol_o_id == o_id) {
        // sum up all amounts (ol_quantity * i_price) of order lines that are in warehouse w_id and in district d_id and
        // order o_id to get the total price of thus order
        orderTotalPrice = orderTotalPrice + ol_rec.ol_amount;
        return true;
      } else {
        return false;
      }
    },
    true);
  return orderTotalPrice;
}

template <template <typename> class AdapterType>
Numeric FTSWorkload<AdapterType>::GetRevenueInDistrict(Integer w_id, Integer d_id) {
  Numeric districtRevenue = 0.0;
  this->orderline.ScanOptimized(
    {w_id, d_id, 0, 0}, {4},
    [&](const OrderLineType::Key &ol_key, const OrderLineType &ol_rec) {
      if (ol_key.ol_w_id == w_id && ol_key.ol_d_id == d_id) {
        // sum up all amounts (ol_quantity * i_price) of order lines that are in warehouse w_id and in district d_id to
        // get the district revenue
        districtRevenue = districtRevenue + ol_rec.ol_amount;
        return true;
      } else {
        return false;
      }
    },
    true);
  return districtRevenue;
}

template <template <typename> class AdapterType>
Numeric FTSWorkload<AdapterType>::GetTotalRevenue() {
  Numeric totalRevenue = 0.0;
  this->orderline.ScanOptimized(
    {0, 0, 0, 0}, {4},
    [&](const OrderLineType::Key &ol_key, const OrderLineType &ol_rec) {
      (void)ol_key;
      // sum up all amounts (ol_quantity * i_price) of all order lines to get the total revenue
      totalRevenue = totalRevenue + ol_rec.ol_amount;
      return true;
    },
    true);
  return totalRevenue;
}

// -------------------------------------------------------------------------------------
// Initial data loader

template <template <typename> class AdapterType>
void FTSWorkload<AdapterType>::LoadOrderLineForWarehouse(Integer w_id) {
  // D_PER_WH districts per warehouse
  for (Integer d_id = 1; d_id <= D_PER_WH; d_id++) {
    Timestamp now = CurrentTimestamp();
    for (Integer o_id = 1; o_id <= C_PER_D; o_id++) {
      Numeric o_ol_cnt = Rand(10) + 5;
      for (Integer ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
        Timestamp ol_delivery_d = 0;
        if (o_id < 2101) { ol_delivery_d = now; }
        Numeric ol_amount     = (o_id < 2101) ? 0 : RandomNumeric(0.01, 9999.99);
        const Integer ol_i_id = Rand(ITEMS_CNT) + 1;
        orderline.Insert({w_id, d_id, o_id, ol_number},
                         {ol_i_id, w_id, ol_delivery_d, 5, ol_amount, RandomString<24>(24, 24)});
      }
    }
  }
}

// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
auto FTSWorkload<AdapterType>::NextTransactionArrivalTime(const std::function<void()> &idle_fn) -> uint64_t {
  return scheduler.Wait(idle_fn);
}

template <template <typename> class AdapterType>
void FTSWorkload<AdapterType>::InitializeThread() {
  if (fts_thread_id > 0) { return; }
  fts_thread_id = fts_thread_id_counter++;
}

template struct FTSWorkload<LeanStoreAdapter>;

}  // namespace fts