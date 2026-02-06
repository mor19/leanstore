#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/tpcc/config.h"
#include "benchmark/tpcc_extended/config.h"
#include "benchmark/tpcc_extended/workload_extended.h"
#include "leanstore/env.h"
#include "leanstore/leanstore.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore TPC-C Extended");  // Extended with adapted Query 2 from TPC-H
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);

  // Statistics
  PerfEvent e;
  PerfController ctrl;

  // Initialize LeanStore
  auto db   = std::make_unique<leanstore::LeanStore>();
  auto tpcc = std::make_unique<tpcc::TPCCWorkloadExtended<LeanStoreAdapter>>(
    FLAGS_tpcc_warehouse_count, true, true, true, static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, *db);

  // TPC-C loader
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    tpcc->LoadItem();
    tpcc->LoadWarehouse();
    tpcc->LoadSupplier();
    db->CommitTransaction();
  });
  for (Integer w_id = 1; w_id <= static_cast<Integer>(FLAGS_tpcc_warehouse_count); w_id++) {
    spdlog::debug("Prepare for warehouse {}", w_id);
    db->worker_pool.ScheduleAsyncJob(w_id % FLAGS_worker_count, [&, w_id]() {
      tpcc->InitializeThread();
      db->StartTransaction();
      tpcc->LoadStock(w_id);
      tpcc->LoadDistrinct(w_id);
      for (Integer d_id = 1; d_id <= tpcc->D_PER_WH; d_id++) {
        tpcc->LoadCustomer(w_id, d_id);
        tpcc->LoadOrder(w_id, d_id);
      }
      db->CommitTransaction();
    });
    spdlog::debug("Prepare warehouse {} successfully", w_id);
  }
  db->worker_pool.JoinAll();
  spdlog::info("Space used: {:.4f} GB", db->AllocatedSize());
  auto initial_wal_size = db->WALSize();

#ifdef DEBUG
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    auto w_cnt = tpcc->warehouse.Count();
    auto d_cnt = tpcc->district.Count();
    spdlog::debug("Warehouse count: {} - District count: {}", w_cnt, d_cnt);
    assert(w_cnt == FLAGS_tpcc_warehouse_count);
    assert(d_cnt == FLAGS_tpcc_warehouse_count * tpcc->D_PER_WH);

    auto c_cnt       = tpcc->customer.Count();
    auto c_index_cnt = tpcc->customer_wdc.Count();
    spdlog::debug("Customer count: {} - Customer's index count: {}", c_cnt, c_index_cnt);
    assert(c_cnt == c_index_cnt);

    auto o_cnt       = tpcc->order.Count();
    auto o_index_cnt = tpcc->order_wdc.Count();
    spdlog::debug("Order count: {} - Order's index count: {}", o_cnt, o_index_cnt);
    assert(o_cnt == o_index_cnt);
    db->CommitTransaction();
  });
#endif
  db->worker_pool.JoinAll();
  // extended TPC-C execution
  double scanDuration = 0;
  // db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  leanstore::start_profiling = true;
  for (auto turn = 0U; turn < 10; turn++) {
    // TPC-C
    db->worker_pool.ScheduleSyncJob(0, [&]() {
      tpcc->InitializeThread();

      for (auto i = 0U; i < FLAGS_tpcc_extended_tpcc_operations; i++) {
        int w_id = UniformRand(1, FLAGS_tpcc_warehouse_count);
        db->StartTransaction();
        tpcc->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
    db->worker_pool.JoinAll();
#ifdef DEBUG
    spdlog::debug("tpcc operations done. moving hot to cold data");
#endif
    // move hot to cold data
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));
    db->worker_pool.ScheduleSyncJob(0, [&]() {
      tpcc->InitializeThread();
      db->StartTransaction();
      for (auto &[type, ptr] : db->indexes) { ptr->ConvertHotDataToColdData(); }
      db->CommitTransaction();
    });
#ifdef DEBUG
    spdlog::debug("moving hot to cold data done. ");
#endif
    // scan (measure time!)
    e.startCounters();
    db->worker_pool.ScheduleSyncJob(0, [&]() {
      db->StartTransaction();
      tpcc->Query2();
      db->CommitTransaction();
    });
    e.stopCounters();
    scanDuration += e.getDuration();
  }
  ctrl.StopPerfRuntime();
  db->Shutdown();
  spdlog::info("Space used: {:.4f} GB - WAL size: {:.4f} GB", db->AllocatedSize(), db->WALSize() - initial_wal_size);
  spdlog::info("scan: {:.4f} tuples/s", leanstore::statistics::total_scanned_tuples.load() / scanDuration);
}
