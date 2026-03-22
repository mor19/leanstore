#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/fts/config.h"
#include "benchmark/fts/workload.h"
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
  gflags::SetUsageMessage("Leanstore FTS Convert");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);

  // Statistics
  PerfEvent e;
  PerfController ctrl;
  std::atomic<bool> keep_running(true);

  // Initialize LeanStore
  auto db = std::make_unique<leanstore::LeanStore>();
  auto fts =
    std::make_unique<fts::FTSWorkload<LeanStoreAdapter>>(static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, *db);

  // TPC-C loader
  for (Integer w_id = 1; w_id <= static_cast<Integer>(FLAGS_fts_warehouse_count); w_id++) {
    spdlog::debug("Prepare for warehouse {}", w_id);
    db->worker_pool.ScheduleAsyncJob(w_id % FLAGS_worker_count, [&, w_id]() {
      fts->InitializeThread();
      db->StartTransaction();
      fts->LoadOrderLineForWarehouse(w_id);
      db->CommitTransaction();
    });
    spdlog::debug("Prepare warehouse {} successfully", w_id);
  }
  db->worker_pool.JoinAll();
  spdlog::info("Space used: {:.4f} GB", db->AllocatedSize());

  // wait for hot data to become old
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));

  u64 convertedTupleCount = 0;
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    fts->InitializeThread();
    db->StartTransaction();
    convertedTupleCount = db->indexes.begin()->second->CountEntries();
    db->CommitTransaction();
  });

  // move hot to cold data & measure
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    fts->InitializeThread();
    db->StartTransaction();
    for (auto &[type, ptr] : db->indexes) { ptr->ConvertHotDataToColdData(); }
    db->CommitTransaction();
  });
  db->worker_pool.JoinAll();
  ctrl.StopPerfRuntime();
  e.stopCounters();
  db->Shutdown();
  spdlog::info("converted {} tuples in {}s", convertedTupleCount, e.getDuration());
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
  spdlog::info("convert hot->cold: {:.4f} tuples/s", convertedTupleCount / e.getDuration());
}
