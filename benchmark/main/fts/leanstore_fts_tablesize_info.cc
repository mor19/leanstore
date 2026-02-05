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
  gflags::SetUsageMessage("Leanstore FTS");
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
    spdlog::debug("Prepare for warehouse %d", w_id);
    db->worker_pool.ScheduleAsyncJob(w_id % FLAGS_worker_count, [&, w_id]() {
      fts->InitializeThread();
      db->StartTransaction();
      fts->LoadOrderLineForWarehouse(w_id);
      db->CommitTransaction();
    });
    spdlog::debug("Prepare warehouse %d successfully", w_id);
  }
  db->worker_pool.JoinAll();
  spdlog::info("Space used: %.4f GB", db->AllocatedSize());

  // get table sizes
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    for (auto &[key, value] : db->indexes) { spdlog::info("%-24s : %-8ld", key.name(), value->CountEntries()); }
    db->CommitTransaction();
  });
  db->worker_pool.JoinAll();
  db->Shutdown();
}
