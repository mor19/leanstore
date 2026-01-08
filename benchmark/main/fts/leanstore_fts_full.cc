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
  gflags::SetUsageMessage("Leanstore FTS Full");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);

  // Statistics
  PerfEvent e;
  PerfController ctrl;
  std::atomic<bool> keep_running(true);
  leanstore::RegisterSEGFAULTHandler();

  // Initialize LeanStore
  auto db = std::make_unique<leanstore::LeanStore>();
  auto fts =
    std::make_unique<fts::FTSWorkload<LeanStoreAdapter>>(static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, *db);
  fts->orderline.ToggleAppendBiasMode(true);

  // TPC-C loader
  for (Integer w_id = 1; w_id <= static_cast<Integer>(FLAGS_fts_warehouse_count); w_id++) {
    LOG_DEBUG("Prepare for warehouse %d", w_id);
    db->worker_pool.ScheduleAsyncJob(w_id % FLAGS_worker_count, [&, w_id]() {
      fts->InitializeThread();
      db->StartTransaction();
      fts->LoadOrderLineForWarehouse(w_id);
      db->CommitTransaction();
    });
    LOG_DEBUG("Prepare warehouse %d successfully", w_id);
  }
  db->worker_pool.JoinAll();
  LOG_INFO("Space used: %.4f GB", db->AllocatedSize());

  // run full operation (GetTotalRevenue())
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  for (auto t_id = 0U; t_id < FLAGS_worker_count; t_id++) {
    db->worker_pool.ScheduleAsyncJob(t_id, [&, thread_id = t_id]() {
      fts->InitializeThread();
      for (auto i = 0U; i < FLAGS_fts_run_queries_count; i++) {
        // select random order
        db->StartTransaction(fts->NextTransactionArrivalTime([&]() { db->CheckDuringIdle(); }));
        fts->GetTotalRevenue();
        db->CommitTransaction();
      }
    });
  }

  db->worker_pool.JoinAll();
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("executed full query %d times on %d worker threads", FLAGS_fts_run_queries_count, FLAGS_worker_count);
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
}
