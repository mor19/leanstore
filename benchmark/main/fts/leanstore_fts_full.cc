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

  // move hot to cold data
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    fts->InitializeThread();
    db->StartTransaction();
    for (auto &[type, ptr] : db->indexes) { ptr->ConvertHotDataToColdData(); }
    db->CommitTransaction();
  });
  db->worker_pool.JoinAll();

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
  spdlog::info("executed full query {} times on {} worker threads", FLAGS_fts_run_queries_count, FLAGS_worker_count);
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
  spdlog::info("scan: {:.4f} tuples/s", leanstore::statistics::total_scanned_tuples.load() / e.getDuration());
}
