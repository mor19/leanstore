#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
#include "storage/hybrid/columnrowstore.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <chrono>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace leanstore::storage {

static constexpr u32 NO_RECORDS          = 10000;
static constexpr u32 TWO_NO_RECORDS      = (2 * NO_RECORDS);
static constexpr u32 MIDHIGH_NO_RECORDS1 = NO_RECORDS + NO_RECORDS / 2;

class TestColumnRowStore : public BaseTest {
 protected:
  std::unique_ptr<ColumnRowStore> columnrowstore_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
  }

  void TearDown() override {
    columnrowstore_.reset();
    BaseTest::TearDown();
  }

  template <typename value_t>
  void Prepare(std::vector<std::pair<int, value_t>> &data, bool get_permutation = false, bool prefill_tree = true,
               u32 recordsNO = NO_RECORDS) {
    columnrowstore_ = std::make_unique<ColumnRowStore>(buffer_.get(), blob_.get(), recovery_.get(),
                                                       std::vector<uint32_t>{sizeof(value_t)}, 0, 1);
    for (size_t idx = 0; idx < recordsNO; idx++) {
      data.emplace_back(static_cast<int>(__builtin_bswap32(idx + 1)), static_cast<value_t>(idx * 100));
    }
    if (get_permutation) {
      for (size_t idx = 0; idx < rand() % recordsNO + recordsNO / 2; idx++) {
        std::next_permutation(data.begin(), data.end());
      }
    }
    Ensure(!columnrowstore_->IsNotEmpty());
    if (prefill_tree) {
      for (auto &pair : data) {
        std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
        std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
        columnrowstore_->Insert(key, payload);
      }
      Ensure(columnrowstore_->IsNotEmpty());
    }
  }
};

// only hot data
TEST_F(TestColumnRowStore, InsertAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  Prepare<__uint128_t>(data, true);

  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(__uint128_t)};

    auto found = columnrowstore_->LookUp(key, [&payload](std::span<u8> data) {
      EXPECT_EQ(payload.size(), data.size());
      for (size_t idx = 0; idx < data.size(); idx++) { EXPECT_EQ(payload[idx], data[idx]); }
    });
    ASSERT_TRUE(found);
  }
}

TEST_F(TestColumnRowStore, RemoveAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  Prepare<__uint128_t>(data, false);
  std::array<bool, NO_RECORDS + 1> removed_f = {false};

  ASSERT_TRUE(columnrowstore_->IsNotEmpty());

  // Remove random
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS); idx++) {
    int int_key     = rand() % NO_RECORDS + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};

    if (!removed_f[int_key]) {
      auto success = columnrowstore_->Remove(key);
      ASSERT_TRUE(success);
      removed_f[int_key] = true;
    } else {
      auto success = columnrowstore_->Remove(key);
      ASSERT_FALSE(success);
    }

    auto found = columnrowstore_->LookUp(key, AccessPayloadFunc());
    ASSERT_FALSE(found);
  }

  // Now remove all
  for (auto idx = 1; idx <= static_cast<int>(NO_RECORDS); idx++) {
    if (!removed_f[idx]) {
      int ordered_key = __builtin_bswap32(idx);
      std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
      auto success = columnrowstore_->Remove(key);
      ASSERT_TRUE(success);
      auto found = columnrowstore_->LookUp(key, AccessPayloadFunc());
      ASSERT_FALSE(found);
    }
  }

  ASSERT_FALSE(columnrowstore_->IsNotEmpty());
}

TEST_F(TestColumnRowStore, UpdateAndQuery) {
  std::vector<std::pair<int, int>> data;
  Prepare<int>(data, false);

  ASSERT_TRUE(columnrowstore_->IsNotEmpty());
  std::unordered_map<int, int> validation;
  for (size_t idx = 0; idx < NO_RECORDS; idx++) { validation[idx + 1] = idx * 100; }

  // Remove random
  for (auto idx = 0ULL; idx < NO_RECORDS; idx++) {
    int int_key     = rand() % NO_RECORDS + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&idx), sizeof(int)};

    auto success        = columnrowstore_->Update(key, payload, [&](std::span<u8> prev) {
      int prev_num;
      std::memcpy(&prev_num, prev.data(), prev.size());
      EXPECT_EQ(prev_num, validation[int_key]);
    });
    validation[int_key] = idx;
    ASSERT_TRUE(success);

    auto found = columnrowstore_->LookUp(key, [&payload](std::span<u8> found_payload) {
      EXPECT_EQ(payload.size(), found_payload.size());
      for (size_t idx = 0; idx < found_payload.size(); idx++) { EXPECT_EQ(payload[idx], found_payload[idx]); }
    });
    ASSERT_TRUE(found);
  }

  ASSERT_EQ(columnrowstore_->CountEntries(), NO_RECORDS);
}

TEST_F(TestColumnRowStore, TreeScan) {
  std::vector<std::pair<int, int>> data;
  Prepare<int>(data, false);
  std::unordered_map<int, int> scan_result;

  // all scans are cold in this version! -> requires conversion to cold data
  columnrowstore_->ConvertHotDataToColdData();

  auto read_cb = [&scan_result](std::span<u8> key, std::span<u8> payload) -> bool {
    scan_result[LoadUnaligned<int>(key.data())] = LoadUnaligned<int>(payload.data());
    return true;
  };

  // ScanOptimized
  std::unordered_set<u32> columnIdxs = {0};
  columnrowstore_->ScanOptimized(std::span<u8>(), columnIdxs, read_cb, true);
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first] == pair.second);
  }

  // Scan Asc
  int start_key = 0;
  std::span key{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  columnrowstore_->ScanAscending(key, read_cb);
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first] == pair.second);
  }

  // Scan Desc
  start_key = NO_RECORDS + 1;
  key       = std::span<u8>{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  scan_result.clear();
  columnrowstore_->ScanDescending(key, read_cb);
  for (auto &pair : data) {
    EXPECT_TRUE(scan_result.find(pair.first) != scan_result.end());
    EXPECT_TRUE(scan_result[pair.first] == pair.second);
  }
  EXPECT_EQ(scan_result.size(), data.size());
}

TEST_F(TestColumnRowStore, InsertAndCheckOrder) {
  // fill tree
  std::vector<std::pair<int, u64>> data;
  columnrowstore_ = std::make_unique<ColumnRowStore>(buffer_.get(), blob_.get(), recovery_.get(),
                                                     std::vector<uint32_t>{sizeof(u64)}, 0, 1);
  for (size_t idx = 0; idx < NO_RECORDS; idx++) {
    data.emplace_back(static_cast<int>(__builtin_bswap32(idx)), static_cast<u64>(idx));
  }

  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(u64)};
    columnrowstore_->Insert(key, payload);
  }

  // all scans are cold in this version! -> requires conversion to cold data
  columnrowstore_->ConvertHotDataToColdData();

  // check order
  u64 last         = 0;
  auto check_order = [&last](std::span<u8> key, std::span<u8> payload) -> bool {
    (void)key;
    u64 current;
    std::memcpy(&current, payload.data(), sizeof(u64));
    // int cur_key;
    // std::memcpy(&cur_key, key.data(), sizeof(int));
    // spdlog::info("{} : {}  %08x : %016lx", cur_key, current, cur_key, current);
    EXPECT_GE(current, last);
    last = current;
    return true;
  };

  int start_key = 0;
  std::span key{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  columnrowstore_->ScanAscending(key, check_order);
}

// with cold data
TEST_F(TestColumnRowStore, InsertAndMoveToColdAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  Prepare<__uint128_t>(data, true, true, NO_RECORDS * 2);

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));

  columnrowstore_->ConvertHotDataToColdData();
  txn_man_->CommitTransaction();
  InitRandTransaction();

  ASSERT_TRUE(columnrowstore_->CountColdEntries() > 0);
  ASSERT_TRUE(columnrowstore_->CountColdEntries() == columnrowstore_->CountHotEntries());
  spdlog::info("cold tuples: {}", columnrowstore_->CountColdEntries());
  spdlog::info("total tuples: {}", columnrowstore_->CountEntries());

  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(__uint128_t)};
    auto found = columnrowstore_->LookUp(key, [&payload](std::span<u8> data) {
      EXPECT_EQ(payload.size(), data.size());
      for (size_t idx = 0; idx < data.size(); idx++) { EXPECT_EQ(payload[idx], data[idx]); }
    });
    ASSERT_TRUE(found);
  }
}

TEST_F(TestColumnRowStore, ColdTreeScan) {
  std::vector<std::pair<int, int>> data;
  Prepare<int>(data, false, true, NO_RECORDS * 2);

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));

  columnrowstore_->ConvertHotDataToColdData();
  txn_man_->CommitTransaction();
  InitRandTransaction();

  ASSERT_TRUE(columnrowstore_->CountColdEntries() > 0);
  ASSERT_TRUE(columnrowstore_->CountColdEntries() == columnrowstore_->CountHotEntries());
  spdlog::info("cold tuples: {}", columnrowstore_->CountColdEntries());
  spdlog::info("total tuples: {}", columnrowstore_->CountEntries());

  std::unordered_map<int, int> scan_result;

  auto read_cb = [&scan_result](std::span<u8> key, std::span<u8> payload) -> bool {
    scan_result[LoadUnaligned<int>(key.data())] = LoadUnaligned<int>(payload.data());
    return true;
  };

  // ScanOptimized
  int start_key = 0;
  std::span key{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  std::unordered_set<u32> columnIdxs = {0};
  columnrowstore_->ScanOptimized(std::span<u8>(), columnIdxs, read_cb, true);
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first] == pair.second);
  }

  // Scan Asc
  // int start_key = 0;
  // std::span key{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  columnrowstore_->ScanAscending(key, read_cb);
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first] == pair.second);
  }

  // Scan Desc
  start_key = NO_RECORDS + 1;
  key       = std::span<u8>{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  scan_result.clear();
  columnrowstore_->ScanDescending(key, read_cb);
  for (auto &pair : data) {
    EXPECT_TRUE(scan_result.find(pair.first) != scan_result.end());
    EXPECT_TRUE(scan_result[pair.first] == pair.second);
  }
  EXPECT_EQ(scan_result.size(), data.size());
}

TEST_F(TestColumnRowStore, ColdScanFull) {
  std::vector<std::pair<int, int>> data;
  Prepare<int>(data, false, true, NO_RECORDS * 2);

  columnrowstore_->ConvertHotDataToColdData();
  txn_man_->CommitTransaction();
  InitRandTransaction();

  ASSERT_TRUE(columnrowstore_->CountColdEntries() > 0);
  ASSERT_TRUE(columnrowstore_->CountColdEntries() == columnrowstore_->CountHotEntries());
  spdlog::info("cold tuples: {}", columnrowstore_->CountColdEntries());
  spdlog::info("total tuples: {}", columnrowstore_->CountEntries());

  std::unordered_map<int, int> scan_result;
  auto count   = 0;
  auto read_cb = [&](std::span<u8> key, std::span<u8> payload) -> bool {
    (void)key;
    count++;
    scan_result[LoadUnaligned<int>(payload.data())] = LoadUnaligned<int>(payload.data());
    return true;
  };

  // ScanFullNoOrder
  std::unordered_set<u32> columnIdxs = {0};
  columnrowstore_->ScanFullNoOrder(columnIdxs, read_cb);
  EXPECT_EQ(count, data.size());
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first * 100) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first * 100] == pair.second);
  }
}

TEST_F(TestColumnRowStore, ColdRemoveAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  Prepare<__uint128_t>(data, false, true, MIDHIGH_NO_RECORDS1);

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));

  columnrowstore_->ConvertHotDataToColdData();
  txn_man_->CommitTransaction();
  InitRandTransaction();

  std::array<bool, MIDHIGH_NO_RECORDS1 + 1> removed_f = {false};

  ASSERT_TRUE(columnrowstore_->IsNotEmpty());

  // Remove random
  for (auto idx = 0; idx < static_cast<int>(MIDHIGH_NO_RECORDS1); idx++) {
    int int_key     = rand() % MIDHIGH_NO_RECORDS1 + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};

    if (!removed_f[int_key]) {
      auto success = columnrowstore_->Remove(key);
      ASSERT_TRUE(success);
      removed_f[int_key] = true;
    } else {
      auto success = columnrowstore_->Remove(key);
      ASSERT_FALSE(success);
    }

    auto found = columnrowstore_->LookUp(key, AccessPayloadFunc());
    ASSERT_FALSE(found);
  }

  // Now remove all
  for (auto idx = 1; idx <= static_cast<int>(MIDHIGH_NO_RECORDS1); idx++) {
    if (!removed_f[idx]) {
      int ordered_key = __builtin_bswap32(idx);
      std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
      auto success = columnrowstore_->Remove(key);
      ASSERT_TRUE(success);
      auto found = columnrowstore_->LookUp(key, AccessPayloadFunc());
      ASSERT_FALSE(found);
    }
  }

  ASSERT_FALSE(columnrowstore_->IsNotEmpty());
}

TEST_F(TestColumnRowStore, ColdUpdateAndQuery) {
  std::vector<std::pair<int, int>> data;
  Prepare<int>(data, false, true, MIDHIGH_NO_RECORDS1);

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_htap_expire_seconds + 1));

  columnrowstore_->ConvertHotDataToColdData();
  txn_man_->CommitTransaction();
  InitRandTransaction();

  ASSERT_TRUE(columnrowstore_->IsNotEmpty());
  std::unordered_map<int, int> validation;
  for (size_t idx = 0; idx < MIDHIGH_NO_RECORDS1; idx++) { validation[idx + 1] = idx * 100; }

  // Remove random
  for (auto idx = 0ULL; idx < MIDHIGH_NO_RECORDS1 / 4; idx++) {
    int int_key     = rand() % MIDHIGH_NO_RECORDS1 + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&idx), sizeof(int)};

    auto success        = columnrowstore_->Update(key, payload, [&](std::span<u8> prev) {
      int prev_num;
      std::memcpy(&prev_num, prev.data(), prev.size());
      EXPECT_EQ(prev_num, validation[int_key]);
    });
    validation[int_key] = idx;
    ASSERT_TRUE(success);

    auto found = columnrowstore_->LookUp(key, [&payload](std::span<u8> found_payload) {
      EXPECT_EQ(payload.size(), found_payload.size());
      for (size_t idx = 0; idx < found_payload.size(); idx++) { EXPECT_EQ(payload[idx], found_payload[idx]); }
    });
    ASSERT_TRUE(found);
  }

  ASSERT_EQ(columnrowstore_->CountHotEntries(), MIDHIGH_NO_RECORDS1);
  ASSERT_TRUE(columnrowstore_->CountColdEntries() < MIDHIGH_NO_RECORDS1);
}

}  // namespace leanstore::storage

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count = 1;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
