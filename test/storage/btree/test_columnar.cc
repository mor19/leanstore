#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"
#include "common/typedefs.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <cstring>
#include <unordered_map>
#include <vector>
#include <tuple>

namespace leanstore::storage {

static constexpr u32 NO_RECORDS = 10000;
static constexpr u32 NO_THREADS = 10;

class TestBTreeColumnar : public BaseTest {
 protected:
  std::unique_ptr<BTree> tree_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<BTree>(buffer_.get());
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }

  template <typename value_t>
  void PrepareData(std::vector<std::pair<int, value_t>> &data, bool get_permutation = false, bool prefill_tree = true) {
    for (size_t idx = 0; idx < NO_RECORDS; idx++) {
      data.emplace_back(static_cast<int>(__builtin_bswap32(idx + 1)), static_cast<value_t>(idx * 100));
    }
    if (get_permutation) {
      for (size_t idx = 0; idx < rand() % NO_RECORDS + NO_RECORDS / 2; idx++) {
        std::next_permutation(data.begin(), data.end());
      }
    }
    Ensure(!tree_->IsNotEmpty());
    if (prefill_tree) {
      for (auto &pair : data) {
        std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
        std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
        tree_->Insert(key, payload);
      }
      Ensure(tree_->IsNotEmpty());
    }
  }
};

// TEST_F(TestBTreeColumnar, ConvertIntoBlobFormat) {
//   std::vector<std::pair<int, __uint128_t>> data;
//   PrepareData<__uint128_t>(data, true);

//   std::vector<u32> key_size = std::vector<u32>();
//   key_size.push_back(sizeof(int));
//   std::vector<u32> value_sizes = std::vector<u32>();
//   value_sizes.push_back(sizeof(__uint128_t));
//   //TODO(moritz):  test
// }

}  // namespace leanstore::storage
