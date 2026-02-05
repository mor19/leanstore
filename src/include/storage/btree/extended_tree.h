#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "storage/page.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/shared_guard.h"

#include <cstring>
#include <functional>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

namespace leanstore::storage {

class ColumnRowStore;

class ExtendedBTree : public KVInterface {
 public:

  explicit ExtendedBTree(buffer::BufferManager *buffer_pool, ColumnRowStore *column_row_store,
                         std::vector<u32> &columnSizes, bool append_bias = false);
  ~ExtendedBTree() override = default;

  /* ExtendedBTree config*/
  void SetComparisonOperator(ComparisonLambda cmp) override;

  /* All ExtendedBTree operators */
  // -------------------------------------------------------------------------------------
  /* Public APIs for external use */
  auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> bool override;
  void Insert(std::span<u8> key, std::span<const u8> payload) override;
  auto Remove(std::span<u8> key) -> bool override;
  auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> bool override;
  auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> bool override;
  void ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) override;
  auto CountEntries() -> u64 override;
  auto SizeInMB() -> float override;
  auto LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const AccessPayloadFunc &read_cb)
    -> bool override;
  void MoveHotDataToColdData();

  // -------------------------------------------------------------------------------------
  /* APIs for use within LeanStore */
  auto IsNotEmpty() -> bool;
  auto CountPages() -> u64;

 private:
  /* Iterate all pages utilities */
  auto IterateAllNodes(sync::OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                       const std::function<u64(BTreeNodeWithTimeStamp &)> &inner_fn,
                       const std::function<u64(BTreeNodeWithTimeStamp &)> &leaf_fn) -> u64;
  auto IterateUntils(sync::OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                     const std::function<bool(BTreeNodeWithTimeStamp &)> &inner_fn,
                     const std::function<bool(BTreeNodeWithTimeStamp &)> &leaf_fn) -> bool;
  void IterateLeafParents(sync::OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                          sync::OptimisticGuard<BTreeNodeWithTimeStamp> &parent, time_t current_time);

  /* Find Leaf Node storing the key */
  auto FindLeafOptimistic(std::span<u8> key) -> sync::OptimisticGuard<BTreeNodeWithTimeStamp>;
  auto FindLeafShared(std::span<u8> key) -> sync::SharedGuard<BTreeNodeWithTimeStamp>;

  /* Split/Merge utilities */
  void TrySplit(sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&node);
  void EnsureSpaceForSplit(BTreeNodeWithTimeStamp *to_split, std::span<u8> key);
  void TryMerge(sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&left,
                sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&right, leng_t left_pos);
  void EnsureUnderfullInnersForMerge(BTreeNodeWithTimeStamp *to_merge);

  /* Core properties */
  buffer::BufferManager *buffer_;
  leng_t metadata_slotid_;
  ColumnRowStore *column_row_store_;
  std::vector<u32> &column_sizes_;
  std::atomic<bool> append_bias_;

  /* Comparison properties */
  ComparisonLambda cmp_lambda_{ComparisonOperator::MEMCMP, std::memcmp};

  /* node remove function */
  void RemoveInnerNode(sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                       sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&node);
};

}  // namespace leanstore::storage