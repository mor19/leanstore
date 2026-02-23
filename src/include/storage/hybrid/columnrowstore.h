#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "leanstore/hkv_interface.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/extended_tree.h"
#include "storage/btree/node.h"
#include "storage/btree/tree.h"
#include "storage/btree/wal.h"
#include "storage/hybrid/column_chunk.h"
#include "storage/page.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/shared_guard.h"

#include <cstring>
#include <functional>
#include <span>
#include <tuple>
#include <set>
#include <utility>
#include <vector>

namespace leanstore::storage {

class ColumnRowStore : public HKVInterface {
 public:
  explicit ColumnRowStore(buffer::BufferManager *buffer_pool, blob::BlobManager *blob_manager,
                          recovery::RecoveryManager *recovery, std::vector<u32> columnSizes, u32 tree_slot_idx,
                          u32 tree_slot_hot_data);
  ~ColumnRowStore() override = default;

  /* config*/
  void SetComparisonOperator(ComparisonLambda cmp) override;

  // -------------------------------------------------------------------------------------
  /* Public APIs for external use */
  auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> bool override;
  void Insert(std::span<u8> key, std::span<const u8> payload) override;
  auto Remove(std::span<u8> key) -> bool override;
  auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> bool override;
  auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> bool override;
  void ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanOptimized(std::span<u8> key, const std::set<u32> &column_idxs, const AccessRecordFunc &fn,
                     const bool ascending) override;
  void ScanFullNoOrder(const std::set<uint32_t> &column_idxs, const AccessRecordFunc &fn) override;
  auto CountEntries() -> u64 override;
  auto SizeInMB() -> float override;
  auto LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const AccessPayloadFunc &read_cb)
    -> bool override;
  void ConvertHotDataToColdData() override;
  auto CountColdEntries() -> u64;
  auto CountHotEntries() -> u64;

  // -------------------------------------------------------------------------------------
  /* APIs for use within LeanStore */
  auto IsNotEmpty() -> bool;
  auto CountPages() -> u64;
  void StoreColdData(std::vector<u8> &rowIds, std::vector<std::vector<u8>> &data);

 private:
  bool InternalRemove(u64 row_id);
  ColumnChunk *FindChunkInColdData(u64 row_id);

  /* Core properties */
  buffer::BufferManager *buffer_;
  blob::BlobManager *blob_;
  std::atomic<u64> next_row_id;
  storage::BTree row_id_index;
  std::vector<u32> columnSizes;
  storage::ExtendedBTree hot_data;
  std::vector<ColumnChunk> cold_data;
  std::set<u32> allColumnIndices;  // contains all column indices
  u32 payloadSize;
   std::vector<u32> columnSizesBeforeSum;
};

}  // namespace leanstore::storage