#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "leanstore/hkv_interface.h"
#include "storage/blob/blob_manager.h"
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
#include <utility>
#include <vector>

namespace leanstore::storage {

class ColumnRowStore : public HKVInterface {
 public:
  explicit ColumnRowStore(buffer::BufferManager *buffer_pool, blob::BlobManager *blob_manager,
                          std::vector<u32> columnSizes, bool append_bias = false);
  ~ColumnRowStore() override = default;

  /* config*/
  void ToggleAppendBiasMode(bool append_bias) override;
  void SetComparisonOperator(ComparisonLambda cmp) override;

  // -------------------------------------------------------------------------------------
  /* Public APIs for external use */
  auto LookUp(std::span<u8> key, const PayloadFunc &read_cb) -> bool override;
  void Insert(std::span<u8> key, std::span<const u8> payload) override;
  auto Remove(std::span<u8> key) -> bool override;
  auto Update(std::span<u8> key, std::span<const u8> payload, const PayloadFunc &func) -> bool override;
  auto UpdateInPlace(std::span<u8> key, const PayloadFunc &func, FixedSizeDelta *delta) -> bool override;
  void ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanOptimized(std::span<u8> key, std::vector<u32> &column_idxs, const AccessRecordFunc &fn) override;
  auto CountEntries() -> u64 override;
  auto SizeInMB() -> float override;
  auto LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const PayloadFunc &read_cb)
    -> bool override;

  // -------------------------------------------------------------------------------------
  /* APIs for use within LeanStore */
  auto IsNotEmpty() -> bool;
  auto CountPages() -> u64;

 private:
  // TODO
  bool InternalRemove(u64 row_id);
  ColumnChunk *FindChunkInColdData(u64 row_id);
  void MoveInternalNodeToColdData();

  /* Core properties */
  buffer::BufferManager *buffer_;
  blob::BlobManager *blob_;
  std::atomic<u64> next_row_id;
  storage::BTree row_id_index;
  storage::BTree hot_data;
  std::vector<ColumnChunk> cold_data;
  std::vector<u32> columnSizes;
  std::vector<u32> allColumnIndices;  // contains all column indices
  storage::BTree row_id_to_key_index; // reverse row id to key index (for cold data, this could/should be stored as extra blob)
};

}  // namespace leanstore::storage