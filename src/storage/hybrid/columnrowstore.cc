#include "storage/hybrid/columnrowstore.h"
#include <stdexcept>
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "storage/blob/aliasing_guard.h"

namespace leanstore::storage {

inline std::span<u8> U64ToSpanU8(u64 &u) { return {reinterpret_cast<u8 *>(&u), sizeof(u)}; }

inline blob::BlobState *CopyBlobState(const blob::BlobState *blobState) {
  auto blobStateSize   = blobState->MallocSize();
  auto copiedBlobState = reinterpret_cast<blob::BlobState *>(std::malloc(blobStateSize));
  std::memcpy(copiedBlobState, blobState, blobStateSize);
  return copiedBlobState;
}

/* constructor */
ColumnRowStore::ColumnRowStore(buffer::BufferManager *buffer_pool, blob::BlobManager *blob_manager,
                               recovery::RecoveryManager *recovery, std::vector<u32> columnSizes, u32 tree_slot_idx,
                               u32 tree_slot_hot_data)
    : buffer_(buffer_pool),
      blob_(blob_manager),
      next_row_id(0),
      row_id_index(buffer_pool, recovery, tree_slot_idx),
      columnSizes(columnSizes),
      hot_data(buffer_pool, recovery, this, this->columnSizes, tree_slot_hot_data),
      cold_data(),
      allColumnIndices(),
      columnSizesBeforeSum() {
  payloadSize = 0;
  // generate vector with all column indices
  for (u32 i = 0; i < this->columnSizes.size(); i++) {
    allColumnIndices.push_back(i);
    columnSizesBeforeSum.push_back(payloadSize);
    payloadSize += this->columnSizes[i];
  }
}

/* config*/

void ColumnRowStore::SetComparisonOperator(ComparisonLambda cmp) { this->hot_data.SetComparisonOperator(cmp); }

/* public apis*/
auto ColumnRowStore::LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // hot data
  if (hot_data.LookUp(U64ToSpanU8(row_id), read_cb)) { return true; }
  // cold data
  ColumnChunk *chunk = FindChunkInColdData(row_id);
  if (chunk == nullptr) { return false; }
  // find idx in row id column
  int idx = -1;
  this->blob_->LoadBlob(chunk->idx_column, 0, [&idx, &chunk, &row_id](std::span<const uint8_t> data) {
    // find idx with binary search
    u32 lower = 0;             // inclusive
    u32 upper = chunk->count;  // exclusive
    // binary search on remaining range
    const u64 *row_id_columns = reinterpret_cast<const u64 *>(data.data());
    while (lower < upper) {
      auto mid = lower + ((upper - lower) / 2);
      auto cmp = std::memcmp(row_id_columns + mid, &row_id, sizeof(uint64_t));
      if (cmp < 0) {
        lower = mid + 1;
      } else if (cmp > 0) {
        upper = mid;
      } else {
        idx = mid;
        break;
      }
    }
  });
  if (idx == -1) {
    this->blob_->UnloadAllBlobs();
    return false;
  }
  // load all payload columns
  std::vector<u8> result;
  // copy tuple value for each column
  for (u32 column_idx = 0; column_idx < this->columnSizes.size(); column_idx++) {
    u32 columnSize = this->columnSizes[column_idx];
    this->blob_->LoadBlob(chunk->column_parts[column_idx], 0,
                          [&idx, &result, &columnSize](std::span<const uint8_t> data) {
                            // copy into result vector
                            for (u32 i = 0; i < columnSize; i++) {
                              result.push_back(*(data.data() + (idx * columnSize + i)));
                            }
                          });
  }
  this->blob_->UnloadAllBlobs();
  // read
  read_cb(result);
  return idx != -1;  // should always be true
}

void ColumnRowStore::Insert(std::span<u8> key, std::span<const u8> payload) {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);
  assert((key.size() + payload.size()) <= BTreeNodeWithTimeStamp::MAX_RECORD_SIZE);
  // get new row id
  u64 row_id = next_row_id.fetch_add(1);
  row_id     = __builtin_bswap64(row_id);  // swap for memcmp sort compability

  // inserted data is always hot data
  this->hot_data.Insert(U64ToSpanU8(row_id), payload);
  // insert row id into index
  this->row_id_index.Insert(key, U64ToSpanU8(row_id));
}

auto ColumnRowStore::Remove(std::span<u8> key) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // remove row id from index
  this->row_id_index.Remove(key);
  return this->InternalRemove(row_id);
}

auto ColumnRowStore::Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // trigger func with old payload if exists
  if (func) { this->LookUp(key, func); }
  // update = insert (without updating the row id of key) + delete old + update row-id index
  u64 new_row_id = next_row_id.fetch_add(1);
  new_row_id     = __builtin_bswap64(new_row_id);  // swap for memcmp sort compability
  hot_data.Insert(U64ToSpanU8(new_row_id), payload);
  this->InternalRemove(row_id);
  row_id_index.Update(key, {reinterpret_cast<const u8 *>(&new_row_id), sizeof(new_row_id)}, nullptr);
  return true;
}

/**
 * func is used to modidy the current payload; delta is ignored! (does also update out of place!)
 */
auto ColumnRowStore::UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // update = insert (without updating the row id of key) + delete old + update row-id index
  u64 new_row_id = next_row_id.fetch_add(1);
  new_row_id     = __builtin_bswap64(new_row_id);  // swap for memcmp sort compability
  // apply fixed delta update to copy
  std::vector<u8> temp;
  this->LookUp(key, [&temp](std::span<u8> payload) { temp.assign(payload.begin(), payload.end()); });
  // apply func
  func({temp.data(), temp.size()});
  // IGNORING DELTA (because it is for delta logging)
  (void)delta;
  hot_data.Insert(U64ToSpanU8(new_row_id), temp);
  this->InternalRemove(row_id);
  row_id_index.Update(key, {reinterpret_cast<const u8 *>(&new_row_id), sizeof(new_row_id)}, nullptr);
  return true;
}

void ColumnRowStore::ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) {
  ScanOptimized(key, this->allColumnIndices, fn, true);
}

void ColumnRowStore::ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) {
  ScanOptimized(key, this->allColumnIndices, fn, false);
}

void ColumnRowStore::ConvertHotDataToColdData() {
  hot_data.MoveHotDataToColdData();
#ifdef DEBUG
  spdlog::debug("{}+{}/{} cold+hot/total tuples (deactivate this message for benchmarking!!!)", CountColdEntries(),
                CountHotEntries(), CountEntries());
#endif
}

/**
 * scan the stored data in no order
 * @param column_idxs list of column indices that should be loaded
 * @param fn record access function
 */
void ColumnRowStore::ScanFullNoOrder(const std::vector<uint32_t> &column_idxs, const AccessRecordFunc &fn) {
  u64 tuples_scanned = 0;
  u8 tmpPayload[payloadSize];
  memset(tmpPayload, 0, payloadSize);
  // for each chunk
  for (ColumnChunk &chunk : cold_data) {
    // load all selected columns
    std::vector<blob::AliasingGuard> guards;
    guards.reserve(column_idxs.size());
    for (u32 col_idx : column_idxs) {
      this->blob_->LoadGuardBlob(chunk.column_parts[col_idx], 0,
                                 [&](blob::AliasingGuard &&guard, std::span<const uint8_t> data) {
                                   (void)data;
                                   guards.emplace_back(std::move(guard));
                                 });
    }
    // for each record in this chunk
    for (u32 record_idx = 0; record_idx < chunk.count; record_idx++) {
      // load requested payload columns
      // copy tuple value for each column
      u32 i = 0;
      for (u32 column_idx : column_idxs) {
        u32 columnSize = this->columnSizes[column_idx];
        // copy into result vector
        std::memcpy(tmpPayload + columnSizesBeforeSum[column_idx], guards[i].GetPtr() + (record_idx * columnSize),
                    columnSize);
        i++;
      }
      // read
      fn(std::span<u8>(), {tmpPayload, payloadSize});  // TODO key
    }
    tuples_scanned += chunk.count;
    guards.clear();
  }

  if (start_profiling) { statistics::total_scanned_tuples += tuples_scanned; }
}

/**
 * scan the stored data in ascending or descending key order
 * @param key starting keyto start from lowest row id
 * @param column_idxs list of column indices that should be loaded
 * @param fn record access function
 * @param ascending whether to scan in ascending or descending key order
 */
void ColumnRowStore::ScanOptimized(std::span<u8> key, const std::vector<u32> &column_idxs, const AccessRecordFunc &fn,
                                   const bool ascending) {
  // iterate over key to row index & lookup row ids
  bool found         = false;
  u64 tuples_scanned = 0;
  ColumnChunk *chunk = nullptr;
  std::optional<blob::AliasingGuard> idxGuard;
  std::vector<blob::AliasingGuard> guards;
  u8 tmpPayload[payloadSize];
  memset(tmpPayload, 0, payloadSize);
  if (ascending) {
    // scan by ascending order
    row_id_index.ScanAscending(key, [&](std::span<u8> tmp_key, std::span<u8> tmp_row_id) {
      // get chunk
      u64 tmpRowIdU64;
      std::memcpy(&tmpRowIdU64, tmp_row_id.data(), sizeof(u64));
      if (chunk != nullptr && std::memcmp(&chunk->minRowId, tmp_row_id.data(), sizeof(u64)) <= 0 &&
          std::memcmp(&chunk->maxRowId, tmp_row_id.data(), sizeof(u64)) >= 0) {
        // row id in the already loaded blob
      } else {
        // new chunk needs to be loaded
        if (chunk != nullptr) {
          idxGuard.reset();
          guards.clear();
          this->blob_->UnloadAllBlobs();
        }
        chunk = FindChunkInColdData(tmpRowIdU64);
        if (chunk == nullptr) {
          throw std::runtime_error("key in index, but not in hot or cold data (chunk not found)");
        }
        // load blobs
        this->blob_->LoadGuardBlob(chunk->idx_column, 0, [&](blob::AliasingGuard guard, std::span<const uint8_t> data) {
          (void)data;
          idxGuard.emplace(std::move(guard));
        });
        for (u32 column_idx : column_idxs) {
          this->blob_->LoadGuardBlob(chunk->column_parts[column_idx], 0,
                                     [&](blob::AliasingGuard guard, std::span<const uint8_t> data) {
                                       (void)data;
                                       guards.emplace_back(std::move(guard));
                                     });
        }
      }
      // get record from chunk
      // find idx in row id column
      int idx    = -1;
      u8 *idxPtr = idxGuard.value().GetPtr();
      // find idx with binary search
      u32 lower = 0;             // inclusive
      u32 upper = chunk->count;  // exclusive
      // binary search on remaining range
      while (lower < upper) {
        auto mid = lower + ((upper - lower) / 2);
        auto cmp = std::memcmp(idxPtr + (mid * sizeof(u64)), tmp_row_id.data(), sizeof(uint64_t));
        if (cmp < 0) {
          lower = mid + 1;
        } else if (cmp > 0) {
          upper = mid;
        } else {
          idx = mid;
          break;
        }
      }

      if (idx == -1) {
        idxGuard.reset();
        guards.clear();
        this->blob_->UnloadAllBlobs();
        throw std::runtime_error("key in index, but not in hot or cold data (index in chunk not found)");
      }
      // load requested payload columns
      // copy tuple value for each column
      u32 i = 0;
      for (u32 column_idx : column_idxs) {
        u32 columnSize = this->columnSizes[column_idx];
        std::memcpy(tmpPayload + columnSizesBeforeSum[column_idx], guards[i].GetPtr() + (idx * columnSize), columnSize);
        i++;
      }
      // read
      tuples_scanned++;
      found = fn(tmp_key, {tmpPayload, payloadSize});

      return found;
    });
  } else {
    // scan by descending order
    row_id_index.ScanDescending(key, [&](std::span<u8> tmp_key, std::span<u8> tmp_row_id) {
      // get chunk
      u64 tmpRowIdU64;
      std::memcpy(&tmpRowIdU64, tmp_row_id.data(), sizeof(u64));
      if (chunk != nullptr && std::memcmp(&chunk->minRowId, tmp_row_id.data(), sizeof(u64)) <= 0 &&
          std::memcmp(&chunk->maxRowId, tmp_row_id.data(), sizeof(u64)) >= 0) {
        // row id in the already loaded blob
      } else {
        // new chunk needs to be loaded
        if (chunk != nullptr) {
          idxGuard.reset();
          guards.clear();
          this->blob_->UnloadAllBlobs();
        }
        chunk = FindChunkInColdData(tmpRowIdU64);
        if (chunk == nullptr) {
          throw std::runtime_error("key in index, but not in hot or cold data (chunk not found)");
        }
        // load blobs
        this->blob_->LoadGuardBlob(chunk->idx_column, 0, [&](blob::AliasingGuard guard, std::span<const uint8_t> data) {
          (void)data;
          idxGuard.emplace(std::move(guard));
        });
        for (u32 column_idx : column_idxs) {
          this->blob_->LoadGuardBlob(chunk->column_parts[column_idx], 0,
                                     [&](blob::AliasingGuard guard, std::span<const uint8_t> data) {
                                       (void)data;
                                       guards.emplace_back(std::move(guard));
                                     });
        }
      }
      // get record from chunk
      // find idx in row id column
      int idx    = -1;
      u8 *idxPtr = idxGuard.value().GetPtr();
      // find idx with binary search
      u32 lower = 0;             // inclusive
      u32 upper = chunk->count;  // exclusive
      // binary search on remaining range
      while (lower < upper) {
        auto mid = lower + ((upper - lower) / 2);
        auto cmp = std::memcmp(idxPtr + (mid * sizeof(u64)), tmp_row_id.data(), sizeof(uint64_t));
        if (cmp < 0) {
          lower = mid + 1;
        } else if (cmp > 0) {
          upper = mid;
        } else {
          idx = mid;
          break;
        }
      }

      if (idx == -1) {
        idxGuard.reset();
        guards.clear();
        this->blob_->UnloadAllBlobs();
        throw std::runtime_error("key in index, but not in hot or cold data (index in chunk not found)");
      }
      // load requested payload columns
      // copy tuple value for each column
      u32 i = 0;
      for (u32 column_idx : column_idxs) {
        u32 columnSize = this->columnSizes[column_idx];
        std::memcpy(tmpPayload + columnSizesBeforeSum[column_idx], guards[i].GetPtr() + (idx * columnSize), columnSize);
        i++;
      }
      // read
      tuples_scanned++;
      found = fn(tmp_key, {tmpPayload, payloadSize});

      return found;
    });
  }

  // unload column chunk
  if (chunk != nullptr) {
    idxGuard.reset();
    guards.clear();
    this->blob_->UnloadAllBlobs();
  }

  if (start_profiling) { statistics::total_scanned_tuples += tuples_scanned; }
}

/**
 * only counts the active entries (deleted entries in the cold data are not counted)
 */
auto ColumnRowStore::CountEntries() -> u64 {
  // hot data
  auto result = hot_data.CountEntries();
  // cold data
  for (ColumnChunk &chunk : this->cold_data) { result += chunk.count_active; }
  return result;
}

/**
 * count cold data entries
 */
auto ColumnRowStore::CountColdEntries() -> u64 {
  auto result = 0;
  for (ColumnChunk &chunk : this->cold_data) { result += chunk.count_active; }
  return result;
}

/**
 * count hot data entries
 */
auto ColumnRowStore::CountHotEntries() -> u64 { return hot_data.CountEntries(); }

auto ColumnRowStore::SizeInMB() -> float { return CountPages() * static_cast<float>(PAGE_SIZE) / MB; }

auto ColumnRowStore::LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp,
                                const AccessPayloadFunc &read_cb) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUpBlob(blob_key, cmp,
                               [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // hot data
  if (hot_data.LookUp({reinterpret_cast<u8 *>(&row_id), sizeof(row_id)}, read_cb)) { return true; }
  // cold data
  ColumnChunk *chunk = FindChunkInColdData(row_id);
  if (chunk == nullptr) { return false; }
  // find idx in row id column
  int idx = -1;
  this->blob_->LoadBlob(chunk->idx_column, 0, [&idx, &chunk, &row_id](std::span<const uint8_t> data) {
    // find idx with binary search
    u32 lower = 0;             // inclusive
    u32 upper = chunk->count;  // exclusive
    // binary search on remaining range
    const u64 *row_id_columns = reinterpret_cast<const u64 *>(data.data());
    while (lower < upper) {
      auto mid = lower + ((upper - lower) / 2);
      auto cmp = std::memcmp(row_id_columns + mid, &row_id, sizeof(uint64_t));
      if (cmp < 0) {
        lower = mid + 1;
      } else if (cmp > 0) {
        upper = mid;
      } else {
        idx = mid;
        break;
      }
    }
  });
  if (idx == -1) {
    this->blob_->UnloadAllBlobs();
    return false;
  }
  // load all payload columns
  std::vector<u8> result;
  // copy tuple value for each column
  for (u32 column_idx = 0; column_idx < this->columnSizes.size(); column_idx++) {
    u32 columnSize = this->columnSizes[column_idx];
    this->blob_->LoadBlob(chunk->column_parts[column_idx], 0,
                          [&idx, &result, &columnSize](std::span<const uint8_t> data) {
                            // copy into result vector
                            for (u32 i = 0; i < columnSize; i++) {
                              result.push_back(*(data.data() + (idx * columnSize + i)));
                            }
                          });
  }
  this->blob_->UnloadAllBlobs();
  // read
  read_cb(result);
  return idx != -1;  // should always be true
}

// -------------------------------------------------------------------------------------
/* APIs for use within LeanStore */
auto ColumnRowStore::IsNotEmpty() -> bool {
  // hot data
  if (this->hot_data.IsNotEmpty()) { return true; }
  // cold data
  for (ColumnChunk &chunk : this->cold_data) {
    if (chunk.count_active > 0) { return true; }
  }
  // everything empty
  return false;
}

/**
 * returns only the page count of the hot data
 */
auto ColumnRowStore::CountPages() -> u64 {
  // // hot data
  // auto count = this->hot_data.CountPages();
  // // cold data
  // for (ColumnChunk &chunk : this->cold_data) {
  //   count += chunk.totalSizeBytes / PAGE_SIZE;
  // }
  // return count;
  return this->hot_data.CountPages();
}

/* internal helper functions */
/**
 * internal remove removes row_id from either hot or cold data, but not from the row id index!
 */
auto ColumnRowStore::InternalRemove(u64 row_id) -> bool {
  // hot data
  hot_data.Remove(U64ToSpanU8(row_id));
  // cold data
  ColumnChunk *chunk = this->FindChunkInColdData(row_id);
  if (chunk != nullptr) {
    // TODO(moritz): mark deletion bitmap
    chunk->count_active = chunk->count_active - 1;
  }
  return true;
}

auto ColumnRowStore::FindChunkInColdData(u64 row_id) -> ColumnChunk * {
  // find the correct chunk with upper bound (first entry that is > row_id and then go one back)
  auto iterator = std::upper_bound(cold_data.begin(), cold_data.end(), row_id, [](u64 value, const ColumnChunk &chunk) {
    return std::memcmp(&value, &chunk.minRowId, sizeof(u64)) < 0;
  });
  if (iterator == cold_data.begin()) {
    // not found
    return nullptr;
  }
  // go 1 entry back for getting the <=
  iterator--;
  if (std::memcmp(&row_id, &iterator->maxRowId, sizeof(u64)) > 0) {
    // not in this chunk
    return nullptr;
  }
  return &(*iterator);
}

void ColumnRowStore::StoreColdData(std::vector<u8> &rowIds, std::vector<std::vector<u8>> &data) {
  // append new column chunk
  cold_data.emplace_back();
  ColumnChunk &newColumnData = cold_data.back();
  newColumnData.count        = rowIds.size() / sizeof(u64);
  newColumnData.count_active = newColumnData.count;
  // write min and max row id (first and last row id)
  std::memcpy(&newColumnData.minRowId, rowIds.data(), sizeof(u64));
  std::memcpy(&newColumnData.maxRowId, rowIds.data() + rowIds.size() - sizeof(u64), sizeof(u64));
  // store rowId column
  newColumnData.idx_column     = CopyBlobState(blob_->AllocateBlob({rowIds.data(), rowIds.size()}, nullptr, false));
  newColumnData.totalSizeBytes = rowIds.size();
  // store columns
  newColumnData.column_parts.reserve(columnSizes.size());
  for (std::vector<u8> &column : data) {
    newColumnData.column_parts.emplace_back(
      CopyBlobState(blob_->AllocateBlob({column.data(), column.size()}, nullptr, false)));
    newColumnData.totalSizeBytes += column.size();
  }
}

}  // namespace leanstore::storage
