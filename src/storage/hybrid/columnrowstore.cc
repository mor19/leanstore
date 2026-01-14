#include "storage/hybrid/columnrowstore.h"
#include <stdexcept>

namespace leanstore::storage {

inline std::span<u8> U64ToSpanU8(u64 &u) { return {reinterpret_cast<u8 *>(&u), sizeof(u)}; }

/* constructor */
ColumnRowStore::ColumnRowStore(buffer::BufferManager *buffer_pool, blob::BlobManager *blob_manager,
                               std::vector<u32> columnSizes, bool append_bias)
    : buffer_(buffer_pool),
      blob_(blob_manager),
      next_row_id(0),
      row_id_index(buffer_pool, true),
      hot_data(buffer_pool, append_bias),
      cold_data(),
      columnSizes(columnSizes),
      allColumnIndices() {
  // generate vector with all column indices
  for (u32 i = 0; i < this->columnSizes.size(); i++) { allColumnIndices.push_back(i); }
}

/* config*/
void ColumnRowStore::ToggleAppendBiasMode(bool append_bias) { this->hot_data.ToggleAppendBiasMode(append_bias); }

void ColumnRowStore::SetComparisonOperator(ComparisonLambda cmp) { this->hot_data.SetComparisonOperator(cmp); }

/* public apis*/
auto ColumnRowStore::LookUp(std::span<u8> key, const PayloadFunc &read_cb) -> bool {
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
  this->blob_->LoadBlob(chunk->idx_column, 0, [&idx, &chunk](std::span<const uint8_t> data) {
    // find idx with binary search
    u32 lower = 0;             // inclusive
    u32 upper = chunk->count;  // exclusive
    // binary search on remaining range
    while (lower < upper) {
      auto mid = ((upper - lower) / 2) + lower;
      auto ret = std::memcmp(data.data() + (lower << 3), data.data() + (upper << 3), 8);
      if (ret < 0) {
        upper = mid;
      } else if (ret > 0) {
        lower = mid + 1;
      } else {
        idx = mid;
        break;
      }
    }
  });
  if (idx == -1) { return false; }
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
  // read
  read_cb(result);
  return idx != -1;  // should always be true
}

void ColumnRowStore::Insert(std::span<u8> key, std::span<const u8> payload) {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);
  // get new row id
  u64 row_id = next_row_id.fetch_add(1);
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
  this->row_id_index.Remove(U64ToSpanU8(row_id));
  return this->InternalRemove(row_id);
}

auto ColumnRowStore::Update(std::span<u8> key, std::span<const u8> payload, const PayloadFunc &func) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // trigger func with old payload if exists
  if (func) { this->hot_data.LookUp(U64ToSpanU8(row_id), func); }
  // update = insert (without updating the row id of key) + delete old + update row-id index
  u64 new_row_id = next_row_id.fetch_add(1);
  hot_data.Insert(U64ToSpanU8(new_row_id), payload);
  this->InternalRemove(row_id);
  row_id_index.Update(key, {reinterpret_cast<const u8 *>(&new_row_id), sizeof(new_row_id)}, nullptr);
  return true;
}

auto ColumnRowStore::UpdateInPlace(std::span<u8> key, const PayloadFunc &func, FixedSizeDelta *delta) -> bool {
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); })) {
    // key not in row id index = key not in column-row-store
    return false;
  }
  // trigger func with old payload if exists
  if (func) { this->hot_data.LookUp(U64ToSpanU8(row_id), func); }
  // update = insert (without updating the row id of key) + delete old + update row-id index
  u64 new_row_id = next_row_id.fetch_add(1);
  // apply fixed delta update to copy
  std::vector<u8> temp;
  this->hot_data.LookUp(U64ToSpanU8(row_id),
                        [&temp](std::span<u8> payload) { temp.assign(payload.begin(), payload.end()); });
  delta->UpdateDeltaPayload(temp);
  hot_data.Insert(U64ToSpanU8(new_row_id), temp);
  this->InternalRemove(row_id);
  row_id_index.Update(key, {reinterpret_cast<const u8 *>(&new_row_id), sizeof(new_row_id)}, nullptr);
  return true;
}

void ColumnRowStore::ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) {
  WarningMessage("Order is insertion order, not PK order!");
  ScanOptimized(key, this->allColumnIndices, fn);
}

void ColumnRowStore::ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) {
  (void)key;
  (void)fn;
  throw std::runtime_error("not implemented");
}

void ColumnRowStore::ScanOptimized(std::span<u8> key, std::vector<u32> &column_idxs, const AccessRecordFunc &fn) {
  // TODO
  // get row id
  u64 row_id = 0;
  row_id_index.LookUp(key, [&](std::span<u8> pl) { std::memcpy(&row_id, pl.data(), sizeof(u64)); });
  // hot data
  hot_data.ScanAscending(U64ToSpanU8(row_id), fn);
  // cold data
  // record size estimate
  size_t estimatedRecordSize = 0;
  for (u32 columnSize : columnSizes) { estimatedRecordSize += columnSize; }
  // TODO key
  u64 record_row_id;
  std::vector<u8> record(estimatedRecordSize);
  std::fill(record.begin(), record.end(), 0);
  for (ColumnChunk &chunk : this->cold_data) {
    // TODO delete/merge empty chunk during iteration
    if (chunk.maxRowId < row_id) { continue; }
    // load blobs
    blob_->LoadBlob(chunk.idx_column, 0, [&](std::span<const uint8_t> data) { (void) data;});
    for (u32 column_idx : column_idxs) {
      blob_->LoadBlob(chunk.column_parts[column_idx], 0, [&](std::span<const uint8_t> data) {(void) data;});
    }
    // build records & apply function
    for (u32 i = 0; i < chunk.count; i++) {
      std::memcpy(&record_row_id, chunk.idx_column->Data(), sizeof(u64));
      if (record_row_id < row_id) { continue; }
      // build record
      record.clear();
      for (u32 column_idx : column_idxs) {
        record.insert(
          record.end(), chunk.column_parts[column_idx]->Data() + (i * columnSizes[column_idx]),
          chunk.column_parts[column_idx]->Data() + (i * columnSizes[column_idx] + columnSizes[column_idx]));
      }
      // call fn
      fn(std::span<u8>(), record);
    }
    // TODO unload blobs?
  }
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

auto ColumnRowStore::SizeInMB() -> float { return CountPages() * static_cast<float>(PAGE_SIZE) / MB; }

auto ColumnRowStore::LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const PayloadFunc &read_cb)
  -> bool {
  // cmp not needed because of unique row id
  (void)cmp;
  // get row id
  u64 row_id;
  if (!row_id_index.LookUp({const_cast<u8 *>(blob_key.data()), blob_key.size()},
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
  this->blob_->LoadBlob(chunk->idx_column, 0, [&idx, &chunk](std::span<const uint8_t> data) {
    // find idx with binary search
    u32 lower = 0;             // inclusive
    u32 upper = chunk->count;  // exclusive
    // binary search on remaining range
    while (lower < upper) {
      auto mid = ((upper - lower) / 2) + lower;
      auto ret = std::memcmp(data.data() + (lower << 3), data.data() + (upper << 3), 8);
      if (ret < 0) {
        upper = mid;
      } else if (ret > 0) {
        lower = mid + 1;
      } else {
        idx = mid;
        break;
      }
    }
  });
  if (idx == -1) { return false; }
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
  // TODO
  // hot data
  if (hot_data.Remove(U64ToSpanU8(row_id))) { return true; }
  // cold data
  // TODO
  ColumnChunk *chunk = this->FindChunkInColdData(row_id);
  if (chunk != nullptr) {
    // TODO mark deletion bitmap
  }
  return true;
}

auto ColumnRowStore::FindChunkInColdData(u64 row_id) -> ColumnChunk * {
  // find the correct chunk with upper bound (first entry that is > row_id and then go one back)
  auto iterator = std::upper_bound(cold_data.begin(), cold_data.end(), row_id,
                                   [](u64 value, const ColumnChunk &chunk) { return value < chunk.minRowId; });
  if (iterator == cold_data.begin()) {
    // not found
    return nullptr;
  }
  // go 1 entry back for getting the <=
  iterator--;
  return &(*iterator);
}

void ColumnRowStore::MoveInternalNodeToColdData() {
  // remove block from hot data
  // TODO
  // buffer data
  // TODO
  // write blobs
  // TODO
}

}  // namespace leanstore::storage
