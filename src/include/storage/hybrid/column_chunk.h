#pragma once

#include "common/typedefs.h"
#include "storage/blob/blob_state.h"

#include <vector>

namespace leanstore::storage {

struct ColumnChunk {
  u64 totalSizeBytes; // total size in B
  u64 minRowId;      // smallest row Id
  u64 maxRowId;      // maximum row Id
  u32 count;         // count of all tuples
  u32 count_active;  // count of tuples that are not marked as deleted
  // TODO bitmap for deletion
  blob::BlobState *idx_column;
  blob::BlobState *key_column;
  std::vector<blob::BlobState *> column_parts;
};

}  // namespace leanstore::storage