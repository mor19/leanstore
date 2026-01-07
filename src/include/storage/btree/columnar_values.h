// TODO delete
#pragma once

#include <vector>
#include "common/typedefs.h"

namespace leanstore::storage {

struct ColumnInfo {
  size_t offset;
  u32 size;
};

/**
 * data layout
 * - data:
 *      - column count // TODO(moritz): can maybe be read from the class/stored more central in Btree?
 *      - column size (in element count)
 *      - for each column:
 *          - column offset (in bytes)
 *          - column data type size (if ==0, then varchar is used and the size is stored in the data)
 *      - for each column:
 *          - column data // TODO(moritz): is each data always the same (max) size? (also varchar?)
 */
struct ColumnarValues {
 public:
  u32 column_count;
  u32 element_count;
  ColumnInfo column_infos[];

  // TODO(moritz): adapt data types with regard to max values

  inline u32 ColumnCount() const { return this->column_count; }

  inline u32 ColumnElementCount() const { return this->element_count; }

  inline std::size_t ColumnOffset(u32 column_idx) const { return this->column_infos[column_idx].offset; }

  inline u32 ColumnSize(u32 column_idx) const { return this->column_infos[column_idx].size; }
};
}  // namespace leanstore::storage