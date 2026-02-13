#pragma once

#include "common/delta.h"
#include "common/typedefs.h"
#include "kv_interface.h"

#include <functional>
#include <span>
#include <unordered_set>

namespace leanstore {

class HKVInterface : public KVInterface {
 public:
  virtual ~HKVInterface() = default;

  // -------------------------------------------------------------------------------------
  virtual void ScanOptimized(std::span<u8> key, const std::unordered_set<u32> &column_idxs, const AccessRecordFunc &fn,
                             const bool ascending)                           = 0;
  virtual void ScanFullNoOrder(const std::unordered_set<uint32_t> &column_idxs,
                                    const AccessRecordFunc &found_record_cb) = 0;
  virtual void ConvertHotDataToColdData()                                    = 0;
};

}  // namespace leanstore
