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
                             const bool ascending) = 0;
};

}  // namespace leanstore
