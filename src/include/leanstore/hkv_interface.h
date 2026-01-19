#pragma once

#include "common/delta.h"
#include "common/typedefs.h"
#include "kv_interface.h"

#include <functional>
#include <span>
#include <vector>

namespace leanstore {

class HKVInterface : public KVInterface {
 public:
  virtual ~HKVInterface() = default;

  // -------------------------------------------------------------------------------------
  virtual void ScanOptimized(std::span<u8> key, const std::vector<u32> &column_idxs, const AccessRecordFunc &fn) = 0;
};

}  // namespace leanstore
