#pragma once

#include "common/delta.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <stdexcept>

namespace tpcc {

struct NationType {
  static constexpr uint32_t TYPE_ID = 10;

  struct Key {
    Integer n_nationkey;
  };

  Varchar<25> n_name;
  Integer n_regionkey;
  Varchar<152> n_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(NationType); }

  static auto FoldKey(uint8_t *out, const NationType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.n_nationkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, NationType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.n_nationkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::n_nationkey); }
};

struct RegionType {
  static constexpr uint32_t TYPE_ID = 11;

  struct Key {
    Integer r_regionkey;
  };

  Varchar<25> r_name;
  Varchar<152> r_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(RegionType); }

  static auto FoldKey(uint8_t *out, const RegionType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.r_regionkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, RegionType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.r_regionkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::r_regionkey); }
};

struct SupplierType {
  static constexpr uint32_t TYPE_ID = 12;

  struct Key {
    Integer su_suppkey;
  };

  Varchar<25> su_name;
  Varchar<40> su_address;
  Integer su_nationkey;
  BytesPayload<15> su_phone;
  Integer su_acctbal;
  Varchar<40> su_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(SupplierType); }

  static auto FoldKey(uint8_t *out, const SupplierType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.su_suppkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, SupplierType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.su_suppkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::su_suppkey); }
};

}  // namespace tpcc