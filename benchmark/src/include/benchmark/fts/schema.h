#pragma once

#include "common/delta.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <stdexcept>

namespace fts {

struct OrderLineType {
  static constexpr uint32_t TYPE_ID = 8;

  struct Key {
    Integer ol_w_id;
    Integer ol_d_id;
    Integer ol_o_id;
    Integer ol_number;
  };

  Integer ol_i_id;
  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  Varchar<24> ol_dist_info;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(OrderLineType); }

  static auto FoldKey(uint8_t *out, const OrderLineType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.ol_w_id);
    pos += Fold(out + pos, key.ol_d_id);
    pos += Fold(out + pos, key.ol_o_id);
    pos += Fold(out + pos, key.ol_number);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, OrderLineType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.ol_w_id);
    pos += Unfold(in + pos, key.ol_d_id);
    pos += Unfold(in + pos, key.ol_o_id);
    pos += Unfold(in + pos, key.ol_number);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t {
    return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) + sizeof(Key::ol_number);
  }
};

}  // namespace tpcc