#pragma once

#include "common/delta.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <stdexcept>

namespace tpch {

struct PartType {
  static constexpr uint32_t TYPE_ID = 0;

  struct Key {
    Integer p_partkey;
  };

  Varchar<55> p_name;
  /*fixed*/ Varchar<25> p_mfgr;
  /*fixed*/ Varchar<10> p_brand;
  Varchar<25> p_type;
  Integer p_size;
  /*fixed*/ Varchar<10> p_container;
  Numeric p_retailprice;
  Varchar<23> p_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(PartType); }

  static auto FoldKey(uint8_t *out, const PartType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.p_partkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, PartType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.p_partkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::p_partkey); }
};

struct SupplierType {
  static constexpr uint32_t TYPE_ID = 1;

  struct Key {
    Integer s_suppkey;
  };

  /*fixed*/ Varchar<25> s_name;
  Varchar<40> s_address;
  Integer s_nationkey; // foreign key to n_nationkey
  /*fixed*/ Varchar<15> s_phone;
  Numeric s_acctbal;
  Varchar<101> s_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(SupplierType); }

  static auto FoldKey(uint8_t *out, const SupplierType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.s_suppkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, SupplierType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.s_suppkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::s_suppkey); }
};

struct PartSuppType {
  static constexpr uint32_t TYPE_ID = 2;

  struct Key {
    Integer ps_partkey; // foreign key to p_partkey
    Integer ps_suppkey; // foreign key to s_suppkey
  };

  Integer ps_availqty;
  Numeric ps_supplycost;
  Varchar<199> ps_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(PartSuppType); }

  static auto FoldKey(uint8_t *out, const PartSuppType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.ps_partkey);
    pos += Fold(out + pos, key.ps_suppkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, PartSuppType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.ps_partkey);
    pos += Unfold(in + pos, key.ps_suppkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::ps_partkey) + sizeof(Key::ps_suppkey); }
};

struct CustomerType {
  static constexpr uint32_t TYPE_ID = 3;

  struct Key {
    Integer c_custkey;
  };

  Varchar<25> c_name;
  Varchar<40> c_address;
  Integer c_nationkey; // foreign key to n_nationkey
  /*fixed*/ Varchar<15> c_phone;
  Numeric c_acctbal;
  /*fixed*/ Varchar<10> c_mktsegment;
  Varchar<117> c_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(CustomerType); }

  static auto FoldKey(uint8_t *out, const CustomerType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.c_custkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, CustomerType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.c_custkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::c_custkey); }
};

struct OrdersType {
  static constexpr uint32_t TYPE_ID = 4;

  struct Key {
    Integer o_orderkey;
  };

  Integer o_custkey; // foreign key to c_custkey
  /*fixed*/ Varchar<1> o_orderstatus;
  Numeric o_totalprice;
  /*fixed*/ Varchar<10> o_orderdate;
  /*fixed*/ Varchar<15> o_orderpriority;
  /*fixed*/ Varchar<15> o_clerk;
  Integer o_shippriority;
  Varchar<79> o_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(OrdersType); }

  static auto FoldKey(uint8_t *out, const OrdersType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.o_orderkey);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, OrdersType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.o_orderkey);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::o_orderkey); }
};

struct LineItemType {
  static constexpr uint32_t TYPE_ID = 5;

  struct Key {
    Integer l_orderkey; // foreign key to o_orderkey
    Integer l_linenumber;
  };

  Integer l_partkey; // foreign key to p_partkey, first part of compound foreign key to (ps_partkey, ps_suppkey)
  Integer l_suppkey; // foreign key to s_suppkey, second part of compound foreign key to (ps_partkey, ps_suppkey)
  Numeric l_quantity;
  Numeric l_extendedprice;
  Numeric l_discount;
  Numeric l_tax;
  /*fixed*/ Varchar<1> l_returnflag;
  /*fixed*/ Varchar<1> l_linestatus;
  /*fixed*/ Varchar<10> l_shipdate;
  /*fixed*/ Varchar<10> l_commitdate;
  /*fixed*/ Varchar<10> l_receiptdate;
  /*fixed*/ Varchar<25> l_shipinstruct;
  /*fixed*/ Varchar<10> l_shipmode;
  Varchar<44> l_comment;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(LineItemType); }

  static auto FoldKey(uint8_t *out, const LineItemType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.l_orderkey);
    pos += Fold(out + pos, key.l_linenumber);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, LineItemType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.l_orderkey);
    pos += Unfold(in + pos, key.l_linenumber);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::l_orderkey) + sizeof(Key::l_linenumber); }
};

struct NationType {
  static constexpr uint32_t TYPE_ID = 6;

  struct Key {
    Integer n_nationkey;
  };

  /*fixed*/ Varchar<25> n_name;
  Integer n_regionkey; // foreign key to r_regionkey
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
  static constexpr uint32_t TYPE_ID = 6;

  struct Key {
    Integer r_regionkey;
  };

  /*fixed*/ Varchar<25> r_name;
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

}  // namespace tpch