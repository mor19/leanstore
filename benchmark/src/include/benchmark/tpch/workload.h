#pragma once

#include "benchmark/tpch/schema.h"
#include "benchmark/utils/scheduler.h"
#include "common/rand.h"
#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "benchmark/tpch/config.h"
#include <algorithm>
#include <array>
#include <variant>
#include <vector>

namespace tpch {

template <template <typename> class AdapterType>
struct TPCHWorkload {
  AdapterType<PartType> part;
  AdapterType<SupplierType> supplier;
  AdapterType<PartSuppType> partsupp;
  AdapterType<CustomerType> customer;
  AdapterType<OrdersType> orders;
  AdapterType<LineItemType> lineitem;
  AdapterType<NationType> nation;
  AdapterType<RegionType> region;

  Integer last_part_id;
  Integer last_supplier_id;
  Integer last_customer_id;
  Integer last_order_id;

  template <typename... Params>
  TPCHWorkload(Params &&...params)
      : part(AdapterType<PartType>(std::forward<Params>(params)...)),
        supplier(AdapterType<SupplierType>(std::forward<Params>(params)...)),
        partsupp(AdapterType<PartSuppType>(std::forward<Params>(params)...)),
        customer(AdapterType<CustomerType>(std::forward<Params>(params)...)),
        orders(AdapterType<OrdersType>(std::forward<Params>(params)...)),
        lineitem(AdapterType<LineItemType>(std::forward<Params>(params)...)),
        nation(AdapterType<NationType>(std::forward<Params>(params)...)),
        region(AdapterType<RegionType>(std::forward<Params>(params)...)),
        last_part_id(0),
        last_supplier_id(0),
        last_customer_id(0),
        last_order_id(0) {}

  void Load() {
    LoadPart();
    LoadSupplier();
    LoadPartSuppLineItem();
    LoadCustomer();
    LoadOrders();
    LoadNation();
    LoadRegion();
  }

  // TODO part, supplier, customer, orders *1000
  inline static Integer PART_SCALE     = 200;
  inline static Integer SUPPLIER_SCALE = 10;
  inline static Integer CUSTOMER_SCALE = 150;
  inline static Integer ORDERS_SCALE   = 1500;
  inline static Integer LINEITEM_SCALE = 6000;
  inline static Integer PARTSUPP_SCALE = 800;
  inline static Integer NATION_COUNT   = 25;
  inline static Integer REGION_COUNT   = 5;


  inline Integer getPartID() { return Rand(last_part_id) + 1; }

  inline Integer getSupplierID() { return Rand(last_supplier_id) + 1; }

  inline Integer getCustomerID() { return Rand(last_customer_id) + 1; }

  inline Integer getOrderID() { return Rand(last_order_id) + 1; }

  inline Integer getNationID() { return Rand(NATION_COUNT) + 1; }

  inline Integer getRegionID() { return Rand(REGION_COUNT) + 1; }

  // ------------------------------------LOAD-------------------------------------------------

  void LoadPart();
  void LoadSupplier();
  void LoadPartSuppLineItem();
  void LoadCustomer();
  void LoadOrders();
  void LoadNation();
  void LoadRegion();

  static void printProgress(std::string msg, Integer i, Integer start, Integer end) {
    auto scale = end - start + 1;
    if (scale < 100) return;
    if (i % 1000 == start % 1000 && FLAGS_tpch_log_process) {
      double progress = (double)(i - start + 1) / scale * 100;
      std::cout << "\rLoading " << scale << " " << msg << ": " << progress << "%------------------------------------";
    }
    if (i == end && scale > 100) {
      std::cout << "Loaded " << scale << " " << msg << " records." << std::endl;
    } else if (i == end && FLAGS_tpch_log_process) {
      std::cout << std::endl;
    }
  }
};

}  // namespace tpch
