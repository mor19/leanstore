#include "benchmark/tpcc_extended/workload_extended.h"

#include "benchmark/adapters/leanstore_adapter.h"
#include "common/rand.h"
#include "share_headers/db_types.h"

#include <algorithm>
#include <random>
#include <vector>

namespace tpcc {

// copied from
// https://github.com/sfu-dis/preemptdb/blob/beb9765c4af009e45c6e60deb5c627dd1ec93155/benchmarks/tpcc/tpcc-config.cc#L61
const NationData NATIONS[] = {
  {48, "ALGERIA", 0},       {49, "ARGENTINA", 1},    {50, "BRAZIL", 1},    {51, "CANADA", 1},
  {52, "EGYPT", 4},         {53, "ETHIOPIA", 0},     {54, "FRANCE", 3},    {55, "GERMANY", 3},
  {56, "INDIA", 2},         {57, "INDONESIA", 2},    {65, "IRAN", 4},      {66, "IRAQ", 4},
  {67, "JAPAN", 2},         {68, "JORDAN", 4},       {69, "KENYA", 0},     {70, "MOROCCO", 0},
  {71, "MOZAMBIQUE", 0},    {72, "PERU", 1},         {73, "CHINA", 2},     {74, "ROMANIA", 3},
  {75, "SAUDI ARABIA", 4},  {76, "VIETNAM", 2},      {77, "RUSSIA", 3},    {78, "UNITED KINGDOM", 3},
  {79, "UNITED STATES", 1}, {80, "CHINA", 2},        {81, "PAKISTAN", 2},  {82, "BANGLADESH", 2},
  {83, "MEXICO", 1},        {84, "PHILIPPINES", 2},  {85, "THAILAND", 2},  {86, "ITALY", 3},
  {87, "SOUTH AFRICA", 0},  {88, "SOUTH KOREA", 2},  {89, "COLOMBIA", 1},  {90, "SPAIN", 3},
  {97, "UKRAINE", 3},       {98, "POLAND", 3},       {99, "SUDAN", 0},     {100, "UZBEKISTAN", 2},
  {101, "MALAYSIA", 2},     {102, "VENEZUELA", 1},   {103, "NEPAL", 2},    {104, "AFGHANISTAN", 2},
  {105, "NORTH KOREA", 2},  {106, "TAIWAN", 2},      {107, "GHANA", 0},    {108, "IVORY COAST", 0},
  {109, "SYRIA", 4},        {110, "MADAGASCAR", 0},  {111, "CAMEROON", 0}, {112, "SRI LANKA", 2},
  {113, "ROMANIA", 3},      {114, "NETHERLANDS", 3}, {115, "CAMBODIA", 2}, {116, "BELGIUM", 3},
  {117, "GREECE", 3},       {118, "PORTUGAL", 3},    {119, "ISRAEL", 4},   {120, "FINLAND", 3},
  {121, "SINGAPORE", 2},    {122, "NORWAY", 3}};

const char *REGIONS[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
std::vector<std::vector<std::pair<Integer, Integer>>> supp_stock_map(10000);

template <template <typename> class AdapterType>
void TPCCWorkloadExtended<AdapterType>::Query2() {
  // Pick target region
  Integer target_region = UniformRand(0, 4);
  // Scan region
  this->region.ScanOptimized({target_region}, {}, [&](const RegionType::Key &r_key, const RegionType &r_rec) {
    (void)r_rec;
    if (r_key.r_regionkey == target_region) {
      // found target region
      // Scan nation
      this->nation.ScanOptimized({0}, {1}, [&](const NationType::Key &n_key, const NationType &n_rec) {
        if (n_rec.n_regionkey != target_region) {
          // ignore nation with wrong region
          return true;
        }
        // Scan suppliers
        this->supplier.ScanOptimized({0}, {2}, [&](const SupplierType::Key &su_key, const SupplierType &su_rec) {
          if (su_rec.su_nationkey != n_key.n_nationkey) {
            // ignore supplier from wrong nation
            return true;
          }
          StockType::Key min_stock_key(0, 0);
          StockType min_stock(0, Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""),
                              Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), 0, 0,
                              0, Varchar<24>(""));
          Integer minQty = std::numeric_limits<Integer>::max();

          for (auto &it : supp_stock_map[su_key.su_suppkey]) {
            StockType::Key k_s = {it.first, it.second};
            StockType v_s(0, Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""),
                          Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), Varchar<24>(""), 0, 0, 0,
                          Varchar<24>(""));
            this->stock.LookUp(k_s, [&](const StockType &s_rec) {
              v_s.s_quantity   = s_rec.s_quantity;
              v_s.s_ytd        = s_rec.s_ytd;
              v_s.s_order_cnt  = s_rec.s_order_cnt;
              v_s.s_remote_cnt = s_rec.s_remote_cnt;
              return true;
            });

            if (minQty > v_s.s_quantity) {
              minQty                 = v_s.s_quantity;
              min_stock_key.s_w_id   = k_s.s_w_id;
              min_stock_key.s_i_id   = k_s.s_i_id;
              min_stock.s_quantity   = v_s.s_quantity;
              min_stock.s_ytd        = v_s.s_ytd;
              min_stock.s_order_cnt  = v_s.s_order_cnt;
              min_stock.s_remote_cnt = v_s.s_remote_cnt;
            }
          }
          if (minQty == std::numeric_limits<Integer>::max()) {
            // no stock found
            return true;
          }
          // fetching the lowest stock level item (data only for simplicity)
          std::string i_data_str = this->item.LookupField({min_stock_key.s_i_id}, &ItemType::i_data).ToString();
          // filtering item (i_data like '%b')
          auto found = i_data_str.find('b');
          if (found != std::string::npos) { return true; }
          // handle found item (...)
          // std::cout << std::to_string(min_stock_key.s_i_id) << ": " << i_data_str << "\n";
          return true;
        });
        return true;
      });
      return false;
    }
    return false; // not directly found -> not in region table
  });
}

// load extra data
template <template <typename> class AdapterType>
void TPCCWorkloadExtended<AdapterType>::LoadSupplier() {
  // add nations
  for (Integer i = 0; i < 62; i++) {
    nation.Insert({NATIONS[i].id}, {Varchar<25>(NATIONS[i].name.c_str()), NATIONS[i].rId, RandomString<152>(10, 20)});
  }
  // add regions
  for (Integer i = 0; i < 5; i++) { region.Insert({i}, {Varchar<25>(REGIONS[i]), RandomString<152>(10, 20)}); }
  // add suppliers
  for (Integer i = 0; i < 10000; i++) {
    supplier.Insert({i},
                    {Varchar<25>((std::string("Supplier#") + std::string("000000000") + std::to_string(i)).c_str()),
                     RandomString<40>(10, 40), NATIONS[UniformRand(0, 61)].id, RandomByteArray<15, '0', '9'>(),
                     UniformRand(0, 10000), RandomString<40>(10, 20)});
  }
  // populate supp_stock_map
  for (Integer w = 1; w <= this->warehouse_count; w++) {
    for (Integer i = 1; i <= TPCCWorkload<AdapterType>::ITEMS_CNT; i++) {
      supp_stock_map[w * i % 10000].push_back(std::make_pair(w, i));
    }
  }
  LOG_DEBUG("Load TPC-C Extended tables successfully");
}

// -------------------------------------------------------------------------------------

template <template <typename> class AdapterType>
auto TPCCWorkloadExtended<AdapterType>::ExecuteTransaction(Integer w_id) -> int {
  // TODO(moritz): change chances
  auto rnd = UniformRand(1, 200);
  if (rnd <= 100) {
    // long running query (adapted TPC-H Query 2)
    Query2();
    return -1;
  }
  rnd -= 100;
  if (rnd <= 43) {
    this->DoPaymentRandom(w_id);
    return 0;
  }
  rnd -= 43;
  if (rnd <= 4) {
    this->DoOrderStatusRandom(w_id);
    return 1;
  }
  rnd -= 4;
  if (rnd <= 4) {
    this->DoDeliveryRandom(w_id);
    return 2;
  }
  rnd -= 4;
  if (rnd <= 4) {
    this->DoStockLevelRandom(w_id);
    return 3;
  }
  Ensure(rnd - 4 <= 45);
  this->DoNewOrderRandom(w_id);
  return 4;
}

template struct TPCCWorkloadExtended<LeanStoreAdapter>;

}  // namespace tpcc