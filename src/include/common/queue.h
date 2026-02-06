#pragma once

#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "sync/hybrid_latch.h"

#include "gtest/gtest_prod.h"

#include <array>
#include <atomic>
#include <cassert>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <type_traits>

namespace leanstore {
template <typename T>
class LockFreeQueue {
 public:
  static constexpr auto NULL_SIZE = std::numeric_limits<uoffset_t>::max();

  LockFreeQueue();
  ~LockFreeQueue();

  constexpr auto CurrentTail() -> u64 { return tail_.load(); }

  /**
   * @brief Push a serialized element to the queue from unserialized data
   */
  template <typename T2>
  void Push(const T2 &element) {
    assert(element.SerializedSize() < NULL_SIZE);
    auto item_size = static_cast<uoffset_t>(element.SerializedSize());
    auto w_tail    = tail_.load();

    /* Circular buffer: no room for this element + a CR entry, so we circular back */
    if (buffer_capacity_ - w_tail < item_size + sizeof(T::NULL_ITEM)) {
      Ensure(buffer_capacity_ - w_tail >= sizeof(T::NULL_ITEM));
      std::memcpy(&buffer_[w_tail], &(T::NULL_ITEM), sizeof(T::NULL_ITEM));
      w_tail = 0;
    }

    /* Wait until the consumer accesses more items to free up some memory */
    auto r_head = head_.load();
    while (ContiguousFreeBytes(r_head, w_tail) < item_size) {
      r_head = head_.load();
      AsmYield();
    }

    /* Have enough memory -> producer write the element to the buffer */
    Ensure(w_tail % CPU_CACHELINE_SIZE == 0);
    auto obj = reinterpret_cast<T *>(&buffer_[w_tail]);
    obj->Construct(element);
    tail_.store(w_tail + item_size);
  }

  /* Erase and loop utilities */
  void Erase(u64 no_bytes);
  auto LoopElements(u64 until_tail, const std::function<bool(T &)> &read_cb) -> u64;

 private:
  FRIEND_TEST(TestQueue, BasicTest);
  FRIEND_TEST(TestQueue, ConcurrencyTest);

  u8 *buffer_;
  u64 buffer_capacity_;
  std::atomic<u64> head_ = {0}; /* Read from head */
  std::atomic<u64> tail_ = {0}; /* Write to tail */

  auto ContiguousFreeBytes(u64 r_head, u64 w_tail) -> u64 {
    // circulate the wal_cursor to the beginning and insert the whole entry
    return (w_tail < r_head) ? r_head - w_tail : buffer_capacity_ - w_tail;
  }
};

template <class T>
class ConcurrentQueue {
 public:
  ConcurrentQueue()  = default;
  ~ConcurrentQueue() = default;

  /**
   * @brief Unsafe operator[], assuming that size of the internal deque is larger than the idx
   * Only used for testing
   */
  auto operator[](u64 idx) -> T & { return internal_[idx]; }

  auto LoopElement(u64 no_elements, const std::function<bool(T &)> &fn) -> u64;
  void Push(T &element);
  auto Erase(u64 no_elements) -> bool;
  auto SizeApprox() -> size_t;

 private:
  std::deque<T> internal_;
  sync::HybridLatch latch_;
};

}  // namespace leanstore