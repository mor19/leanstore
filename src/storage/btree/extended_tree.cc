#include "storage/btree/extended_tree.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "storage/hybrid/columnrowstore.h"

#include <cstring>
#include <ctime>
#include <memory>
#include <tuple>
#include <vector>

using leanstore::sync::DeferLog;
using leanstore::sync::ExclusiveGuard;
using leanstore::sync::OptimisticGuard;
using leanstore::sync::SharedGuard;

#define ACCESS_RECORD_MACRO(node, pos, fn)                                                                 \
  ({                                                                                                       \
    size_t key_len = (node)->header.prefix_len + (node)->slots[pos].key_length;                            \
    u8 key[key_len];                                                                                       \
    auto node_ptr = *((node).Ptr());                                                                       \
    std::memcpy((key), node_ptr.GetPrefix(), node_ptr.header.prefix_len);                                  \
    std::memcpy((key) + node_ptr.header.prefix_len, node_ptr.GetKey(pos), node_ptr.slots[pos].key_length); \
    (fn)({(key), (key_len)}, node_ptr.GetPayload(pos));                                                    \
  })

namespace leanstore::storage {

ExtendedBTree::ExtendedBTree(buffer::BufferManager *buffer_pool, ColumnRowStore *column_row_store,
                             std::vector<u32> &columnSizes, bool append_bias)
    : buffer_(buffer_pool), column_row_store_(column_row_store), column_sizes_(columnSizes), append_bias_(append_bias) {
  ExclusiveGuard<MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
  ExclusiveGuard<BTreeNodeWithTimeStamp> root_page(buffer_, buffer_->AllocPage());
  new (root_page.Ptr()) storage::BTreeNodeWithTimeStamp(true);
  metadata_slotid_                   = BTree::btree_slot_counter++;
  meta_page->roots[metadata_slotid_] = root_page.PageID();
  // -------------------------------------------------------------------------------------
  meta_page.AdvanceGSN();
  root_page.AdvanceGSN();
}

void ExtendedBTree::SetComparisonOperator(ComparisonLambda cmp_op) { cmp_lambda_ = cmp_op; }

auto ExtendedBTree::IterateAllNodes(OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                                    const std::function<u64(BTreeNodeWithTimeStamp &)> &inner_fn,
                                    const std::function<u64(BTreeNodeWithTimeStamp &)> &leaf_fn) -> u64 {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  u64 res = inner_fn(*(node.Ptr()));
  for (auto idx = 0; idx < node->header.count; idx++) {
    OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(idx));
    res += IterateAllNodes(child, inner_fn, leaf_fn);
  }
  OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->header.right_most_child);
  res += IterateAllNodes(child, inner_fn, leaf_fn);
  return res;
}

auto ExtendedBTree::IterateUntils(OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                                  const std::function<bool(BTreeNodeWithTimeStamp &)> &inner_fn,
                                  const std::function<bool(BTreeNodeWithTimeStamp &)> &leaf_fn) -> bool {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  auto res = inner_fn(*(node.Ptr()));
  if (!res) {
    for (auto idx = 0; idx < node->header.count; idx++) {
      OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(idx));
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
      if (res) { break; }
    }
    if (!res) {
      OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->header.right_most_child);
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
    }
  }

  return res;
}

// -------------------------------------------------------------------------------------
auto ExtendedBTree::FindLeafOptimistic(std::span<u8> key) -> OptimisticGuard<BTreeNodeWithTimeStamp> {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  while (node->IsInner()) {
    node = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, node->FindChild(key, cmp_lambda_), node);
  }
  return node;
}

auto ExtendedBTree::FindLeafShared(std::span<u8> key) -> SharedGuard<BTreeNodeWithTimeStamp> {
  while (true) {
    try {
      OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

      while (node->IsInner()) {
        node = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, node->FindChild(key, cmp_lambda_), node);
      }

      return SharedGuard<BTreeNodeWithTimeStamp>(std::move(node));
    } catch (const sync::RestartException &) {}
  }
}

void ExtendedBTree::TrySplit(ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                             ExclusiveGuard<BTreeNodeWithTimeStamp> &&node) {
  // create new root if necessary
  if (parent.PageID() == METADATA_PAGE_ID) {
    auto meta_p = reinterpret_cast<MetadataPage *>(parent.Ptr());
    // Root node is full, alloc a new root
    ExclusiveGuard<BTreeNodeWithTimeStamp> new_root(buffer_, buffer_->AllocPage());
    new (new_root.Ptr()) storage::BTreeNodeWithTimeStamp(false);
    new_root->header.right_most_child = node.PageID();
    if (FLAGS_wal_enable) {
      new_root.PrepareWalEntry<WALNewRoot>(0);
      new_root.SubmitActiveWalEntry();
    }
    // Update root pid
    meta_p->roots[metadata_slotid_] = new_root.PageID();
    parent                          = std::move(new_root);
    parent.AdvanceGSN();
  }

  // split & retrieve new separator
  assert(parent->IsInner());
  auto sep_info = node->FindSeparator(append_bias_.load(), cmp_lambda_);
  u8 sep_key[sep_info.len];
  node->GetSeparatorKey(sep_key, sep_info);

  if (parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t))) {
    // alloc a new child page
    ExclusiveGuard<BTreeNodeWithTimeStamp> new_child(buffer_, buffer_->AllocPage());
    new (new_child.Ptr()) storage::BTreeNodeWithTimeStamp(!node->IsInner());
    // time update not needed because it is automatically done in the constructor if this is a leaf
    // now split the node
    node->SplitNode(parent.Ptr(), new_child.Ptr(), node.PageID(), new_child.PageID(), sep_info.slot,
                    {sep_key, sep_info.len}, cmp_lambda_);
    assert(node->IsInner() == new_child->IsInner());
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL new node
      new_child.PrepareWalEntry<WALInitPage>(0);
      new_child.SubmitActiveWalEntry();
      // WAL logical split
      //  all parent, node, and new_child share the same local log buffer,
      //  hence we don't need to push this wal entry using parent's and new_child's
      auto &entry = node.PrepareWalEntry<WALLogicalSplit>(0);
      std::tie(entry.parent_pid, entry.left_pid, entry.right_pid, entry.sep_slot) =
        std::make_tuple(parent.PageID(), node.PageID(), new_child.PageID(), sep_info.slot);
      node.SubmitActiveWalEntry();
    }
    return;
  }

  // must split parent to make space for separator, restart from root to do this
  node.Unlock();
  EnsureSpaceForSplit(parent.UnlockAndGetPtr(), {sep_key, sep_info.len});
}

void ExtendedBTree::EnsureSpaceForSplit(BTreeNodeWithTimeStamp *to_split, std::span<u8> key) {
  assert(to_split->IsInner());
  while (true) {
    try {
      OptimisticGuard<BTreeNodeWithTimeStamp> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner() && (node.Ptr() != to_split)) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      if (node.Ptr() == to_split) {
        if (node->HasSpaceForKV(key.size(), sizeof(pageid_t))) {
          // someone else did split concurrently
          return;
        }

        ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
        TrySplit(std::move(parent_locked), std::move(node_locked));
      }

      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

void ExtendedBTree::TryMerge(ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                             ExclusiveGuard<BTreeNodeWithTimeStamp> &&left,
                             ExclusiveGuard<BTreeNodeWithTimeStamp> &&right, leng_t left_pos) {
  if (left->MergeNodes(left_pos, parent.Ptr(), right.Ptr(), cmp_lambda_)) {
    // TODO(XXX): Free page left.PageID()
    if (FLAGS_wal_enable) {
      // WAL merge left into right
      auto &entry = left.PrepareWalEntry<WALMergeNodes>(0);
      std::tie(entry.parent_pid, entry.left_pid, entry.right_pid, entry.left_pos) =
        std::make_tuple(parent.PageID(), left.PageID(), right.PageID(), left_pos);
      left.SubmitActiveWalEntry();
    }
    if (parent->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL) {
      left.Unlock();
      right.Unlock();
      // Parent node is underfull, try merge this inner node
      EnsureUnderfullInnersForMerge(parent.UnlockAndGetPtr());
    }
  }
}

void ExtendedBTree::EnsureUnderfullInnersForMerge(BTreeNodeWithTimeStamp *to_merge) {
  assert(to_merge->IsInner());
  auto rep_key = to_merge->GetUpperFence();
  while (true) {
    try {
      // TODO(XXX): Implement tree-level compression
      //  i.e. parent of parent is page 0 (i.e. metadata page)
      //  and we can compress the inner nodes
      OptimisticGuard<BTreeNodeWithTimeStamp> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      leng_t node_pos = 0;
      while (node->IsInner() && (node.Ptr() != to_merge)) {
        parent = std::move(node);
        node =
          OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, parent->FindChild(rep_key, node_pos, cmp_lambda_), parent);
      }

      if (parent.PageID() != METADATA_PAGE_ID &&  // Root node can't be merged
          node.Ptr() == to_merge &&               // Found the correct node to be merged
          node_pos < parent->header.count &&      // Current node is not the right most child
          parent->header.count >= 1 &&            // Parent has more than one children
          (node->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL)  // Current node is underfull
      ) {
        // underfull
        auto right_pid =
          (node_pos < parent->header.count - 1) ? parent->GetChild(node_pos + 1) : parent->header.right_most_child;
        OptimisticGuard<BTreeNodeWithTimeStamp> right(buffer_, right_pid, parent);
        if (right->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL)) {
          ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
          ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
          ExclusiveGuard<BTreeNodeWithTimeStamp> right_locked(std::move(right));
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      }
      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

auto ExtendedBTree::LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> bool {
  while (true) {
    try {
      OptimisticGuard<BTreeNodeWithTimeStamp> node = FindLeafOptimistic(key);
      bool found;
      leng_t pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }

      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return true;
    } catch (const sync::RestartException &) {}
  }
}

void ExtendedBTree::Insert(std::span<u8> key, std::span<const u8> payload) {
  assert((key.size() + payload.size()) <= BTreeNodeWithTimeStamp::MAX_RECORD_SIZE);

  while (true) {
    try {
      OptimisticGuard<BTreeNodeWithTimeStamp> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      // Found the leaf node to insert new data
      if (node->HasSpaceForKV(key.size(), payload.size())) {
        /* Alternative WAL cycle - automatically append WAL entry when the scope ends */
        auto defer_log = DeferLog<BTreeNodeWithTimeStamp>();

        /* Leaf node has enough space -> only latch leaf node */
        {
          ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
          parent.ValidateOrRestart();
          node_locked->InsertKeyValue(key, payload, cmp_lambda_);
          // update time
          std::time(&node_locked->header.timestamp);

          /* Generate the log entry */
          if (FLAGS_wal_enable) { defer_log.Construct<WALInsert>(node_locked, key, payload); }
        }

        return;  // success
      }

      // The leaf node doesn't have enough space, we have to split it
      ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto ExtendedBTree::Remove(std::span<u8> key) -> bool {
  while (true) {
    try {
      OptimisticGuard<BTreeNodeWithTimeStamp> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      leng_t node_pos = 0;
      while (node->IsInner()) {
        parent = std::move(node);
        node = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, parent->FindChild(key, node_pos, cmp_lambda_), parent);
      }

      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }

      auto payload      = node->GetPayload(slot_id);
      leng_t entry_size = node->slots[slot_id].key_length + payload.size();
      if ((node->FreeSpaceAfterCompaction() + entry_size >=
           BTreeNodeHeader::SIZE_UNDER_FULL) &&     // new node is under full
          (parent.PageID() != METADATA_PAGE_ID) &&  // current node is not the root node
          (parent->header.count >= 2) &&            // parent has more than one children
          ((node_pos + 1) < parent->header.count)   // current node has a right sibling
      ) {
        // underfull
        ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
        ExclusiveGuard<BTreeNodeWithTimeStamp> right_locked(buffer_, parent_locked->GetChild(node_pos + 1));
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) { WAL_RECORD(node_locked, WALRemove, key, payload); }
        // --------------------------------------------------------------------------
        // right child is also under full
        if (right_locked->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL)) {
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      } else {
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) { WAL_RECORD(node_locked, WALRemove, key, payload); }
        // --------------------------------------------------------------------------
      }
      return true;
    } catch (const sync::RestartException &) {}
  }
}

/**
 * @brief Atomically update the key-payload pair
 * Return true/false whether the key exists and is updated successfully
 *
 * If `func` is provided, then func(previous payload) is triggered
 */
auto ExtendedBTree::Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> bool {
  assert((key.size() + payload.size()) <= BTreeNodeWithTimeStamp::MAX_RECORD_SIZE);

  while (true) {
    try {
      OptimisticGuard<BTreeNodeWithTimeStamp> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }
      auto curr_payload = node->GetPayload(slot_id);

      // Found the leaf node to insert new data
      if (payload.size() <= curr_payload.size() ||
          node->HasSpaceForKV(key.size(), payload.size() - curr_payload.size())) {
        // only lock leaf
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
        parent.ValidateOrRestart();

        // Log previos payload, trigger func utility if provided, and remove the entry
        if (FLAGS_wal_enable) { WAL_RECORD(node_locked, WALRemove, key, curr_payload); }
        if (func) { func(curr_payload); }
        node_locked->RemoveSlot(slot_id);

        // Insert new payload and add log entry
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        // would require time update, but the update method should not be used in the hot data to improve time ordered
        // location

        if (FLAGS_wal_enable) { WAL_RECORD(node_locked, WALInsert, key, payload); }
        // --------------------------------------------------------------------------
        return true;  // success
      }

      // The leaf node doesn't have enough space, we have to split it
      ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto ExtendedBTree::UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> bool {
  while (true) {
    try {
      auto node = FindLeafOptimistic(key);
      bool found;
      auto pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }

      /* Alternative WAL cycle - automatically append WAL entry when the scope ends */
      auto defer_log = DeferLog<BTreeNodeWithTimeStamp>();

      {
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));

        /* Retrieve previous payload for delta record */
        auto record = node_locked->GetPayload(pos);
        if (FLAGS_wal_enable && delta != nullptr) { delta->UpdateDeltaPayload(record); }

        /* Modify the record */
        func(record);
        // would require time update, but the update method should not be used in the hot data to improve time ordered
        // location

        /* Generate the delta record */
        if (FLAGS_wal_enable) {
          if (delta != nullptr) {
            defer_log.Construct<WALDeltaImage>(node_locked, key,
                                               std::span<u8>(reinterpret_cast<u8 *>(delta), delta->size));
          } else {
            defer_log.Construct<WALAfterImage>(node_locked, key, record);
          }
        }
      }

      return true;
    } catch (const sync::RestartException &) {}
  }
}

void ExtendedBTree::ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) {
  auto node = FindLeafShared(key);
  bool unused;
  auto pos = node->LowerBound(key, unused, cmp_lambda_);
  while (true) {
    if (pos < node->header.count) {
      if (!ACCESS_RECORD_MACRO(node, pos, fn)) { return; }
      pos++;
    } else {
      if (!node->header.HasRightNeighbor()) { return; }
      pos  = 0;
      node = SharedGuard<BTreeNodeWithTimeStamp>(buffer_, node->header.next_leaf_node);
    }
  }
}

void ExtendedBTree::ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) {
  auto node = FindLeafShared(key);
  bool found;
  int pos = static_cast<int>(node->LowerBound(key, found, cmp_lambda_));
  // LowerBound search always return the first position whose key >= the search key
  // hence, if LowerBound doesn't give an exact match,
  //    then the found key will > search key as we scan desc,
  // any key > search key should be overlooked, i.e. start from pos - 1
  if (!found) { pos--; }
  while (true) {
    while (pos >= 0) {
      if (pos < node->header.count) {
        if (!ACCESS_RECORD_MACRO(node, pos, fn)) { return; }
      }
      pos--;
    }
    if (node->header.IsLowerFenceInfinity()) { return; }
    node = FindLeafShared(node->GetLowerFence());
    pos  = node->header.count - 1;
  }
}

auto ExtendedBTree::CountEntries() -> u64 {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(
    node, [](BTreeNodeWithTimeStamp &) { return 0; }, [](BTreeNodeWithTimeStamp &node) { return node.header.count; });
}

// -------------------------------------------------------------------------------------

auto ExtendedBTree::IsNotEmpty() -> bool {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateUntils(
    node, [](BTreeNodeWithTimeStamp &) { return false; },
    [](BTreeNodeWithTimeStamp &node) { return (node.header.count > 0); });
}

auto ExtendedBTree::CountPages() -> u64 {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(node, [](BTreeNodeWithTimeStamp &) { return 1; }, [](BTreeNodeWithTimeStamp &) { return 1; });
}

auto ExtendedBTree::SizeInMB() -> float { return CountPages() * static_cast<float>(PAGE_SIZE) / MB; }

/**
 * @brief Only used for Blob Handler indexes.
 * Similar to LookUp operator, but for Byte String as key
 */
auto ExtendedBTree::LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const AccessPayloadFunc &read_cb)
  -> bool {
  Ensure(cmp_lambda_.op == ComparisonOperator::BLOB_HANDLER);
  Ensure(cmp.op == ComparisonOperator::BLOB_LOOKUP);
  leng_t unused;
  auto search_key = blob::BlobLookupKey(blob_key);

  while (true) {
    try {
      OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, meta->GetRoot(metadata_slotid_), meta);
      while (node->IsInner()) {
        node =
          OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, node->FindChildWithBlobKey(search_key, unused, cmp), node);
      }

      bool found;
      leng_t pos = node->LowerBoundWithBlobKey(search_key, found, cmp);
      if (!found) { return false; }

      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return true;
    } catch (const sync::RestartException &) {}
  }
}

void ExtendedBTree::RemoveInnerNode(sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                                    sync::ExclusiveGuard<BTreeNodeWithTimeStamp> &&node) {
  assert(parent->IsInner());
  assert(node->IsInner());

  // root node children can not be removed
  if (parent.PageID() == METADATA_PAGE_ID) { return; }

  if (parent->header.right_most_child == node.PageID()) {
    // remove rightmost child by overwriting with second last child ptr
    parent->header.right_most_child = parent->GetChild(parent->header.count - 1);
    parent->RemoveSlot(parent->header.count - 1);
  } else {
    // find pos and remove slot
    for (leng_t pos = 0; pos < parent->header.count; pos++) {
      if (parent->GetChild(pos) == node.PageID()) {
        parent->RemoveSlot(pos);
        break;
      }
    }
  }

  // check if parent is underfull -> merge upwards
  if (parent->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL) {
    EnsureUnderfullInnersForMerge(parent.UnlockAndGetPtr());
  }

  // store cold data
  std::vector<u8> tmp_row_ids;
  std::vector<std::vector<u8>> tmp_data;
  tmp_data.resize(column_sizes_.size());
  // read all children
  for (auto idx = 0; idx < node->header.count; idx++) {
    OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(idx));
    for (auto entry_idx = 0; entry_idx < child.Ptr()->header.count; entry_idx++) {
      // row id (also copy prefix!)
      u8 tmpRowIdBuffer[sizeof(u64)];
      std::memcpy(tmpRowIdBuffer, child.Ptr()->GetPrefix(), child.Ptr()->header.prefix_len);
      std::memcpy(tmpRowIdBuffer + child.Ptr()->header.prefix_len, child.Ptr()->GetKey(entry_idx),
                  child.Ptr()->slots[entry_idx].key_length);
      tmp_row_ids.insert(tmp_row_ids.end(), tmpRowIdBuffer, tmpRowIdBuffer + sizeof(u64));
      // payload
      u8 *payloadData     = child.Ptr()->GetPayload(entry_idx).data();
      size_t recordOffset = 0;
      // split payload into column values
      for (size_t col_idx = 0; col_idx < column_sizes_.size(); col_idx++) {
        const u32 size = column_sizes_[col_idx];
        tmp_data[col_idx].insert(tmp_data[col_idx].end(), payloadData + recordOffset,
                                 payloadData + recordOffset + size);
        recordOffset += size;
      }
    }
  }
  {
    OptimisticGuard<BTreeNodeWithTimeStamp> rightmost_child(buffer_, node->header.right_most_child);
    for (auto entry_idx = 0; entry_idx < rightmost_child.Ptr()->header.count; entry_idx++) {
      // row id (also copy prefix!)
      u8 tmpRowIdBuffer[sizeof(u64)];
      std::memcpy(tmpRowIdBuffer, rightmost_child.Ptr()->GetPrefix(), rightmost_child.Ptr()->header.prefix_len);
      std::memcpy(tmpRowIdBuffer + rightmost_child.Ptr()->header.prefix_len, rightmost_child.Ptr()->GetKey(entry_idx),
                  rightmost_child.Ptr()->slots[entry_idx].key_length);
      tmp_row_ids.insert(tmp_row_ids.end(), tmpRowIdBuffer, tmpRowIdBuffer + sizeof(u64));
      // payload
      u8 *payloadData     = rightmost_child.Ptr()->GetPayload(entry_idx).data();
      size_t recordOffset = 0;
      // split payload into column values
      for (size_t col_idx = 0; col_idx < column_sizes_.size(); col_idx++) {
        const u32 size = column_sizes_[col_idx];
        tmp_data[col_idx].insert(tmp_data[col_idx].end(), payloadData + recordOffset,
                                 payloadData + recordOffset + size);
        recordOffset += size;
      }
    }
  }

  // store cold data
  column_row_store_->StoreColdData(tmp_row_ids, tmp_data);

  node.Unlock();
}

void ExtendedBTree::MoveHotDataToColdData() {
  while (true) {
    try {
      // make sure at least root and its children are inner nodes
      OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNodeWithTimeStamp> root(buffer_, meta->GetRoot(metadata_slotid_), meta);
      if (!root->IsInner() || root->header.count == 0) { return; }
      time_t current_time;
      std::time(&current_time);
      leng_t count = root->header.count;
      for (leng_t i = 0; i < count; i++) {
        OptimisticGuard<BTreeNodeWithTimeStamp> root(buffer_, meta->GetRoot(metadata_slotid_), meta);
        OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, root->GetChild(i));
        if (!child->IsInner()) { return; }
        // start iterating from root
        IterateLeafParents(child, root, current_time);
      }
      return;
    } catch (const sync::RestartException &) {}
  }
}

void ExtendedBTree::IterateLeafParents(OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                                       OptimisticGuard<BTreeNodeWithTimeStamp> &parent, time_t current_time) {
  if (!node->IsInner()) return;
  OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->header.right_most_child);
  for (leng_t i = 0; i < node->header.count; i++) {
    OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(i));

    if (!child->IsInner()) {
      // leaf parent -> check time in rightmost child node
      OptimisticGuard<BTreeNodeWithTimeStamp> rightmost_child(buffer_, node->header.right_most_child);
      ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
      if (current_time - rightmost_child->header.timestamp >= FLAGS_htap_expire_seconds) {
        // data is expired -> move to cold data
        RemoveInnerNode(std::move(parent_locked), std::move(node_locked));
        throw sync::RestartException{};
      }
      return;
    } else {
      IterateLeafParents(child, node, current_time);
    }
  }

  // rightmost child
  if (!child->IsInner()) {
    // leaf parent -> check time
    OptimisticGuard<BTreeNodeWithTimeStamp> rightmost_child(buffer_, node->header.right_most_child);
    ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
    ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
    if (current_time - rightmost_child->header.timestamp >= FLAGS_htap_expire_seconds) {
      // data is expired -> move to cold data
      RemoveInnerNode(std::move(parent_locked), std::move(node_locked));
      throw sync::RestartException{};
    }
  } else {
    IterateLeafParents(child, node, current_time);
  }
}

}  // namespace leanstore::storage