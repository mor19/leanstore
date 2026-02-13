#include "storage/btree/extended_tree.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
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

namespace leanstore::storage {

ExtendedBTree::ExtendedBTree(buffer::BufferManager *buffer_pool, recovery::RecoveryManager *recovery,
                             ColumnRowStore *column_row_store, std::vector<u32> &columnSizes, u32 tree_slot)
    : buffer_(buffer_pool),
      recovery_(recovery),
      column_row_store_(column_row_store),
      column_sizes_(columnSizes),
      metadata_slotid_(tree_slot) {
  if (!FLAGS_wal_enable_recovery) {
    ExclusiveGuard<MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
    ExclusiveGuard<BTreeNodeWithTimeStamp> root_page(buffer_, buffer_->AllocPage());
    new (root_page.Ptr()) storage::BTreeNodeWithTimeStamp(true);
    meta_page->roots[metadata_slotid_] = root_page.PageID();
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      GenerateWALNewRoot(meta_page, metadata_slotid_, root_page.PageID());
      GenerateWALFreshPage(root_page, true);
    }
  }
}

void ExtendedBTree::SetComparisonOperator(ComparisonLambda cmp_op) { cmp_lambda_ = cmp_op; }

auto ExtendedBTree::IterateAllNodes(OptimisticGuard<BTreeNodeWithTimeStamp> &node,
                                    const std::function<u64(BTreeNodeWithTimeStamp &)> &inner_fn,
                                    const std::function<u64(BTreeNodeWithTimeStamp &)> &leaf_fn) -> u64 {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  u64 res = inner_fn(*(node.Ptr()));
  for (auto idx = 0; idx < node->header.count; idx++) {
    auto child_pid = node->GetChild(idx);
    InstantRecovery(child_pid);
    OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, child_pid);
    res += IterateAllNodes(child, inner_fn, leaf_fn);
  }
  InstantRecovery(node->header.right_most_child);
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
  auto root_pid = meta->GetRoot(metadata_slotid_);
  InstantRecovery(root_pid);
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, root_pid, meta);

  while (node->IsInner()) {
    auto next_pid = node->FindChild(key, cmp_lambda_);
    InstantRecovery(next_pid);
    node = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, next_pid, node);
  }
  return node;
}

auto ExtendedBTree::FindLeafShared(std::span<u8> key) -> SharedGuard<BTreeNodeWithTimeStamp> {
  while (true) {
    try {
      OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      auto root_pid = meta->GetRoot(metadata_slotid_);
      InstantRecovery(root_pid);
      OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, root_pid, meta);

      while (node->IsInner()) {
        auto next_pid = node->FindChild(key, cmp_lambda_);
        InstantRecovery(next_pid);
        node = OptimisticGuard<BTreeNodeWithTimeStamp>(buffer_, next_pid, node);
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
      GenerateWALNewRoot(parent, metadata_slotid_, new_root.PageID());
      GenerateWALFreshPage(new_root, false, node.PageID());
    }
    // Update root pid
    meta_p->roots[metadata_slotid_] = new_root.PageID();
    parent                          = std::move(new_root);
  }

  // split & retrieve new separator
  assert(parent->IsInner());
  auto sep_info = node->FindSeparator(cmp_lambda_);
  u8 sep_key[sep_info.len];
  node->GetSeparatorKey(sep_key, sep_info);

  if (parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t))) {
    // alloc a new child page
    ExclusiveGuard<BTreeNodeWithTimeStamp> new_child(buffer_, buffer_->AllocPage());
    new (new_child.Ptr()) storage::BTreeNodeWithTimeStamp(!node->IsInner());
    // now split the node
    node->SplitNode(parent.Ptr(), new_child.Ptr(), node.PageID(), new_child.PageID(), sep_info.slot,
                    {sep_key, sep_info.len}, cmp_lambda_);
    assert(node->IsInner() == new_child->IsInner());
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL new node
      GenerateWALNewPage(new_child);
      // WAL separator to parent
      {
        auto &entry      = parent.PrepareWalEntry<WALInsertSep>(sep_info.len);
        entry.left_pid   = node.PageID();
        entry.right_pid  = new_child.PageID();
        entry.sep_length = sep_info.len;
        std::memcpy(entry.sep_key, sep_key, sep_info.len);
        parent.SubmitActiveWalEntry();
      }
      // WAL logical split
      {
        auto &entry = node.PrepareWalEntry<WALLogicalSplit>(0);
        entry.sep   = sep_info;
        if (!node->IsInner()) { entry.next_leaf_node = node->header.next_leaf_node; }
        node.SubmitActiveWalEntry();
      }
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

/** Try merging `left` into `right` node */
void ExtendedBTree::TryMerge(ExclusiveGuard<BTreeNodeWithTimeStamp> &&parent,
                             ExclusiveGuard<BTreeNodeWithTimeStamp> &&left,
                             ExclusiveGuard<BTreeNodeWithTimeStamp> &&right, leng_t left_pos) {
  if (left->MergeNodes(left_pos, parent.Ptr(), right.Ptr(), cmp_lambda_)) {
    // TODO(XXX): Free page left.PageID()
    if (FLAGS_wal_enable) {
      // WAL parent remove slot
      {
        auto &entry   = parent.PrepareWalEntry<WALRemove>(0);
        entry.slot_id = left_pos;
        parent.SubmitActiveWalEntry();
      }
      // WAL new page -- it is simpler to implement, but probably slightly inefficient
      GenerateWALNewPage(right);
    }
    if (parent->FreeSpaceAfterCompaction() >= BTreeNodeHeaderWithTimestamp::SIZE_UNDER_FULL) {
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
      //  i.e. parent of parent is page 0 (i.e. metadata page) and we can compress the inner nodes
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
          (node->FreeSpaceAfterCompaction() >=
           BTreeNodeHeaderWithTimestamp::SIZE_UNDER_FULL)  // Current node is underfull
      ) {
        // underfull
        auto right_pid =
          (node_pos < parent->header.count - 1) ? parent->GetChild(node_pos + 1) : parent->header.right_most_child;
        OptimisticGuard<BTreeNodeWithTimeStamp> right(buffer_, right_pid, parent);
        if (right->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeaderWithTimestamp::SIZE_UNDER_FULL)) {
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
           BTreeNodeHeaderWithTimestamp::SIZE_UNDER_FULL) &&  // new node is under full
          (parent.PageID() != METADATA_PAGE_ID) &&            // current node is not the root node
          (parent->header.count >= 2) &&                      // parent has more than one children
          ((node_pos + 1) < parent->header.count)             // current node has a right sibling
      ) {
        // underfull
        ExclusiveGuard<BTreeNodeWithTimeStamp> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
        ExclusiveGuard<BTreeNodeWithTimeStamp> right_locked(buffer_, parent_locked->GetChild(node_pos + 1));
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) {
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          node_locked.SubmitActiveWalEntry();
        }
        // --------------------------------------------------------------------------
        // right child is also under full
        if (right_locked->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeaderWithTimestamp::SIZE_UNDER_FULL)) {
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      } else {
        ExclusiveGuard<BTreeNodeWithTimeStamp> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) {
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          node_locked.SubmitActiveWalEntry();
        }
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

        // Log previous payload, trigger func utility if provided, and remove the entry
        if (FLAGS_wal_enable) {
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          node_locked.SubmitActiveWalEntry();
        }
        if (func) { func(curr_payload); }
        node_locked->RemoveSlot(slot_id);

        // Insert new payload and add log entry
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        if (FLAGS_wal_enable) {
          GenerateWAL<ExclusiveGuard<BTreeNodeWithTimeStamp>, WALInsert>(node_locked, key, payload);
        }
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

        /* Modify the record, and store the after-value */
        auto record = node_locked->GetPayload(pos);
        func(record);
        if (FLAGS_wal_enable && delta != nullptr) { delta->UpdateDeltaPayload(record); }

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
      if (!AccessRecord(node, pos, fn)) { return; }
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
  // hence, if LowerBound doesn't give an exact match, the found key will > search key as we scan desc,
  // any key > search key should be overlooked, i.e. start from pos - 1
  if (!found) { pos--; }
  while (true) {
    while (pos >= 0) {
      if (pos < node->header.count) {
        if (!AccessRecord(node, pos, fn)) { return; }
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
  auto root_pid = meta->GetRoot(metadata_slotid_);
  InstantRecovery(root_pid);
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, root_pid, meta);

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
auto ExtendedBTree::LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp,
                               const AccessPayloadFunc &read_cb) -> bool {
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

void ExtendedBTree::MoveHotDataToColdData() {
  // make sure at least root and its children are inner nodes
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNodeWithTimeStamp> root(buffer_, meta->GetRoot(metadata_slotid_), meta);
  if (!root->IsInner() || root->header.count == 0) { return; }
  time_t current_time;
  std::time(&current_time);
  // start iterating from root
  // IterateLeafParents(root.PageID(), current_time); // too slow
  u8 tmpRowIdBuffer[sizeof(u64)];
  std::vector<u8> tmp_row_ids;
  std::vector<std::vector<u8>> tmp_data;
  tmp_data.resize(column_sizes_.size());
  IterateAllNodes(
    root, [](BTreeNodeWithTimeStamp &) { return 0; },
    [&](BTreeNodeWithTimeStamp &leaf) {
      for (auto entry_idx = 0; entry_idx < leaf.header.count; entry_idx++) {
        // row id (also copy prefix!)
        std::memcpy(tmpRowIdBuffer, leaf.GetPrefix(), leaf.header.prefix_len);
        std::memcpy(tmpRowIdBuffer + leaf.header.prefix_len, leaf.GetKey(entry_idx), leaf.slots[entry_idx].key_length);
        tmp_row_ids.insert(tmp_row_ids.end(), tmpRowIdBuffer, tmpRowIdBuffer + sizeof(u64));
        // payload
        u8 *payloadData     = leaf.GetPayload(entry_idx).data();
        size_t recordOffset = 0;
        // split payload into column values
        for (size_t col_idx = 0; col_idx < column_sizes_.size(); col_idx++) {
          const u32 size = column_sizes_[col_idx];
          tmp_data[col_idx].insert(tmp_data[col_idx].end(), payloadData + recordOffset,
                                   payloadData + recordOffset + size);
          recordOffset += size;
        }
      }
      // TODO(moritz): tuple limit
      if ((tmp_row_ids.size() >> 3) >= 10000) {
        column_row_store_->StoreColdData(tmp_row_ids, tmp_data);
        tmp_row_ids.clear();
        tmp_data.clear();
        tmp_data.resize(column_sizes_.size());
      }
      return 1;
    });
  if (tmp_row_ids.size() > 0) { column_row_store_->StoreColdData(tmp_row_ids, tmp_data); }
}

void ExtendedBTree::IterateLeafParents(pageid_t nodeId, time_t current_time) {
  // not threadsafe!!!
  OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, nodeId);
  if (!node->IsInner()) { return; }
  OptimisticGuard<BTreeNodeWithTimeStamp> rightmost_child(buffer_, node->header.right_most_child);
  if (!rightmost_child->IsInner()) {
    // leaf parent -> check time in rightmost child node
    if (current_time - rightmost_child->header.timestamp >= FLAGS_htap_expire_seconds) {
      // data is expired -> move to cold data
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

      // store cold data
      column_row_store_->StoreColdData(tmp_row_ids, tmp_data);
    }
    return;
  } else {
    // iterate children
    for (leng_t i = 0; i < node->header.count; i++) {
      OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(i));
      IterateLeafParents(child.PageID(), current_time);
    }
    IterateLeafParents(rightmost_child.PageID(), current_time);
  }
}

// too small granularity
// void ExtendedBTree::IterateLeafParents(pageid_t nodeId, time_t current_time) {
//   // not threadsafe!!!
//   OptimisticGuard<BTreeNodeWithTimeStamp> node(buffer_, nodeId);
//   if (!node->IsInner()) { return; }
//   OptimisticGuard<BTreeNodeWithTimeStamp> rightmost_child(buffer_, node->header.right_most_child);
//   std::vector<pageid_t> toCheck;
//   for (leng_t i = 0; i < node->header.count; i++) {
//     OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(i));
//     if (!child->IsInner()) {
//       // leaf parent -> check time in rightmost child node
//       if (current_time - rightmost_child->header.timestamp >= FLAGS_htap_expire_seconds) {
//         // data is expired -> move to cold data
//         // store cold data
//         std::vector<u8> tmp_row_ids;
//         std::vector<std::vector<u8>> tmp_data;
//         tmp_data.resize(column_sizes_.size());
//         // read all children
//         for (auto idx = 0; idx < node->header.count; idx++) {
//           OptimisticGuard<BTreeNodeWithTimeStamp> child(buffer_, node->GetChild(idx));
//           for (auto entry_idx = 0; entry_idx < child.Ptr()->header.count; entry_idx++) {
//             // row id (also copy prefix!)
//             u8 tmpRowIdBuffer[sizeof(u64)];
//             std::memcpy(tmpRowIdBuffer, child.Ptr()->GetPrefix(), child.Ptr()->header.prefix_len);
//             std::memcpy(tmpRowIdBuffer + child.Ptr()->header.prefix_len, child.Ptr()->GetKey(entry_idx),
//                         child.Ptr()->slots[entry_idx].key_length);
//             tmp_row_ids.insert(tmp_row_ids.end(), tmpRowIdBuffer, tmpRowIdBuffer + sizeof(u64));
//             // payload
//             u8 *payloadData     = child.Ptr()->GetPayload(entry_idx).data();
//             size_t recordOffset = 0;
//             // split payload into column values
//             for (size_t col_idx = 0; col_idx < column_sizes_.size(); col_idx++) {
//               const u32 size = column_sizes_[col_idx];
//               tmp_data[col_idx].insert(tmp_data[col_idx].end(), payloadData + recordOffset,
//                                        payloadData + recordOffset + size);
//               recordOffset += size;
//             }
//           }
//         }

//         for (auto entry_idx = 0; entry_idx < rightmost_child.Ptr()->header.count; entry_idx++) {
//           // row id (also copy prefix!)
//           u8 tmpRowIdBuffer[sizeof(u64)];
//           std::memcpy(tmpRowIdBuffer, rightmost_child.Ptr()->GetPrefix(), rightmost_child.Ptr()->header.prefix_len);
//           std::memcpy(tmpRowIdBuffer + rightmost_child.Ptr()->header.prefix_len,
//                       rightmost_child.Ptr()->GetKey(entry_idx), rightmost_child.Ptr()->slots[entry_idx].key_length);
//           tmp_row_ids.insert(tmp_row_ids.end(), tmpRowIdBuffer, tmpRowIdBuffer + sizeof(u64));
//           // payload
//           u8 *payloadData     = rightmost_child.Ptr()->GetPayload(entry_idx).data();
//           size_t recordOffset = 0;
//           // split payload into column values
//           for (size_t col_idx = 0; col_idx < column_sizes_.size(); col_idx++) {
//             const u32 size = column_sizes_[col_idx];
//             tmp_data[col_idx].insert(tmp_data[col_idx].end(), payloadData + recordOffset,
//                                      payloadData + recordOffset + size);
//             recordOffset += size;
//           }
//         }

//         // store cold data
//         column_row_store_->StoreColdData(tmp_row_ids, tmp_data);
//       }
//       return;
//     } else {
//       toCheck.push_back(child.PageID());
//     }
//   }
//   toCheck.push_back(rightmost_child.PageID());

//   // iterate children
//   for (pageid_t pageId : toCheck) { IterateLeafParents(pageId, current_time); }
// }

}  // namespace leanstore::storage
