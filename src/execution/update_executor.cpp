//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      lock_mgr_(exec_ctx->GetLockManager()),
      table_(table_info_->table_.get()),
      schema_(&(table_info_->schema_)),
      txn_(exec_ctx_->GetTransaction()) {}

void UpdateExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  if (lock_mgr_ != nullptr && txn_ != nullptr) {
    if (!txn_->IsExclusiveLocked(*rid)) {
      if (txn_->IsSharedLocked(*rid)) {
        lock_mgr_->LockUpgrade(txn_, *rid);
      } else {
        lock_mgr_->LockExclusive(txn_, *rid);
      }
    }
  }

  Tuple new_tuple = GenerateUpdatedTuple(*tuple);
  table_->UpdateTuple(new_tuple, *rid, txn_);

  for (auto index_info : index_info_) {
    Index *index = index_info->index_.get();
    Tuple old_key = tuple->KeyFromTuple(*schema_, index_info->key_schema_, index->GetMetadata()->GetKeyAttrs());
    Tuple new_key = new_tuple.KeyFromTuple(*schema_, index_info->key_schema_, index->GetMetadata()->GetKeyAttrs());

    static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index)->DeleteEntry(old_key, *rid, txn_);
    static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index)->InsertEntry(new_key, *rid, txn_);

    // IndexWriteRecord index_write_record = IndexWriteRecord(*rid, plan_->TableOid(), WType::UPDATE, new_tuple,
    // index_info->index_oid_, exec_ctx_->GetCatalog()); txn_->AppendTableWriteRecord(index_write_record);
  }

  *tuple = new_tuple;

  return true;
}
}  // namespace bustub
