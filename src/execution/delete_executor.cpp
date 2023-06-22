//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      lock_mgr_(exec_ctx->GetLockManager()),
      txn_(exec_ctx_->GetTransaction()) {}

void DeleteExecutor::Init() {
  TableMetadata *table_matadata = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_ = table_matadata->table_.get();
  schema_ = &table_matadata->schema_;
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_matadata->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    if (lock_mgr_ != nullptr && txn_ != nullptr) {
      if (!txn_->IsExclusiveLocked(*rid)) {
        if (txn_->IsSharedLocked(*rid)) {
          lock_mgr_->LockUpgrade(txn_, *rid);
        } else {
          lock_mgr_->LockExclusive(txn_, *rid);
        }
      }
    }

    table_->MarkDelete(*rid, txn_);

    for (auto index_info : index_info_) {
      Index *index = index_info->index_.get();
      Tuple key = tuple->KeyFromTuple(*schema_, index_info->key_schema_, index->GetMetadata()->GetKeyAttrs());
      static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index)->DeleteEntry(key, *rid, txn_);
      IndexWriteRecord index_write_record = IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, *tuple,
                                                             index_info->index_oid_, exec_ctx_->GetCatalog());
      txn_->AppendTableWriteRecord(index_write_record);
    }
  }

  return false;
}

}  // namespace bustub
