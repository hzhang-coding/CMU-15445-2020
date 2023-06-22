//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx_->GetTransaction()) {}

void InsertExecutor::Init() {
  TableMetadata *table_matadata = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_ = table_matadata->table_.get();
  schema_ = &table_matadata->schema_;
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_matadata->name_);

  if (plan_->IsRawInsert()) {
    option_ = 0;
    idx_ = 0;
    size_ = plan_->RawValues().size();
  } else {
    option_ = 1;
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (option_ == 0) {
    if (idx_ >= size_) {
      return false;
    }

    *tuple = Tuple(plan_->RawValuesAt(idx_), schema_);
    ++idx_;
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  }

  table_->InsertTuple(*tuple, rid, txn_);
  // TableWriteRecord table_write_record = TableWriteRecord(*rid, WType::INSERT, *tuple, table_);
  // txn_->AppendTableWriteRecord(table_write_record);

  for (auto index_info : index_info_) {
    Index *index = index_info->index_.get();
    Tuple key = tuple->KeyFromTuple(*schema_, index_info->key_schema_, index->GetMetadata()->GetKeyAttrs());
    static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index)->InsertEntry(key, *rid, txn_);
    IndexWriteRecord index_write_record = IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, *tuple,
                                                           index_info->index_oid_, exec_ctx_->GetCatalog());
    txn_->AppendTableWriteRecord(index_write_record);
  }

  return true;
}

}  // namespace bustub
