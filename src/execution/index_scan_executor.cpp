//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      predicate_(plan_->GetPredicate()),
      output_schema_(plan_->OutputSchema()),
      txn_(exec_ctx_->GetTransaction()) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto index = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());
  it_ = index->GetBeginIterator();

  TableMetadata *table_matadata = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  schema_ = &(table_matadata->schema_);
  table_ = table_matadata->table_.get();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (!it_.IsEnd()) {
    *rid = (*it_).second;
    table_->GetTuple(*rid, tuple, txn_);

    if (predicate_ == nullptr || predicate_->Evaluate(tuple, schema_).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &colmun : output_schema_->GetColumns()) {
        values.emplace_back(colmun.GetExpr()->Evaluate(tuple, schema_));
      }

      *tuple = Tuple(values, output_schema_);
      ++it_;

      return true;
    }

    ++it_;
  }

  return false;
}

}  // namespace bustub
