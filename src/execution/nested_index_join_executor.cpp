//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      predicate_(plan_->Predicate()),
      output_schema_(plan_->OutputSchema()),
      outer_schema_(plan_->OuterTableSchema()),
      inner_schema_(plan_->InnerTableSchema()),
      txn_(exec_ctx_->GetTransaction()) {}

void NestIndexJoinExecutor::Init() {
  TableMetadata *table_matadata = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  TableHeap *table = table_matadata->table_.get();

  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), table_matadata->name_);
  auto index = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());

  Tuple tuple;
  for (auto it = index->GetBeginIterator(); !it.IsEnd(); ++it) {
    table->GetTuple((*it).second, &tuple, txn_);
    inner_tuples_.emplace_back(tuple);
  }

  inner_idx_ = 0;
  inner_size_ = inner_tuples_.size();

  child_executor_->Init();
  is_end_ = !child_executor_->Next(&outer_tuple_, &outer_rid_);
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (!is_end_) {
    for (; inner_idx_ < inner_size_; ++inner_idx_) {
      if (predicate_ == nullptr ||
          predicate_->EvaluateJoin(&outer_tuple_, outer_schema_, &inner_tuples_[inner_idx_], inner_schema_)
              .GetAs<bool>()) {
        std::vector<Value> values;
        for (auto &colmun : output_schema_->GetColumns()) {
          values.emplace_back(
              colmun.GetExpr()->EvaluateJoin(&outer_tuple_, outer_schema_, &inner_tuples_[inner_idx_], inner_schema_));
        }

        *tuple = Tuple(values, output_schema_);
        *rid = tuple->GetRid();

        ++inner_idx_;
        return true;
      }
    }

    inner_idx_ = 0;

    if (!child_executor_->Next(&outer_tuple_, &outer_rid_)) {
      is_end_ = true;
    }
  }

  return false;
}

}  // namespace bustub
