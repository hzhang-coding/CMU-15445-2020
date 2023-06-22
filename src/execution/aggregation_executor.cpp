//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      output_schema_(plan_->OutputSchema()),
      having_(plan_->GetHaving()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;

  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    if (having_ == nullptr ||
        having_->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      std::vector<Value> values;
      uint32_t size = output_schema_->GetColumnCount();

      for (uint32_t i = 0; i < size; ++i) {
        values.emplace_back(output_schema_->GetColumn(i).GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_,
                                                                                      aht_iterator_.Val().aggregates_));
      }

      *tuple = Tuple(values, output_schema_);
      *rid = tuple->GetRid();
      ++aht_iterator_;

      return true;
    }

    ++aht_iterator_;
  }

  return false;
}

}  // namespace bustub
