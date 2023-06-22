//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      predicate_(plan_->Predicate()),
      output_schema_(plan_->OutputSchema()),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.emplace_back(tuple);
  }

  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }

  left_size_ = left_tuples_.size();
  right_size_ = right_tuples_.size();
  left_idx_ = 0;
  right_idx_ = 0;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  for (; left_idx_ < left_size_; ++left_idx_) {
    for (; right_idx_ < right_size_; ++right_idx_) {
      if (predicate_ == nullptr ||
          predicate_->EvaluateJoin(&left_tuples_[left_idx_], left_schema_, &right_tuples_[right_idx_], right_schema_)
              .GetAs<bool>()) {
        std::vector<Value> values;
        for (auto &colmun : output_schema_->GetColumns()) {
          values.emplace_back(colmun.GetExpr()->EvaluateJoin(&left_tuples_[left_idx_], left_schema_,
                                                             &right_tuples_[right_idx_], right_schema_));
        }

        *tuple = Tuple(values, output_schema_);
        *rid = tuple->GetRid();

        ++right_idx_;
        return true;
      }
    }

    right_idx_ = 0;
  }

  return false;
}

}  // namespace bustub
