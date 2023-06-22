//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      output_schema_(plan_->OutputSchema()),
      child_schema_(plan_->GetChildPlan()->OutputSchema()) {}

void LimitExecutor::Init() {
  idx_ = 0;
  size_ = plan_->GetLimit();

  Tuple tuple;
  RID rid;

  child_executor_->Init();
  for (size_t i = 0; i < plan_->GetOffset(); ++i) {
    child_executor_->Next(&tuple, &rid);
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (idx_ >= size_) {
    return false;
  }

  if (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values;
    for (auto &colmun : output_schema_->GetColumns()) {
      values.emplace_back(colmun.GetExpr()->Evaluate(tuple, child_schema_));
    }

    *tuple = Tuple(values, output_schema_);
    return true;
  }

  return false;
}

}  // namespace bustub
