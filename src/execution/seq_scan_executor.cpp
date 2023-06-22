//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      predicate_(plan_->GetPredicate()),
      output_schema_(plan_->OutputSchema()),
      lock_mgr_(exec_ctx->GetLockManager()),
      txn_(exec_ctx->GetTransaction()),
      it_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
  TableMetadata *table_matadata = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_ = table_matadata->table_.get();
  it_ = table_->Begin(exec_ctx_->GetTransaction());
  schema_ = &table_matadata->schema_;
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (it_ != table_->End()) {
    if (lock_mgr_ != nullptr && txn_ != nullptr) {
      bool flag = false;
      switch (txn_->GetIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
          *tuple = *it_;
          *rid = (*it_).GetRid();
          break;
        case IsolationLevel::READ_COMMITTED: {
          if (!txn_->IsExclusiveLocked((*it_).GetRid())) {
            lock_mgr_->LockShared(txn_, (*it_).GetRid());
            flag = true;
          }

          *tuple = *it_;
          *rid = (*it_).GetRid();

          if (flag) {
            lock_mgr_->Unlock(txn_, *rid);
          }
        }

        break;
        case IsolationLevel::REPEATABLE_READ:
          if (!txn_->IsSharedLocked((*it_).GetRid()) && !txn_->IsExclusiveLocked((*it_).GetRid())) {
            lock_mgr_->LockShared(txn_, (*it_).GetRid());
          }

          *tuple = *it_;
          *rid = (*it_).GetRid();

          break;
      }
    } else {
      *tuple = *it_;
      *rid = (*it_).GetRid();
    }

    *rid = (*it_).GetRid();
    ++it_;

    if (predicate_ == nullptr || predicate_->Evaluate(tuple, schema_).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &colmun : output_schema_->GetColumns()) {
        values.emplace_back(colmun.GetExpr()->Evaluate(tuple, schema_));
      }

      *tuple = Tuple(values, output_schema_);

      return true;
    }
  }

  return false;
}

}  // namespace bustub
