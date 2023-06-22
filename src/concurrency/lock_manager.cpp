//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  latch_.lock();

  lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());

  auto &request_queue = lock_table_[rid].request_queue_;
  auto txn_id = txn->GetTransactionId();

  bool check = true;
  for (auto &request : request_queue) {
    if (request.lock_mode_ == LockMode::EXCLUSIVE) {
      check = false;
      break;
    }
  }

  if (check) {
    request_queue.emplace_back(LockRequest(txn_id, LockMode::SHARED, true));
    latch_.unlock();
  } else {
    request_queue.emplace_back(LockRequest(txn_id, LockMode::SHARED, false));
    latch_.unlock();

    auto &cv = lock_table_[rid].cv_;
    std::unique_lock<std::mutex> lk(latch_);

    while (true) {
      cv.wait(lk);

      if (txn->GetState() == TransactionState::ABORTED) {
        auto it = request_queue.begin();
        while (it->txn_id_ != txn_id) {
          ++it;
        }
        request_queue.erase(it);
        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
        return false;
      }

      bool check = true;
      auto it = request_queue.begin();
      while (it->txn_id_ != txn_id) {
        if (it->lock_mode_ == LockMode::EXCLUSIVE) {
          check = false;
          break;
        }

        ++it;
      }

      if (check) {
        it->granted_ = true;
        break;
      }
    }
  }

  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  latch_.lock();

  lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());

  auto &request_queue = lock_table_[rid].request_queue_;
  auto txn_id = txn->GetTransactionId();

  if (request_queue.empty()) {
    request_queue.emplace_back(LockRequest(txn_id, LockMode::EXCLUSIVE, true));
    latch_.unlock();
  } else {
    request_queue.emplace_back(LockRequest(txn_id, LockMode::EXCLUSIVE, false));
    latch_.unlock();

    auto &cv = lock_table_[rid].cv_;
    std::unique_lock<std::mutex> lk(latch_);

    while (true) {
      cv.wait(lk);

      if (txn->GetState() == TransactionState::ABORTED) {
        auto it = request_queue.begin();
        while (it->txn_id_ != txn_id) {
          ++it;
        }
        request_queue.erase(it);
        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
        return false;
      }
      /*
auto it = request_queue.begin();
// std::cout << std::to_string(txn_id) + "txn's size = " + std::to_string(request_queue.size()) << std::endl;

while (TransactionManager::GetTransaction(it->txn_id_)->GetState() == TransactionState::ABORTED) {
++it;
}
*/
      if (request_queue.front().txn_id_ == txn_id) {
        request_queue.front().granted_ = true;
        break;
      }
    }
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  latch_.lock();

  if (lock_table_[rid].upgrading_) {
    latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  lock_table_[rid].upgrading_ = true;

  auto &request_queue = lock_table_[rid].request_queue_;
  auto txn_id = txn->GetTransactionId();

  bool check = true;
  if (request_queue.front().txn_id_ != txn_id) {
    check = false;
  } else {
    auto it = request_queue.begin();
    ++it;
    if (it != request_queue.end() && it->granted_) {
      check = false;
    }
  }

  if (check) {
    request_queue.front().lock_mode_ = LockMode::EXCLUSIVE;
    latch_.unlock();
  } else {
    auto it = request_queue.begin();
    while (it->txn_id_ != txn_id) {
      ++it;
    }

    it->lock_mode_ = LockMode::EXCLUSIVE;
    latch_.unlock();

    auto &cv = lock_table_[rid].cv_;
    std::unique_lock<std::mutex> lk(latch_);

    while (true) {
      cv.wait(lk);

      if (txn->GetState() == TransactionState::ABORTED) {
        auto it = request_queue.begin();
        while (it->txn_id_ != txn_id) {
          ++it;
        }
        request_queue.erase(it);

        lock_table_[rid].upgrading_ = false;
        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
        return false;
      }

      if (request_queue.front().txn_id_ == txn_id) {
        lock_table_[rid].upgrading_ = false;
        break;
      }
    }
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  latch_.lock();

  if (lock_table_.find(rid) == lock_table_.end()) {
    latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto &request_queue = lock_table_[rid].request_queue_;
  auto txn_id = txn->GetTransactionId();

  auto it = request_queue.begin();
  while (it != request_queue.end() && it->txn_id_ != txn_id) {
    ++it;
  }

  if (it == request_queue.end()) {
    latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  request_queue.erase(it);

  if (request_queue.empty()) {
    lock_table_.erase(rid);
  } else {
    if (!request_queue.front().granted_) {
      lock_table_[rid].cv_.notify_all();
    } else if (lock_table_[rid].upgrading_ && request_queue.front().lock_mode_ == LockMode::EXCLUSIVE) {
      auto it = request_queue.begin();
      ++it;
      if (it == request_queue.end() || !it->granted_) {
        lock_table_[rid].cv_.notify_all();
      }
    }
    //  lock_table_[rid].cv_.notify_all();
  }

  latch_.unlock();

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (it->lock_mode_ == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1] = {t2};
  } else {
    for (auto &t : waits_for_[t1]) {
      if (t == t2) {
        return;
      }
    }

    waits_for_[t1].emplace_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) != waits_for_.end()) {
    for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); ++it) {
      if (*it == t2) {
        waits_for_[t1].erase(it);
        break;
      }
    }

    if (waits_for_[t1].empty()) {
      waits_for_.erase(t1);
    }
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  if (waits_for_.empty()) {
    return false;
  }

  std::priority_queue<txn_id_t, std::vector<txn_id_t>, std::greater<>> nodes;
  for (auto &pair : waits_for_) {
    nodes.push(pair.first);
  }

  std::unordered_set<txn_id_t> explored;

  while (!nodes.empty()) {
    if (explored.find(nodes.top()) != explored.end()) {
      nodes.pop();
      continue;
    }

    std::unordered_set<txn_id_t> seen;
    seen.insert(nodes.top());

    if (Dfs(nodes.top(), txn_id, &seen)) {
      return true;
    }

    for (auto &t : seen) {
      explored.insert(t);
    }

    nodes.pop();
  }

  return false;
}

bool LockManager::Dfs(txn_id_t curr, txn_id_t *txn_id, std::unordered_set<txn_id_t> *seen) {
  for (auto &next : waits_for_[curr]) {
    if (seen->find(next) != seen->end()) {
      *txn_id = std::max(curr, next);
      return true;
    }

    seen->insert(next);

    if (waits_for_.find(next) != waits_for_.end() && Dfs(next, txn_id, seen)) {
      *txn_id = std::max(*txn_id, curr);
      return true;
    }
  }

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &&[t1, vec] : waits_for_) {
    for (auto &t2 : vec) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      waits_for_.clear();

      std::unordered_map<txn_id_t, RID> mp;

      for (auto &&[rid, lock_resquest_queue] : lock_table_) {
        std::vector<txn_id_t> running;
        std::vector<txn_id_t> waiting;

        for (auto &request : lock_resquest_queue.request_queue_) {
          txn_id_t txn_id = request.txn_id_;

          if (TransactionManager::GetTransaction(txn_id)->GetState() != TransactionState::ABORTED) {
            if (request.granted_) {
              running.emplace_back(txn_id);
            } else {
              waiting.emplace_back(txn_id);
              mp[txn_id] = rid;
            }
          }
        }

        size_t m = running.size();
        size_t n = waiting.size();

        for (size_t i = 0; i < m; ++i) {
          for (size_t j = 0; j < n; ++j) {
            AddEdge(running[i], waiting[j]);
          }
        }

        if (lock_resquest_queue.upgrading_) {
          auto it = lock_resquest_queue.request_queue_.begin();
          while (it != lock_resquest_queue.request_queue_.end() && it->lock_mode_ != LockMode::EXCLUSIVE) {
            ++it;
          }

          mp[it->txn_id_] = rid;

          for (size_t i = 0; i < m; ++i) {
            if (running[i] != it->txn_id_) {
              AddEdge(running[i], it->txn_id_);
            }
          }
        }
      }

      std::cout << "11111111" << std::endl;
      txn_id_t victim;
      while (HasCycle(&victim)) {
        std::cout << "22222222 " + std::to_string(victim) << std::endl;
        TransactionManager::GetTransaction(victim)->SetState(TransactionState::ABORTED);
        if (mp.find(victim) != mp.end()) {
          lock_table_[mp[victim]].cv_.notify_all();
        }
        std::vector<txn_id_t> to_delete;
        for (auto &&[t, vec] : waits_for_) {
          auto it = vec.begin();
          while (it != vec.end() && *it != victim) {
            ++it;
          }

          if (it != vec.end()) {
            vec.erase(it);
          }

          if (vec.empty()) {
            to_delete.emplace_back(t);
          }
        }

        for (auto &t : to_delete) {
          waits_for_.erase(t);
        }

        if (waits_for_.find(victim) != waits_for_.end()) {
          waits_for_.erase(victim);
        }
      }
      continue;
    }
  }
}

}  // namespace bustub
