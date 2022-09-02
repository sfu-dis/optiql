#pragma once

#include <glog/logging.h>
#include <immintrin.h>
#include <stdint.h>

#include <atomic>
#include <cassert>

namespace mcs {

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

class MCSLock;

struct alignas(CACHELINE_SIZE * 2) MCSQNode {
 public:
  alignas(CACHELINE_SIZE * 2) std::atomic<MCSQNode *> next_;
  alignas(CACHELINE_SIZE * 2) std::atomic<uint64_t> granted_;

#ifndef NDEBUG
  MCSLock *lock_;

  void set_omcs(MCSLock *lock) { lock_ = lock; }
  void assert_omcs(MCSLock *lock) { assert(lock_ == lock); }
#endif

  inline void set_next(MCSQNode *next) { next_.store(next, std::memory_order_release); }

  inline void set_granted(uint64_t granted) { granted_.store(granted, std::memory_order_release); }

  inline MCSQNode *const get_next() const { return next_.load(std::memory_order_acquire); }

  inline const uint64_t get_granted() const { return granted_.load(std::memory_order_acquire); }
};

class MCSLock {
 public:
  static constexpr uint64_t kNullPtr = 0;
  static constexpr uint64_t kNotGranted = 0;
  static constexpr uint64_t kGranted = 1;

  static inline MCSQNode *get_qnode_ptr(uint64_t version) {
    return reinterpret_cast<MCSQNode *>(version);
  }

  static inline uint64_t make_qnode_ptr(MCSQNode *const ptr) {
    return reinterpret_cast<uint64_t>(ptr);
  }

 private:
  std::atomic<uint64_t> tail_{0};

 public:
  MCSLock() = default;

  MCSLock(const MCSLock &) = delete;
  MCSLock &operator=(const MCSLock &) = delete;

  inline void lock(MCSQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_omcs(this);
#endif

    qnode->set_next(nullptr);
    qnode->set_granted(kNotGranted);
    uint64_t self = MCSLock::make_qnode_ptr(qnode);
    uint64_t prev = tail_.exchange(self);

    if (prev == MCSLock::kNullPtr) {
      return;
    }

    MCSQNode *pred = MCSLock::get_qnode_ptr(prev);
    pred->set_next(qnode);

    while (qnode->get_granted() == MCSLock::kNotGranted) {
    }
  }

  inline bool try_lock(MCSQNode *qnode) {
    uint64_t curr = tail_.load(std::memory_order_acquire);
    if (curr != MCSLock::kNullPtr) {
      return false;
    }

    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_omcs(this);
#endif

    qnode->set_next(nullptr);
    qnode->set_granted(kNotGranted);

    assert(curr == MCSLock::kNullPtr);
    uint64_t self = MCSLock::make_qnode_ptr(qnode);

    return tail_.compare_exchange_strong(curr, self);
  }

  void unlock(MCSQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->assert_omcs(this);
#endif

    MCSQNode *succ = qnode->get_next();
    if (succ == nullptr) {
      uint64_t self = MCSLock::make_qnode_ptr(qnode);
      if (tail_.compare_exchange_strong(self, kNullPtr)) {
        return;
      }
      do {
        succ = qnode->get_next();
      } while (succ == nullptr);
    }

    assert(succ != nullptr);
    succ->set_granted(kGranted);
  }
};

}  // namespace mcs
