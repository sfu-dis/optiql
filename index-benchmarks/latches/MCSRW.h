#pragma once

#include <glog/logging.h>
#include <immintrin.h>
#include <stdint.h>

#include <atomic>
#include <cassert>

namespace mcsrw {

// Implementation follows https://www.cs.rochester.edu/research/synchronization/pseudocode/rw.html

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

#if defined(RWLOCK_WRITER_PREFERENCE)
#elif defined(RWLOCK_READER_PREFERENCE)
#elif defined(OPT_MCSRW_HYBRID_LOCK) || defined(MCSRW_LOCK_ONLY)
// No need to implement centralized rwlock
#else
static_assert(false, "Centralized rwlock has no implementation");
#endif

#ifdef OMCS_OFFSET
using offset_t = uint16_t;
#else
using offset_t = uint64_t;
#endif

class MCSRWLock;

enum class MCSRWRequestorClass : uint64_t { READING, WRITING };
enum class MCSRWSuccessorClass : uint32_t { NONE, READER, WRITER };

struct alignas(CACHELINE_SIZE * 2) MCSRWQNode {
 public:
  alignas(CACHELINE_SIZE) std::atomic<MCSRWQNode *> next_;
  alignas(CACHELINE_SIZE) std::atomic<MCSRWRequestorClass> class_;
  alignas(CACHELINE_SIZE * 2) union MCSRWQNodeState {
    std::atomic<uint64_t> state_;
    struct MCSRWQNodeStateBlockAndSuccessorClass {
      std::atomic<uint32_t> blocked_;
      std::atomic<MCSRWSuccessorClass> successor_class_;
    } record_;
  } state_;

  static_assert(sizeof(MCSRWQNodeState) == sizeof(uint64_t),
                "sizeof MCSRWQNodeState is not 8-byte");

  static inline uint64_t make_state(uint32_t blocked, MCSRWSuccessorClass successor_class) {
    // XXX(shiges): endianness
    uint64_t result = static_cast<uint64_t>(successor_class) << 32;
    result |= static_cast<uint64_t>(blocked);
    return result;
  }

#ifndef NDEBUG
  MCSRWLock *lock_;

  void set_lock(MCSRWLock *lock) { lock_ = lock; }
  void assert_lock(MCSRWLock *lock) { assert(lock_ == lock); }
#endif

  inline void set_next(MCSRWQNode *next) { next_.store(next, std::memory_order_seq_cst); }

  inline void set_class(MCSRWRequestorClass _class) {
    class_.store(_class, std::memory_order_seq_cst);
  }

  inline void set_state(uint32_t blocked, MCSRWSuccessorClass successor_class) {
    uint64_t state = MCSRWQNode::make_state(blocked, successor_class);
    state_.state_.store(state, std::memory_order_seq_cst);
  }

  inline void set_blocked(uint32_t blocked) {
    state_.record_.blocked_.store(blocked, std::memory_order_seq_cst);
  }

  inline void set_successor_class(MCSRWSuccessorClass successor_class) {
    state_.record_.successor_class_.store(successor_class, std::memory_order_seq_cst);
  }

  inline bool compare_and_set_state(uint64_t expected, uint64_t desired) {
    return state_.state_.compare_exchange_strong(expected, desired);
  }

  inline MCSRWQNode *const get_next() const { return next_.load(std::memory_order_acquire); }

  inline const MCSRWRequestorClass get_class() const {
    return class_.load(std::memory_order_acquire);
  }

  inline const uint32_t get_blocked() const {
    return state_.record_.blocked_.load(std::memory_order_acquire);
  }

  inline const MCSRWSuccessorClass get_successor_class() const {
    return state_.record_.successor_class_.load(std::memory_order_acquire);
  }
};

#ifdef OMCS_OFFSET
extern MCSRWQNode *base_qnode;
#endif

class MCSRWLock {
 public:
  static constexpr uint32_t kFree = 0;
  static constexpr uint32_t kWriterActiveFlag = 0x1;
  static constexpr uint32_t kReaderCountIncr = 0x10;
#ifdef OMCS_OFFSET
#ifdef BTREE_RWLOCK_MCSRW_ONLY
  static constexpr uint64_t kQueueNodeIdBits = 12;  // At most 4096 qnodes
#else
  static constexpr uint64_t kQueueNodeIdBits = 10;  // At most 1024 qnodes
#endif
  static constexpr uint64_t kNumQueueNodes = 1 << kQueueNodeIdBits;
  static_assert(kQueueNodeIdBits + 1 <= 16);
#endif  // OMCS_OFFSET

  static constexpr uint32_t kUnblocked = 0;
  static constexpr uint32_t kBlocked = 1;

#ifdef OMCS_OFFSET
  static constexpr offset_t kLockedBit = 1ull << 15;
  static constexpr offset_t kQNodeIdMask = 0x7fffu;
#endif  // OMCS_OFFSET

  static constexpr offset_t kNullPtr = 0;

  static inline bool is_null(offset_t offset) { return offset == kNullPtr; }

  static inline MCSRWQNode *get_qnode_ptr(offset_t offset) {
#ifdef OMCS_OFFSET
    assert(offset & kLockedBit);
    return base_qnode + static_cast<size_t>(offset & kQNodeIdMask);
#else
    return reinterpret_cast<MCSRWQNode *>(offset);
#endif  // OMCS_OFFSET
  }

  static inline offset_t make_qnode_ptr(MCSRWQNode *const ptr) {
#ifdef OMCS_OFFSET
    return (static_cast<offset_t>(ptr - base_qnode)) | kLockedBit;
#else
    return reinterpret_cast<offset_t>(ptr);
#endif  // OMCS_OFFSET
  }

 private:
#ifdef OMCS_OFFSET
  std::atomic<uint16_t> u16a_{0};
  std::atomic<uint16_t> u16b_{0};
  std::atomic<uint32_t> u32_{0};

#define tail_ u16a_
#define next_writer_ u16b_
#define reader_count_ u32_

#define write_requests_ u16a_
#define write_completions_ u16b_
#define reader_count_and_flag_ u32_
#else
  std::atomic<uint64_t> u64a_{0};
  std::atomic<uint64_t> u64b_{0};
  std::atomic<uint32_t> u32_{0};

#define tail_ u64a_
#define next_writer_ u64b_
#define reader_count_ u32_

#define write_requests_ u64a_
#define write_completions_ u64b_
#define reader_count_and_flag_ u32_
#endif

 public:
  MCSRWLock() = default;

  MCSRWLock(const MCSRWLock &) = delete;
  MCSRWLock &operator=(const MCSRWLock &) = delete;

  inline void lock() {
#if defined(RWLOCK_WRITER_PREFERENCE)
    auto prev_writer = write_requests_.fetch_add(1);
    while (write_completions_.load(std::memory_order_acquire) != prev_writer) {
    }
    while (true) {
      auto expected = kFree;
      if (reader_count_and_flag_.compare_exchange_strong(expected, kWriterActiveFlag)) {
        return;
      }
    }
#elif defined(RWLOCK_READER_PREFERENCE)
    while (true) {
      auto expected = kFree;
      if (reader_count_and_flag_.compare_exchange_strong(expected, kWriterActiveFlag)) {
        return;
      }
    }
#else
    LOG(FATAL) << "Not implemented";
#endif
  }

  inline void unlock() {
#if defined(RWLOCK_WRITER_PREFERENCE)
    reader_count_and_flag_.fetch_sub(kWriterActiveFlag);
    write_completions_.fetch_add(1);
#elif defined(RWLOCK_READER_PREFERENCE)
    reader_count_and_flag_.fetch_sub(kWriterActiveFlag);
#else
    LOG(FATAL) << "Not implemented";
#endif
  }

  inline void read_lock() {
#if defined(RWLOCK_WRITER_PREFERENCE)
    while (true) {
      auto requested_writer = write_requests_.load(std::memory_order_acquire);
      auto finished_writer = write_completions_.load(std::memory_order_acquire);
      if (requested_writer == finished_writer) {
        break;
      }
    }
    reader_count_and_flag_.fetch_add(kReaderCountIncr);
    while (reader_count_and_flag_.load(std::memory_order_acquire) & kWriterActiveFlag) {
    }
#elif defined(RWLOCK_READER_PREFERENCE)
    reader_count_and_flag_.fetch_add(kReaderCountIncr);
    while (reader_count_and_flag_.load(std::memory_order_acquire) & kWriterActiveFlag) {
    }
#else
    LOG(FATAL) << "Not implemented";
#endif
  }

  inline void read_unlock() {
#if defined(RWLOCK_WRITER_PREFERENCE)
    reader_count_and_flag_.fetch_sub(kReaderCountIncr);
#elif defined(RWLOCK_READER_PREFERENCE)
    reader_count_and_flag_.fetch_sub(kReaderCountIncr);
#else
    LOG(FATAL) << "Not implemented";
#endif
  }

#if defined(MCSRW_LOCK_CENTRALIZED)
  inline void lock(MCSRWQNode *qnode) { return lock(); }
  inline void unlock(MCSRWQNode *qnode) { return unlock(); }
  inline void read_lock(MCSRWQNode *qnode) { return read_lock(); }
  inline void read_unlock(MCSRWQNode *qnode) { return read_unlock(); }

#else
  inline void lock(MCSRWQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_lock(this);
#endif

    qnode->set_class(MCSRWRequestorClass::WRITING);
    qnode->set_state(kBlocked, MCSRWSuccessorClass::NONE);
    qnode->set_next(nullptr);
    offset_t self = MCSRWLock::make_qnode_ptr(qnode);
    offset_t prev = tail_.exchange(self);

    if (MCSRWLock::is_null(prev)) {
      next_writer_.exchange(self);
      if (reader_count_.load(std::memory_order_acquire) == 0) {
        if (next_writer_.exchange(kNullPtr) == self) {
          qnode->set_blocked(kUnblocked);
          return;
        }
      }
    } else {
      MCSRWQNode *pred = MCSRWLock::get_qnode_ptr(prev);
      pred->set_successor_class(MCSRWSuccessorClass::WRITER);
      std::atomic_thread_fence(std::memory_order_seq_cst);
      pred->set_next(qnode);
    }

    while (qnode->get_blocked() == kBlocked) {
    }
  }

  inline void unlock(MCSRWQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->assert_lock(this);
#endif

    MCSRWQNode *succ = qnode->get_next();
    if (succ == nullptr) {
      offset_t self = MCSRWLock::make_qnode_ptr(qnode);
      if (tail_.compare_exchange_strong(self, kNullPtr)) {
        return;
      }

      do {
        succ = qnode->get_next();
      } while (succ == nullptr);
    }

    assert(succ != nullptr);

    if (succ->get_class() == MCSRWRequestorClass::READING) {
      reader_count_.fetch_add(1);
    }
    succ->set_blocked(kUnblocked);
  }

  inline void read_lock(MCSRWQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_lock(this);
#endif

    qnode->set_class(MCSRWRequestorClass::READING);
    qnode->set_state(kBlocked, MCSRWSuccessorClass::NONE);
    qnode->set_next(nullptr);
    offset_t self = MCSRWLock::make_qnode_ptr(qnode);
    offset_t prev = tail_.exchange(self);

    if (prev == kNullPtr) {
      reader_count_.fetch_add(1);
      qnode->set_blocked(kUnblocked);
    } else {
      MCSRWQNode *pred = MCSRWLock::get_qnode_ptr(prev);
      if (pred->get_class() == MCSRWRequestorClass::WRITING ||
          pred->compare_and_set_state(
              MCSRWQNode::make_state(kBlocked, MCSRWSuccessorClass::NONE),
              MCSRWQNode::make_state(kBlocked, MCSRWSuccessorClass::READER))) {
        pred->set_next(qnode);
        while (qnode->get_blocked() == kBlocked) {
        }
      } else {
        reader_count_.fetch_add(1);
        pred->set_next(qnode);
        qnode->set_blocked(kUnblocked);
      }
    }

    if (qnode->get_successor_class() == MCSRWSuccessorClass::READER) {
      MCSRWQNode *succ = qnode->get_next();
      if (succ == nullptr) {
        do {
          succ = qnode->get_next();
        } while (succ == nullptr);
      }

      reader_count_.fetch_add(1);
      succ->set_blocked(kUnblocked);
    }
  }

  inline void read_unlock(MCSRWQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->assert_lock(this);
#endif

    MCSRWQNode *succ = qnode->get_next();
    if (succ == nullptr) {
      offset_t self = MCSRWLock::make_qnode_ptr(qnode);
      if (tail_.compare_exchange_strong(self, kNullPtr)) {
        goto reader_exit;
      }

      do {
        succ = qnode->get_next();
      } while (succ == nullptr);
    }

    assert(succ != nullptr);

    if (qnode->get_successor_class() == MCSRWSuccessorClass::WRITER) {
      offset_t next = MCSRWLock::make_qnode_ptr(succ);
      next_writer_.exchange(next);
    }

  reader_exit:
    offset_t w = kNullPtr;
    if (reader_count_.fetch_sub(1) == 1 &&
        (w = next_writer_.load(std::memory_order_acquire)) != kNullPtr &&
        reader_count_.load(std::memory_order_acquire) == 0 &&
        next_writer_.compare_exchange_strong(w, kNullPtr)) {
      MCSRWQNode *next_writer = MCSRWLock::get_qnode_ptr(w);
      next_writer->set_blocked(kUnblocked);
    }
  }
#endif

#undef tail_
#undef next_writer_
#undef reader_count_

#undef write_requests_
#undef write_completions_
#undef reader_count_and_flag_
};

}  // namespace mcsrw
