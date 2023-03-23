#pragma once

#include <glog/logging.h>
#include <immintrin.h>
#include <stdint.h>

#include <atomic>
#include <cassert>
#include <type_traits>

namespace omcs_impl {

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

class OMCSLock;

struct alignas(CACHELINE_SIZE * 2) OMCSQNode {
 public:
  alignas(CACHELINE_SIZE * 2) std::atomic<OMCSQNode *> next_;
  alignas(CACHELINE_SIZE * 2) std::atomic<uint64_t> version_;

#ifndef NDEBUG
  OMCSLock *lock_;

  void set_omcs(OMCSLock *lock) { lock_ = lock; }
  void assert_omcs(OMCSLock *lock) { assert(lock_ == lock); }
#endif

  inline void set_next(OMCSQNode *next) { next_.store(next, std::memory_order_release); }

  inline void set_version(uint64_t version) { version_.store(version, std::memory_order_release); }

  inline OMCSQNode *const get_next() const { return next_.load(std::memory_order_acquire); }

  inline const uint64_t get_version() const { return version_.load(std::memory_order_acquire); }
};

#ifdef OMCS_OFFSET
extern OMCSQNode *base_qnode;
#endif

/*
 * The OMCSLock class implements two different OMCS Locks: one that uses
 * virtual memory pointers to queue nodes, and one that uses queue node IDs.
 * The latter is turned on by the OMCS_OFFSET macro; its purpose is to allow
 * encoding more information on the lock word, to support correct opportunistic
 * read that addresses its ABA problem.
 *
 * Conceptually, for the version that uses queue node IDs, the lock word
 * carries the following information:
 * - one bit for the mode (i.e., whether the lock is carrying an queue node ID
 *   or a version number);
 * - one bit for the opportunistic read feature
 * - n bits for encoding the queue node id
 * - remaining 62-n bits for encoding the version number
 *
 * Here, [n] is determined by the maximum number of queue nodes we'd like to
 * support in the system.  This can be passed in at runtime as a parameter to
 * the ctor.
 *
 * For simplicity, we fix it to be 1024 (so n = 10), as (1) usually a thread
 * only holds one lock, allowing us to support up to 1024 threads which is
 * fairly large in our target applications (DBMSs) that don't oversubscript the
 * system, and (2) our target applications usually don't take many locks at the
 * same time, e.g., lock coupling, in the worst case it's bounded by the tree
 * height which is typically not higher than a few levels. In any case, [n] is
 * adjustable either at runtime or compile-time.
 *
 * Systems like FOEDUS already use this approach, although for a different
 * reason (to share queue nodes between processes, rather than making room for
 * a generation number to solve ABA problems under opportunistic read).
 *
 * With n=10, we have bits 0-9 as the queue node id, and bits 10-61 as the
 * opread sequence number, which essentially is the "version number" that was
 * maintained by the lock word and in the queue node. We have 61-10+1=52 bits
 * for this, so we are essentially using 52 bits for version numbers, inlcuding
 * in the queue nodes. This gives 2^52 - 1 versions before wrapping off, which
 * is practically good enough: assuming 100M ops per second, this will give us
 * 2^52 / 100000000 ~= 45035996 seconds ~= 12509 hours = 521 days of up time
 * before wrapping up, which would require a quiescent point for all threads.
 *
 * For easier implementation, we encode the version number in LSBs because the
 * version number will never grow beyond 52 bits anyway:
 * |-63-|--62--|---------61-52--------|-----51-0-----|
 * |mode|opread|--tail queue node id--|version number|
 *
 * To be NUMA-aware, the queue node should be allocated locally. There are two
 * approaches. The first one is to further partition the queue node id to
 * consist of a NUMA node id and a local node id. For now to fit our machine
 * (40-core, dual socket), we just use 3 bits for NUMA node (allowing 8
 * sockets) and 7 bits for threads per socket (allowing 2^7 = 128 HTs per NUMA
 * node).
 *
 * If needed, to support larger scales (more sockets and cores) by reducing the
 * number of bits dedicated to version number. For example, if we use 51 bits
 * for version number, we are still left with ~260 days before wrap up under
 * the very high 100M/s throughput, to expand the number of queue nodes to be
 * 2x (double the number of sockets or cores per socket, for example, to become
 * 256-core per socket). So far the biggest scale for x86 DB servers on the
 * market is perhaps below 32 sockets, with up to 128 cores per socket. This
 * would need 5 bits for socket id, and 7 bits for core id, totaling 12 bits,
 * leaving 64-14=50 bits for version number ==> around 130 days before wrap
 * around.
 *
 * The downside of this is we may run out of bits if scale really gets large.
 * Another approach is to fix the total number of queue nodes, say 1024, and
 * then allocate pages of locks in an interleaved manner across all the NUMA
 * nodes. This can be done by numa_alloc_interleaved (which only works on page
 * level, so we have to allocate N * pages for an N-socket system). Then each
 * thread only gets queue nodes from queue node pages that are local to it.
 * This is the current implementation when OMCS_OFFSET_NUMA_QNODE is defined.
 */

class OMCSLock {
 public:
#ifdef OMCS_OFFSET
  static constexpr uint64_t kQueueNodeIdBits = 10;  // At most 1024 qnodes
  static constexpr uint64_t kNumQueueNodes = 1 << kQueueNodeIdBits;
  static constexpr uint64_t kVersionBits = 52;
  static_assert(kQueueNodeIdBits + kVersionBits + 2 <= 64);
#endif

  static constexpr uint64_t kLockedBit = 1ull << 63;
#ifdef OMCS_OP_READ
  static constexpr uint64_t kConsistentBit = 1ull << 62;
#endif
  static constexpr uint64_t kInvalidVersion = 0;
  static constexpr uint64_t kVersionStride = 1;
  static constexpr uint64_t kNextUnlockedVersion = kVersionStride - kLockedBit;

#ifdef OMCS_OFFSET
  static constexpr uint64_t kVersionMask = 0xfffffffffffff;
#ifdef OMCS_OP_READ
  static constexpr uint64_t kQNodeIdMask = ~(kVersionMask | kLockedBit | kConsistentBit);
#else
  static constexpr uint64_t kQNodeIdMask = ~(kVersionMask | kLockedBit);
#endif  // OMCS_OP_READ
#endif  // OMCS_OFFSET

  static constexpr int kMaxHTMRetries = 1024;

  template <typename T>
  static inline bool has_locked_bit(const T val) {
    return (reinterpret_cast<uint64_t>(val) & kLockedBit);
  }

  static inline bool is_version(const uint64_t ptr) { return !has_locked_bit(ptr); }

  static inline OMCSQNode *get_qnode_ptr(uint64_t version) {
    assert(has_locked_bit(version));
#ifdef OMCS_OFFSET
    // [version] is a snapshot of the lock word
    return base_qnode + ((version & kQNodeIdMask) >> kVersionBits);
#else
#ifdef OMCS_OP_READ
    return reinterpret_cast<OMCSQNode *>(version & (~(kLockedBit | kConsistentBit)));
#else
    return reinterpret_cast<OMCSQNode *>(version & ~kLockedBit);
#endif  // OMCS_OP_READ
#endif  // OMCS_OFFSET
  }

  static inline uint64_t make_qnode_ptr(OMCSQNode *const ptr) {
    assert(!has_locked_bit(ptr));
#ifdef OMCS_OFFSET
    return (static_cast<uint64_t>(ptr - base_qnode) << kVersionBits) | kLockedBit;
#else
    return reinterpret_cast<uint64_t>(ptr) | kLockedBit;
#endif
  }

  static inline uint64_t make_locked_version(const uint64_t v) {
    assert(!has_locked_bit(v));
    return v | kLockedBit;
  }

 private:
  std::atomic<uint64_t> tail_{0};

 public:
  OMCSLock() = default;

  OMCSLock(const OMCSLock &) = delete;
  OMCSLock &operator=(const OMCSLock &) = delete;

  uint64_t lock() {
    int cas_failure = 0;
    while (true) {
      bool restart = false;
      uint64_t version = try_begin_read(restart);
      if (restart) {
        continue;
      }

      uint64_t curr = version;
      uint64_t locked = OMCSLock::make_locked_version(version);
      if (!tail_.compare_exchange_strong(curr, locked)) {
        // XXX(shiges): This seem to hinder performance; maybe tune params?
        // cas_failure++;
        continue;
      }
      // lock acquired
      return version;
    }
  }

  bool try_lock(uint64_t version) {
    if (!validate_read(version)) {
      return false;
    }

    uint64_t curr = version;
    uint64_t locked = OMCSLock::make_locked_version(version);
    return tail_.compare_exchange_strong(curr, locked);
  }

  void unlock() {
    assert(OMCSLock::has_locked_bit(tail_.load()));
#if defined(NO_FAA)
    uint64_t v = tail_.load(std::memory_order_acquire);
    tail_.store(v + kNextUnlockedVersion, std::memory_order_release);
#else
    tail_.fetch_add(kNextUnlockedVersion);
#endif
  }

  void unlock(uint64_t v) {
#if defined(ST_ONLY)
    assert(!(OMCSLock::has_locked_bit(v)));
    tail_.store(v + kVersionStride, std::memory_order_release);
#else
    (void)v;
    unlock();
#endif
  }

  inline bool lock_begin(OMCSQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_omcs(this);
#endif

    qnode->set_next(nullptr);
    qnode->set_version(OMCSLock::kInvalidVersion);
    uint64_t self = OMCSLock::make_qnode_ptr(qnode);
    uint64_t version = tail_.exchange(self);

    if (OMCSLock::is_version(version)) {
#ifdef OMCS_OFFSET
      assert((version & kQNodeIdMask) == 0);
      // Doesn't matter if opread is enabled, we need to apply the mask
      uint64_t v0 = version & kVersionMask;
#else
#ifdef OMCS_OP_READ
      uint64_t v0 = version & ~kConsistentBit;
#else
      uint64_t v0 = version;
#endif  // OMCS_OP_READ
#endif  // OMCS_OFFSET
      uint64_t v1 = v0 + kVersionStride;
      qnode->set_version(v1);
      return false;
    }

    OMCSQNode *pred = OMCSLock::get_qnode_ptr(version);
    pred->set_next(qnode);

    while (qnode->get_version() == OMCSLock::kInvalidVersion) {
    }
    return true;
  }

  inline void lock_turn_off_opread() {
#ifdef OMCS_OP_READ
#ifdef OMCS_OFFSET
    // Remove the consistent bit + version (actually not necessary to remove version, but just to be clear)
    tail_.fetch_and(~(kConsistentBit | kVersionMask));
#else
    tail_.fetch_and(~kConsistentBit);
#endif
#endif
  }

  inline void lock(OMCSQNode *qnode) { lock(qnode, nullptr); }

  template <class Callback>
  inline void lock(OMCSQNode *qnode, Callback &&cb) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_omcs(this);
#endif

    qnode->set_next(nullptr);
    qnode->set_version(OMCSLock::kInvalidVersion);
    uint64_t self = OMCSLock::make_qnode_ptr(qnode);
    uint64_t version = tail_.exchange(self);

    if (OMCSLock::is_version(version)) {
#ifdef OMCS_OFFSET
      assert((version & kQNodeIdMask) == 0);
      // Doesn't matter if opread is enabled, we need to apply the mask
      uint64_t v0 = version & kVersionMask;
#else
#ifdef OMCS_OP_READ
      uint64_t v0 = version & ~kConsistentBit;
#else
      uint64_t v0 = version;
#endif  // OMCS_OP_READ
#endif  // OMCS_OFFSET
      uint64_t v1 = v0 + kVersionStride;
      qnode->set_version(v1);

      if constexpr (std::is_invocable_v<Callback>) {
        cb();
      }
      return;
    }

    OMCSQNode *pred = OMCSLock::get_qnode_ptr(version);
    pred->set_next(qnode);

    while (qnode->get_version() == OMCSLock::kInvalidVersion) {
    }

#if !defined(OMCS_OP_READ_NEW_API_CALLBACK_BASELINE)
    if constexpr (std::is_invocable_v<Callback>) {
      cb();
    }
#endif
#ifdef OMCS_OP_READ
#ifdef OMCS_OFFSET
    // Remove the consistent bit + version (actually not necessary to remove version, but just to be clear)
    tail_.fetch_and(~(kConsistentBit | kVersionMask));
#else
    tail_.fetch_and(~kConsistentBit);
#endif
#endif
#if defined(OMCS_OP_READ_NEW_API_CALLBACK_BASELINE)
    if constexpr (std::is_invocable_v<Callback>) {
      cb();
    }
#endif
  }

  inline bool try_lock(OMCSQNode *qnode, uint64_t version) {
    if (!validate_read(version)) {
      return false;
    }

    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->set_omcs(this);
#endif

    uint64_t v0 = version;
    uint64_t v1 = v0 + kVersionStride;

    qnode->set_next(nullptr);
    qnode->set_version(v1);

    uint64_t curr = version;
    uint64_t self = OMCSLock::make_qnode_ptr(qnode);

    return tail_.compare_exchange_strong(curr, self);
  }

  void unlock(OMCSQNode *qnode) {
    assert(qnode != nullptr);

#ifndef NDEBUG
    qnode->assert_omcs(this);
#endif

    OMCSQNode *succ = qnode->get_next();
    if (succ == nullptr) {
      uint64_t v0 = qnode->get_version();
      assert(!OMCSLock::has_locked_bit(v0));
      uint64_t self = OMCSLock::make_qnode_ptr(qnode);
      if (tail_.compare_exchange_strong(self, v0)) {
        return;
      }

#ifdef OMCS_OP_READ
#ifdef OMCS_OFFSET
      // Add a special "allow to read" bit together with my (new) version
      // number to give the readers a chance
      tail_.fetch_or(kConsistentBit | qnode->get_version());
#else
      // Add a special "allow to read" bit to give the readers a chance
      tail_.fetch_or(kConsistentBit);
#endif  // OMCS_OFFSET
#endif  // OMCS_OP_READ

      do {
        succ = qnode->get_next();
      } while (succ == nullptr);
    } else {
#ifdef OMCS_OP_READ
      // Add a special "allow to read" bit to give the readers a chance
#ifdef OMCS_OFFSET
      tail_.fetch_or(kConsistentBit | qnode->get_version());
#else
      tail_.fetch_or(kConsistentBit);
#endif
#endif
    }

    assert(succ != nullptr);
    uint64_t v0 = qnode->get_version();
    uint64_t v1 = v0 + kVersionStride;
    assert(v1 != OMCSLock::kInvalidVersion);
    succ->set_version(v1);
  }

  uint64_t begin_read() const {
    while (true) {
      bool restart = false;
      uint64_t version = try_begin_read(restart);
      if (!restart) {
        return version;
      }
    }
  }

  uint64_t try_begin_read(bool &restart) const {
    uint64_t version = tail_.load(std::memory_order_acquire);
#ifdef OMCS_OP_READ
    restart = OMCSLock::has_locked_bit(version) && !(version & kConsistentBit);
#else
    restart = OMCSLock::has_locked_bit(version);
#endif
    // If [restart] hasn't been changed to false, [version]
    // is guaranteed to be a version.
    return version;
  }

  bool validate_read(uint64_t version) const {
#ifndef OMCS_OP_READ
    assert(!OMCSLock::has_locked_bit(version));
#endif
    uint64_t v = tail_.load(std::memory_order_acquire);
    return version == v;
  }

  bool is_locked() const {
    uint64_t version = tail_.load(std::memory_order_acquire);
    return OMCSLock::has_locked_bit(version);
  }
};

}  // namespace omcs_impl
