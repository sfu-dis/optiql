//
// Created by florian on 05.08.15.
//

#ifndef ART_OPTIMISTIC_LOCK_COUPLING_N_H
#define ART_OPTIMISTIC_LOCK_COUPLING_N_H
// #define ART_NOREADLOCK
// #define ART_NOWRITELOCK
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>

#include <atomic>

#include "Key.h"
#include "latches/OMCS.h"
#include "latches/OMCSOffset.h"

using TID = uint64_t;

#if defined(IS_CONTEXTFUL)
#if defined(OMCS_OFFSET)
#define DEFINE_CONTEXT(q, i) OMCSLock::Context &q = *offset::get_qnode(i)
#else
#define DEFINE_CONTEXT(q, i) OMCSLock::Context q
#endif
#define LOCK_NODE(n, q) n->writeLockOrRestart(q, needRestart)
#define UNLOCK_NODE(n, q) n->writeUnlock(q)
#define READ_LOCK_NODE(n, q) n->readLockOrRestart(q, needRestart)
#define READ_UNLOCK_NODE(n, q) n->readUnlock(q)
#else
#define DEFINE_CONTEXT(q, i) OMCSLock::Context q
#define LOCK_NODE(n, q) n->writeLockOrRestart(needRestart)
#define UNLOCK_NODE(n, q) n->writeUnlock()
#define READ_LOCK_NODE(n, q) n->readLockOrRestart(needRestart)
#define READ_UNLOCK_NODE(n, q) n->readUnlock()
#endif

namespace ART_OLC {
/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */

enum class NTypes : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

static constexpr uint32_t maxStoredPrefixLength = 15;

using Prefix = uint8_t[maxStoredPrefixLength];

class N {
 public:
  using Lock = OMCSLock;

 protected:
  N(NTypes type, const uint8_t *prefix, uint32_t prefixLength) {
    setType(type);
    setPrefix(prefix, prefixLength);
  }

  N(const N &) = delete;

  N(N &&) = delete;

  Lock lock;

  struct TaggedPrefixCount {
    // 2b type 1b obsolete 29b prefixCount
    static const uint32_t kMaxPrefixCount = (1 << 29) - 1;
    static const uint32_t kObsoleteBit = 1 << 29;
    static const uint32_t kBitsMask = 0xE0000000;
    static const uint32_t kTypeBitsMask = 0xC0000000;

    uint32_t value;

    TaggedPrefixCount() = default;
    TaggedPrefixCount(uint32_t v) : value(v) {}

    TaggedPrefixCount &operator=(const uint32_t v) {
      assert(v < kMaxPrefixCount);
      value = v | (value & kBitsMask);
      return *this;
    }

    TaggedPrefixCount &operator+=(const uint32_t v) {
      uint32_t newv = get() + v;
      assert(newv < kMaxPrefixCount);
      value = newv | (value & kBitsMask);
      return *this;
    }

    uint32_t get() const { return value & (~kBitsMask); }

    uint32_t getRaw() const { return value; }

    uint32_t getObsolete() const { return value & kObsoleteBit; }

    void setType(uint32_t type) { value = (value & (~kTypeBitsMask)) | type; }

    void setObsolete() { value |= kObsoleteBit; }
  };

  TaggedPrefixCount prefixCount = 0;

  uint8_t count = 0;
  Prefix prefix;

 public:
  uint32_t hotness = 0;

 protected:
  void setType(NTypes type);

  static uint32_t convertTypeToPrefixCount(NTypes type);

 public:
  NTypes getType() const;

  uint32_t getCount() const;

  inline void checkObsoleteOrRestart(bool &needRestart) const {
    if (needRestart) {
      return;
    }
    if (isObsolete()) {
      needRestart = true;
      return;
    }
  }

  void writeLockOrRestart(bool &needRestart) {
    lock.writeLock();
    checkObsoleteOrRestart(needRestart);
    if (needRestart) {
      lock.writeUnlock();
    }
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    LOG(FATAL) << "Not supported";
  }

  void writeUnlock() {
    // XXX(shiges): no need to check obsolete
    lock.writeUnlock();
  }

  void writeLockOrRestart(Lock::Context *q, bool &needRestart) {
    lock.writeLock(q);
    checkObsoleteOrRestart(needRestart);
    if (needRestart) {
      lock.writeUnlock(q);
    }
  }

  void writeUnlock(Lock::Context *q) {
    // XXX(shiges): no need to check obsolete
    lock.writeUnlock(q);
  }

  void readLockOrRestart(bool &needRestart) {
    lock.readLock();
    checkObsoleteOrRestart(needRestart);
    if (needRestart) {
      lock.readUnlock();
    }
  }

  void readUnlock() {
    // XXX(shiges): no need to check obsolete
    lock.readUnlock();
  }

  void readLockOrRestart(Lock::Context *q, bool &needRestart) {
    lock.readLock(q);
    checkObsoleteOrRestart(needRestart);
    if (needRestart) {
      lock.readUnlock(q);
    }
  }

  void readUnlock(Lock::Context *q) {
    // XXX(shiges): no need to check obsolete
    lock.readUnlock(q);
  }

  bool isObsolete() const { return prefixCount.getObsolete(); }

  void setObsolete() { prefixCount.setObsolete(); }

  static N *getChild(const uint8_t k, const N *node);

  static void insertAndUnlock(N *node, uint64_t v, N *parentNode, uint64_t parentVersion,
                              uint8_t keyParent, uint8_t key, N *val, bool &needRestart,
                              N *&obsoleteN);

  static void insertAndUnlockPessimistic(N *node, N *parentNode, OMCSLock::Context *q,
                                         OMCSLock::Context *pq, uint8_t keyParent, uint8_t key,
                                         N *val, bool &needRestart, N *&obsoleteN);

  static bool change(N *node, uint8_t key, N *val);

  static void removeAndUnlock(N *node, uint64_t v, uint8_t key, N *parentNode,
                              uint64_t parentVersion, uint8_t keyParent, bool &needRestart,
                              N *&obsoleteN);

  bool hasPrefix() const;

  const uint8_t *getPrefix() const;

  void setPrefix(const uint8_t *prefix, uint32_t length);

  void addPrefixBefore(N *node, uint8_t key);

  uint32_t getPrefixLength() const;

  static TID getLeaf(const N *n);

  static bool isLeaf(const N *n);

  static N *setLeaf(TID tid);

  static N *getAnyChild(const N *n);

  static TID getAnyChildTid(N *n, bool &needRestart);

  static void deleteChildren(N *node);

  static void deleteNode(N *node);

  static std::tuple<N *, uint8_t> getSecondChild(N *node, const uint8_t k);

  template <typename curN, typename biggerN>
  static void insertGrow(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion,
                         uint8_t keyParent, uint8_t key, N *val, bool &needRestart, N *&obsoleteN);

  template <typename curN, typename biggerN>
  static void insertGrowPessimistic(curN *n, N *parentNode, OMCSLock::Context *q,
                                    OMCSLock::Context *pq, uint8_t keyParent, uint8_t key, N *val,
                                    bool &needRestart, N *&obsoleteN);

  template <typename curN, typename smallerN>
  static void removeAndShrink(curN *n, uint64_t v, N *parentNode, uint64_t parentVersion,
                              uint8_t keyParent, uint8_t key, bool &needRestart, N *&obsoleteN);

  static uint64_t getChildren(N *node, uint8_t start, uint8_t end,
                              std::tuple<uint8_t, N *> children[], uint32_t &childrenCount);
};

class N4 : public N {
 public:
  uint8_t keys[4];
  N *children[4] = {nullptr, nullptr, nullptr, nullptr};

 public:
  N4(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N4, prefix, prefixLength) {}

  void insert(uint8_t key, N *n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, N *val);

  N *getChild(const uint8_t k) const;

  void remove(uint8_t k);

  N *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  std::tuple<N *, uint8_t> getSecondChild(const uint8_t key) const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount);
};

static_assert(sizeof(N) == sizeof(N::Lock) + 24);

class N16 : public N {
 public:
  uint8_t keys[16];
  N *children[16];

  static uint8_t flipSign(uint8_t keyByte) {
    // Flip the sign bit, enables signed SSE comparison of unsigned values, used by Node16
    return keyByte ^ 128;
  }

  static inline unsigned ctz(uint16_t x) {
    // Count trailing zeros, only defined for x>0
#ifdef __GNUC__
    return __builtin_ctz(x);
#else
    // Adapted from Hacker's Delight
    unsigned n = 1;
    if ((x & 0xFF) == 0) {
      n += 8;
      x = x >> 8;
    }
    if ((x & 0x0F) == 0) {
      n += 4;
      x = x >> 4;
    }
    if ((x & 0x03) == 0) {
      n += 2;
      x = x >> 2;
    }
    return n - (x & 1);
#endif
  }

  N *const *getChildPos(const uint8_t k) const;

 public:
  N16(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N16, prefix, prefixLength) {
    memset(keys, 0, sizeof(keys));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, N *n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, N *val);

  N *getChild(const uint8_t k) const;

  void remove(uint8_t k);

  N *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount);
};

class N48 : public N {
  uint8_t childIndex[256];
  N *children[48];

 public:
  static const uint8_t emptyMarker = 48;

  N48(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N48, prefix, prefixLength) {
    memset(childIndex, emptyMarker, sizeof(childIndex));
    memset(children, 0, sizeof(children));
  }

  void insert(uint8_t key, N *n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, N *val);

  N *getChild(const uint8_t k) const;

  void remove(uint8_t k);

  N *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount);
};

class N256 : public N {
  N *children[256];

 public:
  N256(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N256, prefix, prefixLength) {
    memset(children, '\0', sizeof(children));
  }

  void insert(uint8_t key, N *val);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, N *n);

  N *getChild(const uint8_t k) const;

  void remove(uint8_t k);

  N *getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  void deleteChildren();

  uint64_t getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                       uint32_t &childrenCount);
};
}  // namespace ART_OLC
#endif  // ART_OPTIMISTIC_LOCK_COUPLING_N_H
