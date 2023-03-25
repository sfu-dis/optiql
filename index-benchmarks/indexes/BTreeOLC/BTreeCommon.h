#pragma once

#include <glog/logging.h>
#include <immintrin.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <utility>

#include "latches/OMCS.h"

namespace btreeolc {

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

constexpr uint64_t kPageSizeLinearSearchCutoff = 256;  // Use binary search if larger
constexpr uint64_t pageSize = BTREE_PAGE_SIZE;
constexpr uint64_t kMaxLevels = 16;
constexpr int kMaxInsertRetries = 3;

struct NodeBase : public OMCSLock {
  uint8_t level;
  uint16_t count;

  PageType getType() const { return (level == 1) ? PageType::BTreeLeaf : PageType::BTreeInner; }

  static void *operator new(std::size_t count) {
    void *space = aligned_alloc(pageSize, pageSize);
    return ::operator new(count, space);
  }
};

template <class Key, class Payload>
struct BTreeLeaf : public NodeBase {
  // This is the element type of the leaf node
  struct KeyValueType {
    Key first;
    Payload second;
  };
  static constexpr size_t entrySize = sizeof(KeyValueType);
  // XXX(shiges): one spot less to accept the new key-val pair when splitting
  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase) - sizeof(BTreeLeaf<Key, Payload> *)) / entrySize - 1;

  // Singly linked list pointer to my sibling
  BTreeLeaf<Key, Payload> *next_leaf;

  // This is the array that we perform search on
  alignas(entrySize) KeyValueType data[maxEntries + 1];

  BTreeLeaf() {
    level = 1;
    count = 0;
    next_leaf = nullptr;
  }

  bool isFull() { return count == maxEntries; };

  unsigned lowerBound(Key k) {
    if constexpr (pageSize <= kPageSizeLinearSearchCutoff) {
      unsigned lower = 0;
      while (lower < count) {
        const Key &next_key = data[lower].first;

        if (k <= next_key) {
          return lower;
        } else {
          lower++;
        }
      }
      return lower;
    } else {
      unsigned lower = 0;
      unsigned upper = count;
      do {
        unsigned mid = ((upper - lower) / 2) + lower;
        // This is the key at the pivot position
        const Key &middle_key = data[mid].first;

        if (k < middle_key) {
          upper = mid;
        } else if (k > middle_key) {
          lower = mid + 1;
        } else {
          return mid;
        }
      } while (lower < upper);
      return lower;
    }
  }

  bool insert(Key k, Payload p) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        // key already exists
        return false;
      }
      memmove(data + pos + 1, data + pos, sizeof(KeyValueType) * (count - pos));
      // memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
      data[pos].first = k;
      data[pos].second = p;
    } else {
      data[0].first = k;
      data[0].second = p;
    }
    count++;
    return true;
  }

  bool remove(Key k) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        // key found
        memmove(data + pos, data + pos + 1, sizeof(KeyValueType) * (count - pos));
        count--;
        return true;
      }
    }
    return false;
  }

  bool update(Key k, Payload p) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        // Update
        data[pos].second = p;
        return true;
      }
    }
    return false;
  }

#if defined(OMCS_OP_READ_NEW_API)
  bool update(Key k, Payload p, bool opread) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        // Update
        if (opread) {
          writeLockTurnOffOpRead();
        }
        data[pos].second = p;
        return true;
      }
    }
    if (opread) {
      writeLockTurnOffOpRead();
    }
    return false;
  }
#endif

  BTreeLeaf *split(Key &sep) {
    BTreeLeaf *newLeaf = new BTreeLeaf();
    newLeaf->count = count - (count / 2);
    count = count - newLeaf->count;
    memcpy(newLeaf->data, data + count, sizeof(KeyValueType) * newLeaf->count);
    newLeaf->next_leaf = next_leaf;
    next_leaf = newLeaf;
    sep = data[count - 1].first;
    return newLeaf;
  }
};

template <class Key>
struct BTreeInner : public NodeBase {
  static constexpr size_t entrySize = sizeof(Key) + sizeof(NodeBase *);
  // XXX(shiges): one spot less to accept the new key-val pair when splitting
  static const uint64_t maxEntries = (pageSize - sizeof(NodeBase)) / entrySize - 1;
  alignas(entrySize) NodeBase *children[maxEntries + 1];
  Key keys[maxEntries + 1];

  BTreeInner(uint8_t node_level) {
    level = node_level;
    count = 0;
  }

  bool isFull() { return count == (maxEntries - 1); };

  unsigned lowerBoundBF(Key k) {
    auto base = keys;
    unsigned n = count;
    while (n > 1) {
      const unsigned half = n / 2;
      base = (base[half] < k) ? (base + half) : base;
      n -= half;
    }
    return (*base < k) + base - keys;
  }

  unsigned lowerBound(Key k) {
    if constexpr (pageSize <= kPageSizeLinearSearchCutoff) {
      unsigned lower = 0;
      while (lower < count) {
        const Key &next_key = keys[lower];

        if (k <= next_key) {
          return lower;
        } else {
          lower++;
        }
      }
      return lower;
    } else {
      unsigned lower = 0;
      unsigned upper = count;
      do {
        unsigned mid = ((upper - lower) / 2) + lower;
        if (k < keys[mid]) {
          upper = mid;
        } else if (k > keys[mid]) {
          lower = mid + 1;
        } else {
          return mid;
        }
      } while (lower < upper);
      return lower;
    }
  }

  BTreeInner *split(Key &sep) {
    uint8_t level = children[0]->level + 1;
    BTreeInner *newInner = new BTreeInner(level);
    newInner->count = count - (count / 2);
    count = count - newInner->count - 1;
    sep = keys[count];
    memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
    memcpy(newInner->children, children + count + 1, sizeof(NodeBase *) * (newInner->count + 1));
    return newInner;
  }

  void insert(Key k, NodeBase *child) {
    assert(count <= maxEntries - 1);
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
    memmove(children + pos + 1, children + pos, sizeof(NodeBase *) * (count - pos + 1));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    count++;
  }
};

// BTree with common and read-only operations
template <class Key, class Value>
struct BTreeBase {
  std::atomic<NodeBase *> root;

  void makeRoot(Key k, NodeBase *leftChild, NodeBase *rightChild) {
    assert(leftChild->level == rightChild->level);
    auto inner = new BTreeInner<Key>(leftChild->level + 1);
    inner->count = 1;
    inner->keys[0] = k;
    inner->children[0] = leftChild;
    inner->children[1] = rightChild;
    root = inner;
  }

  inline void yield(int count) {
    (void)count;
    _mm_pause();
  }

#if defined(BTREE_NO_SYNC)
  // A sequential lookup implementation.
  // Only used to measure the overhead of version validation.
  bool lookup(Key k, Value &result) {
    NodeBase *node = root;

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node = next;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
      success = true;
      result = leaf->data[pos].second;
    }

    return success;
  }
#else
  // A concurrent lookup implementation with OLC.
  bool lookup(Key k, Value &result) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    NodeBase *node = root;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || node != root) goto restart;

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      uint64_t versionNext = next->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      node = next;
      versionNode = versionNext;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
      success = true;
      result = leaf->data[pos].second;
    }
    node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;

    return success;
  }
#endif

  uint64_t scan(Key k, int range, Value *output) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    NodeBase *node = root;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || node != root) goto restart;

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      uint64_t versionNext = next->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      node = next;
      versionNode = versionNext;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    int count = 0;

    while (leaf && count < range) {
      for (unsigned i = pos; i < leaf->count && count < range; i++) {
        output[count++] = leaf->data[i].second;
      }

      if (count == range) {
        // scan() finishes at [leaf]
        break;
      } else {
        // proceed with next leaf
        auto next_leaf = leaf->next_leaf;
        leaf->checkOrRestart(versionNode, needRestart);
        if (needRestart) goto restart;

        if (!next_leaf) {
          // scan() finishes at [leaf]
          break;
        }
        uint64_t versionNext = next_leaf->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
        leaf->readUnlockOrRestart(versionNode, needRestart);
        if (needRestart) goto restart;

        leaf = next_leaf;
        versionNode = versionNext;
        pos = 0;
      }
    }
    return count;
  }

 protected:
  BTreeBase() {}
};

}  // namespace btreeolc