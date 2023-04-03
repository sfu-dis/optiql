#pragma once

// Do not include this file together with other headers defining this macro.
#if defined(BTREE_SYNC_IMPL)
#error "BTree synchronization implementation is defined multiple times."
#endif
#define BTREE_SYNC_IMPL

#include <glog/logging.h>
#include <immintrin.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <utility>

#include "latches/OMCS.h"
#include "latches/OMCSOffset.h"

// This implementation uses OptLocks on inner nodes and MCS RW locks on leaf nodes

#define DEFINE_CONTEXT(q, i) OMCSLock::Context &q = *offset::get_qnode(i)

namespace btreeolc {

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

constexpr uint64_t kPageSizeLinearSearchCutoff = 256;  // Use binary search if larger
constexpr uint64_t pageSize = BTREE_PAGE_SIZE;
constexpr uint64_t kMaxLevels = 16;
constexpr int kMaxInsertRetries = 3;

struct NodeBase;

struct LeafNodeBase : public MCSRWLock {
  uint8_t level;
  uint16_t count;
  uint16_t padding;

  static void *operator new(std::size_t count) {
    void *space = aligned_alloc(pageSize, pageSize);
    return ::operator new(count, space);
  }

  NodeBase *base() { return reinterpret_cast<NodeBase *>(this); }
  const NodeBase *base() const { return reinterpret_cast<const NodeBase *>(this); }
};

struct InnerNodeBase : public OptLock {
  uint8_t level;
  uint16_t count;

  static void *operator new(std::size_t count) {
    void *space = aligned_alloc(pageSize, pageSize);
    return ::operator new(count, space);
  }

  NodeBase *base() { return reinterpret_cast<NodeBase *>(this); }
  const NodeBase *base() const { return reinterpret_cast<const NodeBase *>(this); }
};

struct NodeBase {
  uint64_t lock;
  uint8_t level;
  uint16_t count;
  uint16_t padding;

  PageType getType() const { return (level == 1) ? PageType::BTreeLeaf : PageType::BTreeInner; }

  LeafNodeBase *leaf() { return reinterpret_cast<LeafNodeBase *>(this); }
  const LeafNodeBase *leaf() const { return reinterpret_cast<const LeafNodeBase *>(this); }

  InnerNodeBase *inner() { return reinterpret_cast<InnerNodeBase *>(this); }
  const InnerNodeBase *inner() const { return reinterpret_cast<const InnerNodeBase *>(this); }
};

// XXX(shiges): This probably would result in UB...
static_assert(sizeof(NodeBase) == sizeof(LeafNodeBase),
              "Sizes of NodeBase and LeafNodeBase are not the same!");
static_assert(sizeof(NodeBase) == sizeof(InnerNodeBase),
              "Sizes of NodeBase and InnerNodeBase are not the same!");
static_assert(offsetof(NodeBase, level) == offsetof(LeafNodeBase, level),
              "Offsets of `level' on NodeBase and LeafNodeBase are not the same!");
static_assert(offsetof(NodeBase, level) == offsetof(InnerNodeBase, level),
              "Offsets of `level' on NodeBase and InnerNodeBase are not the same!");
static_assert(offsetof(NodeBase, count) == offsetof(LeafNodeBase, count),
              "Offsets of `count' on NodeBase and LeafNodeBase are not the same!");
static_assert(offsetof(NodeBase, count) == offsetof(InnerNodeBase, count),
              "Offsets of `count' on NodeBase and InnerNodeBase are not the same!");

template <typename NodeBase>
struct NodeBaseTrait {};

template <>
struct NodeBaseTrait<LeafNodeBase> {
  // static PageType getType(const LeafNodeBase *node) { return node->getType(); }
};

template <>
struct NodeBaseTrait<InnerNodeBase> {
  // static PageType getType(const InnerNodeBase *node) { return node->getType(); }
};

template <class Key, class Payload>
struct BTreeLeaf : public LeafNodeBase {
  // This is the element type of the leaf node
  struct KeyValueType {
    Key first;
    Payload second;
  };
  static constexpr size_t entrySize = sizeof(KeyValueType);
  // XXX(shiges): one spot less to accept the new key-val pair when splitting
  static const uint64_t maxEntries =
      (pageSize - sizeof(LeafNodeBase) - sizeof(BTreeLeaf<Key, Payload> *)) / entrySize - 1;

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
struct BTreeInner : public InnerNodeBase {
  static constexpr size_t entrySize = sizeof(Key) + sizeof(NodeBase *);
  // XXX(shiges): one spot less to accept the new key-val pair when splitting
  static const uint64_t maxEntries = (pageSize - sizeof(InnerNodeBase)) / entrySize - 1;
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

template <class Key, class Value>
struct BTreeOLCHybrid {
  enum LockType { Sh, Ex };

  std::atomic<NodeBase *> root;

  void makeRoot(Key k, NodeBase *leftChild, NodeBase *rightChild) {
    assert(leftChild->level == rightChild->level);
    auto inner = new BTreeInner<Key>(leftChild->level + 1);
    inner->count = 1;
    inner->keys[0] = k;
    inner->children[0] = leftChild;
    inner->children[1] = rightChild;
    root = inner->base();
  }

  inline void yield(int count) {
    (void)count;
    _mm_pause();
  }

  BTreeOLCHybrid() {
    std::cout << "========================================" << std::endl;
    std::cout << "BTree with OLC on inner nodes and MCS-RW on leaf nodes." << std::endl;
    std::cout << "Lock impl: " << OMCSLock::name << std::endl;
    std::cout << "OptLock size: " << sizeof(OptLock) << std::endl;
    std::cout << "MCSRWLock size: " << sizeof(MCSRWLock) << std::endl;
    std::cout << "Page size (bytes): " << pageSize << std::endl;
    std::cout << "Max #entries: Leaf: " << BTreeLeaf<Key, Value>::maxEntries
              << ", Inner: " << BTreeInner<Key>::maxEntries << std::endl;
    std::cout << "Using bottom-up lock upgrading for SMOs." << std::endl;
    std::cout << "========================================" << std::endl;
    root = (new BTreeLeaf<Key, Value>())->base();
  }

  struct UnsafeNodeStack {
    // When used in optimistic bottom-up insertion, the stack holds references
    // to nodes that are not exclusively latched, and upgrades them after
    // reaching the leaf node.
    // When used in pessimistic top-down insertion, the stack holds references
    // to exclusively latched nodes.
   private:
    std::pair<NodeBase *, uint64_t> nodes[kMaxLevels];
    size_t top = 0;

   public:
    void Push(NodeBase *node, uint64_t version) {
      assert(top < kMaxLevels);
      nodes[top++] = {node, version};
    }
    std::pair<NodeBase *, uint64_t> Pop() {
      assert(top > 0);
      return nodes[--top];
    }
    std::pair<NodeBase *, uint64_t> Top() const {
      assert(top > 0);
      return nodes[top - 1];
    }
    size_t Size() const { return top; }
    void UpgradeAll(bool &needRestart) const {
      // Upgrade latches bottom-up. Upon any validation failure,
      // exclusively-latched nodes are released top-down.
      for (size_t i = top; i; i--) {
        auto [n, version] = nodes[i - 1];
        assert(n->getType() == PageType::BTreeInner);
        n->inner()->upgradeToWriteLockOrRestart(version, needRestart);
        if (needRestart) {
          for (size_t j = i; j < top; j++) {
            nodes[j].first->inner()->writeUnlock(nodes[j].second);
          }
          return;
        }
      }
    }
  };

  bool insertOptimistically(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    DEFINE_CONTEXT(q, 0);
    UnsafeNodeStack read_nodes;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    NodeBase *node = root;
    if (node->getType() == PageType::BTreeInner) {
      versionNode = node->inner()->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    } else {
      node->leaf()->writeLock(&q);
    }
    read_nodes.Push(node, versionNode);
    if (node != root) {
      if (node->getType() == PageType::BTreeLeaf) {
        node->leaf()->writeUnlock(&q);
      }
      goto restart;
    }

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node->inner());

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node->inner()->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      // if [next] is not full, drop all remembered versions
      // on its ancestors
      bool release_ancestors = false;
      uint64_t versionNext = OMCSLock::kInvalidVersion;
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        versionNext = next->inner()->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
        auto next_inner = static_cast<BTreeInner<Key> *>(next->inner());
        if (!next_inner->isFull()) {
          release_ancestors = true;
        }
      } else {
        // [next] is a leaf node
        next->leaf()->writeLock(&q);
        auto next_leaf = static_cast<BTreeLeaf<Key, Value> *>(next->leaf());
        if (!next_leaf->isFull()) {
          release_ancestors = true;
        }
      }
      node->inner()->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (next->getType() == PageType::BTreeLeaf) {
          next->leaf()->writeUnlock(&q);
        }
        goto restart;
      }

      if (release_ancestors) {
        while (read_nodes.Size() > 0) {
          read_nodes.Pop();
        }
      }
      read_nodes.Push(next, versionNext);

      node = next;
      versionNode = versionNext;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node->leaf());
    if (leaf->isFull()) {
      // handle splits
      assert(leaf->base() == read_nodes.Top().first);
      read_nodes.Pop();
      read_nodes.UpgradeAll(needRestart);
      if (needRestart) {
        node->leaf()->writeUnlock(&q);
        goto restart;
      }
      Key sep;
      bool ok = leaf->insert(k, v);
      NodeBase *newNode = leaf->split(sep)->base();
      if (read_nodes.Size() == 0) {
        // [leaf] has to be root
        assert(root == leaf->base());
        makeRoot(sep, leaf->base(), newNode);
        node->leaf()->writeUnlock(&q);
      } else {
        // [leaf] has a parent
        node->leaf()->writeUnlock(&q);
        while (read_nodes.Size() > 1) {
          auto [n, version] = read_nodes.Pop();
          assert(n->getType() == PageType::BTreeInner);
          auto parent = static_cast<BTreeInner<Key> *>(n->inner());
          parent->insert(sep, newNode);
          newNode = parent->split(sep)->base();
          n->inner()->writeUnlock(version);
        }
        assert(read_nodes.Size() == 1);
        auto [n, version] = read_nodes.Pop();
        assert(n->getType() == PageType::BTreeInner);
        auto parent = static_cast<BTreeInner<Key> *>(n->inner());
        if (parent->isFull()) {
          assert(root == n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep)->base();
          makeRoot(sep, parent->base(), newNode);
        } else {
          // We have finally found some space
          parent->insert(sep, newNode);
        }
        n->inner()->writeUnlock(version);
      }
      return ok;
    } else {
      // no need to split, just insert into [leaf]
      assert(read_nodes.Size() == 1);
      bool ok = leaf->insert(k, v);
      node->leaf()->writeUnlock(&q);
      return ok;
    }
  }

  template <LockType ty>
  bool traverseToLeaf(Key k, OMCSLock::Context &q, LeafNodeBase *&node_out) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    NodeBase *node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      if constexpr (ty == Sh) {
        node->leaf()->readLock(&q);
      } else {
        node->leaf()->writeLock(&q);
      }
      if (node != root) {
        if constexpr (ty == Sh) {
          node->leaf()->readUnlock(&q);
        } else {
          node->leaf()->writeUnlock(&q);
        }
        goto restart;
      }
      node_out = node->leaf();
      return true;
    }
    uint64_t versionNode = node->inner()->readLockOrRestart(needRestart);
    if (needRestart || node != root) goto restart;

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node->inner());

      NodeBase *next = inner->children[inner->lowerBound(k)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      uint64_t versionNext = OMCSLock::kInvalidVersion;
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        versionNext = next->inner()->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
      } else {
        // [next] is a leaf node, just take (shared or exclusive) latch
        if constexpr (ty == Sh) {
          next->leaf()->readLock(&q);
        } else {
          next->leaf()->writeLock(&q);
        }
      }
      inner->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (next->getType() == PageType::BTreeLeaf) {
          if constexpr (ty == Sh) {
            next->leaf()->readUnlock(&q);
          } else {
            next->leaf()->writeUnlock(&q);
          }
        }
        goto restart;
      }

      node = next;
      versionNode = versionNext;
    }

    // We now have a latch on [node]
    node_out = node->leaf();
    return true;
  }

  bool lookup(Key k, Value &result) {
    LeafNodeBase *node = nullptr;
    DEFINE_CONTEXT(q, 0);
    traverseToLeaf<Sh>(k, q, node);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
      success = true;
      result = leaf->data[pos].second;
    }
    node->readUnlock(&q);
    return success;
  }

  // FIXME(shiges): support scan in BTreeOLCHybrid
  uint64_t scan(Key k, int range, Value *output) {
    LOG(FATAL) << "Not supported";
    return 0;
#if 0
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    NodeBase *node = root;
    uint64_t versionNode = node->inner()->readLockOrRestart(needRestart);
    if (needRestart || node != root) goto restart;

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      uint64_t versionNext = next->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
      node->inner()->readUnlockOrRestart(versionNode, needRestart);
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
#endif
  }

  bool insert(Key k, Value v) { return insertOptimistically(k, v); }

  bool remove(Key k) {
    LeafNodeBase *node = nullptr;
    DEFINE_CONTEXT(q, 0);
    traverseToLeaf<Ex>(k, q, node);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->remove(k);
    node->writeUnlock(&q);
    return ok;
  }

  bool update(Key k, Value v) {
    LeafNodeBase *node = nullptr;
    DEFINE_CONTEXT(q, 0);
    traverseToLeaf<Ex>(k, q, node);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    node->writeUnlock(&q);
    return ok;
  }
};

}  // namespace btreeolc
