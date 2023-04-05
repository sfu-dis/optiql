#pragma once

// Do not include this file together with other headers defining this macro.
#if defined(BTREE_SYNC_IMPL)
#error "BTree synchronization implementation is defined multiple times."
#endif
#define BTREE_SYNC_IMPL

#include "BTreeCommon.h"
#include "latches/OMCSOffset.h"

// This implementation uses MCSRW everywhere.

#if not defined(OMCS_OFFSET)
static_assert(false, "OMCS_OFFSET must be defined for BTreeLCMCSRWOnly");
#endif
#define DEFINE_CONTEXT(q, i) OMCSLock::Context *q = offset::get_qnode(i)
#define GET_CONTEXT(i) offset::get_qnode(i)

namespace btreeolc {
template <class Key, class Value>
struct BTreeLC : public BTreeBase<Key, Value> {
  using BTreeBase<Key, Value>::root;
  using BTreeBase<Key, Value>::yield;
  using BTreeBase<Key, Value>::makeRoot;
  // FIXME(shiges): support scan in BTreeLC
  using BTreeBase<Key, Value>::scan;

  enum LockType { Sh, Ex };

  BTreeLC() {
    std::cout << "========================================" << std::endl;
    std::cout << "BTree with lock coupling." << std::endl;
    std::cout << "Lock impl: " << OMCSLock::name << std::endl;
    std::cout << "Lock size: " << sizeof(OMCSLock) << std::endl;
    std::cout << "Page size (bytes): " << pageSize << std::endl;
    std::cout << "Max #entries: Leaf: " << BTreeLeaf<Key, Value>::maxEntries
              << ", Inner: " << BTreeInner<Key>::maxEntries << std::endl;
    std::cout << "Using top-down lock coupling for SMOs." << std::endl;
    std::cout << "========================================" << std::endl;
    root = new BTreeLeaf<Key, Value>();
  }

  struct UnsafeTlsContextAllocator {
   private:
    size_t free_list_size_ = 0;
    uint64_t next_id_ = 0;
    uint64_t free_list_[kMaxLevels];

   public:
    uint64_t allocate() {
      if (free_list_size_) {
        return free_list_[--free_list_size_];
      }
      return next_id_++;
    }

    void free(uint64_t id) { free_list_[free_list_size_++] = id; }
  };

  struct UnsafeNodeStack {
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
  };

  bool insertPessimistically(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);

    UnsafeTlsContextAllocator qnode_allocator;
    UnsafeNodeStack latched_nodes;
    NodeBase *node = root;
    uint64_t i = qnode_allocator.allocate();
    OMCSLock::Context *q = GET_CONTEXT(i);
    node->writeLock(q);
    latched_nodes.Push(node, i);
    if (node != root) {
      node->writeUnlock(q);
      goto restart;
    }

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];

      // if [next] is not full, release all held exclusive latches
      // on its ancestors
      bool release_ancestors = false;
      uint64_t j = qnode_allocator.allocate();
      OMCSLock::Context *nq = GET_CONTEXT(j);
      next->writeLock(nq);
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        auto next_inner = static_cast<BTreeInner<Key> *>(next);
        if (!next_inner->isFull()) {
          release_ancestors = true;
        }
      } else {
        // [next] is a leaf node
        auto next_leaf = static_cast<BTreeLeaf<Key, Value> *>(next);
        if (!next_leaf->isFull()) {
          release_ancestors = true;
        }
      }

      if (release_ancestors) {
        while (latched_nodes.Size() > 0) {
          auto [n, i] = latched_nodes.Pop();
          OMCSLock::Context *q = GET_CONTEXT(i);
          n->writeUnlock(q);
          qnode_allocator.free(i);
        }
      }
      latched_nodes.Push(next, j);

      node = next;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // handle splits
      assert(leaf == latched_nodes.Top().first);
      auto [n, i] = latched_nodes.Pop();
      assert(n == leaf);
      OMCSLock::Context *q = GET_CONTEXT(i);
      Key sep;
      bool ok = leaf->insert(k, v);
      NodeBase *newNode = leaf->split(sep);
      if (latched_nodes.Size() == 0) {
        // [leaf] has to be root
        assert(root == leaf);
        makeRoot(sep, leaf, newNode);
        node->writeUnlock(q);
      } else {
        // [leaf] has a parent
        node->writeUnlock(q);
        while (latched_nodes.Size() > 1) {
          auto [n, i] = latched_nodes.Pop();
          assert(n->getType() == PageType::BTreeInner);
          OMCSLock::Context *q = GET_CONTEXT(i);
          auto parent = static_cast<BTreeInner<Key> *>(n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          n->writeUnlock(q);
        }
        assert(latched_nodes.Size() == 1);
        auto [n, i] = latched_nodes.Pop();
        assert(n->getType() == PageType::BTreeInner);
        OMCSLock::Context *q = GET_CONTEXT(i);
        auto parent = static_cast<BTreeInner<Key> *>(n);
        if (parent->isFull()) {
          assert(root == n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          makeRoot(sep, parent, newNode);
        } else {
          // We have finally found some space
          parent->insert(sep, newNode);
        }
        n->writeUnlock(q);
      }
      return ok;
    } else {
      // no need to split, just insert into [leaf]
      assert(latched_nodes.Size() == 1);
      auto [n, i] = latched_nodes.Pop();
      assert(n == node);
      OMCSLock::Context *q = GET_CONTEXT(i);
      bool ok = leaf->insert(k, v);
      node->writeUnlock(q);
      return ok;
    }
  }

  template <LockType ty>
  bool traverseToLeaf(Key k, OMCSLock::Context *q, OMCSLock::Context *nq, NodeBase *&node,
                      OMCSLock::Context *&q_out) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);

    node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      if constexpr (ty == Sh) {
        node->readLock(q);
      } else {
        node->writeLock(q);
      }
      if (node != root) {
        if constexpr (ty == Sh) {
          node->readUnlock(q);
        } else {
          node->writeUnlock(q);
        }
        goto restart;
      }
      q_out = q;
      return true;
    }
    node->readLock(q);
    if (node != root) {
      node->readUnlock(q);
      goto restart;
    }

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];

      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        next->readLock(nq);
      } else {
        // [next] is a leaf node, just take (shared or exclusive) latch
        if constexpr (ty == Sh) {
          next->readLock(nq);
        } else {
          next->writeLock(nq);
        }
      }
      node->readUnlock(q);

      node = next;
      std::swap(q, nq);
    }

    // We now have a latch on [node]
    q_out = q;
    return true;
  }

  bool lookup(Key k, Value &result) {
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q0, 0);
    DEFINE_CONTEXT(q1, 1);
    OMCSLock::Context *q = nullptr;
    traverseToLeaf<Sh>(k, q0, q1, node, q);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
      success = true;
      result = leaf->data[pos].second;
    }
    node->readUnlock(q);
    return success;
  }

  bool insert(Key k, Value v) {
    constexpr int kMaxInsertRetries = 0;
    int restartCount = 0;
  restart:
    if (restartCount++ == kMaxInsertRetries) {
      return insertPessimistically(k, v);
    }
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q0, 0);
    DEFINE_CONTEXT(q1, 1);
    OMCSLock::Context *q = nullptr;
    traverseToLeaf<Ex>(k, q0, q1, node, q);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // We have bad luck. Unlock [node] and retry the entire traversal,
      // taking exclusive latches along the way
      node->writeUnlock(q);
      goto restart;
    } else {
      bool ok = leaf->insert(k, v);
      node->writeUnlock(q);
      return ok;
    }
  }

  bool remove(Key k) {
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q0, 0);
    DEFINE_CONTEXT(q1, 1);
    OMCSLock::Context *q = nullptr;
    traverseToLeaf<Ex>(k, q0, q1, node, q);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->remove(k);
    node->writeUnlock(q);
    return ok;
  }

  bool update(Key k, Value v) {
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q0, 0);
    DEFINE_CONTEXT(q1, 1);
    OMCSLock::Context *q = nullptr;
    traverseToLeaf<Ex>(k, q0, q1, node, q);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    node->writeUnlock(q);
    return ok;
  }
};

}  // namespace btreeolc
