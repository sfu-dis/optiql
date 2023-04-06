#pragma once

// Do not include this file together with other headers defining this macro.
#if defined(BTREE_SYNC_IMPL)
#error "BTree synchronization implementation is defined multiple times."
#endif
#define BTREE_SYNC_IMPL

#include "BTreeCommon.h"
#include "latches/OMCSOffset.h"

// This implementation uses MCSRW only on leaf nodes.
// For inner nodes, centralized RW locks are used.

#if defined(OMCS_OFFSET)
#define DEFINE_CONTEXT(q, i) OMCSLock::Context &q = *offset::get_qnode(i)
#else
#define DEFINE_CONTEXT(q, i) OMCSLock::Context q
#endif

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

  struct UnsafeNodeStack {
    // When used in pessimistic top-down insertion, the stack holds references
    // to exclusively latched nodes.
   private:
    NodeBase *nodes[kMaxLevels];
    size_t top = 0;

   public:
    void Push(NodeBase *node) {
      assert(top < kMaxLevels);
      nodes[top++] = node;
    }
    NodeBase *Pop() {
      assert(top > 0);
      return nodes[--top];
    }
    NodeBase *Top() const {
      assert(top > 0);
      return nodes[top - 1];
    }
    size_t Size() const { return top; }
  };

  bool insertPessimistically(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);

    DEFINE_CONTEXT(q, 0);
    UnsafeNodeStack latched_nodes;
    NodeBase *node = root;
    if (node->getType() == PageType::BTreeInner) {
      node->writeLock();
    } else {
      node->writeLock(&q);
    }
    latched_nodes.Push(node);
    if (node != root) {
      if (node->getType() == PageType::BTreeInner) {
        node->writeUnlock();
      } else {
        node->writeUnlock(&q);
      }
      goto restart;
    }

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];

      // if [next] is not full, release all held exclusive latches
      // on its ancestors
      bool release_ancestors = false;
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        next->writeLock();
        auto next_inner = static_cast<BTreeInner<Key> *>(next);
        if (!next_inner->isFull()) {
          release_ancestors = true;
        }
      } else {
        // [next] is a leaf node
        next->writeLock(&q);
        auto next_leaf = static_cast<BTreeLeaf<Key, Value> *>(next);
        if (!next_leaf->isFull()) {
          release_ancestors = true;
        }
      }

      if (release_ancestors) {
        while (latched_nodes.Size() > 0) {
          auto n = latched_nodes.Pop();
          n->writeUnlock();
        }
      }
      latched_nodes.Push(next);

      node = next;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // handle splits
      assert(leaf == latched_nodes.Top());
      latched_nodes.Pop();
      Key sep;
      bool ok = leaf->insert(k, v);
      NodeBase *newNode = leaf->split(sep);
      if (latched_nodes.Size() == 0) {
        // [leaf] has to be root
        assert(root == leaf);
        makeRoot(sep, leaf, newNode);
        node->writeUnlock(&q);
      } else {
        // [leaf] has a parent
        node->writeUnlock(&q);
        while (latched_nodes.Size() > 1) {
          auto n = latched_nodes.Pop();
          assert(n->getType() == PageType::BTreeInner);
          auto parent = static_cast<BTreeInner<Key> *>(n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          n->writeUnlock();
        }
        assert(latched_nodes.Size() == 1);
        auto n = latched_nodes.Pop();
        assert(n->getType() == PageType::BTreeInner);
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
        n->writeUnlock();
      }
      return ok;
    } else {
      // no need to split, just insert into [leaf]
      assert(latched_nodes.Size() == 1);
      bool ok = leaf->insert(k, v);
      node->writeUnlock(&q);
      return ok;
    }
  }

  template <LockType ty>
  bool traverseToLeaf(Key k, OMCSLock::Context &q, NodeBase *&node) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);

    node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      if constexpr (ty == Sh) {
        node->readLock(&q);
      } else {
        node->writeLock(&q);
      }
      if (node != root) {
        if constexpr (ty == Sh) {
          node->readUnlock(&q);
        } else {
          node->writeUnlock(&q);
        }
        goto restart;
      }
      return true;
    }
    node->readLock();
    if (node != root) {
      node->readUnlock();
      goto restart;
    }

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];

      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        next->readLock();
      } else {
        // [next] is a leaf node, just take (shared or exclusive) latch
        if constexpr (ty == Sh) {
          next->readLock(&q);
        } else {
          next->writeLock(&q);
        }
      }
      node->readUnlock();

      node = next;
    }

    // We now have a latch on [node]
    return true;
  }

  bool lookup(Key k, Value &result) {
    NodeBase *node = nullptr;
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

  bool insert(Key k, Value v) {
    constexpr int kMaxInsertRetries = 0;
    int restartCount = 0;
  restart:
    if (restartCount++ == kMaxInsertRetries) {
      return insertPessimistically(k, v);
    }
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q, 0);
    traverseToLeaf<Ex>(k, q, node);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // We have bad luck. Unlock [node] and retry the entire traversal,
      // taking exclusive latches along the way
      node->writeUnlock(&q);
      goto restart;
    } else {
      bool ok = leaf->insert(k, v);
      node->writeUnlock(&q);
      return ok;
    }
  }

  bool remove(Key k) {
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q, 0);
    traverseToLeaf<Ex>(k, q, node);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->remove(k);
    node->writeUnlock(&q);
    return ok;
  }

  bool update(Key k, Value v) {
    NodeBase *node = nullptr;
    DEFINE_CONTEXT(q, 0);
    traverseToLeaf<Ex>(k, q, node);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    node->writeUnlock(&q);
    return ok;
  }
};

}  // namespace btreeolc
