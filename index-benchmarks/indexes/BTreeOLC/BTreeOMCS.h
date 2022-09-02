#pragma once

// Do not include this file together with other headers defining this macro.
#if defined(BTREE_SYNC_IMPL)
#error "BTree synchronization implementation is defined multiple times."
#endif
#define BTREE_SYNC_IMPL

#include "BTreeCommon.h"

// This implementation uses OMCS on all nodes.

namespace btreeolc {
template <class Key, class Value>
struct BTreeOMCS : public BTreeBase<Key, Value> {
  using BTreeBase<Key, Value>::root;
  using BTreeBase<Key, Value>::yield;
  using BTreeBase<Key, Value>::makeRoot;
  using BTreeBase<Key, Value>::lookup;
  using BTreeBase<Key, Value>::scan;

  BTreeOMCS() {
    std::cout << "========================================" << std::endl;
    std::cout << "BTree with OMCS." << std::endl;
    std::cout << "Optimistic lock impl: " << OMCSLock::name << std::endl;
    std::cout << "Page size (bytes): " << pageSize << std::endl;
    std::cout << "Max #entries: Leaf: " << BTreeLeaf<Key, Value>::maxEntries
              << ", Inner: " << BTreeInner<Key>::maxEntries << std::endl;
#if defined(PESSIMISTIC_LOCK_COUPLING_INSERT)
    std::cout << "Using top-down lock coupling for SMOs." << std::endl;
#else
    std::cout << "Using bottom-up lock upgrading for SMOs." << std::endl;
#endif
    std::cout << "========================================" << std::endl;
    root = new BTreeLeaf<Key, Value>();
  }

  struct UnsafeOptimisticallyReadNodeStack {
   private:
    std::pair<NodeBase *, uint64_t> nodes[kMaxLevels];
    OMCSLock::Context qnodes[kMaxLevels];
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
    OMCSLock::Context &TopQNode() {
      assert(top > 0);
      return qnodes[top - 1];
    }
    size_t Size() const { return top; }
    void UpgradeAll(bool &needRestart) {
      // Upgrade latches bottom-up. Upon any validation failure,
      // exclusively-latched nodes are released top-down.
      for (size_t i = top; i; i--) {
        auto [n, version] = nodes[i - 1];
        auto &q = qnodes[i - 1];
        n->upgradeToWriteLockOrRestart(version, &q, needRestart);
        if (needRestart) {
          for (size_t j = i; j < top; j++) {
            nodes[j].first->writeUnlock(&qnodes[j]);
          }
          return;
        }
      }
    }
  };

  struct UnsafeXLatchedNodeDeque {
   private:
    NodeBase *nodes[kMaxLevels];
    OMCSLock::Context qnodes[kMaxLevels];
    size_t head = 0;
    size_t tail = 0;

   public:
    OMCSLock::Context *Enqueue(NodeBase *node) {
      assert(tail < kMaxLevels);
      nodes[tail] = node;
      return &qnodes[tail++];
    }
    std::pair<NodeBase *, OMCSLock::Context *> DequeueHead() {
      assert(head < tail);
      auto node = nodes[head];
      auto q = &qnodes[head++];
      return {node, q};
    }
    std::pair<NodeBase *, OMCSLock::Context *> DequeueTail() {
      assert(head < tail);
      auto node = nodes[--tail];
      auto q = &qnodes[tail];
      return {node, q};
    }
    NodeBase *Tail() const {
      assert(head < tail);
      return nodes[tail - 1];
    }
    size_t Size() const { return tail - head; }
  };

  bool insertOptimistically(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    OMCSLock::Context q;
    UnsafeOptimisticallyReadNodeStack read_nodes;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    NodeBase *node = root;
    if (node->getType() == PageType::BTreeInner) {
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    } else {
      node->writeLock(&q);
    }
    read_nodes.Push(node, versionNode);
    if (node != root) {
      if (node->getType() == PageType::BTreeLeaf) {
        node->writeUnlock(&q);
      }
      goto restart;
    }

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      // if [next] is not full, drop all remembered versions
      // on its ancestors
      bool release_ancestors = false;
      uint64_t versionNext = OMCSLock::kInvalidVersion;
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        versionNext = next->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
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
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (next->getType() == PageType::BTreeLeaf) {
          next->writeUnlock(&q);
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

    assert(versionNode == OMCSLock::kInvalidVersion);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // handle splits
      assert(leaf == read_nodes.Top().first);
      read_nodes.Pop();
      read_nodes.UpgradeAll(needRestart);
      if (needRestart) {
        node->writeUnlock(&q);
        goto restart;
      }
      Key sep;
      bool ok = leaf->insert(k, v);
      NodeBase *newNode = leaf->split(sep);
      if (read_nodes.Size() == 0) {
        // [leaf] has to be root
        assert(root == leaf);
        makeRoot(sep, leaf, newNode);
        node->writeUnlock(&q);
      } else {
        // [leaf] has a parent
        node->writeUnlock(&q);
        while (read_nodes.Size() > 1) {
          auto &parentQ = read_nodes.TopQNode();
          auto [n, version] = read_nodes.Pop();
          assert(n->getType() == PageType::BTreeInner);
          auto parent = static_cast<BTreeInner<Key> *>(n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          n->writeUnlock(&parentQ);
        }
        assert(read_nodes.Size() == 1);
        auto &parentQ = read_nodes.TopQNode();
        auto [n, version] = read_nodes.Pop();
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
        n->writeUnlock(&parentQ);
      }
      return ok;
    } else {
      // no need to split, just insert into [leaf]
      assert(read_nodes.Size() == 1);
      bool ok = leaf->insert(k, v);
      node->writeUnlock(&q);
      return ok;
    }
  }

  bool insertPessimistically(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);

    UnsafeXLatchedNodeDeque latched_nodes;
    NodeBase *node = root;
    OMCSLock::Context *q = latched_nodes.Enqueue(node);
    node->writeLock(q);
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
      OMCSLock::Context *q = latched_nodes.Enqueue(next);
      next->writeLock(q);
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
        while (latched_nodes.Size() > 1) {
          auto [n, q] = latched_nodes.DequeueHead();
          n->writeUnlock(q);
        }
      }

      node = next;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // handle splits
      assert(leaf == latched_nodes.Tail());
      auto [n, q] = latched_nodes.DequeueTail();
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
          auto [n, parentQ] = latched_nodes.DequeueTail();
          assert(n->getType() == PageType::BTreeInner);
          auto parent = static_cast<BTreeInner<Key> *>(n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          n->writeUnlock(parentQ);
        }
        assert(latched_nodes.Size() == 1);
        auto [n, parentQ] = latched_nodes.DequeueTail();
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
        n->writeUnlock(parentQ);
      }
      return ok;
    } else {
      // no need to split, just insert into [leaf]
      assert(latched_nodes.Size() == 1);
      auto [n, q] = latched_nodes.DequeueTail();
      bool ok = leaf->insert(k, v);
      node->writeUnlock(q);
      return ok;
    }
  }

  NodeBase *traverseToLeafEx(Key k, OMCSLock::Context &q) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    uint64_t versionNode = OMCSLock::kInvalidVersion;
    NodeBase *node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      node->writeLock(&q);
      if (node != root) {
        node->writeUnlock(&q);
        goto restart;
      }
      return node;
    }
    versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || node != root) goto restart;

    while (node->getType() == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key> *>(node);

      NodeBase *next = inner->children[inner->lowerBound(k)];
      node->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      uint64_t versionNext = OMCSLock::kInvalidVersion;
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        versionNext = next->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
      } else {
        // [next] is a leaf node, just take exclusive latch
        next->writeLock(&q);
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (next->getType() == PageType::BTreeLeaf) {
          next->writeUnlock(&q);
        }
        goto restart;
      }

      node = next;
      versionNode = versionNext;
    }

    assert(versionNode == OMCSLock::kInvalidVersion);
    // We now have exclusive latch on [node]
    return node;
  }

  bool insert(Key k, Value v) {
#if not defined(PESSIMISTIC_LOCK_COUPLING_INSERT)
    return insertOptimistically(k, v);
#else
    int restartCount = 0;
  restart:
    if (restartCount++ == kMaxInsertRetries) {
      return insertPessimistically(k, v);
    }
    OMCSLock::Context q;
    NodeBase *node = traverseToLeafEx(k, q);
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
#endif
  }

  bool remove(Key k) {
    OMCSLock::Context q;
    NodeBase *node = traverseToLeafEx(k, q);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->remove(k);
    node->writeUnlock(&q);
    return ok;
  }

  bool update(Key k, Value v) {
    OMCSLock::Context q;
    NodeBase *node = traverseToLeafEx(k, q);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    node->writeUnlock(&q);
    return ok;
  }
};

}  // namespace btreeolc
