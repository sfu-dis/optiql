#pragma once

// Do not include this file together with other headers defining this macro.
#if defined(BTREE_SYNC_IMPL)
#error "BTree synchronization implementation is defined multiple times."
#endif
#define BTREE_SYNC_IMPL

#include "BTreeCommon.h"
#include "latches/OMCSOffset.h"

// This implementation uses OMCS only on leaf nodes.

#if defined(OMCS_OFFSET)
#define DEFINE_CONTEXT(q, i) OMCSLock::Context &q = *offset::get_qnode(i)
#else
#define DEFINE_CONTEXT(q, i) OMCSLock::Context q
#endif

namespace btreeolc {
template <class Key, class Value>
struct BTreeOMCSLeaf : public BTreeBase<Key, Value> {
  using BTreeBase<Key, Value>::root;
  using BTreeBase<Key, Value>::yield;
  using BTreeBase<Key, Value>::makeRoot;
  using BTreeBase<Key, Value>::lookup;
  using BTreeBase<Key, Value>::scan;

  BTreeOMCSLeaf() {
    std::cout << "========================================" << std::endl;
    std::cout << "BTree with OMCS on leaf nodes." << std::endl;
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
        n->upgradeToWriteLockOrRestart(version, needRestart);
        if (needRestart) {
          for (size_t j = i; j < top; j++) {
            nodes[j].first->writeUnlock(nodes[j].second);
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
          auto [n, version] = read_nodes.Pop();
          assert(n->getType() == PageType::BTreeInner);
          auto parent = static_cast<BTreeInner<Key> *>(n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          n->writeUnlock(version);
        }
        assert(read_nodes.Size() == 1);
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
        n->writeUnlock(version);
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

    DEFINE_CONTEXT(q, 0);
    UnsafeNodeStack latched_nodes;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    NodeBase *node = root;
    if (node->getType() == PageType::BTreeInner) {
      versionNode = node->writeLock();
    } else {
      node->writeLock(&q);
    }
    latched_nodes.Push(node, versionNode);
    if (node != root) {
      if (node->getType() == PageType::BTreeInner) {
        node->writeUnlock(versionNode);
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
      uint64_t versionNext = OMCSLock::kInvalidVersion;
      if (next->getType() == PageType::BTreeInner) {
        // [next] is an inner node
        versionNext = next->writeLock();
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
          auto [n, version] = latched_nodes.Pop();
          n->writeUnlock(version);
        }
      }
      latched_nodes.Push(next, versionNext);

      node = next;
      versionNode = versionNext;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    if (leaf->isFull()) {
      // handle splits
      assert(leaf == latched_nodes.Top().first);
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
          auto [n, version] = latched_nodes.Pop();
          assert(n->getType() == PageType::BTreeInner);
          auto parent = static_cast<BTreeInner<Key> *>(n);
          parent->insert(sep, newNode);
          newNode = parent->split(sep);
          n->writeUnlock(version);
        }
        assert(latched_nodes.Size() == 1);
        auto [n, version] = latched_nodes.Pop();
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
        n->writeUnlock(version);
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

  bool traverseToLeafEx(Key k, OMCSLock::Context &q, NodeBase *&node, uint64_t &versionNode) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    versionNode = OMCSLock::kInvalidVersion;
    node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      node->writeLock(&q);
      if (node != root) {
        node->writeUnlock(&q);
        goto restart;
      }
      return true;
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

    // We now have exclusive latch on [node]
    return true;
  }

#if defined(OMCS_OP_READ_NEW_API)
  bool traverseToLeafExNewAPI(Key k, OMCSLock::Context &q, NodeBase *&node, uint64_t &versionNode, bool &opread) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    versionNode = OMCSLock::kInvalidVersion;
    node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      opread = node->writeLockBegin(&q);
      if (node != root) {
        if (opread) {
          node->writeLockTurnOffOpRead();
        }
        node->writeUnlock(&q);
        goto restart;
      }
      return true;
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
        opread = next->writeLockBegin(&q);
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (next->getType() == PageType::BTreeLeaf) {
          if (opread) {
            next->writeLockTurnOffOpRead();
          }
          next->writeUnlock(&q);
        }
        goto restart;
      }

      node = next;
      versionNode = versionNext;
    }

    // We now have exclusive latch on [node]
    return true;
  }
#endif

  bool insert(Key k, Value v) {
#if not defined(PESSIMISTIC_LOCK_COUPLING_INSERT)
    return insertOptimistically(k, v);
#else
    int restartCount = 0;
  restart:
    if (restartCount++ == kMaxInsertRetries) {
      return insertPessimistically(k, v);
    }
    NodeBase *node = nullptr;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    DEFINE_CONTEXT(q, 0);
    traverseToLeafEx(k, q, node, versionNode);
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
    NodeBase *node = nullptr;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    DEFINE_CONTEXT(q, 0);
    traverseToLeafEx(k, q, node, versionNode);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->remove(k);
    node->writeUnlock(&q);
    return ok;
  }

#if !defined(OMCS_OP_READ_NEW_API) && !defined(OMCS_OP_READ_NEW_API_CALLBACK)
  bool update(Key k, Value v) {
    NodeBase *node = nullptr;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    DEFINE_CONTEXT(q, 0);
    traverseToLeafEx(k, q, node, versionNode);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    node->writeUnlock(&q);
    return ok;
  }
#elif defined(OMCS_OP_READ_NEW_API)
#if defined(OMCS_OP_READ_NEW_API_BASELINE)
  bool update(Key k, Value v) {
    NodeBase *node = nullptr;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    bool opread = false;
    DEFINE_CONTEXT(q, 0);
    traverseToLeafExNewAPI(k, q, node, versionNode, opread);
    if (opread) {
      node->writeLockTurnOffOpRead();
    }
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    node->writeUnlock(&q);
    return ok;
  }
#else
  bool update(Key k, Value v) {
    NodeBase *node = nullptr;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    bool opread = false;
    DEFINE_CONTEXT(q, 0);
    traverseToLeafExNewAPI(k, q, node, versionNode, opread);
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    bool ok = leaf->update(k, v, opread);
    node->writeUnlock(&q);
    return ok;
  }
#endif
#else
  bool update(Key k, Value v) {
    bool ok = false;
    unsigned pos = 0;

    NodeBase *node = nullptr;
    uint64_t versionNode = OMCSLock::kInvalidVersion;
    DEFINE_CONTEXT(q, 0);

    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    versionNode = OMCSLock::kInvalidVersion;
    node = root;
    if (node->getType() == PageType::BTreeLeaf) {
      // The root node is a leaf node.
      node->writeLockWithRead(&q, [&]() {
        pos = 0;
        ok = false;
        auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
        assert(leaf->count <= (BTreeLeaf<Key, Value>::maxEntries));
        if (leaf->count) {
          pos = leaf->lowerBound(k);
          if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
            ok = true;
          }
        }
      });
      if (node != root) {
        node->writeUnlock(&q);
        goto restart;
      }
      goto do_update;
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
        next->writeLockWithRead(&q, [&]() {
          pos = 0;
          ok = false;
          auto leaf = static_cast<BTreeLeaf<Key, Value> *>(next);
          assert(leaf->count <= (BTreeLeaf<Key, Value>::maxEntries));
          if (leaf->count) {
            pos = leaf->lowerBound(k);
            if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
              ok = true;
            }
          }
        });
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

    // We now have exclusive latch on [node]
  do_update:
    auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
    // Update
    if (ok) {
      leaf->data[pos].second = v;
    }
    node->writeUnlock(&q);
    return ok;
  }
#endif
};

}  // namespace btreeolc
