#include "Tree.h"

#include <assert.h>

#include <algorithm>
#include <functional>

#include "Key.h"
#include "N.cpp"

namespace ART_OLC {

Tree::Tree(LoadKeyFunction loadKey, RemoveNodeFunction removeNode)
    : root(new N256(nullptr, 0)), loadKey(loadKey), removeNode(removeNode) {}

Tree::~Tree() {
  N::deleteChildren(root);
  N::deleteNode(root);
}

TID Tree::lookup(const Key &k) const {
  DEFINE_CONTEXT(q0, 0);
  DEFINE_CONTEXT(q1, 1);

  OMCSLock::Context *q = &q0;
  OMCSLock::Context *nq = &q1;

restart:
  bool needRestart = false;

  N *node;
  N *parentNode = nullptr;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  node = root;
  READ_LOCK_NODE(node, q);
  if (needRestart) goto restart;
  while (true) {
    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        READ_UNLOCK_NODE(node, q);
        return 0;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
        // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          READ_UNLOCK_NODE(node, q);
          return 0;
        }
        parentNode = node;
        node = N::getChild(k[level], parentNode);

        if (node == nullptr) {
          READ_UNLOCK_NODE(parentNode, q);
          return 0;
        }
        if (N::isLeaf(node)) {
          READ_UNLOCK_NODE(parentNode, q);

          TID tid = N::getLeaf(node);
          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            return checkKey(tid, k);
          }
          return tid;
        }
        level++;
    }
    READ_LOCK_NODE(node, nq);
    if (needRestart) {
      READ_UNLOCK_NODE(parentNode, q);
      goto restart;
    }

    READ_UNLOCK_NODE(parentNode, q);
    std::swap(q, nq);
  }
}

bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                       std::size_t resultSize, std::size_t &resultsFound) const {
  LOG(FATAL) << "Not supported";
  return false;
#if 0
  for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
    if (start[i] > end[i]) {
      resultsFound = 0;
      return false;
    } else if (start[i] < end[i]) {
      break;
    }
  }

  TID toContinue = 0;
  std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue,
                                         &copy](const N *node) {
    if (N::isLeaf(node)) {
      if (resultsFound == resultSize) {
        toContinue = N::getLeaf(node);
        return;
      }
      result[resultsFound] = N::getLeaf(node);
      resultsFound++;
    } else {
      std::tuple<uint8_t, N *> children[256];
      uint32_t childrenCount = 0;
      N::getChildren(node, 0u, 255u, children, childrenCount);
      for (uint32_t i = 0; i < childrenCount; ++i) {
        const N *n = std::get<1>(children[i]);
        copy(n);
        if (toContinue != 0) {
          break;
        }
      }
    }
  };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart =
      [&copy, &start, &findStart, &toContinue, this](N *node, uint8_t nodeK, uint32_t level,
                                                     const N *parentNode, uint64_t vp) {
        if (N::isLeaf(node)) {
          copy(node);
          return;
        }
        uint64_t v;
        PCCompareResults prefixResult;

        {
        readAgain:
          bool needRestart = false;
          v = node->readLockOrRestart(needRestart);
          if (needRestart) goto readAgain;

          prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
          if (needRestart) goto readAgain;

          parentNode->readUnlockOrRestart(vp, needRestart);
          if (needRestart) {
          readParentAgain:
            needRestart = false;
            vp = parentNode->readLockOrRestart(needRestart);
            if (needRestart) goto readParentAgain;

            node = N::getChild(nodeK, parentNode);

            parentNode->readUnlockOrRestart(vp, needRestart);
            if (needRestart) goto readParentAgain;

            if (node == nullptr) {
              return;
            }
            if (N::isLeaf(node)) {
              copy(node);
              return;
            }
            goto readAgain;
          }
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto readAgain;
        }

        switch (prefixResult) {
          case PCCompareResults::Bigger:
            copy(node);
            break;
          case PCCompareResults::Equal: {
            uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
            std::tuple<uint8_t, N *> children[256];
            uint32_t childrenCount = 0;
            v = N::getChildren(node, startLevel, 255, children, childrenCount);
            for (uint32_t i = 0; i < childrenCount; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              N *n = std::get<1>(children[i]);
              if (k == startLevel) {
                findStart(n, k, level + 1, node, v);
              } else if (k > startLevel) {
                copy(n);
              }
              if (toContinue != 0) {
                break;
              }
            }
            break;
          }
          case PCCompareResults::Smaller:
            break;
        }
      };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findEnd =
      [&copy, &end, &toContinue, &findEnd, this](N *node, uint8_t nodeK, uint32_t level,
                                                 const N *parentNode, uint64_t vp) {
        if (N::isLeaf(node)) {
          return;
        }
        uint64_t v;
        PCCompareResults prefixResult;
        {
        readAgain:
          bool needRestart = false;
          v = node->readLockOrRestart(needRestart);
          if (needRestart) goto readAgain;

          prefixResult = checkPrefixCompare(node, end, 255, level, loadKey, needRestart);
          if (needRestart) goto readAgain;

          parentNode->readUnlockOrRestart(vp, needRestart);
          if (needRestart) {
          readParentAgain:
            needRestart = false;
            vp = parentNode->readLockOrRestart(needRestart);
            if (needRestart) goto readParentAgain;

            node = N::getChild(nodeK, parentNode);

            parentNode->readUnlockOrRestart(vp, needRestart);
            if (needRestart) goto readParentAgain;

            if (node == nullptr) {
              return;
            }
            if (N::isLeaf(node)) {
              return;
            }
            goto readAgain;
          }
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto readAgain;
        }
        switch (prefixResult) {
          case PCCompareResults::Smaller:
            copy(node);
            break;
          case PCCompareResults::Equal: {
            uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
            std::tuple<uint8_t, N *> children[256];
            uint32_t childrenCount = 0;
            v = N::getChildren(node, 0, endLevel, children, childrenCount);
            for (uint32_t i = 0; i < childrenCount; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              N *n = std::get<1>(children[i]);
              if (k == endLevel) {
                findEnd(n, k, level + 1, node, v);
              } else if (k < endLevel) {
                copy(n);
              }
              if (toContinue != 0) {
                break;
              }
            }
            break;
          }
          case PCCompareResults::Bigger:
            break;
        }
      };

restart:
  bool needRestart = false;

  resultsFound = 0;

  uint32_t level = 0;
  N *node = nullptr;
  N *nextNode = root;
  N *parentNode;
  uint64_t v = 0;
  uint64_t vp;

  while (true) {
    parentNode = node;
    vp = v;
    node = nextNode;
    PCEqualsResults prefixResult;
    v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;
    prefixResult = checkPrefixEquals(node, level, start, end, loadKey, needRestart);
    if (needRestart) goto restart;
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(vp, needRestart);
      if (needRestart) goto restart;
    }
    node->readUnlockOrRestart(v, needRestart);
    if (needRestart) goto restart;

    switch (prefixResult) {
      case PCEqualsResults::NoMatch: {
        return false;
      }
      case PCEqualsResults::Contained: {
        copy(node);
        break;
      }
      case PCEqualsResults::BothMatch: {
        uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
        uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
        if (startLevel != endLevel) {
          std::tuple<uint8_t, N *> children[256];
          uint32_t childrenCount = 0;
          v = N::getChildren(node, startLevel, endLevel, children, childrenCount);
          for (uint32_t i = 0; i < childrenCount; ++i) {
            const uint8_t k = std::get<0>(children[i]);
            N *n = std::get<1>(children[i]);
            if (k == startLevel) {
              findStart(n, k, level + 1, node, v);
            } else if (k > startLevel && k < endLevel) {
              copy(n);
            } else if (k == endLevel) {
              findEnd(n, k, level + 1, node, v);
            }
            if (toContinue) {
              break;
            }
          }
        } else {
          nextNode = N::getChild(startLevel, node);
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;
          level++;
          continue;
        }
        break;
      }
    }
    break;
  }
  if (toContinue != 0) {
    loadKey(toContinue, continueKey);
    return true;
  } else {
    return false;
  }
#endif
}

bool Tree::lookupRange(const Key &start, TID result[], std::size_t resultSize,
                       std::size_t &resultsFound) const {
  LOG(FATAL) << "Not supported";
  return false;
#if 0
  TID toContinue = 0;
  std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue,
                                         &copy](const N *node) {
    if (N::isLeaf(node)) {
      if (resultsFound == resultSize) {
        toContinue = N::getLeaf(node);
        return;
      }
      result[resultsFound] = N::getLeaf(node);
      resultsFound++;
    } else {
      std::tuple<uint8_t, N *> children[256];
      uint32_t childrenCount = 0;
      N::getChildren(node, 0u, 255u, children, childrenCount);
      for (uint32_t i = 0; i < childrenCount; ++i) {
        const N *n = std::get<1>(children[i]);
        copy(n);
        if (toContinue != 0) {
          break;
        }
      }
    }
  };
  std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart =
      [&copy, &start, &findStart, &toContinue, this](N *node, uint8_t nodeK, uint32_t level,
                                                     const N *parentNode, uint64_t vp) {
        if (N::isLeaf(node)) {
          copy(node);
          return;
        }
        uint64_t v;
        PCCompareResults prefixResult;

        {
        readAgain:
          bool needRestart = false;
          v = node->readLockOrRestart(needRestart);
          if (needRestart) goto readAgain;

          prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
          if (needRestart) goto readAgain;

          parentNode->readUnlockOrRestart(vp, needRestart);
          if (needRestart) {
          readParentAgain:
            needRestart = false;
            vp = parentNode->readLockOrRestart(needRestart);
            if (needRestart) goto readParentAgain;

            node = N::getChild(nodeK, parentNode);

            parentNode->readUnlockOrRestart(vp, needRestart);
            if (needRestart) goto readParentAgain;

            if (node == nullptr) {
              return;
            }
            if (N::isLeaf(node)) {
              copy(node);
              return;
            }
            goto readAgain;
          }
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto readAgain;
        }

        switch (prefixResult) {
          case PCCompareResults::Bigger:
            copy(node);
            break;
          case PCCompareResults::Equal: {
            uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
            std::tuple<uint8_t, N *> children[256];
            uint32_t childrenCount = 0;
            v = N::getChildren(node, startLevel, 255, children, childrenCount);
            for (uint32_t i = 0; i < childrenCount; ++i) {
              const uint8_t k = std::get<0>(children[i]);
              N *n = std::get<1>(children[i]);
              if (k == startLevel) {
                findStart(n, k, level + 1, node, v);
              } else if (k > startLevel) {
                copy(n);
              }
              if (toContinue != 0) {
                break;
              }
            }
            break;
          }
          case PCCompareResults::Smaller:
            break;
        }
      };

restart:
  bool needRestart = false;

  resultsFound = 0;

  uint32_t level = 0;
  N *node = nullptr;
  N *nextNode = root;
  N *parentNode;
  uint64_t v = 0;
  uint64_t vp;

  while (true) {
    parentNode = node;
    vp = v;
    node = nextNode;
    PCCompareResults compareResult;
    v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;
    compareResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
    if (needRestart) goto restart;
    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(vp, needRestart);
      if (needRestart) goto restart;
    }
    node->readUnlockOrRestart(v, needRestart);
    if (needRestart) goto restart;

    switch (compareResult) {
      case PCCompareResults::Smaller: {
        return false;
      }
      case PCCompareResults::Equal:
      case PCCompareResults::Bigger: {
        uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
        uint8_t endLevel = 255;
        if (startLevel != endLevel) {
          std::tuple<uint8_t, N *> children[256];
          uint32_t childrenCount = 0;
          v = N::getChildren(node, startLevel, endLevel, children, childrenCount);
          for (uint32_t i = 0; i < childrenCount; ++i) {
            const uint8_t k = std::get<0>(children[i]);
            N *n = std::get<1>(children[i]);
            if (k == startLevel) {
              findStart(n, k, level + 1, node, v);
            } else if (k > startLevel) {
              copy(n);
            }
            if (toContinue) {
              break;
            }
          }
        } else {
          nextNode = N::getChild(startLevel, node);
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;
          level++;
          continue;
        }
        break;
      }
    }
    break;
  }

  return false;
#endif
}

TID Tree::checkKey(const TID tid, const Key &k) const {
  Key kt;
  this->loadKey(tid, kt);
  if (k == kt) {
    return tid;
  }
  return 0;
}

bool Tree::insertPessimistic(const Key &k, TID tid) {
  DEFINE_CONTEXT(q0, 0);
  DEFINE_CONTEXT(q1, 1);

  OMCSLock::Context *q = &q0;
  OMCSLock::Context *pq = &q1;

restart:
  bool needRestart = false;

  N *node = nullptr;
  N *nextNode = root;
  N *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    std::swap(q, pq);
    LOCK_NODE(node, q);
    if (needRestart) {
      if (parentNode != nullptr) {
        UNLOCK_NODE(parentNode, pq);
      }
      goto restart;
    }

    uint32_t nextLevel = level;

    uint8_t nonMatchingKey;
    Prefix remainingPrefix;
    auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                      this->loadKey, needRestart);  // increases level
    if (needRestart) {
      if (parentNode != nullptr) {
        UNLOCK_NODE(parentNode, pq);
      }
      goto restart;
    }
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        // 1) Create new node which will be parent of node, Set common prefix, level to this node
        auto newNode = new N4(node->getPrefix(), nextLevel - level);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[nextLevel], N::setLeaf(tid));
        newNode->insert(nonMatchingKey, node);

        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
        N::change(parentNode, parentKey, newNode);
        UNLOCK_NODE(parentNode, pq);

        // 4) update prefix of node, unlock
        node->setPrefix(remainingPrefix, node->getPrefixLength() - ((nextLevel - level) + 1));

        UNLOCK_NODE(node, q);
        return true;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = nextLevel;
    nodeKey = k[level];
    nextNode = N::getChild(nodeKey, node);

    if (nextNode == nullptr) {
      N *obsoleteN = nullptr;
      N::insertAndUnlockPessimistic(node, parentNode, q, pq, parentKey, nodeKey, N::setLeaf(tid),
                                    needRestart, obsoleteN);
      if (needRestart) goto restart;
      if (obsoleteN) {
        this->removeNode(obsoleteN);
      }
      return true;
    }

    if (parentNode != nullptr) {
      UNLOCK_NODE(parentNode, pq);
    }

    if (N::isLeaf(nextNode)) {
      Key key;
      loadKey(N::getLeaf(nextNode), key);

      if (key == k) {
        // key already exists
        UNLOCK_NODE(node, q);
        return false;
      }

      level++;
      uint32_t prefixLength = 0;
      while (key[level + prefixLength] == k[level + prefixLength]) {
        prefixLength++;
      }

      auto n4 = new N4(&k[level], prefixLength);
      n4->insert(k[level + prefixLength], N::setLeaf(tid));
      n4->insert(key[level + prefixLength], nextNode);
      N::change(node, k[level - 1], n4);
      UNLOCK_NODE(node, q);
      return true;
    }
    level++;
  }
}

bool Tree::traverseToLeafEx(const Key &k, OMCSLock::Context *q, OMCSLock::Context *nq,
                            bool &found_out, N *&parentNode_out, uint32_t &level_out,
                            OMCSLock::Context *&q_out) {
restart:
  bool needRestart = false;

  N *node;
  N *parentNode = nullptr;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;
  bool exLockAcquired = false;

  node = root;
  READ_LOCK_NODE(node, q);
  if (needRestart) goto restart;
  while (true) {
    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        READ_UNLOCK_NODE(node, q);
        found_out = false;
        return true;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
        // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          READ_UNLOCK_NODE(node, q);
          found_out = false;
          return true;
        }
        parentNode = node;
        node = N::getChild(k[level], parentNode);

        if (node == nullptr) {
          // nonexistent key
          if (exLockAcquired) {
            UNLOCK_NODE(parentNode, q);
          } else {
            READ_UNLOCK_NODE(parentNode, q);
          }
          found_out = false;
          return true;
        }
        if (N::isLeaf(node)) {
          TID old_tid = N::getLeaf(node);
          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            if (checkKey(old_tid, k) != old_tid) {
              if (exLockAcquired) {
                UNLOCK_NODE(parentNode, q);
              } else {
                READ_UNLOCK_NODE(parentNode, q);
              }
              found_out = false;
              return true;
            }
          }

          if (exLockAcquired) {
            // Lock was already taken
            found_out = true;
            parentNode_out = parentNode;
            level_out = level;
            q_out = q;
            return true;
          }

          // We want to upgrade the lock on parentNode from
          // shared to exclusive, but since we cannot do that,
          // we need to fall back to the pessimistic approach
          READ_UNLOCK_NODE(parentNode, q);
          return false;
        }
        level++;
    }
    if (level == k.getKeyLen() - 1) {
      // [node] is at the last level of the ART that corresponds to an ART_OLC::N
      // I should lock [node] exclusively
      LOCK_NODE(node, nq);
      exLockAcquired = true;
    } else {
      READ_LOCK_NODE(node, nq);
    }
    if (needRestart) {
      READ_UNLOCK_NODE(parentNode, q);
      goto restart;
    }

    READ_UNLOCK_NODE(parentNode, q);
    std::swap(q, nq);
  }
}

bool Tree::traverseToLeafPessimisticEx(const Key &k, OMCSLock::Context *q, OMCSLock::Context *nq,
                                       N *&parentNode_out, uint32_t &level_out,
                                       OMCSLock::Context *&q_out) {
restart:
  bool needRestart = false;

  N *node;
  N *parentNode = nullptr;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  node = root;
  LOCK_NODE(node, q);
  if (needRestart) goto restart;
  while (true) {
    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        UNLOCK_NODE(node, q);
        return false;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
        // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          UNLOCK_NODE(node, q);
          return false;
        }
        parentNode = node;
        node = N::getChild(k[level], parentNode);

        if (node == nullptr) {
          UNLOCK_NODE(parentNode, q);
          return false;
        }
        if (N::isLeaf(node)) {
          TID old_tid = N::getLeaf(node);
          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            if (checkKey(old_tid, k) != old_tid) {
              UNLOCK_NODE(parentNode, q);
              return false;
            }
          }

          parentNode_out = parentNode;
          level_out = level;
          q_out = q;
          return true;
        }
        level++;
    }
    LOCK_NODE(node, nq);
    if (needRestart) {
      UNLOCK_NODE(parentNode, q);
      goto restart;
    }

    UNLOCK_NODE(parentNode, q);
    std::swap(q, nq);
  }
}

void Tree::tryExpand(const Key &k, N *parentNode, uint32_t level, N *tid) {
  if (rng.next() >= Tree::kSampleProb) {
    return;
  }

  parentNode->hotness++;
  if (parentNode->hotness <= Tree::kHotnessThreshold) {
    return;
  }

expand:
  parentNode->hotness = 0;

  N *leaf = nullptr;
  if (level + 1 <= k.getKeyLen() - 2) {
    uint32_t prefixLength = k.getKeyLen() - 2 - (level + 1);
    auto n4p = new N4(&k[level + 1], prefixLength);
    auto n4c = new N4(&k[level + 1 + prefixLength], 0);
    n4p->insert(k[level + 1 + prefixLength], n4c);
    n4c->insert(k[level + 1 + prefixLength + 1], tid);
    leaf = n4p;
  } else if (level + 1 == k.getKeyLen() - 1) {
    auto n4 = new N4(&k[level + 1], 0);
    n4->insert(k[level + 1], tid);
    leaf = n4;
  }

  if (leaf) {
    N::change(parentNode, k[level], leaf);
  }
}

bool Tree::insert(const Key &k, TID tid) { return insertPessimistic(k, tid); }

bool Tree::update(const Key &k, TID new_tid) {
  DEFINE_CONTEXT(q0, 0);
  DEFINE_CONTEXT(q1, 1);
  OMCSLock::Context *q = nullptr;

  bool found = false;
  N *parentNode = nullptr;
  uint32_t level = 0;
  bool shouldExpand = false;

  if (!traverseToLeafEx(k, &q0, &q1, found, parentNode, level, q)) {
    shouldExpand = true;
    found = traverseToLeafPessimisticEx(k, &q0, &q1, parentNode, level, q);
  }
  if (!found) {
    return false;
  }
  N *tid = N::setLeaf(new_tid);
  N::change(parentNode, k[level], tid);

  if (shouldExpand) {
    tryExpand(k, parentNode, level, tid);
  }

  UNLOCK_NODE(parentNode, q);
  return true;
}

bool Tree::remove(const Key &k, TID tid) {
  LOG(FATAL) << "Not supported";
  return false;
#if 0
restart:
  bool needRestart = false;

  N *node = nullptr;
  N *nextNode = root;
  N *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        node->readUnlockOrRestart(v, needRestart);
        if (needRestart) goto restart;
        return false;
      case CheckPrefixResult::OptimisticMatch:
        // fallthrough
      case CheckPrefixResult::Match: {
        nodeKey = k[level];
        nextNode = N::getChild(nodeKey, node);

        node->checkOrRestart(v, needRestart);
        if (needRestart) goto restart;

        if (nextNode == nullptr) {
          node->readUnlockOrRestart(v, needRestart);
          if (needRestart) goto restart;
          return false;
        }
        if (N::isLeaf(nextNode)) {
          if (N::getLeaf(nextNode) != tid) {
            return false;
          }
          assert(parentNode == nullptr || node->getCount() != 1);
          if (node->getCount() == 2 && parentNode != nullptr) {
            DEFINE_CONTEXT(parentQ, 1);
            DEFINE_CONTEXT(q, 0);
            UPGRADE_PARENT();

            if (needRestart) goto restart;

            UPGRADE_NODE(node);
            if (needRestart) {
              UNLOCK_PARENT();
              goto restart;
            }
            // 1. check remaining entries
            N *secondNodeN;
            uint8_t secondNodeK;
            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
            if (N::isLeaf(secondNodeN)) {
              // N::remove(node, k[level]); not necessary
              N::change(parentNode, parentKey, secondNodeN);

              UNLOCK_PARENT();
              node->setObsolete();
              UNLOCK_NODE(node);
              this->removeNode(node);
            } else {
              DEFINE_CONTEXT(secondQ, 2);
#if defined(IS_CONTEXTFUL)
              secondNodeN->writeLockOrRestart(&secondQ, needRestart);
#else
              secondNodeN->writeLockOrRestart(needRestart);
#endif
              if (needRestart) {
                UNLOCK_NODE(node);
                UNLOCK_PARENT();
                goto restart;
              }

              // N::remove(node, k[level]); not necessary
              N::change(parentNode, parentKey, secondNodeN);
              UNLOCK_PARENT();

              secondNodeN->addPrefixBefore(node, secondNodeK);
#if defined(IS_CONTEXTFUL)
              secondNodeN->writeUnlock(&secondQ);
#else
              secondNodeN->writeUnlock();
#endif

              node->setObsolete();
              UNLOCK_NODE(node);
              this->removeNode(node);
            }
          } else {
            N *obsoleteN = nullptr;
            N::removeAndUnlock(node, v, k[level], parentNode, parentVersion, parentKey, needRestart,
                               obsoleteN);
            if (needRestart) goto restart;
            if (obsoleteN) {
              this->removeNode(obsoleteN);
            }
          }
          return true;
        }
        level++;
        parentVersion = v;
      }
    }
  }
#endif
}

inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
  if (n->hasPrefix()) {
    if (k.getKeyLen() <= level + n->getPrefixLength()) {
      return CheckPrefixResult::NoMatch;
    }
    for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
      if (n->getPrefix()[i] != k[level]) {
        return CheckPrefixResult::NoMatch;
      }
      ++level;
    }
    if (n->getPrefixLength() > maxStoredPrefixLength) {
      level = level + (n->getPrefixLength() - maxStoredPrefixLength);
      return CheckPrefixResult::OptimisticMatch;
    }
  }
  return CheckPrefixResult::Match;
}

typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(
    N *n, const Key &k, uint32_t &level, uint8_t &nonMatchingKey, Prefix &nonMatchingPrefix,
    LoadKeyFunction loadKey, bool &needRestart) {
  uint32_t prefixLen = n->getPrefixLength();

  if (prefixLen) {
    uint32_t prevLevel = level;
    Key kt;
    for (uint32_t i = 0; i < prefixLen; ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(n, needRestart);
        if (needRestart) return CheckPrefixPessimisticResult::Match;
        loadKey(anyTID, kt);
      }
      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey != k[level]) {
        nonMatchingKey = curKey;
        if (prefixLen > maxStoredPrefixLength) {
          if (i < maxStoredPrefixLength) {
            auto anyTID = N::getAnyChildTid(n, needRestart);
            if (needRestart) return CheckPrefixPessimisticResult::Match;
            loadKey(anyTID, kt);
          }
          memcpy(nonMatchingPrefix, &kt[0] + level + 1,
                 std::min((prefixLen - (level - prevLevel) - 1), maxStoredPrefixLength));
        } else {
          memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, prefixLen - i - 1);
        }
        return CheckPrefixPessimisticResult::NoMatch;
      }
      ++level;
    }
  }
  return CheckPrefixPessimisticResult::Match;
}

typename Tree::PCCompareResults Tree::checkPrefixCompare(N *n, const Key &k, uint8_t fillKey,
                                                         uint32_t &level, LoadKeyFunction loadKey,
                                                         bool &needRestart) {
  if (n->hasPrefix()) {
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(n, needRestart);
        if (needRestart) return PCCompareResults::Equal;
        loadKey(anyTID, kt);
      }
      uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey < kLevel) {
        return PCCompareResults::Smaller;
      } else if (curKey > kLevel) {
        return PCCompareResults::Bigger;
      }
      ++level;
    }
  }
  return PCCompareResults::Equal;
}

typename Tree::PCEqualsResults Tree::checkPrefixEquals(N *n, uint32_t &level, const Key &start,
                                                       const Key &end, LoadKeyFunction loadKey,
                                                       bool &needRestart) {
  if (n->hasPrefix()) {
    Key kt;
    for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(n, needRestart);
        if (needRestart) return PCEqualsResults::BothMatch;
        loadKey(anyTID, kt);
      }
      uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
      uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey > startLevel && curKey < endLevel) {
        return PCEqualsResults::Contained;
      } else if (curKey < startLevel || curKey > endLevel) {
        return PCEqualsResults::NoMatch;
      }
      ++level;
    }
  }
  return PCEqualsResults::BothMatch;
}
}  // namespace ART_OLC
