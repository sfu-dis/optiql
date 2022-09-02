//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_H
#define ART_OPTIMISTICLOCK_COUPLING_N_H
#include <limits>

#include "N.h"
#include "common/random.h"

namespace ART_OLC {

class Tree {
 public:
  using LoadKeyFunction = void (*)(TID tid, Key &key);
  using RemoveNodeFunction = void (*)(void *);

  // XXX(shiges): tuning parameters
  inline static constexpr uint32_t kSampleProb = 0.1 * std::numeric_limits<uint32_t>::max();
  inline static constexpr uint32_t kHotnessThreshold = 1024;

 private:
  N *const root;

  TID checkKey(const TID tid, const Key &k) const;

  LoadKeyFunction loadKey;
  RemoveNodeFunction removeNode;
  uniform_random_generator rng;

 public:
  enum class CheckPrefixResult : uint8_t { Match, NoMatch, OptimisticMatch };

  enum class CheckPrefixPessimisticResult : uint8_t {
    Match,
    NoMatch,
  };

  enum class PCCompareResults : uint8_t {
    Smaller,
    Equal,
    Bigger,
  };
  enum class PCEqualsResults : uint8_t { BothMatch, Contained, NoMatch };
  static CheckPrefixResult checkPrefix(N *n, const Key &k, uint32_t &level);

  static CheckPrefixPessimisticResult checkPrefixPessimistic(
      N *n, uint64_t v, const Key &k, uint32_t &level, uint8_t &nonMatchingKey,
      Prefix &nonMatchingPrefix, LoadKeyFunction loadKey, bool &needRestart);

  static PCCompareResults checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey,
                                             uint32_t &level, LoadKeyFunction loadKey,
                                             bool &needRestart);

  static PCEqualsResults checkPrefixEquals(const N *n, uint32_t &level, const Key &start,
                                           const Key &end, LoadKeyFunction loadKey,
                                           bool &needRestart);

#if defined(IS_CONTEXTFUL)
  bool traverseToLeafEx(const Key &k, OMCSLock::Context &q, N *&parentNode, uint32_t &level,
                        bool &upgraded);
  bool traverseToLeafUpgradeEx(const Key &k, OMCSLock::Context &q, N *&parentNode, uint32_t &level);
  bool traverseToLeafAcquireEx(const Key &k, OMCSLock::Context qnodes[], N *&parentNode,
                               uint32_t &level, OMCSLock::Context *&q);
#else
  bool traverseToLeafEx(const Key &k, N *&parentNode, uint32_t &level, bool &upgraded);
  bool traverseToLeafUpgradeEx(const Key &k, N *&parentNode, uint32_t &level);
  bool traverseToLeafAcquireEx(const Key &k, N *&parentNode, uint32_t &level);
#endif

  void tryExpand(const Key &k, N *parentNode, uint32_t level, N *tid);

 public:
  Tree(LoadKeyFunction loadKey, RemoveNodeFunction removeNode);

  Tree(const Tree &) = delete;

  Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) {}

  ~Tree();

  TID lookup(const Key &k) const;

  bool lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                   std::size_t resultLen, std::size_t &resultCount) const;

  bool lookupRange(const Key &start, TID result[], std::size_t resultLen,
                   std::size_t &resultCount) const;

  bool insert(const Key &k, TID tid);

  bool update(const Key &k, TID tid);

#if 0
  bool update(const Key &k, const char *value, size_t value_sz);

  bool update(const Key &k);
#endif

#ifdef ART_UPSERT
  bool upsert(const Key &k, TID tid);
#endif

  bool remove(const Key &k, TID tid);
};
}  // namespace ART_OLC
#endif  // ART_OPTIMISTICLOCK_COUPLING_N_H
