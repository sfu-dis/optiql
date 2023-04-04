#pragma once

#include <glog/logging.h>

#include "latches/OMCSOffset.h"
#if defined(BTREE_OL_CENTRALIZED)
#include "indexes/BTreeOLC/BTreeOLC.h"
using BTree = btreeolc::BTreeOLC<uint64_t, uint64_t>;
#elif defined(BTREE_OLC_UPGRADE)
#include "indexes/BTreeOLC/BTreeOLCNB.h"
using BTree = btreeolc::BTreeOLC<uint64_t, uint64_t>;
#elif defined(BTREE_OMCS_LEAF_ONLY)
#include "indexes/BTreeOLC/BTreeOMCSLeaf.h"
using BTree = btreeolc::BTreeOMCSLeaf<uint64_t, uint64_t>;
#elif defined(BTREE_OMCS_ALL)
#include "indexes/BTreeOLC/BTreeOMCS.h"
using BTree = btreeolc::BTreeOMCS<uint64_t, uint64_t>;
#elif defined(BTREE_RWLOCK)
#include "indexes/BTreeOLC/BTreeLC.h"
using BTree = btreeolc::BTreeLC<uint64_t, uint64_t>;
#elif defined(BTREE_RWLOCK_MCSRW_ONLY)
#include "indexes/BTreeOLC/BTreeLCMCSRWOnly.h"
using BTree = btreeolc::BTreeLC<uint64_t, uint64_t>;
#elif defined(BTREE_OLC_HYBRID)
#include "indexes/BTreeOLC/BTreeOLCHybrid.h"
using BTree = btreeolc::BTreeOLCHybrid<uint64_t, uint64_t>;
#else
#error "BTree synchronization implementation is not defined."
#endif

#include "tree_api.hpp"

class btreeolc_wrapper : public tree_api {
 public:
  btreeolc_wrapper(const tree_options_t &opt);
  virtual ~btreeolc_wrapper();

  virtual bool bulk_load(const char *data, size_t num_records, size_t key_sz,
                         size_t value_sz) override final;
  virtual bool find(const char *key, size_t key_sz, char *value_out) override final;
  virtual bool insert(const char *key, size_t key_sz, const char *value,
                      size_t value_sz) override final;
  virtual bool update(const char *key, size_t key_sz, const char *value,
                      size_t value_sz) override final;
  virtual bool remove(const char *key, size_t key_sz) override final;
  virtual int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) override final;
  virtual void tls_setup() override final;

 private:
  BTree *tree;
};

btreeolc_wrapper::btreeolc_wrapper(const tree_options_t &opt) {
  offset::init_qnodes();
  tree = new BTree();
}

btreeolc_wrapper::~btreeolc_wrapper() { delete tree; }

bool btreeolc_wrapper::bulk_load(const char *data, size_t num_records, size_t key_sz,
                                 size_t value_sz) {
  // Fake bulk loading
  const char *pos = data;
  for (uint64_t i = 0; i < num_records; ++i) {
    uint64_t ikey = *reinterpret_cast<const uint64_t *>(pos);
    ikey = __builtin_bswap64(ikey);
    pos += key_sz;
    uint64_t ival = 0;
    memcpy(&ival, pos, sizeof(uint64_t));
    pos += value_sz;
    bool ok = tree->insert(ikey, ival);
    if (!ok) {
      return false;
    }
  }
  return true;
}

bool btreeolc_wrapper::find(const char *key, size_t key_sz, char *value_out) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  uint64_t ival = 0;
  bool ok = tree->lookup(ikey, ival);
  *reinterpret_cast<uint64_t *>(value_out) = ival;
  return ok;
}

bool btreeolc_wrapper::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  uint64_t ival = 0;
  memcpy(&ival, value, sizeof(uint64_t));
  return tree->insert(ikey, ival);
}

bool btreeolc_wrapper::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  uint64_t ival = 0;
  memcpy(&ival, value, sizeof(uint64_t));
  return tree->update(ikey, ival);
}

bool btreeolc_wrapper::remove(const char *key, size_t key_sz) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  return tree->remove(ikey);
}

int btreeolc_wrapper::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {
  static thread_local uint64_t buffer[1 << 16];
  values_out = reinterpret_cast<char *>(buffer);
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  return tree->scan(ikey, scan_sz, buffer);
}

void btreeolc_wrapper::tls_setup() {
  // XXX(shiges): hack
  offset::reset_tls_qnodes();
}
