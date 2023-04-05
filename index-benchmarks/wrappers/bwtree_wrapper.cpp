#include <cstdint>
#include <vector>

#include "indexes/BwTree/bwtree.h"

#include "bwtree_wrapper.hpp"

extern "C" tree_api *create_tree(const tree_options_t &opt) { return new bwtree_wrapper(opt); }

bwtree_wrapper::bwtree_wrapper(const tree_options_t &opt) {
  tree = new BwTreeType{};
  // XXX(shiges): If verification in PiBench is needed, make
  // this opt.num_threads * 3 since it does not recycle threads
  tree->UpdateThreadLocal(opt.num_threads * 2);
}

bwtree_wrapper::~bwtree_wrapper() {
  // FIXME(shiges): we let it leak
  // delete tree;
}

bool bwtree_wrapper::bulk_load(const char *data, size_t num_records, size_t key_sz,
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
    bool ok = tree->Insert(ikey, ival);
    if (!ok) {
      return false;
    }
  }
  return true;
}

bool bwtree_wrapper::find(const char *key, size_t key_sz, char *value_out) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  std::vector<uint64_t> values;
  values.reserve(1);
  tree->GetValue(ikey, values);
  if (values.size() == 0) {
    return false;
  }
  *reinterpret_cast<uint64_t *>(value_out) = values.at(0);
  return true;
}

bool bwtree_wrapper::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  uint64_t ival = 0;
  memcpy(&ival, value, sizeof(uint64_t));
  return tree->Insert(ikey, ival);
}

bool bwtree_wrapper::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  // FIXME(shiges): BwTree does not support Update(). If the given key is not present,
  // this method will *insert* this new pair using the Upsert() interface.
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  uint64_t ival = 0;
  memcpy(&ival, value, sizeof(uint64_t));
  // XXX(shiges): BwTree Upsert() returns false to indicate an update was successful
  return !tree->Upsert(ikey, ival);
}

bool bwtree_wrapper::remove(const char *key, size_t key_sz) {
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  uint64_t old_val;
  return tree->Delete(ikey, old_val);
}

int bwtree_wrapper::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {
  static thread_local uint64_t buffer[1 << 16];
  values_out = reinterpret_cast<char *>(buffer);
  uint64_t ikey = *reinterpret_cast<const uint64_t *>(key);
  ikey = __builtin_bswap64(ikey);
  auto it = tree->Begin(ikey);
  int values_idx = 0;
  for (; values_idx < scan_sz && !it.IsEnd(); values_idx++, it++) {
    buffer[values_idx] = it->second;
  }
  return values_idx;
}

void bwtree_wrapper::tls_setup() {
  tree->RegisterThread();
}
