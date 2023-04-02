#pragma once

#include <glog/logging.h>

#ifdef OMCS_LOCK
#include "indexes/ARTOLC/Tree.h"
#elif defined(MCSRW_LOCK) || defined(STDRW_LOCK)
#include "indexes/ARTLC/Tree.h"
#endif
#include "latches/OMCSOffset.h"
#include "third_party/art_ebr/Epoche.h"
#include "tree_api.hpp"

class artolc_wrapper : public tree_api {
 public:
  artolc_wrapper(const tree_options_t &opt);
  virtual ~artolc_wrapper();

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
  ART_OLC::Tree *tree;
  static size_t key_size, value_size;
  static ART::Epoche *epoch;

  static void loadKey(TID tid, Key &key) {
    const char *record = reinterpret_cast<const char *>(tid);
    key.set(record, key_size);
  }

  static void removeNode(void *node) {
    ART::ThreadInfo t(*epoch);
    epoch->markNodeForDeletion(node, t);
  };

  static TID makeRecord(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    // FIXME(shiges): memory leak
    char *record = (char *)malloc(key_sz + value_sz);
    memcpy(record, key, key_sz);
    memcpy(record + key_sz, value, value_sz);
    return reinterpret_cast<TID>(record);
  }
};

artolc_wrapper::artolc_wrapper(const tree_options_t &opt) {
  offset::init_qnodes();
  tree = new ART_OLC::Tree(loadKey, removeNode);
  key_size = opt.key_size;
  value_size = opt.value_size;
  epoch = reinterpret_cast<ART::Epoche *>(opt.data);
}

artolc_wrapper::~artolc_wrapper() {
  // delete tree;
}

bool artolc_wrapper::bulk_load(const char *data, size_t num_records, size_t key_sz,
                               size_t value_sz) {
  // Fake bulk loading
  const char *pos = data;
  Key tkey;
  for (uint64_t i = 0; i < num_records; ++i) {
    auto tid = makeRecord(pos, key_sz, pos + key_sz, value_sz);
    pos += key_sz + value_sz;

    loadKey(tid, tkey);
    bool ok = tree->insert(tkey, tid);
    if (!ok) {
      return false;
    }
  }
  return true;
}

bool artolc_wrapper::find(const char *key, size_t key_sz, char *value_out) {
  Key tkey;
  tkey.set(key, key_sz);
  auto tid = tree->lookup(tkey);
  if (tid == 0) {
    // key not found
    return false;
  }

  const char *record = reinterpret_cast<const char *>(tid);

  memcpy(value_out, record + key_sz, value_size);
  return true;
}

bool artolc_wrapper::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  auto tid = makeRecord(key, key_sz, value, value_sz);

  Key tkey;
  loadKey(tid, tkey);

  return tree->insert(tkey, tid);
}

bool artolc_wrapper::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
#if defined(ART_IN_PLACE_UPDATE) || defined(ART_NO_UPDATE)
  static_assert(false, "Not supported");
  Key tkey;
  tkey.set(key, key_sz);

#if defined(ART_IN_PLACE_UPDATE)
  return tree->update(tkey, value, value_sz);
#elif defined(ART_NO_UPDATE)
  return tree->update(tkey);
#endif
#else
  auto tid = makeRecord(key, key_sz, value, value_sz);

  Key tkey;
  loadKey(tid, tkey);

#if defined(ART_UPSERT)
  return tree->upsert(tkey, tid);
#else
  return tree->update(tkey, tid);
#endif
#endif
}

bool artolc_wrapper::remove(const char *key, size_t key_sz) {
  Key tkey;
  tkey.set(key, key_sz);
  auto tid = tree->lookup(tkey);
  if (tid == 0) {
    // key not found
    return false;
  }

  return tree->remove(tkey, tid);
}

int artolc_wrapper::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {
  // XXX(shiges): buffer size
  static thread_local TID results[1024];
  static thread_local char buffer[8192];

  Key tkey;
  tkey.set(key, key_sz);
  size_t result_sz = 0;
  tree->lookupRange(tkey, results, scan_sz, result_sz);

  char *next = buffer;
  for (size_t i = 0; i < result_sz; ++i) {
    const char *record = reinterpret_cast<const char *>(results[i]);

    memcpy(next, record + key_sz, value_size);
    next += value_size;
  }
  values_out = buffer;
  return result_sz;
}

void artolc_wrapper::tls_setup() {
  // XXX(shiges): hack
  offset::reset_tls_qnodes();
}
