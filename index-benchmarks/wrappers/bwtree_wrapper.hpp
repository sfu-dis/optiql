#pragma once

#include <cstdint>

#include "indexes/BwTree/bwtree.h"

#include "tree_api.hpp"

class bwtree_wrapper : public tree_api {
public:
  bwtree_wrapper(const tree_options_t &opt);
  virtual ~bwtree_wrapper();

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
  using BwTreeType = wangziqi2013::bwtree::BwTree<uint64_t, uint64_t>;
  BwTreeType *tree;
};
