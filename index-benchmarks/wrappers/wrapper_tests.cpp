
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
#include <utility>
#include <vector>

#include "btreeolc_wrapper.hpp"
#include "tree_api.hpp"

static constexpr uint64_t kNumThreads = 4;
static constexpr uint64_t kNumKeys = 8192;

template <typename T>
class WrapperTest : public ::testing::Test {};

using WrapperTypes = ::testing::Types<btreeolc_wrapper>;
TYPED_TEST_SUITE(WrapperTest, WrapperTypes);

TYPED_TEST(WrapperTest, InsertThenSearch) {
  tree_options_t tree_opt;
  tree_api *tree = new TypeParam(tree_opt);

  std::vector<std::thread *> threads;
  std::vector<uint64_t> tids;

  for (uint64_t i = 0; i < kNumThreads; ++i) {
    tids.push_back(i);
  }

  std::atomic<uint64_t> barrier1(kNumThreads);
  for (uint64_t i = 0; i < kNumThreads; ++i) {
    threads.push_back(new std::thread(
        [&](uint64_t tid) {
          --barrier1;
          while (barrier1 > 0) {
          }
          for (uint64_t k = 0; k < kNumKeys; ++k) {
            if (k % kNumThreads == tid) {
              uint64_t key = __builtin_bswap64(k);
              bool ok = tree->insert(reinterpret_cast<const char *>(&key), 8,
                                     reinterpret_cast<const char *>(&tids[tid]), 8);
              ASSERT_TRUE(ok);
            }
          }
        },
        i));
  }
  for (auto &t : threads) {
    t->join();
    delete t;
  }
  threads.clear();

  std::atomic<uint64_t> barrier2(kNumThreads);
  for (uint64_t i = 0; i < kNumThreads; ++i) {
    threads.push_back(new std::thread(
        [&](uint64_t tid) {
          --barrier2;
          while (barrier2 > 0) {
          }
          for (uint64_t k = 0; k < kNumKeys; ++k) {
            uint64_t key = __builtin_bswap64(k);
            uint64_t value = ~0ull;
            bool ok = tree->find(reinterpret_cast<const char *>(&key), 8,
                                 reinterpret_cast<char *>(&value));
            ASSERT_TRUE(ok);
            ASSERT_EQ(value, k % kNumThreads);
          }
        },
        i));
  }
  for (auto &t : threads) {
    t->join();
    delete t;
  }
  threads.clear();

  delete tree;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}