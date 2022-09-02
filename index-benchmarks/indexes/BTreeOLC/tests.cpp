// Adapted from: https://github.com/wangziqi2016/index-microbench/blob/master/ARTOLC/example.cpp

#include <tbb/tbb.h>

#include <chrono>
#include <iostream>

#include "BTreeOLC.h"

using namespace std;

void multithreaded(int argc, char **argv) {
  std::cout << "multi threaded:" << std::endl;

  uint64_t n = std::atoll(argv[1]);
  uint64_t *keys = new uint64_t[n];

  // Generate keys
  for (uint64_t i = 0; i < n; i++)
    // dense, sorted
    keys[i] = i + 1;
  if (atoi(argv[2]) == 1)
    // dense, random
    std::random_shuffle(keys, keys + n);
  if (atoi(argv[2]) == 2)
    // "pseudo-sparse"
    for (uint64_t i = 0; i < n; i++)
      keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

  int num_threads = (argc == 3) ? -1 : atoi(argv[3]);
  if (num_threads < 1) {
    num_threads = tbb::info::default_concurrency();
  }
  tbb::global_control global_limit(tbb::global_control::max_allowed_parallelism, num_threads);

  printf("operation,n,ops/s\n");
  btreeolc::BTreeOLC<uint64_t, uint64_t> tree;
  // Build tree
  {
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          auto ival = __builtin_bswap64(keys[i]);
                          bool ok = tree.insert(keys[i], ival);
                          if (!ok) {
                            std::cout << "key insertion failed: " << keys[i] << std::endl;
                            throw;
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(
        tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
          for (uint64_t i = range.begin(); i != range.end(); i++) {
            uint64_t val = 0;
            bool found = tree.lookup(keys[i], val);
            if (!found) {
              std::cout << "key not found: " << keys[i] << std::endl;
              throw;
            }
            auto ival = __builtin_bswap64(keys[i]);
            if (val != ival) {
              std::cout << "wrong key read: " << val << " expected:" << ival << std::endl;
              throw;
            }
          }
        });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    auto starttime = std::chrono::system_clock::now();

    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          bool ok = tree.remove(keys[i]);
                          if (!ok) {
                            std::cout << "key removal failed: " << keys[i] << std::endl;
                            throw;
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          uint64_t val = 0;
                          bool found = tree.lookup(keys[i], val);
                          if (found) {
                            std::cout << "key not removed: " << keys[i] << std::endl;
                            throw;
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }
  delete[] keys;
}

int main(int argc, char **argv) {
  if (argc != 3 && argc != 4) {
    printf(
        "usage: %s n 0|1|2 <threads>\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse "
        "keys\n",
        argv[0]);
    return 1;
  }

  multithreaded(argc, argv);

  return 0;
}
