#include <chrono>
#include <iostream>

#include "tbb/tbb.h"

using namespace std;

#include "Tree.h"

void loadKey(TID tid, Key &key) {
  // Store the key of the tuple into the key vector
  // Implementation is database specific
  auto record = reinterpret_cast<std::pair<uint64_t, uint64_t> *>(tid);
  key.setKeyLen(sizeof(record->first));
  reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(record->first);
}

void deleteNode(void *n) { return; }

void singlethreaded(char **argv) {
  std::cout << "single threaded:" << std::endl;

  uint64_t n = std::atoll(argv[1]);
  uint64_t *keys = new uint64_t[n];
  std::pair<uint64_t, uint64_t> *records = new std::pair<uint64_t, uint64_t>[n];
  std::pair<uint64_t, uint64_t> *records2 = new std::pair<uint64_t, uint64_t>[n];

  // Generate keys
  for (uint64_t i = 0; i < n; i++)
    // dense, sorted
    keys[i] = i + 1;
  if (atoi(argv[2]) == 1)
    // dense, random
    std::random_shuffle(keys, keys + n);
  if (atoi(argv[2]) == 2)
    // "pseudo-sparse" (the most-significant leaf bit gets lost)
    for (uint64_t i = 0; i < n; i++)
      keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

  for (uint64_t i = 0; i < n; i++) records[i] = {keys[i], i};
  for (uint64_t i = 0; i < n; i++) records2[i] = {keys[i], i + 1};

  printf("operation,n,ops/s\n");
  ART_OLC::Tree tree(loadKey, deleteNode);

  // Build tree
  {
    auto starttime = std::chrono::system_clock::now();

    for (uint64_t i = 0; i != n; i++) {
      Key key;
      auto tid = reinterpret_cast<TID>(&records[i]);
      loadKey(tid, key);
      // printf("inserting %lu\n", i);
      tree.insert(key, tid);
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();

    for (uint64_t i = 0; i != n; i++) {
      Key key;
      auto tid = reinterpret_cast<TID>(&records[i]);
      loadKey(tid, key);
      auto val = tree.lookup(key);
      if (val != tid) {
        std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
        throw;
      }
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Update
    auto starttime = std::chrono::system_clock::now();

    for (uint64_t i = 0; i != n; i++) {
      Key key;
      auto tid = reinterpret_cast<TID>(&records[i]);
      auto tid2 = reinterpret_cast<TID>(&records2[i]);
      loadKey(tid, key);
      bool ok = tree.update(key, tid2);
      if (!ok) {
        std::cout << "Update failed at: " << keys[i];
      }
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("update,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();

    for (uint64_t i = 0; i != n; i++) {
      Key key;
      auto tid = reinterpret_cast<TID>(&records[i]);
      auto tid2 = reinterpret_cast<TID>(&records2[i]);
      loadKey(tid, key);
      auto val = tree.lookup(key);
      if (val != tid2) {
        std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
        throw;
      }
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    auto starttime = std::chrono::system_clock::now();

    for (uint64_t i = 0; i != n; i++) {
      Key key;
      auto tid2 = reinterpret_cast<TID>(&records2[i]);
      loadKey(tid2, key);
      bool ok = tree.remove(key, tid2);
      if (!ok) {
        std::cout << "Update failed at: " << keys[i];
      }
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
  }
  delete[] keys;

  std::cout << std::endl;
}

void multithreaded(char **argv) {
  std::cout << "multi threaded:" << std::endl;

  uint64_t n = std::atoll(argv[1]);
  uint64_t *keys = new uint64_t[n];
  std::pair<uint64_t, uint64_t> *records = new std::pair<uint64_t, uint64_t>[n];
  std::pair<uint64_t, uint64_t> *records2 = new std::pair<uint64_t, uint64_t>[n];

  // Generate keys
  for (uint64_t i = 0; i < n; i++)
    // dense, sorted
    keys[i] = i + 1;
  if (atoi(argv[2]) == 1)
    // dense, random
    std::random_shuffle(keys, keys + n);
  if (atoi(argv[2]) == 2)
    // "pseudo-sparse" (the most-significant leaf bit gets lost)
    for (uint64_t i = 0; i < n; i++)
      keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

  for (uint64_t i = 0; i < n; i++) records[i] = {keys[i], i};
  for (uint64_t i = 0; i < n; i++) records2[i] = {keys[i], i + 1};

  printf("operation,n,ops/s\n");
  ART_OLC::Tree tree(loadKey, deleteNode);
  // ART_ROWEX::Tree tree(loadKey);

  // Build tree
  {
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          Key key;
                          auto tid = reinterpret_cast<TID>(&records[i]);
                          loadKey(tid, key);
                          tree.insert(key, tid);
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
            Key key;
            auto tid = reinterpret_cast<TID>(&records[i]);
            loadKey(tid, key);
            auto val = tree.lookup(key);
            if (val != tid) {
              std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
              throw;
            }
          }
        });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Update
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          Key key;
                          auto tid = reinterpret_cast<TID>(&records[i]);
                          auto tid2 = reinterpret_cast<TID>(&records2[i]);
                          loadKey(tid, key);
                          bool ok = tree.update(key, tid2);
                          if (!ok) {
                            std::cout << "Update failed at: " << keys[i];
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("update,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(
        tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
          for (uint64_t i = range.begin(); i != range.end(); i++) {
            Key key;
            auto tid = reinterpret_cast<TID>(&records[i]);
            auto tid2 = reinterpret_cast<TID>(&records2[i]);
            loadKey(tid, key);
            auto val = tree.lookup(key);
            if (val != tid2) {
              std::cout << "wrong key read: " << val << " expected:" << keys[i] << std::endl;
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
                          Key key;
                          auto tid2 = reinterpret_cast<TID>(&records2[i]);
                          loadKey(tid2, key);
                          bool ok = tree.remove(key, tid2);
                          if (!ok) {
                            std::cout << "Update failed at: " << keys[i];
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
  }
  delete[] keys;
}

int main(int argc, char **argv) {
  if (argc != 3) {
    printf("usage: %s n 0|1|2\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse keys\n",
           argv[0]);
    return 1;
  }

  singlethreaded(argv);

  multithreaded(argv);

  return 0;
}