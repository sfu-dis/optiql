#pragma once

#include <immintrin.h>

#include <atomic>

class TATAS {
  std::atomic<uint64_t> lock_;

 public:
  TATAS() { std::atomic_init(&lock_, 0ull); }

  void lock() {
    int cas_failure = 0;
  retry:
    auto locked = lock_.load(std::memory_order_acquire);
    if (locked) {
      goto retry;
    }
    if (!lock_.compare_exchange_strong(locked, 1ul)) {
      cas_failure++;
      goto retry;
    }
  }

  void unlock() {
#if defined(ST_ONLY)
    lock_.store(0, std::memory_order_release);
#else
    lock_.exchange(0);
#endif
  }
};
