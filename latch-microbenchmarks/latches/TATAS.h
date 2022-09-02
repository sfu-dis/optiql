#pragma once

#include <immintrin.h>

#include <atomic>

#include "common/delay.h"

class TATAS {
  std::atomic<uint64_t> lock_;

 public:
  TATAS() { std::atomic_init(&lock_, 0ull); }

  void lock() {
    int cas_failure = 0;
#if defined(EXP_BACKOFF)
    uint64_t seed = (uintptr_t)(&cas_failure);  // FIXME(shiges): hack
    auto next_u32 = [&]() {
      seed = seed * 0xD04C3175 + 0x53DA9022;
      return (seed >> 32) ^ (seed & 0xFFFFFFFF);
    };
    next_u32();
    int maxDelay = kExpBackoffBase;
#endif
  retry:
    auto locked = lock_.load(std::memory_order_acquire);
    if (locked) {
#if defined(FIXED_BACKOFF)
      DELAY(kFixedBackoffDelay);
#elif defined(EXP_BACKOFF)
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      DELAY(delay);
#endif
      goto retry;
    }
    if (!lock_.compare_exchange_strong(locked, 1ul)) {
      cas_failure++;
#if defined(FIXED_BACKOFF)
      DELAY(kFixedBackoffDelay);
#elif defined(EXP_BACKOFF)
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      DELAY(delay);
#endif
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
