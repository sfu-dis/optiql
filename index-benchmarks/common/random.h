#pragma once

#include <atomic>
#include <cstdint>

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

class uniform_random_generator {
 public:
  uint32_t next() {
    if (seed_ == 0) {
      seed_ = next_seed_.fetch_add(1ull);
    }

    seed_ = seed_ * 0xD04C3175 + 0x53DA9022;
    return (seed_ >> 32) ^ (seed_ & 0xFFFFFFFF);
  }

 private:
  alignas(CACHELINE_SIZE) inline static std::atomic<uint64_t> next_seed_{0xDEADBEEF};
  alignas(CACHELINE_SIZE) inline static thread_local uint64_t seed_{0};
};
