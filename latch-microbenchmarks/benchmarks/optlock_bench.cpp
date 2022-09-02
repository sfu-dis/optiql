#include "bench_config.hpp"
#include "optlock_bench.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

bool OptLockBench::LatchVersionRead(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  bool restart = false;
  uint64_t v = latch->try_begin_read(restart);
  if (restart) {
    return false;
  }
  CriticalSection();
  return latch->validate_read(v);
}

void OptLockBench::LatchAcquireRelease(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
#if defined(ST_ONLY)
  uint64_t v = latch->lock();
  CriticalSection();
  latch->unlock(v);
#else
  latch->lock();
  CriticalSection();
  latch->unlock();
#endif
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  OptLockBench test;
  test.Run();

  return 0;
}

