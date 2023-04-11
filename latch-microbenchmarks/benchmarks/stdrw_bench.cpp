#include "bench_config.hpp"
#include "stdrw_bench.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

bool STDRWBench::LatchVersionRead(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  latch->read_lock();
  CriticalSection();
  latch->read_unlock();
  return true;
}

void STDRWBench::LatchAcquireRelease(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  latch->lock();
  CriticalSection();
  latch->unlock();
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  STDRWBench test;
  test.Run();

  return 0;
}

