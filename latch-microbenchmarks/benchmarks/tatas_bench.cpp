#include "bench_config.hpp"
#include "tatas_bench.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

bool TATASBench::LatchVersionRead(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  return false;
}

void TATASBench::LatchAcquireRelease(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  latch->lock();
  CriticalSection();
  latch->unlock();
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  TATASBench test;
  test.Run();

  return 0;
}

