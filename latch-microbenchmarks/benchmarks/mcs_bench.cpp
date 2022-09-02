#include "bench_config.hpp"
#include "mcs_bench.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

bool MCSBench::LatchVersionRead(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  return false;
}

void MCSBench::LatchAcquireRelease(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  QNode q;

  latch->lock(&q);
  CriticalSection();
  latch->unlock(&q);
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  MCSBench test;
  test.Run();

  return 0;
}

