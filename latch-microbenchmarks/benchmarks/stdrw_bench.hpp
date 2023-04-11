#pragma once

#include "latches/STDRW.h"
#include "perf.hpp"

struct STDRWBench : public Bench<std_lock::STDRWLock> {
  using Latch = std_lock::STDRWLock;

  // Constructor
  STDRWBench() : Bench<Latch>() {}

  // Destructor
  ~STDRWBench() {}

  // Version read operation
  bool LatchVersionRead(size_t node, size_t idx) override;

  // Acquire-release operation
  void LatchAcquireRelease(size_t node, size_t idx) override;
};
