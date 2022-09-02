#pragma once

#include "latches/MCS.h"
#include "perf.hpp"

struct MCSBench : public Bench<mcs::MCSLock> {
  using Latch = mcs::MCSLock;
  using QNode = mcs::MCSQNode;

  // Constructor
  MCSBench() : Bench<mcs::MCSLock>() {}

  // Destructor
  ~MCSBench() {}

  // Version read operation
  bool LatchVersionRead(size_t node, size_t idx) override;

  // Acquire-release operation
  void LatchAcquireRelease(size_t node, size_t idx) override;
};
