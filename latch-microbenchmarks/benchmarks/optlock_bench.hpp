#pragma once

#include "latches/OMCSImpl.h"
#include "perf.hpp"

struct OptLockBench : public Bench<omcs_impl::OMCSLock> {
  using Latch = omcs_impl::OMCSLock;

  // Constructor
  OptLockBench() : Bench<Latch>() {}

  // Destructor
  ~OptLockBench() {}

  // Version read operation
  bool LatchVersionRead(size_t node, size_t idx) override;

  // Acquire-release operation
  void LatchAcquireRelease(size_t node, size_t idx) override;
};
