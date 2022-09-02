#pragma once

#include <atomic>

#include "latches/TATAS.h"
#include "perf.hpp"

struct TATASBench : public Bench<TATAS> {
  using Latch = TATAS;

  // Constructor
  TATASBench() : Bench<TATAS>() {}

  // Destructor
  ~TATASBench() {}

  // Version read operation
  bool LatchVersionRead(size_t node, size_t idx) override;

  // Acquire-release operation
  void LatchAcquireRelease(size_t node, size_t idx) override;
};
