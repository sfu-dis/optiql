#pragma once

#include "latches/OMCSImpl.h"
#include "perf.hpp"

#ifdef OMCS_OFFSET
extern omcs_impl::OMCSQNode *omcs_impl::base_qnode;
#endif

struct OMCSBench : public Bench<omcs_impl::OMCSLock> {
  using Latch = omcs_impl::OMCSLock;
  using QNode = omcs_impl::OMCSQNode;

  // Constructor
  OMCSBench() : Bench<Latch>() {}

  // Destructor
  ~OMCSBench() {}

  // Version read operation
  bool LatchVersionRead(size_t node, size_t idx) override;

  // Acquire-release operation
  void LatchAcquireRelease(size_t node, size_t idx) override;
};


