#pragma once

#include "latches/MCSRW.h"
#include "perf.hpp"

#ifdef OMCS_OFFSET
extern mcsrw::MCSRWQNode *mcsrw::base_qnode;
#endif

struct MCSRWBench : public Bench<mcsrw::MCSRWLock> {
  using Latch = mcsrw::MCSRWLock;
  using QNode = mcsrw::MCSRWQNode;

  // Constructor
  MCSRWBench() : Bench<Latch>() {}

  // Destructor
  ~MCSRWBench() {}

  QNode *GetNode();

  // Version read operation
  bool LatchVersionRead(size_t node, size_t idx) override;

  // Acquire-release operation
  void LatchAcquireRelease(size_t node, size_t idx) override;
};


