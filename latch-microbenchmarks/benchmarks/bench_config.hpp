#include <x86intrin.h>
#include <gflags/gflags.h>
#include "distribution.hpp"
#include "perf.hpp"

DECLARE_uint64(threads);
DECLARE_uint64(seconds);
DECLARE_uint64(array_size);
DECLARE_uint64(ver_read_pct);
DECLARE_uint64(acq_rel_pct);
DECLARE_string(dist);
DECLARE_uint64(cs_cycles);
DECLARE_uint64(ps_cycles);

template<class Latch>
struct Bench : public PerformanceTest {
  // Benchmark worker function
  void WorkerRun(uint32_t node_id, uint32_t thread_id);

  // Constructor
  Bench();

  // Destructor
  ~Bench() {}

  inline Latch *GetLatch(size_t node, size_t idx) {
    return reinterpret_cast<Latch *>(&space[node][idx * CACHELINE_SIZE * 2]);
  }

  // Version read operation
  virtual bool LatchVersionRead(size_t node, size_t idx) = 0;

  // Acquire-release operation
  virtual void LatchAcquireRelease(size_t node, size_t idx) = 0;

  void Load();

  void CriticalSection();

  void ParallelSection();

  Distribution distribution;

 private:
  void Work(int64_t cycles);

  char *space[8] CACHE_ALIGNED;
};
