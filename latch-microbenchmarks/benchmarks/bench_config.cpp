#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>

#include "latches/MCS.h"
#include "latches/TATAS.h"
#include "latches/OMCSImpl.h"
#if defined(MCSRW_LOCK_ONLY)
#include "latches/MCSRW.h"
#endif
#include "bench_config.hpp"
#include "distribution.hpp"
#include "common/delay.h"

// Command line arguments defined using gflags:
DEFINE_uint64(threads, 1, "Number of worker threads");
DEFINE_uint64(seconds, 10, "Number of seconds to run the benchmark");
DEFINE_uint64(array_size, 128, "Number of latches in the benchmark");

// Operation mix
DEFINE_uint64(ver_read_pct, 80, "Version read percentage");
DEFINE_uint64(acq_rel_pct, 20, "Latch acq/rel percentage");

// Distribution
DEFINE_string(dist, "fixed", "Latch array indexes distribution");
DEFINE_validator(dist, ValidateDistribution);

// Critical/parallel section cycles
DEFINE_uint64(cs_cycles, 1000, "Critical section cycles");
DEFINE_uint64(ps_cycles, 200000, "Parallel section cycles");

template<class Latch>
void Bench<Latch>::Load() {
  if (FLAGS_ver_read_pct + FLAGS_acq_rel_pct != 100) {
    LOG(FATAL) << "Operation mix doesn't sum up to 100%!";
  }
  std::cout << "Initiating " << FLAGS_array_size << " latches...";

  size_t array_size = CACHELINE_SIZE * 2 * FLAGS_array_size;
  int N = 1;
  if (distribution == Distribution::FIXED) {
    N = numa_max_node() + 1;
  }
  for (int n = 0; n < N; ++n) {
    space[n] = (char *)numa_alloc_onnode(array_size, n);
    memset(space[n], 0, array_size);
    for (size_t i = 0; i < FLAGS_array_size; ++i) {
      new (GetLatch(n, i)) Latch();
    }
  }

  std::cout << "Done" << std::endl;
}

template<class Latch>
void Bench<Latch>::CriticalSection() {
  Work(FLAGS_cs_cycles);
}

// Borrowed from asynclib: https://github.com/sfu-dis/omcs-ascylib/blob/3c2d1a231eac3518a4167d7d4a709cb22a3cff6b/include/latency.h
static inline uint64_t getticks(void) {
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

//uint64_t shint = 100;

template<class Latch>
void Bench<Latch>::Work(int64_t cycles) {
  /*
   * when cycles == 0: rdtscp: 380m, rdtsc: 480m
  unsigned int tsc_aux = 0;
  unsigned long long start = 0;
  start = getticks();// __rdtscp(&tsc_aux);

  unsigned long long now = 0;
  do {
    now = getticks();// __rdtscp(&tsc_aux);
  } while (now - start < cycles);
  */

  /*
   * rdtsc: 765m, rdtscp:640m
  unsigned int tsc_aux = 0;
  unsigned long long now = 0;
  unsigned long long start = 0;
  start = getticks(); //__rdtscp(&tsc_aux);
  while (now - start < cycles) {
    now = getticks(); //__rdtscp(&tsc_aux);
  }
  */

  // On Cascade Lake (6252/6242R Xeon Gold) PAUSE takes ~44 cycles. On Skylake it's ~120 cycles
  // On previous platforms it was about 10 cycles.
  // e.g.: https://github.com/dotnet/runtime/issues/53532
  // while (cycles--) {_mm_pause();};

  /* 2.1b 
  while (cycles) {
    auto now = getticks(); //__rdtscp(&tsc_aux);
    _mm_pause();
    cycles -= (getticks() - now); //__rdtscp(&tsc_aux);
  }
  */

  /* nothing: 1.9b */

  /* atomics/tls: 2b (tls seems to be optimized out regardless cycles value) 
   * atomic (CAS or FAA) works, FAA gives slightly better steps:
   * cycles = 0, 1, 2, 3 gives TATAS and MCS respectively:
   * 6 8 13 20,  5 6 8 10
   * I.e., TATAS/MCS = 6/5 if we do 3 FAA, 8/6 if we do 2 FAA, and 20/10 if we don't do any FAA
   * 
  while (cycles--) {
    std::atomic<uint64_t> myint(100);
    myint++;
  }
  */

  // Similar to the FAA soln. above, but more fine grained control because ++ an int is cheaper
  // volatile int myint = 100;
  // while (cycles--) {
  //   ++myint;
  // }

  /* shared variable: 15m (!)
  ++shint;
  */
  DELAY(cycles);
}

template<class Latch>
void Bench<Latch>::ParallelSection() {
  Work(FLAGS_ps_cycles);
}

template<class Latch>
void Bench<Latch>::WorkerRun(uint32_t node_id, uint32_t thread_id) {
  // Do real work until shutdown
  foedus::assorted::UniformRandom rng(thread_id);
  size_t my_noperations = 0;
  size_t my_nsuccesses = 0;
  size_t my_reads = 0;
  size_t my_read_successes = 0;
  while (!shutdown) {
    bool succeeded = false;
    uint64_t k = rng.uniform_within(0, 99);
    uint64_t node = 0;
    uint64_t idx = 0;
    if (distribution == Distribution::FIXED) {
      node = node_id;
      idx = thread_id;
    } else if (distribution == Distribution::UNIFORM) {
      idx = rng.uniform_within(0, FLAGS_array_size - 1);
    }
    if (k < FLAGS_ver_read_pct) {
      succeeded = LatchVersionRead(node, idx);
      ++my_reads;
      if (succeeded) {
        ++my_read_successes;
      }
    } else if (k < FLAGS_ver_read_pct + FLAGS_acq_rel_pct) {
      succeeded = true;
      LatchAcquireRelease(node, idx);
    }

    ParallelSection();

    ++my_noperations;
    if (succeeded) {
      ++my_nsuccesses;
    }
  }
  noperations[thread_id] = my_noperations;
  nsuccesses[thread_id] = my_nsuccesses;
  reads[thread_id] = my_reads;
  read_successes[thread_id] = my_read_successes;
}

template <class Latch>
Bench<Latch>::Bench() : PerformanceTest(FLAGS_threads, FLAGS_seconds) {
  {
    std::string dist(FLAGS_dist);
    std::transform(dist.begin(), dist.end(), dist.begin(), ::tolower);
    if (dist == "fixed") {
      distribution = Distribution::FIXED;
    } else if (dist == "uniform") {
      distribution = Distribution::UNIFORM;
    } else {
      LOG(FATAL) << "unknown distribution";
    }
  }

  std::cout << "Setup: " << std::endl;
  std::cout << "  threads: " << FLAGS_threads << std::endl;
  std::cout << "  duration (s): " << FLAGS_seconds << "s" << std::endl;
  std::cout << "  number of latches: " << FLAGS_array_size << std::endl;
  std::cout << "Operation mix (%): " << std::endl;
  std::cout << "  version-read :   " << FLAGS_ver_read_pct << std::endl;
  std::cout << "  acquire-release: " << FLAGS_acq_rel_pct << std::endl;
  std::cout << "Index distribution: " << std::endl;
  std::cout << "  distribution:   " << FLAGS_dist << std::endl;
  std::cout << "Critical section:   " << FLAGS_cs_cycles << " cycles" << std::endl;
  std::cout << "Parallel section:   " << FLAGS_ps_cycles << " cycles" << std::endl;

  Load();
}

// Instantiate templates for each lock
template class Bench<mcs::MCSLock>;
template class Bench<TATAS>;
template class Bench<omcs_impl::OMCSLock>;
#if defined(MCSRW_LOCK_ONLY)
template class Bench<mcsrw::MCSRWLock>;
#endif
