#pragma once

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <iostream>
#include <thread>

#include "third_party/foedus/uniform_random.hpp"

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 64
#endif

#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

struct PerformanceTest {
  // Constructor
  // @threads: number of benchmark worker threads
  // @seconds: benchmark duration in seconds
  PerformanceTest(uint32_t threads, uint32_t seconds);

  // Destructor
  ~PerformanceTest();

  // Function handle for worker threads; invokes WorkerRun()
  void Execute(uint32_t thread_id);

  // Virtual method interface for worker threads
  virtual void WorkerRun(uint32_t node_id, uint32_t thread_id) = 0;

  // Entrance function to run a benchmark
  void Run();

  // One entry per thread to record the number of finished operations
  std::vector<uint64_t> noperations;
  std::vector<uint64_t> nsuccesses;
  std::vector<uint64_t> reads;
  std::vector<uint64_t> read_successes;

  // Benchmark start barrier: worker threads can only proceed if set to true
  std::atomic<bool> bench_start_barrier;

  // Thread start barrier: a counter of "ready-to-start" threads
  std::atomic<uint32_t> thread_start_barrier;

  // Whether the benchmark should stop and worker threads should shutdown
  std::atomic<bool> shutdown;

  // List of all worker threads
  std::vector<std::thread *> workers;

  // Number of worker threads
  uint32_t nthreads;

  // Benchmark duration in seconds
  uint32_t seconds;
};
