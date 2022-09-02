#include "perf.hpp"

#include <numa.h>
#include <iomanip>

#include "sched.hpp"

PerformanceTest::PerformanceTest(uint32_t threads, uint32_t seconds)
    : bench_start_barrier(false),
      thread_start_barrier(0),
      shutdown(false),
      nthreads(threads),
      seconds(seconds) {}

PerformanceTest::~PerformanceTest() {
  // Destructor - nothing needed here
}

void PerformanceTest::Execute(uint32_t thread_id) {
  // Pin self to the corresponding CPU core
  set_affinity(thread_id);

  // 1. Mark self as ready using the thread start barrier
  ++thread_start_barrier;

  // 2. Wait for others to become ready
  while (!bench_start_barrier) {
  }

  // 3. Do real work until shutdown
  WorkerRun(get_node_id(thread_id), thread_id);
}

void PerformanceTest::Run() {
  // 1. Start threads and initialize the commit/abort stats for each thread to 0
  for (uint32_t i = 0; i < nthreads; ++i) {
    noperations.emplace_back(0);
    nsuccesses.emplace_back(0);
    reads.emplace_back(0);
    read_successes.emplace_back(0);
    workers.push_back(new std::thread(&PerformanceTest::Execute, this, i));
  }

  // 2. Wait for all threads to become ready
  while (thread_start_barrier != nthreads) {
  }

  // 3. Allow everyone to go ahead
  bench_start_barrier = true;

  // 4. Sleep for the benchmark duration
  std::cout << "Benchmarking..." << std::flush;
  sleep(seconds);

  // 5. Issue a 'stop' signal to all threads
  shutdown = true;
  std::cout << "Done" << std::endl;

  // 6. Wait for all workers to finish
  for (auto &t : workers) {
    t->join();
    delete t;
  }

  // 7. Dump stats
  std::cout << "=====================" << std::endl;
  std::cout << "Thread,Operations/s,Successes/s: " << std::endl;

  std::cout << std::fixed;
  std::cout << std::setprecision(3);

  uint64_t total_operations = 0;
  uint64_t total_successes = 0;
  uint64_t total_reads = 0;
  uint64_t total_read_successes = 0;
  for (uint32_t i = 0; i < noperations.size(); ++i) {
    std::cout << i
              << "," << noperations[i] / (double)seconds
              << "," << nsuccesses[i] / (double)seconds
              << "," << reads[i] / (double)seconds
              << "," << read_successes[i] / (double)seconds
              << std::endl;
    total_operations += noperations[i];
    total_successes += nsuccesses[i];
    total_reads += reads[i];
    total_read_successes += read_successes[i];
  }

  std::cout << "---------------------" << std::endl;
  std::cout << "All"
            << "," << total_operations / (double)seconds
            << "," << total_successes / (double)seconds
            << "," << total_reads / (double)seconds
            << "," << total_read_successes / (double)seconds
            << std::endl;
}
