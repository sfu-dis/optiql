#include <numa.h>
#include <mutex>

#include "bench_config.hpp"
#include "omcs_bench.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

#ifdef OMCS_OFFSET
OMCSBench::QNode *omcs_impl::base_qnode = nullptr;
#ifdef OMCS_OFFSET_NUMA_QNODE
#define PAGE_SIZE 4096
struct socket_queue_node_index {
  std::atomic<uint64_t> index;
  char padding[CACHELINE_SIZE - sizeof(index)];
};
socket_queue_node_index *socket_qnode_index;
#else
std::atomic<uint32_t> next_node(0);
#endif  // OMCS_OFFSET_NUMA_QNODE
#endif  // OMCS_OFFSET

bool OMCSBench::LatchVersionRead(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
  bool restart = false;
  uint64_t v = latch->try_begin_read(restart);
  if (restart) {
    return false;
  }
  CriticalSection();
  return latch->validate_read(v);
}

std::mutex l;
void OMCSBench::LatchAcquireRelease(size_t node, size_t idx) {
  Latch *latch = GetLatch(node, idx);
#ifdef OMCS_OFFSET
  thread_local QNode *qnode = nullptr;
  if (!qnode) {
#ifdef OMCS_OFFSET_NUMA_QNODE
    uint32_t socket = numa_node_of_cpu(sched_getcpu());
    uint32_t qnodes_per_page = PAGE_SIZE / sizeof(OMCSBench::QNode);
    uint32_t index = socket_qnode_index[socket].index++;

    uint32_t nsockets = numa_max_node() + 1;
    uint32_t page_num = index / qnodes_per_page * nsockets + socket; 
    index = page_num * qnodes_per_page + index % qnodes_per_page;
    qnode = &omcs_impl::base_qnode[index];
#else
    qnode = &omcs_impl::base_qnode[next_node++];
#endif  // OMCS_OFFSET_NUMA_QNODE
  }
  QNode &q = *qnode;
  new (qnode) QNode;
#else
  QNode q;
#endif
  latch->lock(&q);
  CriticalSection();
  latch->unlock(&q);
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

#ifdef OMCS_OFFSET
#ifdef OMCS_OFFSET_NUMA_QNODE
  static_assert(PAGE_SIZE % sizeof(OMCSBench::QNode) == 0);

  // Round up to the proper number of pages
  uint32_t qnodes_per_page = PAGE_SIZE / sizeof(OMCSBench::QNode);
  uint32_t npages = 0;
  uint32_t qnodes = 0;
  uint32_t sockets = numa_max_node() + 1;
  while (qnodes < omcs_impl::OMCSLock::kNumQueueNodes) {
    qnodes += qnodes_per_page * sockets;
    npages += sockets;
  }

  std::cout << "Allocated " << qnodes << " queue nodes over "
            << npages << " pages across " << sockets << " sockets" << std::endl;

  socket_qnode_index = (socket_queue_node_index *)malloc(sockets * sizeof(socket_queue_node_index));
  for (uint32_t i = 0; i < sockets; ++i) {
    socket_qnode_index[i].index = 0;
  }

  omcs_impl::base_qnode = (OMCSBench::QNode *)numa_alloc_interleaved(npages * PAGE_SIZE);
  if (!omcs_impl::base_qnode) {
    abort();
  }
#else
  int ret = posix_memalign((void **)&omcs_impl::base_qnode, 
                           CACHELINE_SIZE * 2,
                           sizeof(OMCSBench::QNode) * omcs_impl::OMCSLock::kNumQueueNodes);
  if (ret) {
    abort();
  }

  for (uint32_t i = 0; i < omcs_impl::OMCSLock::kNumQueueNodes; ++i) {
    new (omcs_impl::base_qnode + i) OMCSBench::QNode;
  }
#endif  // OMCS_OFFSET_NUMA_QNODE
#endif  // OMCS_OFFSET

  OMCSBench test;
  test.Run();

  return 0;
}

