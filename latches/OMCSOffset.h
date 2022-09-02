#pragma once

#include <numa.h>

#include <iostream>

#include "OMCSImpl.h"

namespace omcs_impl {

#ifdef OMCS_OFFSET
inline OMCSQNode *base_qnode = nullptr;
#ifdef OMCS_OFFSET_NUMA_QNODE
#define PAGE_SIZE 4096
struct socket_queue_node_index {
  std::atomic<uint64_t> index;
  char padding[CACHELINE_SIZE - sizeof(index)];
};
inline socket_queue_node_index *socket_qnode_index;
#else
inline std::atomic<uint64_t> next_node(0);
#endif  // OMCS_OFFSET_NUMA_QNODE
#endif  // OMCS_OFFSET

inline void init_qnodes() {
#ifdef OMCS_OFFSET
#ifdef OMCS_OFFSET_NUMA_QNODE
  static_assert(PAGE_SIZE % sizeof(OMCSQNode) == 0);

  // Round up to the proper number of pages
  uint32_t qnodes_per_page = PAGE_SIZE / sizeof(OMCSQNode);
  uint32_t npages = 0;
  uint32_t qnodes = 0;
  uint32_t sockets = numa_max_node() + 1;
  while (qnodes < OMCSLock::kNumQueueNodes) {
    qnodes += qnodes_per_page * sockets;
    npages += sockets;
  }

  std::cout << "Allocated " << qnodes << " queue nodes over " << npages << " pages across "
            << sockets << " sockets" << std::endl;

  socket_qnode_index = (socket_queue_node_index *)malloc(sockets * sizeof(socket_queue_node_index));
  for (uint32_t i = 0; i < sockets; ++i) {
    socket_qnode_index[i].index = 0;
  }

  base_qnode = (OMCSQNode *)numa_alloc_interleaved(npages * PAGE_SIZE);
  if (!base_qnode) {
    abort();
  }
#else
  int ret = posix_memalign((void **)&base_qnode, CACHELINE_SIZE * 2,
                           sizeof(OMCSQNode) * OMCSLock::kNumQueueNodes);
  if (ret) {
    abort();
  }

  for (uint32_t i = 0; i < OMCSLock::kNumQueueNodes; ++i) {
    new (base_qnode + i) OMCSQNode;
  }
#endif  // OMCS_OFFSET_NUMA_QNODE
#endif  // OMCS_OFFSET
  return;
}

#ifdef OMCS_OFFSET
inline thread_local OMCSQNode *qnodes = nullptr;

inline OMCSQNode *get_qnode(size_t i) {
  assert(qnodes);
  new (&qnodes[i]) OMCSQNode;
  return &qnodes[i];
}
#endif

inline void reset_tls_qnodes() {
#ifdef OMCS_OFFSET
  // XXX(shiges): grab 4 qnodes every time
  constexpr size_t QNODES_PER_THREAD = 4;
#ifdef OMCS_OFFSET_NUMA_QNODE
  uint32_t socket = numa_node_of_cpu(sched_getcpu());
  uint32_t qnodes_per_page = PAGE_SIZE / sizeof(OMCSQNode);
  uint32_t index = socket_qnode_index[socket].index.fetch_add(QNODES_PER_THREAD);

  uint32_t nsockets = numa_max_node() + 1;
  uint32_t page_num = index / qnodes_per_page * nsockets + socket;
  index = page_num * qnodes_per_page + index % qnodes_per_page;
  qnodes = &omcs_impl::base_qnode[index];
#else
  uint32_t index = next_node.fetch_add(QNODES_PER_THREAD);
  qnodes = &omcs_impl::base_qnode[index];
#endif  // OMCS_OFFSET_NUMA_QNODE
#endif
}
}  // namespace omcs_impl
