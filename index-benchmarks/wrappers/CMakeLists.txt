##################### PiBench #########################
FetchContent_Declare(
        pibench 
        GIT_REPOSITORY https://github.com/sfu-dis/pibench.git
        GIT_TAG optiql
)
FetchContent_GetProperties(pibench)
if (NOT pibench_POPULATED)
  FetchContent_Populate(pibench)
  include_directories(${pibench_SOURCE_DIR}/include)
  add_subdirectory(${pibench_SOURCE_DIR} ${pibench_BINARY_DIR})
endif ()

macro(add_wrapper)
  cmake_parse_arguments(WRAPPER "" "NAME;SOURCE" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_library(${WRAPPER_NAME}_wrapper SHARED ${WRAPPER_SOURCE})
  target_link_libraries(${WRAPPER_NAME}_wrapper glog ${WRAPPER_LIBRARIES})
  target_compile_definitions(${WRAPPER_NAME}_wrapper PUBLIC ${WRAPPER_DEFINITIONS})
  if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
    target_link_libraries(${WRAPPER_NAME}_wrapper ${JEMALLOC_LIBRARIES})
  endif ()
endmacro()

list(APPEND page_sizes 256 512 1024 2048 4096 8192 16384)
list(APPEND page_size_suffixes "" "_512" "_1K" "_2K" "_4K" "_8K" "_16K")

# B+ tree
foreach(page_size page_size_suffix IN ZIP_LISTS page_sizes page_size_suffixes)
  add_wrapper(
    NAME btreeolc${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OL_CENTRALIZED BTREE_PAGE_SIZE=${page_size}
  )

  # add_wrapper(
  #   NAME btreeolc_plci${page_size_suffix}
  #   SOURCE btreeolc_wrapper.cpp
  #   DEFINITIONS OMCS_LOCK BTREE_OL_CENTRALIZED BTREE_PAGE_SIZE=${page_size} PESSIMISTIC_LOCK_COUPLING_INSERT
  # )

  add_wrapper(
    NAME btreeolc_nosync${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OL_CENTRALIZED BTREE_NO_SYNC BTREE_PAGE_SIZE=${page_size}
  )

  add_wrapper(
    NAME btreeolc_upgrade${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OLC_UPGRADE BTREE_PAGE_SIZE=${page_size}
  )

  # add_wrapper(
  #   NAME btreeomcs${page_size_suffix}
  #   SOURCE btreeolc_wrapper.cpp
  #   DEFINITIONS OMCS_LOCK BTREE_OMCS_ALL BTREE_PAGE_SIZE=${page_size}
  # )

  # add_wrapper(
  #   NAME btreeomcs_plci${page_size_suffix}
  #   SOURCE btreeolc_wrapper.cpp
  #   DEFINITIONS OMCS_LOCK BTREE_OMCS_ALL BTREE_PAGE_SIZE=${page_size} PESSIMISTIC_LOCK_COUPLING_INSERT
  # )

  add_wrapper(
    NAME btreeomcs_leaf${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY BTREE_PAGE_SIZE=${page_size}
  )

  add_wrapper(
    NAME btreeomcs_leaf_offset_gnp${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OFFSET BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_op_read_gnp${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_offset${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_op_read${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_op_read_new_api${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE OMCS_OP_READ_NEW_API BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_op_read_new_api_baseline${page_size_suffix}${omcs_impl_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE OMCS_OP_READ_NEW_API OMCS_OP_READ_NEW_API_BASELINE BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_op_read_callback${page_size_suffix}${omcs_impl_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE OMCS_OP_READ_NEW_API_CALLBACK BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeomcs_leaf_op_read_callback_baseline${page_size_suffix}${omcs_impl_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE OMCS_OP_READ_NEW_API_CALLBACK OMCS_OP_READ_NEW_API_CALLBACK_BASELINE BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  # add_wrapper(
  #   NAME btreeomcs_leaf_plci${page_size_suffix}
  #   SOURCE btreeolc_wrapper.cpp
  #   DEFINITIONS OMCS_LOCK BTREE_OMCS_LEAF_ONLY BTREE_PAGE_SIZE=${page_size} PESSIMISTIC_LOCK_COUPLING_INSERT
  # )

  add_wrapper(
    NAME btreelc_stdrw${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS RWLOCK STDRW_LOCK BTREE_RWLOCK BTREE_PAGE_SIZE=${page_size}
  )

  add_wrapper(
    NAME btreelc_mcsrw_cwp${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS RWLOCK MCSRW_LOCK RWLOCK_WRITER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_RWLOCK BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreelc_mcsrw_crp${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS RWLOCK MCSRW_LOCK RWLOCK_READER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_RWLOCK BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreelc_crw_cwp${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_CENTRALIZED RWLOCK_WRITER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_RWLOCK BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreelc_crw_crp${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_CENTRALIZED RWLOCK_READER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_RWLOCK BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreelc_mcsrw${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_ONLY OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_RWLOCK_MCSRW_ONLY BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

  add_wrapper(
    NAME btreeolc_mcsrw_hybrid${page_size_suffix}
    SOURCE btreeolc_wrapper.cpp
    DEFINITIONS OPT_MCSRW_HYBRID_LOCK OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_OLC_HYBRID BTREE_PAGE_SIZE=${page_size}
    LIBRARIES numa
  )

endforeach()

# ART
add_wrapper(
  NAME artolc
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK
  LIBRARIES artolc tbb
)

add_wrapper(
  NAME artolc_upgrade
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK ART_OLC_UPGRADE
  LIBRARIES artolc_upgrade tbb
)

# add_wrapper(
#   NAME artolc_acquire
#   SOURCE artolc_wrapper.cpp
#   DEFINITIONS OMCS_LOCK ART_OLC_ACQUIRE
#   LIBRARIES artolc_acquire tbb
# )

add_wrapper(
  NAME artolc_upsert
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK ART_UPSERT
  LIBRARIES artolc_upsert tbb
)

add_wrapper(
  NAME artomcs
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL
  LIBRARIES artomcs tbb
)

add_wrapper(
  NAME artomcs_offset_gnp
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OFFSET
  LIBRARIES artomcs_offset_gnp tbb numa
)

add_wrapper(
  NAME artomcs_op_read_gnp
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET
  LIBRARIES artomcs_op_read_gnp tbb numa
)

add_wrapper(
  NAME artomcs_offset
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  LIBRARIES artomcs_offset tbb numa
)

add_wrapper(
  NAME artomcs_op_read
  SOURCE artolc_wrapper.cpp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  LIBRARIES artomcs_op_read tbb numa
)

# add_wrapper(
#   NAME artomcs_acquire
#   SOURCE artolc_wrapper.cpp
#   DEFINITIONS OMCS_LOCK ART_OLC_ACQUIRE IS_CONTEXTFUL
#   LIBRARIES artomcs_acquire tbb
# )

add_wrapper(
  NAME artlc_stdrw
  SOURCE artolc_wrapper.cpp
  DEFINITIONS RWLOCK STDRW_LOCK
  LIBRARIES artlc_stdrw tbb numa
)

add_wrapper(
  NAME artlc_mcsrw
  SOURCE artolc_wrapper.cpp
  DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_ONLY IS_CONTEXTFUL OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  LIBRARIES artlc_mcsrw tbb numa
)

add_wrapper(
  NAME artlc_crw_cwp
  SOURCE artolc_wrapper.cpp
  DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_CENTRALIZED RWLOCK_WRITER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  LIBRARIES artlc_crw_cwp tbb numa
)

add_wrapper(
  NAME artlc_crw_crp
  SOURCE artolc_wrapper.cpp
  DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_CENTRALIZED RWLOCK_READER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  LIBRARIES artlc_crw_crp tbb numa
)

add_executable(wrapper_tests wrapper_tests.cpp)
target_link_libraries(wrapper_tests gtest btreeolc_wrapper pthread)

# Bw-Tree
add_wrapper(
  NAME bwtree
  SOURCE bwtree_wrapper.cpp
  LIBRARIES bwtree
)

add_executable(bwtree_wrapper_tests bwtree_wrapper_tests.cpp)
target_link_libraries(bwtree_wrapper_tests gtest bwtree_wrapper pthread)
