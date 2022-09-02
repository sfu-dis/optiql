##################### PiBench #########################
FetchContent_Declare(
        pibench 
        GIT_REPOSITORY https://github.com/sfu-dis/pibench.git
        GIT_TAG tls-cleanup
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

list(APPEND page_sizes 256 4096)
list(APPEND page_size_suffixes "" "_4K")

# B+ tree
foreach(omcs_impl omcs_impl_suffix IN ZIP_LISTS omcs_impls omcs_impl_suffixes)
  foreach(page_size page_size_suffix IN ZIP_LISTS page_sizes page_size_suffixes)
    add_wrapper(
      NAME btreeolc${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OL_CENTRALIZED BTREE_PAGE_SIZE=${page_size}
    )

    # add_wrapper(
    #   NAME btreeolc_fixed_backoff${page_size_suffix}${omcs_impl_suffix}
    #   SOURCE btreeolc_wrapper.cpp
    #   DEFINITIONS ${omcs_impl} BTREE_OL_CENTRALIZED OL_CENTRALIZED_FIXED_BACKOFF BTREE_PAGE_SIZE=${page_size}
    # )

    add_wrapper(
      NAME btreeolc_exp_backoff${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OL_CENTRALIZED OL_CENTRALIZED_EXP_BACKOFF BTREE_PAGE_SIZE=${page_size}
    )

    # add_wrapper(
    #   NAME btreeolc_plci${page_size_suffix}${omcs_impl_suffix}
    #   SOURCE btreeolc_wrapper.cpp
    #   DEFINITIONS ${omcs_impl} BTREE_OL_CENTRALIZED BTREE_PAGE_SIZE=${page_size} PESSIMISTIC_LOCK_COUPLING_INSERT
    # )

    add_wrapper(
      NAME btreeolc_nosync${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OL_CENTRALIZED BTREE_NO_SYNC BTREE_PAGE_SIZE=${page_size}
    )

    add_wrapper(
      NAME btreeolc_upgrade${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OLC_UPGRADE BTREE_PAGE_SIZE=${page_size}
    )

    # add_wrapper(
    #   NAME btreeomcs${page_size_suffix}${omcs_impl_suffix}
    #   SOURCE btreeolc_wrapper.cpp
    #   DEFINITIONS ${omcs_impl} BTREE_OMCS_ALL BTREE_PAGE_SIZE=${page_size}
    # )

    # add_wrapper(
    #   NAME btreeomcs_plci${page_size_suffix}${omcs_impl_suffix}
    #   SOURCE btreeolc_wrapper.cpp
    #   DEFINITIONS ${omcs_impl} BTREE_OMCS_ALL BTREE_PAGE_SIZE=${page_size} PESSIMISTIC_LOCK_COUPLING_INSERT
    # )

    add_wrapper(
      NAME btreeomcs_leaf${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OMCS_LEAF_ONLY BTREE_PAGE_SIZE=${page_size}
    )

    add_wrapper(
      NAME btreeomcs_leaf_offset${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OMCS_LEAF_ONLY OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_PAGE_SIZE=${page_size}
      LIBRARIES numa
    )

    add_wrapper(
      NAME btreeomcs_leaf_op_read${page_size_suffix}${omcs_impl_suffix}
      SOURCE btreeolc_wrapper.cpp
      DEFINITIONS ${omcs_impl} BTREE_OMCS_LEAF_ONLY OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE BTREE_PAGE_SIZE=${page_size}
      LIBRARIES numa
    )

    # add_wrapper(
    #   NAME btreeomcs_leaf_plci${page_size_suffix}${omcs_impl_suffix}
    #   SOURCE btreeolc_wrapper.cpp
    #   DEFINITIONS ${omcs_impl} BTREE_OMCS_LEAF_ONLY BTREE_PAGE_SIZE=${page_size} PESSIMISTIC_LOCK_COUPLING_INSERT
    # )

  endforeach()
endforeach()

# ART
foreach(omcs_impl omcs_impl_suffix IN ZIP_LISTS omcs_impls omcs_impl_suffixes)
  add_wrapper(
    NAME artolc${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl}
    LIBRARIES artolc${omcs_impl_suffix} tbb
  )

  add_wrapper(
    NAME artolc_upgrade${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl} ART_OLC_UPGRADE
    LIBRARIES artolc_upgrade${omcs_impl_suffix} tbb
  )

  # add_wrapper(
  #   NAME artolc_acquire${omcs_impl_suffix}
  #   SOURCE artolc_wrapper.cpp
  #   DEFINITIONS ${omcs_impl} ART_OLC_ACQUIRE
  #   LIBRARIES artolc_acquire${omcs_impl_suffix} tbb
  # )

  add_wrapper(
    NAME artolc_exp_backoff${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl} OL_CENTRALIZED_EXP_BACKOFF OL_CENTRALIZED_EXP_BACKOFF_BASE=1600
    LIBRARIES artolc_exp_backoff${omcs_impl_suffix} tbb
  )

  # add_wrapper(
  #   NAME artolc_acquire_exp_backoff${omcs_impl_suffix}
  #   SOURCE artolc_wrapper.cpp
  #   DEFINITIONS ${omcs_impl} ART_OLC_ACQUIRE OL_CENTRALIZED_EXP_BACKOFF OL_CENTRALIZED_EXP_BACKOFF_BASE=1600
  #   LIBRARIES artolc_acquire_exp_backoff${omcs_impl_suffix} tbb
  # )

  add_wrapper(
    NAME artolc_upsert${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl} ART_UPSERT
    LIBRARIES artolc_upsert${omcs_impl_suffix} tbb
  )

  add_wrapper(
    NAME artomcs${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl} IS_CONTEXTFUL
    LIBRARIES artomcs${omcs_impl_suffix} tbb
  )

  add_wrapper(
    NAME artomcs_offset${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl} IS_CONTEXTFUL OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
    LIBRARIES artomcs_offset${omcs_impl_suffix} tbb numa
  )

  add_wrapper(
    NAME artomcs_op_read${omcs_impl_suffix}
    SOURCE artolc_wrapper.cpp
    DEFINITIONS ${omcs_impl} IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
    LIBRARIES artomcs_op_read${omcs_impl_suffix} tbb numa
  )

  # add_wrapper(
  #   NAME artomcs_acquire${omcs_impl_suffix}
  #   SOURCE artolc_wrapper.cpp
  #   DEFINITIONS ${omcs_impl} ART_OLC_ACQUIRE IS_CONTEXTFUL
  #   LIBRARIES artomcs_acquire${omcs_impl_suffix} tbb
  # )
endforeach()

add_executable(wrapper_tests wrapper_tests.cpp)
target_link_libraries(wrapper_tests gtest btreeolc_wrapper pthread)
