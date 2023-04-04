
macro(add_libart)
  cmake_parse_arguments(LIBART "" "NAME" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_library(${LIBART_NAME} ${ARTOLC_SRC})
  target_link_libraries(${LIBART_NAME} glog ${LIBART_LIBRARIES})
  target_compile_definitions(${LIBART_NAME} PUBLIC ${LIBART_DEFINITIONS})
endmacro()

set(ARTOLC_SRC Tree.cpp N.cpp N4.cpp N16.cpp N48.cpp N256.cpp)

add_libart(
  NAME artlc_stdrw
  DEFINITIONS RWLOCK STDRW_LOCK
)

add_libart(
  NAME artlc_mcsrw
  DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_ONLY IS_CONTEXTFUL OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
)

add_libart(
  NAME artlc_crw_cwp
  DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_CENTRALIZED RWLOCK_WRITER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
)

add_libart(
  NAME artlc_crw_crp
  DEFINITIONS RWLOCK MCSRW_LOCK MCSRW_LOCK_CENTRALIZED RWLOCK_READER_PREFERENCE OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
)

add_executable(artlc_mcsrw_example example.cpp)
target_link_libraries(artlc_mcsrw_example artlc_mcsrw tbb)
