
macro(add_libart)
  cmake_parse_arguments(LIBART "" "NAME" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_library(${LIBART_NAME} ${ARTOLC_SRC})
  target_link_libraries(${LIBART_NAME} glog ${LIBART_LIBRARIES})
  target_compile_definitions(${LIBART_NAME} PUBLIC ${LIBART_DEFINITIONS})
endmacro()

set(ARTOLC_SRC Tree.cpp N.cpp N4.cpp N16.cpp N48.cpp N256.cpp)

add_libart(
  NAME artolc
  DEFINITIONS OMCS_LOCK
)

add_libart(
  NAME artolc_upgrade
  DEFINITIONS OMCS_LOCK ART_OLC_UPGRADE
)

# add_libart(
#   NAME artolc_acquire
#   DEFINITIONS OMCS_LOCK ART_OLC_ACQUIRE
# )

add_libart(
  NAME artolc_upsert
  DEFINITIONS OMCS_LOCK ART_UPSERT
)

add_libart(
  NAME artomcs
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL
)

add_libart(
  NAME artomcs_offset_gnp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OFFSET
)

add_libart(
  NAME artomcs_op_read_gnp
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET
)

add_libart(
  NAME artomcs_offset
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
)

add_libart(
  NAME artomcs_op_read
  DEFINITIONS OMCS_LOCK IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
)

# add_libart(
#   NAME artomcs_acquire
#   DEFINITIONS OMCS_LOCK ART_OLC_ACQUIRE IS_CONTEXTFUL
# )

add_executable(example example.cpp)
target_link_libraries(example artolc tbb)
