
macro(add_libart)
  cmake_parse_arguments(LIBART "" "NAME" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_library(${LIBART_NAME} ${ARTOLC_SRC})
  target_link_libraries(${LIBART_NAME} glog ${LIBART_LIBRARIES})
  target_compile_definitions(${LIBART_NAME} PUBLIC ${LIBART_DEFINITIONS})
endmacro()

set(ARTOLC_SRC Tree.cpp N.cpp N4.cpp N16.cpp N48.cpp N256.cpp)
foreach(omcs_impl omcs_impl_suffix IN ZIP_LISTS omcs_impls omcs_impl_suffixes)
  add_libart(
    NAME artolc${omcs_impl_suffix}
    DEFINITIONS ${omcs_impl}
  )

  add_libart(
    NAME artolc_upgrade${omcs_impl_suffix}
    DEFINITIONS ${omcs_impl} ART_OLC_UPGRADE
  )

  # add_libart(
  #   NAME artolc_acquire${omcs_impl_suffix}
  #   DEFINITIONS ${omcs_impl} ART_OLC_ACQUIRE
  # )

  add_libart(
    NAME artolc_upsert${omcs_impl_suffix}
    DEFINITIONS ${omcs_impl} ART_UPSERT
  )

  add_libart(
    NAME artomcs${omcs_impl_suffix}
    DEFINITIONS ${omcs_impl} IS_CONTEXTFUL
  )

  add_libart(
    NAME artomcs_offset${omcs_impl_suffix}
    DEFINITIONS ${omcs_impl} IS_CONTEXTFUL OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  )

  add_libart(
    NAME artomcs_op_read${omcs_impl_suffix}
    DEFINITIONS ${omcs_impl} IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
  )

  # add_libart(
  #   NAME artomcs_acquire${omcs_impl_suffix}
  #   DEFINITIONS ${omcs_impl} ART_OLC_ACQUIRE IS_CONTEXTFUL
  # )
endforeach()

add_executable(example example.cpp)
target_link_libraries(example artolc tbb)
