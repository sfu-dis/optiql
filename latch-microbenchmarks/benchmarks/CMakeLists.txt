add_library(perf perf.cpp)

add_executable(omcs_offset_bench bench_config.cpp omcs_bench.cpp)
target_compile_definitions(omcs_offset_bench PUBLIC OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE)
target_link_libraries(omcs_offset_bench gtest glog gflags perf pthread numa)

add_executable(omcs_offset_op_read_bench bench_config.cpp omcs_bench.cpp)
target_compile_definitions(omcs_offset_op_read_bench PUBLIC OMCS_OP_READ OMCS_OFFSET)
target_link_libraries(omcs_offset_op_read_bench gtest glog gflags perf pthread numa)

add_executable(omcs_offset_op_read_numa_qnode_bench bench_config.cpp omcs_bench.cpp)
target_compile_definitions(omcs_offset_op_read_numa_qnode_bench PUBLIC OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE)
target_link_libraries(omcs_offset_op_read_numa_qnode_bench gtest glog gflags perf pthread numa)

add_executable(omcs_bench bench_config.cpp omcs_bench.cpp)
target_link_libraries(omcs_bench gtest glog gflags perf pthread numa)

add_executable(optlock_faa_bench bench_config.cpp optlock_bench.cpp)
target_link_libraries(optlock_faa_bench gtest glog gflags perf pthread numa)

add_executable(optlock_ldst_bench bench_config.cpp optlock_bench.cpp)
target_compile_definitions(optlock_ldst_bench PUBLIC NO_FAA)
target_link_libraries(optlock_ldst_bench gtest glog gflags perf pthread numa)

add_executable(optlock_st_bench bench_config.cpp optlock_bench.cpp)
target_compile_definitions(optlock_st_bench PUBLIC ST_ONLY)
target_link_libraries(optlock_st_bench gtest glog gflags perf pthread numa)

add_executable(tatas_xchg_bench bench_config.cpp tatas_bench.cpp)
target_link_libraries(tatas_xchg_bench gtest glog gflags perf pthread numa)

add_executable(tatas_st_bench bench_config.cpp tatas_bench.cpp)
target_compile_definitions(tatas_st_bench PUBLIC ST_ONLY)
target_link_libraries(tatas_st_bench gtest glog gflags perf pthread numa)

add_executable(mcs_bench bench_config.cpp mcs_bench.cpp)
target_link_libraries(mcs_bench gtest glog gflags perf pthread numa)
