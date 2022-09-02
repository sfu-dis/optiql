add_executable(btreeolc_tests tests.cpp)
target_compile_definitions(btreeolc_tests PUBLIC OMCS_LOCK BTREE_PAGE_SIZE=256)
target_link_libraries(btreeolc_tests tbb glog)
