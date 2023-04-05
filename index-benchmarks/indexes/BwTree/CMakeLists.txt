add_library(bwtree STATIC bwtree.cpp)
target_compile_definitions(bwtree PUBLIC BWTREE_NODEBUG)
target_link_libraries(bwtree atomic)
