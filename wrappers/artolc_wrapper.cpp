#include "artolc_wrapper.hpp"

size_t artolc_wrapper::key_size = 0;
size_t artolc_wrapper::value_size = 0;
ART::Epoche *artolc_wrapper::epoch = nullptr;

extern "C" tree_api *create_tree(const tree_options_t &opt) { return new artolc_wrapper(opt); }
