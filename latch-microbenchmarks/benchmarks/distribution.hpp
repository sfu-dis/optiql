#pragma once

#include <algorithm>
#include <string>

enum class Distribution {
  FIXED = 0,
  UNIFORM = 1,
};

inline static bool ValidateDistribution(const char *flagname, const std::string &value) {
  std::string dist(value);
  std::transform(dist.begin(), dist.end(), dist.begin(), ::tolower);
  if (dist == "fixed" || dist == "uniform") {
    return true;
  }
  printf("Unknown distribution %s\n", value.c_str());
  return false;
}
