#pragma once

#include <memory>
#include <shared_mutex>

namespace std_lock {

class STDRWLock {
 public:
  STDRWLock() {}

  inline void lock(uint64_t* = nullptr) { lock_.lock(); }

  inline void unlock(uint64_t* = nullptr) { lock_.unlock(); }

  inline void read_lock(uint64_t* = nullptr) { lock_.lock_shared(); }

  inline void read_unlock(uint64_t* = nullptr) { lock_.unlock_shared(); }

 private:
  std::shared_mutex lock_;
};

}  // namespace std_lock