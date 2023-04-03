#pragma once

#include <atomic>
#include <cstdint>

#if defined(OMCS_LOCK)
#include "OMCSImpl.h"
#elif defined(STDRW_LOCK)
#include "STDRW.h"
#elif defined(MCSRW_LOCK)
#include "MCSRW.h"
#elif defined(OPT_MCSRW_HYBRID_LOCK)
#include "MCSRW.h"
#include "OMCSImpl.h"
#endif

struct OMCSLock {
#if defined(OMCS_LOCK)
  using Lock = omcs_impl::OMCSLock;
  using Context = omcs_impl::OMCSQNode;
  static constexpr uint64_t kInvalidVersion = omcs_impl::OMCSLock::kInvalidVersion;
  static constexpr const char *name = "OMCS Lock";
#elif defined(STDRW_LOCK)
  using Lock = std_lock::STDRWLock;
  using Context = uint64_t;
  static constexpr const char *name = "STD RW Lock";
#elif defined(MCSRW_LOCK)
  using Lock = mcsrw::MCSRWLock;
  using Context = mcsrw::MCSRWQNode;
  static constexpr const char *name = "MCS RW Lock";
#elif defined(OPT_MCSRW_HYBRID_LOCK)
  using Lock = uint64_t;
  using Context = mcsrw::MCSRWQNode;
  static constexpr uint64_t kInvalidVersion = omcs_impl::OMCSLock::kInvalidVersion;
  static constexpr const char *name = "OptLock + MCS RW Lock";
#else
#error "OMCS implementation is not defined."
#endif

  Lock lock;

#if defined(OMCS_LOCK)
  // Optimistic lock API

  bool isLocked() const { return lock.is_locked(); }

  uint64_t readLock() const { return lock.begin_read(); }

  uint64_t readLockOrRestart(bool &needRestart) const { return lock.try_begin_read(needRestart); }

  uint64_t writeLock() { return lock.lock(); }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    // FIXME(shiges): update [version]
    needRestart = !lock.try_lock(version);
  }

  void writeUnlock() { return lock.unlock(); }

  void writeUnlock(uint64_t version) { return lock.unlock(version); }

  bool writeLockBegin(Context *q) { return lock.lock_begin(q); }

  void writeLockTurnOffOpRead() { lock.lock_turn_off_opread(); }

  void writeLock(Context *q) { lock.lock(q); }

#if defined(OMCS_OP_READ_NEW_API_CALLBACK)
  template <class Callback>
  void writeLockWithRead(Context *q, Callback &&cb) {
    lock.lock(q, std::forward<Callback &&>(cb));
  }
#endif

  void upgradeToWriteLockOrRestart(uint64_t &version, Context *q, bool &needRestart) {
    // FIXME(shiges): update [version]
    needRestart = !lock.try_lock(q, version);
  }

  void writeUnlock(Context *q) { return lock.unlock(q); }

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    needRestart = !lock.validate_read(startRead);
  }

#elif defined(RWLOCK)
  // Readers-writer lock API

  void writeLock() { lock.lock(); }

  void writeUnlock() { lock.unlock(); }

  void readLock() { lock.read_lock(); }

  void readUnlock() { lock.read_unlock(); }

  void writeLock(Context *q) { lock.lock(q); }

  void writeUnlock(Context *q) { lock.unlock(q); }

  void readLock(Context *q) { lock.read_lock(q); }

  void readUnlock(Context *q) { lock.read_unlock(q); }

  // Dummy APIs
  uint64_t readLockOrRestart(bool &needRestart) const { LOG(FATAL) << "Not supported"; }

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    LOG(FATAL) << "Not supported";
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    LOG(FATAL) << "Not supported";
  }

#endif
};

#if defined(OPT_MCSRW_HYBRID_LOCK)
#if not defined(OMCS_OFFSET)
static_assert(false, "OMCS_OFFSET must be defined for OptLock + MCS RW Lock hybrid");
#endif

struct OptLock {
  omcs_impl::OMCSLock lock;

  // Optimistic lock API
  uint64_t readLockOrRestart(bool &needRestart) const { return lock.try_begin_read(needRestart); }

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    needRestart = !lock.validate_read(startRead);
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    // FIXME(shiges): update [version]
    needRestart = !lock.try_lock(version);
  }

  void writeUnlock(uint64_t version) { return lock.unlock(version); }
};

struct MCSRWLock {
  mcsrw::MCSRWLock lock;

  // Readers-writer lock API
  void writeLock(OMCSLock::Context *q) { lock.lock(q); }

  void writeUnlock(OMCSLock::Context *q) { lock.unlock(q); }

  void readLock(OMCSLock::Context *q) { lock.read_lock(q); }

  void readUnlock(OMCSLock::Context *q) { lock.read_unlock(q); }
};
#endif

#if defined(OMCS_LOCK)
static_assert(sizeof(OMCSLock) == 8, "sizeof OMCSLock is not 8-byte");
#elif defined(STDRW_LOCK)
static_assert(sizeof(OMCSLock) == 56, "sizeof OMCSLock is not 56-byte");
#elif defined(MCSRW_LOCK)
#if defined(OMCS_OFFSET)
static_assert(sizeof(OMCSLock) == 8, "sizeof OMCSLock is not 8-byte");
#endif
#elif defined(OPT_MCSRW_HYBRID_LOCK)
static_assert(sizeof(OptLock) == 8, "sizeof OptLock is not 8-byte");
static_assert(sizeof(MCSRWLock) == 8, "sizeof MCSRWLock is not 8-byte");
#endif
