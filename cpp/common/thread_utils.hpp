// common/thread_utils.hpp — CPU affinity, realtime priority, and thread naming.
//
// Linux-only implementation (uses pthread + sched_setaffinity).
// On non-Linux builds, the functions compile but are no-ops.
//

#pragma once

#include <pthread.h>
#include <sched.h>
#include <string>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "logger.hpp"

namespace qf {

// Pin the calling thread to a specific logical CPU core.
// Returns true on success.
inline bool pin_thread_to_core(int core_id) noexcept {
#ifdef __linux__

  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);

  CPU_SET(core_id, &cpuset);

  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  if (rc != 0) {
    LOG_WARN("pin_thread_to_core({}) failed: {}", core_id, std::strerror(rc));

    return false;
  }

  LOG_INFO("Thread pinned to core {}", core_id);

  return true;

#else

  (void)core_id;
  return true;

#endif
}

// Set SCHED_FIFO realtime priority (1–99).
// Requires CAP_SYS_NICE or running as root.
// Use priority 80 for market-data IO, 70 for strategies, 60 for OMS.
inline bool set_realtime_priority(int priority) noexcept {
#ifdef __linux__

  sched_param sp{};
  sp.sched_priority = priority;

  int rc = pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp);

  if (rc != 0) {
    LOG_WARN("set_realtime_priority({}) failed "
             "(need CAP_SYS_NICE): {}",
             priority, std::strerror(rc));

    return false;
  }

  LOG_INFO("Thread set to SCHED_FIFO priority {}", priority);

  return true;

#else

  (void)priority;
  return true;

#endif
}

// Name the calling thread (visible in htop, perf, gdb).
inline void set_thread_name(const std::string &name) noexcept {
#ifdef __linux__

  // Max 15 chars + null.
  std::string n = name.substr(0, 15);

  prctl(PR_SET_NAME, n.c_str(), 0, 0, 0);

#endif
}

// Configure a thread for the hot path:
// pin core + realtime priority + name.
inline void setup_hot_thread(int core_id, int rt_priority,
                             const std::string &name) {
  set_thread_name(name);

  pin_thread_to_core(core_id);

  set_realtime_priority(rt_priority);
}

} // namespace qf