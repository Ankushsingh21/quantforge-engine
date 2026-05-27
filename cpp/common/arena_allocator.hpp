// common/arena_allocator.hpp — Pre-allocated slab allocator for hot-path
// objects.
//
// Eliminates glibc malloc latency on the order/tick critical path.
// Reset at EOD; never individually frees. Thread-local variant exists for
// per-strategy workers.
//

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

namespace qf {

class ArenaAllocator {
public:
  static constexpr size_t DEFAULT_POOL_SIZE = 64 * 1024 * 1024; // 64 MB

  explicit ArenaAllocator(size_t pool_bytes = DEFAULT_POOL_SIZE)
      : pool_size_(pool_bytes), pool_(static_cast<uint8_t *>(::operator new(
                                    pool_bytes, std::align_val_t{64}))) {}

  ~ArenaAllocator() { ::operator delete(pool_, std::align_val_t{64}); }

  // Not copyable — owns raw memory.
  ArenaAllocator(const ArenaAllocator &) = delete;
  ArenaAllocator &operator=(const ArenaAllocator &) = delete;
  ArenaAllocator(ArenaAllocator &&) = delete;

  // Allocate 'size' bytes with 'align' alignment.
  // Returns nullptr if the arena is full (caller must handle).
  [[nodiscard]] void *allocate(size_t size, size_t align = 8) noexcept {
    size_t cur = offset_.load(std::memory_order_relaxed);

    for (;;) {
      size_t aligned_cur = (cur + align - 1) & ~(align - 1);
      size_t next = aligned_cur + size;

      if (next > pool_size_)
        return nullptr;

      if (offset_.compare_exchange_weak(cur, next, std::memory_order_release,
                                        std::memory_order_relaxed)) {
        return pool_ + aligned_cur;
      }
    }
  }

  // Typed allocate + placement-new.
  template <typename T, typename... Args>
  [[nodiscard]] T *
  make(Args &&...args) noexcept(noexcept(T(std::forward<Args>(args)...))) {
    void *p = allocate(sizeof(T), alignof(T));

    if (!p)
      return nullptr;

    return new (p) T(std::forward<Args>(args)...);
  }

  // Reset the arena (end-of-day). Does NOT call destructors — only
  // use for POD/trivially-destructible types or explicitly destroyed objects.
  void reset() noexcept { offset_.store(0, std::memory_order_release); }

  [[nodiscard]] size_t used_bytes() const noexcept {
    return offset_.load(std::memory_order_acquire);
  }

  [[nodiscard]] size_t total_bytes() const noexcept { return pool_size_; }

  [[nodiscard]] double utilization() const noexcept {
    return static_cast<double>(used_bytes()) /
           static_cast<double>(total_bytes());
  }

private:
  const size_t pool_size_;
  uint8_t *pool_;
  alignas(64) std::atomic<size_t> offset_{0};
};

// Thread-local 8 MB arena per strategy worker thread.
// DO NOT use across thread boundaries.
inline thread_local ArenaAllocator g_thread_arena{8 * 1024 * 1024};

} // namespace qf