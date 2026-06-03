// oms/order_pool.hpp - Pre-allocated object pool for
// Order + OrderStateMachine.
//
// Eliminates per-order heap allocation on the hot path.
// Pool is sized for 10 000 simultaneous orders.

#pragma once

#include <array>
#include <memory>
#include <mutex>
#include <stack>

#include "../common/types.hpp"
#include "order_state.hpp"

namespace qf {

struct PooledOrder {
  Order order{};
  OrderStateMachine sm{OrderStatus::PENDING};
};

template <size_t PoolSize = 10000> class OrderPool {
public:
  OrderPool() {
    for (auto &o : pool_)
      free_list_.push(&o);
  }

  // Acquire a zeroed order from the pool.
  // Returns nullptr if exhausted.

  PooledOrder *acquire() {
    std::lock_guard lk(mtx_);

    if (free_list_.empty())
      return nullptr;

    PooledOrder *p = free_list_.top();
    free_list_.pop();

    // Reset to clean state
    p->order = Order{};
    p->sm = OrderStateMachine(OrderStatus::PENDING);

    return p;
  }

  // Return an order to the pool
  // (called on terminal state)

  void release(PooledOrder *p) {
    if (!p)
      return;

    std::lock_guard lk(mtx_);

    free_list_.push(p);
  }

  [[nodiscard]]
  size_t available() const {
    std::lock_guard lk(mtx_);
    return free_list_.size();
  }

  static constexpr size_t pool_size() { return PoolSize; }

private:
  std::array<PooledOrder, PoolSize> pool_;
  std::stack<PooledOrder *> free_list_;
  mutable std::mutex mtx_;
};

} // namespace qf