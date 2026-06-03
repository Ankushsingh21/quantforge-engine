// oms/order_state.hpp - Order state-machine transitions.
//
// The state machine enforces legal transitions only.
// All transitions use atomic CAS to be safe across threads.

#pragma once

#include <atomic>
#include <optional>
#include <stdexcept>
#include <string>

#include "../common/logger.hpp"
#include "../common/types.hpp"

namespace qf {

// Legal state-machine transitions:

// PENDING --> OPEN --> COMPLETE
//               \
//                -> PARTIAL --> COMPLETE
//                            \
//                             -> CANCELLED
//
// PENDING --> REJECTED
// PENDING --> CANCELLED
// PENDING --> TRIGGER_PENDING --> OPEN

class OrderStateMachine {
public:
  explicit OrderStateMachine(OrderStatus initial = OrderStatus::PENDING)
      : status_(initial) {}

  [[nodiscard]]
  OrderStatus current() const noexcept {
    return status_.load(std::memory_order_acquire);
  }

  // Returns true if the transition is legal and was applied.
  bool transition(OrderStatus to) noexcept {
    OrderStatus from = status_.load(std::memory_order_acquire);

    if (!is_legal(from, to)) {
      LOG_WARN("[OMS] Illegal transition {} -> {}", status_name(from),
               status_name(to));
      return false;
    }

    return status_.compare_exchange_strong(from, to, std::memory_order_acq_rel,
                                           std::memory_order_acquire);
  }

  [[nodiscard]]
  bool is_terminal() const noexcept {
    auto s = current();

    return s == OrderStatus::COMPLETE || s == OrderStatus::CANCELLED ||
           s == OrderStatus::REJECTED;
  }
  static const char *status_name(OrderStatus s) noexcept {
    switch (s) {

    case OrderStatus::PENDING:
      return "PENDING";

    case OrderStatus::OPEN:
      return "OPEN";

    case OrderStatus::COMPLETE:
      return "COMPLETE";

    case OrderStatus::CANCELLED:
      return "CANCELLED";

    case OrderStatus::REJECTED:
      return "REJECTED";

    case OrderStatus::PARTIAL:
      return "PARTIAL";

    case OrderStatus::TRIGGER_PENDING:
      return "TRIGGER_PENDING";

    default:
      return "UNKNOWN";
    }
  }

private:
  static bool is_legal(OrderStatus from, OrderStatus to) noexcept {

    switch (from) {

    case OrderStatus::PENDING:
      return to == OrderStatus::OPEN || to == OrderStatus::REJECTED ||
             to == OrderStatus::CANCELLED || to == OrderStatus::TRIGGER_PENDING;

    case OrderStatus::TRIGGER_PENDING:
      return to == OrderStatus::OPEN || to == OrderStatus::CANCELLED;

    case OrderStatus::OPEN:
      return to == OrderStatus::COMPLETE || to == OrderStatus::PARTIAL ||
             to == OrderStatus::CANCELLED;

    case OrderStatus::PARTIAL:
      return to == OrderStatus::COMPLETE || to == OrderStatus::CANCELLED;

    default:
      return false;
      // terminal states have no valid outgoing transitions
    }
  }

  std::atomic<OrderStatus> status_;
};

} // namespace qf