// transport/kafka_publisher.hpp
//
// Async Kafka producer (librdkafka C++ API).
// Publishes:
//
//   Topic: market.ticks        -> NormalizedTick (protobuf)
//   Topic: order.fills         -> Fill (protobuf)
//   Topic: portfolio.updates   -> PortfolioSnapshot (protobuf)
//   Topic: risk.alerts         -> RiskAlert (protobuf)
//
// All produces are non-blocking (async delivery to Kafka broker).
// Delivery reports are polled in a background thread.
// On queue-full: drops oldest message (overrun policy) to never block hot path.
//

#pragma once

#ifdef QUANTFORGE_KAFKA_ENABLED

#include <atomic>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <string>
#include <thread>

#include "../common/logger.hpp"
#include "../common/types.hpp"

namespace qf {

class KafkaPublisher {
public:
  struct TopicNames {
    std::string ticks{"market.ticks"};
    std::string fills{"order.fills"};
    std::string portfolio{"portfolio.updates"};
    std::string alerts{"risk.alerts"};
  };

  explicit KafkaPublisher(const std::string &brokers, TopicNames topics = {});

  ~KafkaPublisher();

  KafkaPublisher(const KafkaPublisher &) = delete;
  KafkaPublisher &operator=(const KafkaPublisher &) = delete;

  [[nodiscard]]
  bool is_healthy() const noexcept {
    return healthy_.load(std::memory_order_acquire);
  }

  // Non-blocking publish — drops if Kafka queue full.
  void publish_tick(const NormalizedTick &tick);
  void publish_fill(const Fill &fill);
  void publish_portfolio(const PortfolioSnapshot &snap);
  void publish_alert(const RiskAlert &alert);

  // Flush all pending messages (called on shutdown, max wait_ms).
  void flush(int wait_ms = 5000);

private:
  void delivery_poll_loop();

  // Minimal serializers (protobuf assumed compiled;
  // use raw bytes for now).
  static std::string serialize_tick(const NormalizedTick &t);
  static std::string serialize_fill(const Fill &f);
  static std::string serialize_portfolio(const PortfolioSnapshot &s);
  static std::string serialize_alert(const RiskAlert &a);

  std::unique_ptr<RdKafka::Producer> producer_;

  std::unique_ptr<RdKafka::Topic> topic_ticks_;
  std::unique_ptr<RdKafka::Topic> topic_fills_;
  std::unique_ptr<RdKafka::Topic> topic_portfolio_;
  std::unique_ptr<RdKafka::Topic> topic_alerts_;

  TopicNames topics_;

  std::atomic<bool> stop_{false};
  std::atomic<bool> healthy_{false};

  std::thread poll_thread_;

  std::atomic<uint64_t> published_{0};
  std::atomic<uint64_t> errors_{0};
};

} // namespace qf

#else // QUANTFORGE_KAFKA_ENABLED -- stub

namespace qf {

struct NormalizedTick;
struct Fill;
struct PortfolioSnapshot;
struct RiskAlert;

class KafkaPublisher {
public:
  explicit KafkaPublisher(const std::string & = {}) {}

  [[nodiscard]]
  bool is_healthy() const noexcept {
    return false;
  }

  void publish_tick(const NormalizedTick &) {}
  void publish_fill(const Fill &) {}
  void publish_portfolio(const PortfolioSnapshot &) {}
  void publish_alert(const RiskAlert &) {}

  void flush(int = 0) {}
};

} // namespace qf

#endif // QUANTFORGE_KAFKA_ENABLED